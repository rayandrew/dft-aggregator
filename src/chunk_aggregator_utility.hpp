#pragma once

#include <dftracer/utils/core/utilities/utility.h>
#include <dftracer/utils/utilities/composites/indexed_file_reader_utility.h>
#include <dftracer/utils/utilities/composites/types.h>
#include <dftracer/utils/utilities/reader/internal/stream_config.h>
#include <yyjson.h>

#include <chrono>
#include <cstring>
#include <string_view>
#include <thread>

#include "aggregation_output.hpp"
#include "association_tracker.hpp"
#include "json_parser_utility.hpp"

using namespace dftracer::utils;

// Input for processing a single chunk (byte range) of a file
struct ChunkAggregatorInput {
    std::string file_path;
    std::string idx_path;
    std::size_t start_byte;
    std::size_t end_byte;
    std::size_t start_line;
    std::size_t end_line;
    AggregationConfig config;
    std::size_t checkpoint_size;
    int chunk_index;  // For tracking/debugging

    // Performance tuning
    std::size_t batch_size = 4 * 1024 * 1024;

    // Builder pattern - each field has its own builder method
    ChunkAggregatorInput& with_file_path(const std::string& path) {
        file_path = path;
        return *this;
    }

    ChunkAggregatorInput& with_idx_path(const std::string& path) {
        idx_path = path;
        return *this;
    }

    ChunkAggregatorInput& with_byte_range(std::size_t start, std::size_t end) {
        start_byte = start;
        end_byte = end;
        return *this;
    }

    ChunkAggregatorInput& with_line_range(std::size_t start, std::size_t end) {
        start_line = start;
        end_line = end;
        return *this;
    }

    ChunkAggregatorInput& with_chunk_index(int index) {
        chunk_index = index;
        return *this;
    }

    ChunkAggregatorInput& with_config(const AggregationConfig& cfg) {
        config = cfg;
        return *this;
    }

    ChunkAggregatorInput& with_checkpoint_size(std::size_t size) {
        checkpoint_size = size;
        return *this;
    }

    ChunkAggregatorInput& with_batch_size(std::size_t size) {
        batch_size = size;
        return *this;
    }
};

// Output from association extraction for a chunk
struct ChunkAssociationOutput {
    AssociationTracker tracker;
    int chunk_index;
    std::size_t events_processed;
    bool success;
};

// Utility that aggregates events from a single chunk (byte range)
class ChunkAggregatorUtility
    : public utilities::Utility<ChunkAggregatorInput, ChunkAggregationOutput,
                                utilities::tags::Parallelizable> {
   private:
    // Thread-local reusable buffer for batch reading (resized as needed)
    thread_local static std::vector<char> read_buffer_;

    std::uint64_t compute_time_bucket(std::uint64_t timestamp,
                                      const AggregationConfig& config) const {
        if (config.use_relative_time) {
            timestamp -= config.reference_timestamp;
        }
        return (timestamp / config.time_interval_us) * config.time_interval_us;
    }

    // Helper: Build aggregation key from JsonValue (reuse json wrapper)
    AggregationKey build_key(
        const JsonValue& json, const JsonValue& args, std::uint64_t timestamp,
        const AggregationConfig& config,
        const std::shared_ptr<AssociationTracker>& local_tracker) const {
        AggregationKey key;

        // Extract core fields
        key.cat = json["cat"].get<std::string_view>();
        key.name = json["name"].get<std::string_view>();
        key.pid = json["pid"].get<std::uint64_t>();
        key.tid = json["tid"].get<std::uint64_t>();

        // Extract from args
        key.hhash = args["hhash"].get<std::string_view>();
        key.fhash = args["fhash"].get<std::string_view>();

        // Time bucket
        key.time_bucket = compute_time_bucket(timestamp, config);

        // Extra group keys
        if (!config.extra_group_keys.empty()) {
            for (const auto& extra_key : config.extra_group_keys) {
                std::string_view value =
                    args[extra_key].get<std::string_view>();
                if (!value.empty()) {
                    key.extra_keys[extra_key] = value;
                }
            }
        }

        return key;
    }

    // Helper: Process a single event and update metrics
    void process_event(
        yyjson_val* event, const std::string& trace_file,
        const AggregationConfig& config,
        std::unordered_map<AggregationKey, AggregationMetrics,
                           AggregationKeyHash>& local_aggregations,
        const std::shared_ptr<AssociationTracker>& local_tracker,
        long long& assoc_time_us, long long& build_key_time_us,
        long long& hash_lookup_time_us, long long& metrics_update_time_us) {
        JsonValue json(event);

        // Skip metadata events (ph:"M")
        std::string_view ph = json["ph"].get<std::string_view>();
        if (ph == "M") {
            return;
        }

        // Pre-compute commonly accessed values to avoid redundant lookups
        std::uint64_t timestamp = json["ts"].get<std::uint64_t>();
        JsonValue args = json["args"];

        // Extract associations from this event (fork/spawn, boundary events)
        if (local_tracker) {
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
            auto assoc_start = std::chrono::high_resolution_clock::now();
#endif
            local_tracker->extract_from_event(json, args, config);
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
            auto assoc_end = std::chrono::high_resolution_clock::now();
            assoc_time_us +=
                std::chrono::duration_cast<std::chrono::microseconds>(
                    assoc_end - assoc_start)
                    .count();
#endif
        }

        // Filter by category if specified
        if (!config.include_categories.empty()) {
            std::string_view cat = json["cat"].get<std::string_view>();
            if (std::find(config.include_categories.begin(),
                          config.include_categories.end(),
                          cat) == config.include_categories.end()) {
                return;
            }
        }

        // Filter by name if specified
        if (!config.include_names.empty()) {
            std::string_view name = json["name"].get<std::string_view>();
            if (std::find(config.include_names.begin(),
                          config.include_names.end(),
                          name) == config.include_names.end()) {
                return;
            }
        }

        // Build key
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
        auto build_key_start = std::chrono::high_resolution_clock::now();
#endif
        AggregationKey key =
            build_key(json, args, timestamp, config, local_tracker);
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
        auto build_key_end = std::chrono::high_resolution_clock::now();
        build_key_time_us +=
            std::chrono::duration_cast<std::chrono::microseconds>(
                build_key_end - build_key_start)
                .count();
#endif

        // Get or create metrics entry
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
        auto hash_start = std::chrono::high_resolution_clock::now();
#endif
        auto& metrics = local_aggregations[key];
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
        auto hash_end = std::chrono::high_resolution_clock::now();
        hash_lookup_time_us +=
            std::chrono::duration_cast<std::chrono::microseconds>(hash_end -
                                                                  hash_start)
                .count();
#endif

        // Extract event data and update metrics
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
        auto metrics_start = std::chrono::high_resolution_clock::now();
#endif

        std::uint64_t duration = json["dur"].get<std::uint64_t>();
        std::uint64_t size = 0;

        // Update metrics
        metrics.update_duration(duration);
        metrics.update_timestamp(timestamp, duration);

        // Handle size (from args.ret) - reuse args
        JsonValue ret = args["ret"];
        if (ret.exists()) {
            size = ret.get<std::uint64_t>();
            metrics.update_size(size);
        }

        // Handle custom metrics - reuse args JsonValue
        if (!config.custom_metric_fields.empty()) {
            for (const auto& field : config.custom_metric_fields) {
                JsonValue field_val = args[field];
                if (field_val.exists()) {
                    std::uint64_t value = field_val.get<std::uint64_t>();
                    metrics.update_custom_metric(field, value);
                }
            }
        }

#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
        auto metrics_end = std::chrono::high_resolution_clock::now();
        metrics_update_time_us +=
            std::chrono::duration_cast<std::chrono::microseconds>(metrics_end -
                                                                  metrics_start)
                .count();
#endif
    }

   public:
    ChunkAggregatorUtility() = default;

    ChunkAggregationOutput process(const ChunkAggregatorInput& input) override {
        auto start_time = std::chrono::high_resolution_clock::now();

        ChunkAggregationOutput output;
        output.chunk_index = input.chunk_index;
        output.events_processed = 0;
        output.bytes_processed = input.end_byte - input.start_byte;
        output.file_path = input.file_path;  // OK to copy - input is const&
        output.success = false;

        // Log progress for every chunk (INFO level for visibility)
        if (input.chunk_index % 100 == 0) {
            DFTRACER_UTILS_LOG_INFO("Starting chunk %d: %s [bytes %zu-%zu]",
                                    input.chunk_index, input.file_path.c_str(),
                                    input.start_byte, input.end_byte);
        }

        auto reader_start = std::chrono::high_resolution_clock::now();

        // Create indexed file reader
        auto reader_input =
            utilities::composites::IndexedReadInput::from_file(input.file_path)
                .with_checkpoint_size(input.checkpoint_size)
                .with_index(input.idx_path);

        utilities::composites::IndexedFileReaderUtility reader_utility;
        auto reader = reader_utility.process(reader_input);

        if (!reader) {
            DFTRACER_UTILS_LOG_ERROR("Chunk %d: Failed to create reader for %s",
                                     input.chunk_index,
                                     input.file_path.c_str());
            return output;
        }

        auto reader_end = std::chrono::high_resolution_clock::now();
        auto reader_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(reader_end -
                                                                  reader_start)
                .count();

        if (input.chunk_index % 100 == 0) {
            DFTRACER_UTILS_LOG_INFO("Chunk %d: Reader created in %ld ms",
                                    input.chunk_index, reader_time);
        }

        // Create stream using MULTI_LINES_BYTES for batch reading (much
        // faster!)
        auto stream_start = std::chrono::high_resolution_clock::now();
        auto stream = reader->stream(
            utilities::reader::internal::StreamConfig()
                .stream_type(
                    utilities::reader::internal::StreamType::MULTI_LINES_BYTES)
                .range_type(utilities::reader::internal::RangeType::BYTE_RANGE)
                .from(input.start_byte)
                .to(input.end_byte));

        if (!stream) {
            DFTRACER_UTILS_LOG_ERROR("Chunk %d: Failed to create stream for %s",
                                     input.chunk_index,
                                     input.file_path.c_str());
            return output;
        }

        auto stream_end = std::chrono::high_resolution_clock::now();
        auto stream_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(stream_end -
                                                                  stream_start)
                .count();

        if (input.chunk_index % 100 == 0) {
            DFTRACER_UTILS_LOG_INFO("Chunk %d: Stream created in %ld ms",
                                    input.chunk_index, stream_time);
        }

        // Thread-local aggregation map (lock-free!)
        std::unordered_map<AggregationKey, AggregationMetrics,
                           AggregationKeyHash>
            local_aggregations;

        // Reserve space for likely number of keys to reduce rehashing
        local_aggregations.reserve(10000);

        // Create a local association tracker
        // only if association tracking is enabled
        std::shared_ptr<AssociationTracker> local_tracker;
        if (input.config.track_process_parents ||
            !input.config.boundary_events.empty()) {
            local_tracker = std::make_shared<AssociationTracker>();
        }

        // Resize buffer if needed (reuse existing buffer when possible)
        if (read_buffer_.size() < input.batch_size) {
            read_buffer_.resize(input.batch_size);
        }

        std::size_t lines_processed = 0;

#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
        // Timing accumulators for profiling (debug only)
        long long total_io_time_us = 0;
        long long total_json_parse_time_us = 0;
        long long total_process_time_us = 0;
        long long total_loop_overhead_us = 0;

        // Detailed breakdown of process_event() time
        long long total_assoc_time_us = 0;
        long long total_build_key_time_us = 0;
        long long total_hash_lookup_time_us = 0;
        long long total_metrics_update_time_us = 0;
#endif

        while (!stream->done()) {
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
            auto io_start = std::chrono::high_resolution_clock::now();
#endif
            std::size_t bytes_read =
                stream->read(read_buffer_.data(), input.batch_size);
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
            auto io_end = std::chrono::high_resolution_clock::now();
            total_io_time_us +=
                std::chrono::duration_cast<std::chrono::microseconds>(io_end -
                                                                      io_start)
                    .count();
#endif

            if (bytes_read == 0) break;

            const char* data = read_buffer_.data();
            std::size_t pos = 0;

            while (pos < bytes_read) {
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
                auto loop_start = std::chrono::high_resolution_clock::now();
#endif

                // Find next newline
                const char* line_start = data + pos;
                const char* newline = static_cast<const char*>(
                    memchr(line_start, '\n', bytes_read - pos));

                if (!newline) {
                    break;
                }

                std::size_t line_len = newline - line_start;

                // Parse JSON line
                if (line_len > 0) {
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
                    auto parse_start =
                        std::chrono::high_resolution_clock::now();
#endif
                    yyjson_read_flag flg = YYJSON_READ_NOFLAG;
                    yyjson_doc* doc =
                        yyjson_read_opts(const_cast<char*>(line_start),
                                         line_len, flg, nullptr, nullptr);
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
                    auto parse_end = std::chrono::high_resolution_clock::now();
                    total_json_parse_time_us +=
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            parse_end - parse_start)
                            .count();
#endif

                    if (doc) {
                        yyjson_val* root = yyjson_doc_get_root(doc);
                        if (root && yyjson_is_obj(root)) {
#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
                            auto process_start =
                                std::chrono::high_resolution_clock::now();
                            process_event(root, input.file_path, input.config,
                                          local_aggregations, local_tracker,
                                          total_assoc_time_us,
                                          total_build_key_time_us,
                                          total_hash_lookup_time_us,
                                          total_metrics_update_time_us);
                            auto process_end =
                                std::chrono::high_resolution_clock::now();
                            total_process_time_us +=
                                std::chrono::duration_cast<
                                    std::chrono::microseconds>(process_end -
                                                               process_start)
                                    .count();
#else
                            // No timing parameters in release mode
                            long long dummy1 = 0, dummy2 = 0, dummy3 = 0,
                                      dummy4 = 0;
                            process_event(root, input.file_path, input.config,
                                          local_aggregations, local_tracker,
                                          dummy1, dummy2, dummy3, dummy4);
#endif

                            output.events_processed++;
                            lines_processed++;
                        }
                        yyjson_doc_free(doc);
                    }
                }

#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
                auto loop_end = std::chrono::high_resolution_clock::now();
                total_loop_overhead_us +=
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        loop_end - loop_start)
                        .count();
#endif

                pos = (newline - data) + 1;  // Move past newline
            }
        }

        output.aggregations = std::move(local_aggregations);
        if (local_tracker) {
            local_tracker->finalize();
            output.local_tracker = std::move(local_tracker);
        }
        output.success = true;

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                            end_time - start_time)
                            .count();

#if DFTRACER_UTILS_LOGGER_DEBUG_ENABLED == 1
        // Log completion for every chunk (INFO level)
        if (input.chunk_index % 100 == 0 || output.events_processed > 0) {
            auto thread_id = std::this_thread::get_id();
            DFTRACER_UTILS_LOG_DEBUG(
                "[Thread %zu] Chunk %d DONE: %zu events → %zu keys in %ld ms "
                "(%.2f events/sec)",
                std::hash<std::thread::id>{}(thread_id), input.chunk_index,
                output.events_processed, output.aggregations.size(), duration,
                duration > 0 ? (output.events_processed * 1000.0 / duration)
                             : 0.0);

            // Calculate timing breakdown percentages (debug only)
            long long total_time_us = duration * 1000;  // ms to us
            double io_pct = (total_time_us > 0)
                                ? (total_io_time_us * 100.0 / total_time_us)
                                : 0;
            double parse_pct =
                (total_time_us > 0)
                    ? (total_json_parse_time_us * 100.0 / total_time_us)
                    : 0;
            double process_pct =
                (total_time_us > 0)
                    ? (total_process_time_us * 100.0 / total_time_us)
                    : 0;
            double overhead_pct =
                (total_time_us > 0)
                    ? (total_loop_overhead_us * 100.0 / total_time_us)
                    : 0;

            // Note: loop_overhead includes parse + process time, so compute
            // "other"
            double accounted_time_us = total_io_time_us +
                                       total_json_parse_time_us +
                                       total_process_time_us;
            double other_pct = (total_time_us > 0)
                                   ? ((total_time_us - accounted_time_us) *
                                      100.0 / total_time_us)
                                   : 0;

            // Log timing breakdown
            DFTRACER_UTILS_LOG_INFO(
                "[Thread %zu] Chunk %d TIMING: I/O=%.1f%% (%lld ms), "
                "JSON=%.1f%% (%lld ms), Process=%.1f%% (%lld ms), Other=%.1f%%",
                std::hash<std::thread::id>{}(thread_id), input.chunk_index,
                io_pct, total_io_time_us / 1000, parse_pct,
                total_json_parse_time_us / 1000, process_pct,
                total_process_time_us / 1000, other_pct);

            // Log detailed breakdown of Process time
            double assoc_pct =
                (total_time_us > 0)
                    ? (total_assoc_time_us * 100.0 / total_time_us)
                    : 0;
            double build_key_pct =
                (total_time_us > 0)
                    ? (total_build_key_time_us * 100.0 / total_time_us)
                    : 0;
            double hash_pct =
                (total_time_us > 0)
                    ? (total_hash_lookup_time_us * 100.0 / total_time_us)
                    : 0;
            double metrics_pct =
                (total_time_us > 0)
                    ? (total_metrics_update_time_us * 100.0 / total_time_us)
                    : 0;

            DFTRACER_UTILS_LOG_INFO(
                "[Thread %zu] Chunk %d PROCESS DETAIL: Assoc=%.1f%% (%lld "
                "ms), BuildKey=%.1f%% (%lld ms), HashLookup=%.1f%% (%lld ms), "
                "MetricsUpdate=%.1f%% (%lld ms)",
                std::hash<std::thread::id>{}(thread_id), input.chunk_index,
                assoc_pct, total_assoc_time_us / 1000, build_key_pct,
                total_build_key_time_us / 1000, hash_pct,
                total_hash_lookup_time_us / 1000, metrics_pct,
                total_metrics_update_time_us / 1000);
        }
#endif

        return output;
    }
};

// Define thread-local static buffer (empty initially, resized on first use)
thread_local std::vector<char> ChunkAggregatorUtility::read_buffer_;
