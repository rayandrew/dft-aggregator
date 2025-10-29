#pragma once

#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/utilities/utilities.h>
#include <dftracer/utils/reader/line_processor.h>
#include <dftracer/utils/utilities/composites/dft/metadata_collector.h>
#include <dftracer/utils/utilities/composites/indexed_file_reader.h>
#include <yyjson.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "aggregation_config.hpp"
#include "aggregation_key.hpp"
#include "aggregation_metrics.hpp"
#include "association_tracker.hpp"
#include "trace_parser.hpp"

using namespace dftracer::utils;

/**
 * @brief Input for event aggregation from DFTracer metadata files.
 */
struct EventAggregatorUtilityInput {
    std::vector<utilities::composites::dft::MetadataCollectorUtilityOutput>
        metadata;
    AggregationConfig config;
    std::size_t checkpoint_size;

    static EventAggregatorUtilityInput from_metadata(
        std::vector<utilities::composites::dft::MetadataCollectorUtilityOutput>
            meta) {
        EventAggregatorUtilityInput input;
        input.metadata = std::move(meta);
        input.checkpoint_size = 0;
        return input;
    }

    EventAggregatorUtilityInput& with_config(const AggregationConfig& cfg) {
        config = cfg;
        return *this;
    }

    EventAggregatorUtilityInput& with_checkpoint_size(std::size_t size) {
        checkpoint_size = size;
        return *this;
    }
};

/**
 * @brief Output: aggregated metrics grouped by composite keys.
 */
struct EventAggregatorUtilityOutput {
    std::unordered_map<AggregationKey, AggregationMetrics, AggregationKeyHash>
        aggregations;
    std::size_t total_events_processed = 0;
    std::size_t total_files_processed = 0;
    bool success = true;
};

/**
 * @brief Utility for aggregating events from DFTracer trace files.
 *
 * This utility reads trace files and aggregates events based on composite keys
 * (cat, name, pid, tid, hhash, fhash, time_bucket, and custom keys).
 * It uses indexed file reading for efficient parallel processing.
 */
class EventAggregatorUtility
    : public utilities::Utility<EventAggregatorUtilityInput,
                                EventAggregatorUtilityOutput> {
   private:
    // Helper: Extract rank from filename
    int extract_rank_from_filename(const std::string& filename) const {
        std::size_t rank_pos = filename.find("rank_");
        if (rank_pos != std::string::npos) {
            std::size_t num_start = rank_pos + 5;
            std::size_t num_end =
                filename.find_first_not_of("0123456789", num_start);
            if (num_end != std::string::npos) {
                std::string rank_str =
                    filename.substr(num_start, num_end - num_start);
                return std::stoi(rank_str);
            }
        }
        return -1;
    }

    // Helper: Compute time bucket
    std::uint64_t compute_time_bucket(std::uint64_t timestamp,
                                      const AggregationConfig& config) const {
        if (config.use_relative_time) {
            timestamp -= config.reference_timestamp;
        }
        return (timestamp / config.time_interval_us) * config.time_interval_us;
    }

    // Helper: Build aggregation key from yyjson event
    AggregationKey build_key(
        yyjson_val* event, const AggregationConfig& config,
        const AssociationTracker* association_tracker = nullptr) const {
        AggregationKey key;

        // Extract core fields
        yyjson_val* val;
        val = yyjson_obj_get(event, "cat");
        if (val && yyjson_is_str(val)) key.cat = yyjson_get_str(val);

        val = yyjson_obj_get(event, "name");
        if (val && yyjson_is_str(val)) key.name = yyjson_get_str(val);

        val = yyjson_obj_get(event, "pid");
        if (val && yyjson_is_uint(val)) key.pid = yyjson_get_uint(val);

        val = yyjson_obj_get(event, "tid");
        if (val && yyjson_is_uint(val)) key.tid = yyjson_get_uint(val);

        // Extract from args
        yyjson_val* args = yyjson_obj_get(event, "args");
        if (args && yyjson_is_obj(args)) {
            val = yyjson_obj_get(args, "hhash");
            if (val && yyjson_is_str(val)) key.hhash = yyjson_get_str(val);

            val = yyjson_obj_get(args, "fhash");
            if (val && yyjson_is_str(val)) key.fhash = yyjson_get_str(val);

            // Extra group keys
            for (const auto& extra_key : config.extra_group_keys) {
                val = yyjson_obj_get(args, extra_key.c_str());
                if (val && yyjson_is_str(val)) {
                    key.extra_keys[extra_key] = yyjson_get_str(val);
                }
            }
        }

        // Time bucket
        val = yyjson_obj_get(event, "ts");
        std::uint64_t timestamp = 0;
        if (val && yyjson_is_uint(val)) {
            timestamp = yyjson_get_uint(val);
            key.time_bucket = compute_time_bucket(timestamp, config);
        }

        // Add boundary associations (epoch, step, etc.) to the key for proper
        // grouping
        if (association_tracker && !config.boundary_events.empty()) {
            auto associations = association_tracker->get_boundary_associations(
                key.pid, timestamp);
            for (const auto& [assoc_name, assoc_value] : associations) {
                key.extra_keys[assoc_name] = assoc_value;
            }
        }

        return key;
    }

    // Helper: Process a single event
    void process_event(
        yyjson_val* event, int rank, const std::string& trace_file,
        const AggregationConfig& config,
        const AssociationTracker& association_tracker,
        std::unordered_map<AggregationKey, AggregationMetrics,
                           AggregationKeyHash>& local_aggregations) const {
        // Extract basic fields for filtering
        yyjson_val* val;
        std::string cat, name;

        val = yyjson_obj_get(event, "cat");
        if (val && yyjson_is_str(val)) cat = yyjson_get_str(val);

        val = yyjson_obj_get(event, "name");
        if (val && yyjson_is_str(val)) name = yyjson_get_str(val);

        // Apply filters
        if (!config.passes_filters(cat, name)) {
            return;
        }

        // Build aggregation key (include associations for proper grouping)
        AggregationKey key = build_key(event, config, &association_tracker);

        // Get or create metrics entry
        auto& metrics = local_aggregations[key];

        // Extract event data
        std::uint64_t duration = 0, timestamp = 0, size = 0;

        val = yyjson_obj_get(event, "dur");
        if (val && yyjson_is_uint(val)) duration = yyjson_get_uint(val);

        val = yyjson_obj_get(event, "ts");
        if (val && yyjson_is_uint(val)) timestamp = yyjson_get_uint(val);

        // Update metrics
        metrics.update_duration(duration);
        metrics.update_timestamp(timestamp);

        // Handle size (from args.ret)
        yyjson_val* args = yyjson_obj_get(event, "args");
        if (args && yyjson_is_obj(args)) {
            val = yyjson_obj_get(args, "ret");
            if (val && yyjson_is_uint(val)) {
                size = yyjson_get_uint(val);
                metrics.update_size(size);
            }

            // Handle custom metrics
            for (const auto& field : config.custom_metric_fields) {
                val = yyjson_obj_get(args, field.c_str());
                if (val && yyjson_is_uint(val)) {
                    std::uint64_t value = yyjson_get_uint(val);
                    metrics.update_custom_metric(field, value);
                }
            }
        }

        // Track contributing sources
        if (config.include_trace_metadata) {
            metrics.add_contributing_source(rank, trace_file);
        }

        // Add association data if first time seeing this key
        if (metrics.count == 1) {
            // Extract pid and timestamp for association lookup
            std::uint64_t pid = TraceParser::get_uint64(event, "pid");
            std::uint64_t ts = TraceParser::get_uint64(event, "ts");

            // Get boundary associations (epoch, step, etc.)
            if (!config.boundary_events.empty()) {
                metrics.boundary_associations =
                    association_tracker.get_boundary_associations(pid, ts);
            }

            // Get parent process
            if (config.track_process_parents) {
                metrics.parent_pid = association_tracker.get_parent_pid(pid);
            }
        }
    }

   public:
    EventAggregatorUtilityOutput process(
        const EventAggregatorUtilityInput& input) override {
        EventAggregatorUtilityOutput output;

        // Local aggregations map (thread-local within utility execution)
        std::unordered_map<AggregationKey, AggregationMetrics,
                           AggregationKeyHash>
            local_aggregations;

        // Association tracker for boundary events and process relationships
        AssociationTracker association_tracker;

        // PASS 1: Extract associations if configured
        if (input.config.track_process_parents ||
            !input.config.boundary_events.empty()) {
            DFTRACER_UTILS_LOG_INFO(
                "Pass 1: Extracting associations (boundary events and process "
                "relationships)...");

            for (const auto& meta : input.metadata) {
                if (!meta.success) continue;

                // Read entire file for association extraction
                auto reader_input =
                    utilities::composites::IndexedReadInput::from_file(
                        meta.file_path)
                        .with_checkpoint_size(input.checkpoint_size)
                        .with_index(meta.idx_path);

                utilities::composites::IndexedFileReaderUtility reader_utility;
                auto reader = reader_utility.process(reader_input);
                if (!reader) continue;

                auto stream =
                    reader->stream(StreamType::LINE, RangeType::LINE_RANGE, 1,
                                   reader->get_num_lines());
                if (!stream) continue;

                // Process each line (event) for association extraction
                constexpr std::size_t BUFFER_SIZE = 65536;
                char buffer[BUFFER_SIZE];

                while (!stream->done()) {
                    std::size_t bytes_read = stream->read(buffer, BUFFER_SIZE);
                    if (bytes_read == 0) break;

                    // Parse JSON line
                    yyjson_doc* doc =
                        yyjson_read(buffer, bytes_read, YYJSON_READ_NOFLAG);
                    if (doc) {
                        yyjson_val* event = yyjson_doc_get_root(doc);
                        association_tracker.extract_from_event(event,
                                                               input.config);
                        yyjson_doc_free(doc);
                    }
                }
            }

            // Finalize association tracker (sort intervals, etc.)
            association_tracker.finalize();

            DFTRACER_UTILS_LOG_INFO(
                "Association extraction complete. Process relationships: %s, "
                "Boundary events: %s",
                association_tracker.has_process_tree() ? "yes" : "no",
                association_tracker.has_boundary_events() ? "yes" : "no");
        }

        // PASS 2: Process each metadata file for aggregation
        for (const auto& meta : input.metadata) {
            if (!meta.success) {
                DFTRACER_UTILS_LOG_WARN("Skipping unsuccessful file: %s",
                                        meta.file_path.c_str());
                continue;
            }

            // Extract rank from filename
            int rank = extract_rank_from_filename(meta.file_path);

            DFTRACER_UTILS_LOG_DEBUG(
                "Processing file: %s (rank %d, %zu events)",
                meta.file_path.c_str(), rank, meta.valid_events);

            // Create indexed file reader for this trace file
            auto reader_input =
                utilities::composites::IndexedReadInput::from_file(
                    meta.file_path)
                    .with_checkpoint_size(input.checkpoint_size)
                    .with_index(meta.idx_path);

            utilities::composites::IndexedFileReaderUtility reader_utility;
            auto reader = reader_utility.process(reader_input);

            if (!reader) {
                DFTRACER_UTILS_LOG_ERROR("Failed to create reader for file: %s",
                                         meta.file_path.c_str());
                continue;
            }

            // Create a stream for line-by-line reading (lines are 1-based)
            auto stream =
                reader->stream(StreamType::LINE, RangeType::LINE_RANGE, 1,
                               reader->get_num_lines());

            if (!stream) {
                DFTRACER_UTILS_LOG_ERROR("Failed to create stream for file: %s",
                                         meta.file_path.c_str());
                continue;
            }

            // Process each line (event)
            constexpr std::size_t BUFFER_SIZE = 65536;  // 64KB buffer
            char buffer[BUFFER_SIZE];

            while (!stream->done()) {
                std::size_t bytes_read = stream->read(buffer, BUFFER_SIZE);
                if (bytes_read == 0) break;

                // Parse JSON line using yyjson
                yyjson_doc* doc =
                    yyjson_read(buffer, bytes_read, YYJSON_READ_NOFLAG);
                if (!doc) {
                    continue;  // Skip invalid JSON
                }

                yyjson_val* root = yyjson_doc_get_root(doc);
                if (root && yyjson_is_obj(root)) {
                    process_event(root, rank, meta.file_path, input.config,
                                  association_tracker, local_aggregations);
                    output.total_events_processed++;
                }

                yyjson_doc_free(doc);
            }

            output.total_files_processed++;
        }

        // Move local aggregations to output
        output.aggregations = std::move(local_aggregations);

        DFTRACER_UTILS_LOG_INFO(
            "Aggregation complete: %zu unique keys, %zu events from %zu files",
            output.aggregations.size(), output.total_events_processed,
            output.total_files_processed);

        return output;
    }
};
