#pragma once

#include <dftracer/utils/core/utilities/utility.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utilities/composites/composite_types.h>
#include <dftracer/utils/utilities/composites/indexed_file_reader.h>
#include <yyjson.h>

#include <string_view>

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
    int rank;         // Extracted from filename

    // Builder pattern
    static ChunkAggregatorInput from_metadata(
        const std::string& file_path, const std::string& idx_path,
        std::size_t start_byte, std::size_t end_byte, std::size_t start_line,
        std::size_t end_line, int chunk_index, int rank) {
        ChunkAggregatorInput input;
        input.file_path = file_path;
        input.idx_path = idx_path;
        input.start_byte = start_byte;
        input.end_byte = end_byte;
        input.start_line = start_line;
        input.end_line = end_line;
        input.chunk_index = chunk_index;
        input.rank = rank;
        return input;
    }

    ChunkAggregatorInput& with_config(const AggregationConfig& cfg) {
        config = cfg;
        return *this;
    }

    ChunkAggregatorInput& with_checkpoint_size(std::size_t size) {
        checkpoint_size = size;
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

// Utility that aggregates events from a single chunk (byte range) using
// thread-local maps
class ChunkAggregatorUtility
    : public utilities::Utility<ChunkAggregatorInput, ChunkAggregationOutput,
                                utilities::tags::Parallelizable> {
   private:
    // Shared association tracker (read-only after Pass 1 merge)
    const AssociationTracker* association_tracker_;

    // Helper: Compute time bucket
    std::uint64_t compute_time_bucket(std::uint64_t timestamp,
                                      const AggregationConfig& config) const {
        if (config.use_relative_time) {
            timestamp -= config.reference_timestamp;
        }
        return (timestamp / config.time_interval_us) * config.time_interval_us;
    }

    // Helper: Build aggregation key from yyjson event
    AggregationKey build_key(yyjson_val* event,
                             const AggregationConfig& config) const {
        AggregationKey key;

        // Use JsonParserUtility (stack allocation)
        JsonParserUtility parser;
        JsonValue json = parser.process(event);

        // Extract core fields
        key.cat = json["cat"].get<std::string_view>();
        key.name = json["name"].get<std::string_view>();
        key.pid = json["pid"].get<std::uint64_t>();
        key.tid = json["tid"].get<std::uint64_t>();

        // Extract from args
        key.hhash = json["args"]["hhash"].get<std::string_view>();
        key.fhash = json["args"]["fhash"].get<std::string_view>();

        // Time bucket
        std::uint64_t timestamp = json["ts"].get<std::uint64_t>();
        key.time_bucket = compute_time_bucket(timestamp, config);

        // Extra group keys
        for (const auto& extra_key : config.extra_group_keys) {
            std::string field_path = "args." + extra_key;
            std::string_view value =
                json.at(field_path).get<std::string_view>();
            if (!value.empty()) {
                key.extra_keys[extra_key] = value;
            }
        }

        // Add boundary associations (epoch, step, etc.) to the key for proper
        // grouping
        if (association_tracker_ && !config.boundary_events.empty()) {
            auto associations = association_tracker_->get_boundary_associations(
                key.pid, timestamp);
            for (const auto& [assoc_name, assoc_value] : associations) {
                key.extra_keys[assoc_name] = assoc_value;
            }
        }

        return key;
    }

    // Helper: Process a single event and update metrics
    void process_event(
        yyjson_val* event, int rank, const std::string& trace_file,
        const AggregationConfig& config,
        std::unordered_map<AggregationKey, AggregationMetrics,
                           AggregationKeyHash>& local_aggregations) {
        JsonParserUtility parser;
        JsonValue json = parser.process(event);

        // Skip metadata events (ph:"M")
        std::string_view ph = json["ph"].get<std::string_view>();
        if (ph == "M") {
            return;
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
        AggregationKey key = build_key(event, config);

        // Get or create metrics entry
        auto& metrics = local_aggregations[key];

        // Extract event data
        std::uint64_t duration = json["dur"].get<std::uint64_t>();
        std::uint64_t timestamp = json["ts"].get<std::uint64_t>();
        std::uint64_t size = 0;

        // Update metrics
        metrics.update_duration(duration);
        metrics.update_timestamp(timestamp);

        // Handle size (from args.ret)
        if (json["args"]["ret"].exists()) {
            size = json["args"]["ret"].get<std::uint64_t>();
            metrics.update_size(size);
        }

        // Handle custom metrics
        for (const auto& field : config.custom_metric_fields) {
            std::string field_path = "args." + field;
            if (json.at(field_path).exists()) {
                std::uint64_t value = json.at(field_path).get<std::uint64_t>();
                metrics.update_custom_metric(field, value);
            }
        }

        // Track contributing sources
        if (config.include_trace_metadata) {
            metrics.add_contributing_source(rank, trace_file);
        }

        // Add association data if first time seeing this key
        if (metrics.count == 1) {
            std::uint64_t pid = json["pid"].get<std::uint64_t>();
            std::uint64_t ts = json["ts"].get<std::uint64_t>();

            // Get boundary associations (epoch, step, etc.)
            if (association_tracker_ && !config.boundary_events.empty()) {
                metrics.boundary_associations =
                    association_tracker_->get_boundary_associations(pid, ts);
            }

            // Get parent PID if tracking process relationships
            if (association_tracker_ && config.track_process_parents) {
                metrics.parent_pid = association_tracker_->get_parent_pid(pid);
            }
        }
    }

   public:
    ChunkAggregatorUtility() : association_tracker_(nullptr) {}

    // Set the shared association tracker (must be called before process())
    void set_association_tracker(const AssociationTracker* tracker) {
        association_tracker_ = tracker;
    }

    ChunkAggregationOutput process(const ChunkAggregatorInput& input) override {
        ChunkAggregationOutput output;
        output.chunk_index = input.chunk_index;
        output.events_processed = 0;
        output.success = false;

        DFTRACER_UTILS_LOG_DEBUG(
            "Chunk %d: Aggregating events from %s [bytes %zu-%zu]",
            input.chunk_index, input.file_path.c_str(), input.start_byte,
            input.end_byte);

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

        // Create stream using LINE_BYTES for byte-range access
        auto stream =
            reader->stream(StreamType::LINE_BYTES, RangeType::BYTE_RANGE,
                           input.start_byte, input.end_byte);

        if (!stream) {
            DFTRACER_UTILS_LOG_ERROR("Chunk %d: Failed to create stream for %s",
                                     input.chunk_index,
                                     input.file_path.c_str());
            return output;
        }

        // Thread-local aggregation map (lock-free!)
        std::unordered_map<AggregationKey, AggregationMetrics,
                           AggregationKeyHash>
            local_aggregations;

        // Process each line (event)
        constexpr std::size_t BUFFER_SIZE = 65536;  // 64KB buffer
        char buffer[BUFFER_SIZE];

        while (!stream->done()) {
            std::size_t bytes_read = stream->read(buffer, BUFFER_SIZE);
            if (bytes_read == 0) break;

            // Null-terminate for safety
            if (bytes_read < BUFFER_SIZE) {
                buffer[bytes_read] = '\0';
            }

            // Parse JSON line
            yyjson_doc* doc =
                yyjson_read(buffer, bytes_read, YYJSON_READ_NOFLAG);
            if (doc) {
                yyjson_val* root = yyjson_doc_get_root(doc);
                if (root && yyjson_is_obj(root)) {
                    process_event(root, input.rank, input.file_path,
                                  input.config, local_aggregations);
                    output.events_processed++;
                }
                yyjson_doc_free(doc);
            }
        }

        // Move local aggregations to output
        output.aggregations = std::move(local_aggregations);
        output.success = true;

        DFTRACER_UTILS_LOG_DEBUG(
            "Chunk %d: Aggregated %zu events into %zu unique keys",
            input.chunk_index, output.events_processed,
            output.aggregations.size());

        return output;
    }
};
