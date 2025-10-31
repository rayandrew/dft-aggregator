#pragma once

#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/utilities/compression/zlib/streaming_compressor_utility.h>
#include <dftracer/utils/utilities/io/streaming_file_writer_utility.h>

#include <algorithm>
#include <cstdio>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "aggregation_key.hpp"
#include "aggregation_metrics.hpp"
#include "association_resolver_utility.hpp"

class PerfettoCounterWriter {
   private:
    const char* hostname_hash_;

    // Append escaped JSON string to buffer
    void append_json_string(std::string& buffer, const std::string& str) const {
        for (char c : str) {
            switch (c) {
                case '"':
                    buffer += "\\\"";
                    break;
                case '\\':
                    buffer += "\\\\";
                    break;
                case '\b':
                    buffer += "\\b";
                    break;
                case '\f':
                    buffer += "\\f";
                    break;
                case '\n':
                    buffer += "\\n";
                    break;
                case '\r':
                    buffer += "\\r";
                    break;
                case '\t':
                    buffer += "\\t";
                    break;
                default:
                    if (c >= 32 && c < 127) {
                        buffer += c;
                    } else {
                        char hex[7];
                        std::snprintf(hex, sizeof(hex), "\\u%04x",
                                      (unsigned char)c);
                        buffer += hex;
                    }
                    break;
            }
        }
    }

   public:
    PerfettoCounterWriter(const char* hhash = "unknown")
        : hostname_hash_(hhash) {}

    void set_hostname_hash(const char* hhash) { hostname_hash_ = hhash; }

    // Write all aggregated metrics as counter events
    bool write_aggregated_counters(
        const std::string& output_path,
        const AssociationResolverOutput& resolver_output,
        bool compute_statistics = true, bool compress = false,
        int compression_level = Z_DEFAULT_COMPRESSION) {
        const auto& aggregations = resolver_output.aggregations.aggregations;
        const auto& root_pids = resolver_output.root_pids;
        // Use a buffer to collect output before writing
        std::string buffer;
        buffer.reserve(1024 * 1024);  // Reserve 1MB initially

        buffer += "[\n";

        // Emit metadata events for root processes first
        if (!root_pids.empty()) {
            for (std::uint64_t pid : root_pids) {
                buffer +=
                    "{\"name\":\"root_process\",\"cat\":\"dftracer\",\"ph\":"
                    "\"M\",\"pid\":";
                buffer += std::to_string(pid);
                buffer += ",\"tid\":";
                buffer += std::to_string(pid);
                buffer += ",\"args\":{\"hhash\":\"";
                buffer += hostname_hash_;
                buffer += "\",\"is_root\":\"true\"}},\n";
            }
        }

        // Emit one counter event per aggregation bucket with all metrics in
        // args Note: No sorting needed - events are already grouped by
        // time_bucket
        for (const auto& [key, metrics] : aggregations) {
            buffer += "{\"name\":\"";
            append_json_string(buffer, key.name);
            buffer += "\",\"cat\":\"";
            append_json_string(buffer, key.cat);

            char temp[512];
            std::snprintf(temp, sizeof(temp),
                          "\",\"ts\":%llu,\"ph\":\"C\",\"pid\":%llu,"
                          "\"tid\":%llu,\"args\":{\"hhash\":\"",
                          key.time_bucket, key.pid, key.tid);
            buffer += temp;
            append_json_string(buffer, key.hhash);
            buffer += "\"";

            if (!key.fhash.empty()) {
                buffer += ",\"fhash\":\"";
                append_json_string(buffer, key.fhash);
                buffer += "\"";
            }

            for (const auto& [k, v] : key.extra_keys) {
                buffer += ",\"";
                append_json_string(buffer, k);
                buffer += "\":\"";
                append_json_string(buffer, v);
                buffer += "\"";
            }

            // Add all metrics to args
            std::snprintf(temp, sizeof(temp), ",\"count\":%llu", metrics.count);
            buffer += temp;

            // Duration metrics as nested object
            buffer += ",\"dur\":{";
            std::snprintf(temp, sizeof(temp), "\"total\":%llu",
                          metrics.total_duration);
            buffer += temp;
            std::snprintf(temp, sizeof(temp), ",\"mean\":%.17g",
                          metrics.mean_duration);
            buffer += temp;

            if (metrics.min_duration !=
                std::numeric_limits<std::uint64_t>::max()) {
                std::snprintf(temp, sizeof(temp), ",\"min\":%llu",
                              metrics.min_duration);
                buffer += temp;
            }
            if (metrics.max_duration > 0) {
                std::snprintf(temp, sizeof(temp), ",\"max\":%llu",
                              metrics.max_duration);
                buffer += temp;
            }
            if (compute_statistics && metrics.count >= 2) {
                std::snprintf(temp, sizeof(temp), ",\"stddev\":%.17g",
                              metrics.get_stddev_duration());
                buffer += temp;
            }
            buffer += "}";

            // Size metrics as nested object (if present)
            if (metrics.total_size > 0) {
                buffer += ",\"size\":{";
                std::snprintf(temp, sizeof(temp), "\"total\":%llu",
                              metrics.total_size);
                buffer += temp;
                std::snprintf(temp, sizeof(temp), ",\"mean\":%.17g",
                              metrics.mean_size);
                buffer += temp;
                if (metrics.min_size !=
                    std::numeric_limits<std::uint64_t>::max()) {
                    std::snprintf(temp, sizeof(temp), ",\"min\":%llu",
                                  metrics.min_size);
                    buffer += temp;
                }
                if (metrics.max_size > 0) {
                    std::snprintf(temp, sizeof(temp), ",\"max\":%llu",
                                  metrics.max_size);
                    buffer += temp;
                }
                if (compute_statistics && metrics.count >= 2) {
                    std::snprintf(temp, sizeof(temp), ",\"stddev\":%.17g",
                                  metrics.get_stddev_size());
                    buffer += temp;
                }
                buffer += "}";
            }

            // Custom metrics as nested objects
            for (const auto& [metric_name, sum_value] : metrics.custom_sum) {
                buffer += ",\"";
                append_json_string(buffer, metric_name);
                buffer += "\":{";

                std::snprintf(temp, sizeof(temp), "\"total\":%llu", sum_value);
                buffer += temp;

                std::snprintf(temp, sizeof(temp), ",\"mean\":%.17g",
                              metrics.custom_mean.at(metric_name));
                buffer += temp;

                if (metrics.custom_min.find(metric_name) !=
                        metrics.custom_min.end() &&
                    metrics.custom_min.at(metric_name) !=
                        std::numeric_limits<std::uint64_t>::max()) {
                    std::snprintf(temp, sizeof(temp), ",\"min\":%llu",
                                  metrics.custom_min.at(metric_name));
                    buffer += temp;
                }
                if (metrics.custom_max.find(metric_name) !=
                        metrics.custom_max.end() &&
                    metrics.custom_max.at(metric_name) > 0) {
                    std::snprintf(temp, sizeof(temp), ",\"max\":%llu",
                                  metrics.custom_max.at(metric_name));
                    buffer += temp;
                }
                if (compute_statistics && metrics.count >= 2) {
                    std::snprintf(temp, sizeof(temp), ",\"stddev\":%.17g",
                                  metrics.get_custom_stddev(metric_name));
                    buffer += temp;
                }

                buffer += "}";
            }

            // Timestamp range as array [first, last]
            buffer += ",\"ts_range\":[";
            std::snprintf(temp, sizeof(temp), "%llu,%llu", metrics.first_ts,
                          metrics.last_ts);
            buffer += temp;
            buffer += "]";

            // Add boundary associations (epoch, step, etc.)
            for (const auto& [assoc_name, assoc_value] :
                 metrics.boundary_associations) {
                buffer += ",\"";
                append_json_string(buffer, assoc_name);
                buffer += "\":\"";
                append_json_string(buffer, assoc_value);
                buffer += "\"";
            }

            // Add parent process if available
            if (metrics.parent_pid > 0) {
                std::snprintf(temp, sizeof(temp), ",\"parent_pid\":%llu",
                              metrics.parent_pid);
                buffer += temp;
            }

            buffer += "}}\n";
        }

        buffer += "]\n";

        // Write output (compressed or uncompressed)
        try {
            if (compress) {
                // Use streaming compression
                using namespace dftracer::utils::utilities;

                compression::zlib::ManualStreamingCompressorUtility compressor(
                    compression_level,
                    compression::zlib::CompressionFormat::GZIP);

                io::StreamingFileWriterUtility writer(output_path);

                // Compress the buffer
                io::RawData raw_data;
                raw_data.data.assign(buffer.begin(), buffer.end());
                auto compressed_chunks = compressor.process(raw_data);

                for (const auto& chunk : compressed_chunks) {
                    io::RawData raw_chunk{chunk.data};
                    writer.process(raw_chunk);
                }

                // Finalize compression
                auto final_chunks = compressor.finalize();
                for (const auto& chunk : final_chunks) {
                    io::RawData raw_chunk{chunk.data};
                    writer.process(raw_chunk);
                }

                writer.close();
            } else {
                // Uncompressed output using simple FILE*
                FILE* fp = std::fopen(output_path.c_str(), "w");
                if (!fp) {
                    DFTRACER_UTILS_LOG_ERROR("Failed to open output file: %s",
                                             output_path.c_str());
                    return false;
                }
                std::fwrite(buffer.data(), 1, buffer.size(), fp);
                std::fclose(fp);
            }

            return true;
        } catch (const std::exception& e) {
            DFTRACER_UTILS_LOG_ERROR("Failed to write output: %s", e.what());
            return false;
        }
    }

    // Write summary statistics
    void write_summary(
        const std::unordered_map<AggregationKey, AggregationMetrics,
                                 AggregationKeyHash>& aggregations) {
        std::printf("=== Aggregation Summary ===\n");
        std::printf("Total unique aggregation keys: %zu\n",
                    aggregations.size());

        std::uint64_t total_events = 0;
        for (const auto& [key, metrics] : aggregations) {
            total_events += metrics.count;
        }
        std::printf("Total events aggregated: %llu\n", total_events);

        // Category breakdown
        std::unordered_map<std::string, std::uint64_t> category_counts;
        for (const auto& [key, metrics] : aggregations) {
            category_counts[key.cat] += metrics.count;
        }

        std::printf("\nEvents by category:\n");
        for (const auto& [cat, count] : category_counts) {
            std::printf("  %s: %llu events\n", cat.c_str(), count);
        }

        std::printf("\n");
    }
};
