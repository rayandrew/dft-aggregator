#pragma once

#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/utilities/compression/zlib/streaming_compressor.h>
#include <dftracer/utils/utilities/io/streaming_file_writer.h>

#include <algorithm>
#include <cstdio>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "aggregation_key.hpp"
#include "aggregation_metrics.hpp"

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

    // Write all aggregated metrics as counter events using fprintf (fast!)
    bool write_aggregated_counters(
        const std::string& output_path,
        const std::unordered_map<AggregationKey, AggregationMetrics,
                                 AggregationKeyHash>& aggregations,
        bool compute_statistics = true, bool compress = false,
        int compression_level = Z_DEFAULT_COMPRESSION) {
        // Use a buffer to collect output before writing
        std::string buffer;
        buffer.reserve(1024 * 1024);  // Reserve 1MB initially

        // Sort by time bucket
        std::vector<std::pair<AggregationKey, AggregationMetrics>> sorted_aggs;
        sorted_aggs.reserve(aggregations.size());
        for (const auto& [key, metrics] : aggregations) {
            sorted_aggs.emplace_back(key, metrics);
        }
        std::sort(sorted_aggs.begin(), sorted_aggs.end(),
                  [](const auto& a, const auto& b) {
                      if (a.first.time_bucket != b.first.time_bucket)
                          return a.first.time_bucket < b.first.time_bucket;
                      return a.first < b.first;
                  });

        buffer += "[\n";

        // Emit counter events for each aggregation bucket
        for (const auto& [key, metrics] : sorted_aggs) {
            auto write_counter = [&](const std::string& metric_suffix,
                                     double value) {
                // Use original name with metric suffix
                std::string full_name = key.name + "[" + metric_suffix + "]";

                buffer += "{\"name\":\"";
                append_json_string(buffer, full_name);
                buffer += "\",\"cat\":\"";
                append_json_string(buffer, key.cat);

                char temp[256];
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

                std::snprintf(temp, sizeof(temp), ",\"value\":%.17g}}\n",
                              value);
                buffer += temp;
            };

            write_counter("count", static_cast<double>(metrics.count));
            write_counter("dur.total",
                          static_cast<double>(metrics.total_duration));
            write_counter("dur.mean", metrics.mean_duration);

            if (metrics.min_duration != std::numeric_limits<uint64_t>::max()) {
                write_counter("dur.min",
                              static_cast<double>(metrics.min_duration));
            }
            if (metrics.max_duration > 0) {
                write_counter("dur.max",
                              static_cast<double>(metrics.max_duration));
            }
            if (compute_statistics && metrics.count >= 2) {
                write_counter("dur.stddev", metrics.get_stddev_duration());
            }

            if (metrics.total_size > 0) {
                write_counter("size.total",
                              static_cast<double>(metrics.total_size));
                write_counter("size.mean", metrics.mean_size);
                if (metrics.min_size != std::numeric_limits<uint64_t>::max()) {
                    write_counter("size.min",
                                  static_cast<double>(metrics.min_size));
                }
                if (metrics.max_size > 0) {
                    write_counter("size.max",
                                  static_cast<double>(metrics.max_size));
                }
                if (compute_statistics && metrics.count >= 2) {
                    write_counter("size.stddev", metrics.get_stddev_size());
                }
            }

            for (const auto& [metric_name, sum_value] : metrics.custom_sum) {
                write_counter(metric_name + ".total",
                              static_cast<double>(sum_value));
                write_counter(metric_name + ".mean",
                              metrics.custom_mean.at(metric_name));
                if (metrics.custom_min.find(metric_name) !=
                        metrics.custom_min.end() &&
                    metrics.custom_min.at(metric_name) !=
                        std::numeric_limits<uint64_t>::max()) {
                    write_counter(metric_name + ".min",
                                  static_cast<double>(
                                      metrics.custom_min.at(metric_name)));
                }
                if (metrics.custom_max.find(metric_name) !=
                        metrics.custom_max.end() &&
                    metrics.custom_max.at(metric_name) > 0) {
                    write_counter(metric_name + ".max",
                                  static_cast<double>(
                                      metrics.custom_max.at(metric_name)));
                }
                if (compute_statistics && metrics.count >= 2) {
                    write_counter(metric_name + ".stddev",
                                  metrics.get_custom_stddev(metric_name));
                }
            }
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

        uint64_t total_events = 0;
        for (const auto& [key, metrics] : aggregations) {
            total_events += metrics.count;
        }
        std::printf("Total events aggregated: %llu\n", total_events);

        // Category breakdown
        std::unordered_map<std::string, uint64_t> category_counts;
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
