#pragma once

#include <atomic>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "aggregation_config.hpp"
#include "aggregation_key.hpp"
#include "aggregation_metrics.hpp"
#include "trace_parser.hpp"
#include "yyjson.h"

struct TraceMetadata {
    std::string trace_file_path;
    int rank;
    std::uint64_t min_timestamp = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t max_timestamp = 0;
};

class ThreadLocalAggregator {
   private:
    struct alignas(64) ThreadLocalData {
        std::unordered_map<AggregationKey, AggregationMetrics,
                           AggregationKeyHash>
            local_map;
        std::thread::id thread_id;
        int thread_index;
        std::uint64_t events_processed = 0;
    };

    static thread_local ThreadLocalData* tls_data;

    std::vector<std::unique_ptr<ThreadLocalData>> all_thread_data_;
    std::atomic<int> thread_counter_{0};
    std::mutex registration_mutex_;

    AggregationConfig config_;
    std::vector<TraceMetadata> traces_;

    ThreadLocalData* get_thread_local_data() {
        if (tls_data == nullptr) {
            auto data = std::make_unique<ThreadLocalData>();
            data->thread_id = std::this_thread::get_id();
            data->thread_index = thread_counter_.fetch_add(1);

            tls_data = data.get();

            std::lock_guard<std::mutex> lock(registration_mutex_);
            all_thread_data_.push_back(std::move(data));
        }
        return tls_data;
    }

    std::uint64_t compute_time_bucket(std::uint64_t timestamp) const {
        if (config_.use_relative_time) {
            timestamp -= config_.reference_timestamp;
        }
        return (timestamp / config_.time_interval_us) *
               config_.time_interval_us;
    }

    AggregationKey build_key(yyjson_val* event, std::uint64_t reference_ts) {
        AggregationKey key;

        key.cat = TraceParser::get_string(event, "cat");
        key.name = TraceParser::get_string(event, "name");
        key.pid = TraceParser::get_uint64(event, "pid");
        key.tid = TraceParser::get_uint64(event, "tid");

        // Get hhash and fhash from args
        key.hhash = TraceParser::get_arg_string(event, "hhash");
        key.fhash = TraceParser::get_arg_string(event, "fhash");

        // Time bucket
        std::uint64_t timestamp = TraceParser::get_uint64(event, "ts");
        key.time_bucket = compute_time_bucket(timestamp);

        // Extra keys from args
        for (const auto& extra_key : config_.extra_group_keys) {
            std::string value =
                TraceParser::get_arg_string(event, extra_key.c_str());
            if (!value.empty()) {
                key.extra_keys[extra_key] = value;
            }
        }

        return key;
    }

   public:
    ThreadLocalAggregator() = default;

    void set_config(const AggregationConfig& config) { config_ = config; }

    const AggregationConfig& get_config() const { return config_; }

    void process_event(yyjson_val* event, int rank,
                       const std::string& trace_file) {
        auto* thread_data = get_thread_local_data();

        // Extract basic fields for filtering
        std::string cat = TraceParser::get_string(event, "cat");
        std::string name = TraceParser::get_string(event, "name");

        // Apply filters
        if (!config_.passes_filters(cat, name)) {
            return;
        }

        // Build aggregation key
        AggregationKey key = build_key(event, 0);
        auto& metrics = thread_data->local_map[key];

        std::uint64_t duration = TraceParser::get_uint64(event, "dur");
        std::uint64_t timestamp = TraceParser::get_uint64(event, "ts");
        metrics.update_duration(duration);
        metrics.update_timestamp(timestamp);

        // Handle size (from args.ret)
        if (TraceParser::has_arg(event, "ret")) {
            std::uint64_t size = TraceParser::get_arg_uint64(event, "ret");
            metrics.update_size(size);
        }

        // Handle custom metrics
        for (const auto& field : config_.custom_metric_fields) {
            if (TraceParser::has_arg(event, field.c_str())) {
                std::uint64_t value =
                    TraceParser::get_arg_uint64(event, field.c_str());
                metrics.update_custom_metric(field, value);
            }
        }

        // Track contributing sources
        if (config_.include_trace_metadata) {
            metrics.add_contributing_source(rank, trace_file);
        }

        thread_data->events_processed++;
    }

    void process_trace_file(const std::string& trace_file, int rank) {
        // Read entire file
        std::ifstream file(trace_file, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Failed to open file: " << trace_file << std::endl;
            return;
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string content = buffer.str();

        // Parse with yyjson
        yyjson_read_err err;
        yyjson_doc* doc =
            yyjson_read_opts(const_cast<char*>(content.c_str()), content.size(),
                             YYJSON_READ_NOFLAG, nullptr, &err);

        if (!doc) {
            std::cerr << "JSON parse error in " << trace_file << ": " << err.msg
                      << std::endl;
            return;
        }

        yyjson_val* root = yyjson_doc_get_root(doc);

        // If root is array, iterate through events
        if (yyjson_is_arr(root)) {
            std::size_t idx, max;
            yyjson_val* event;
            yyjson_arr_foreach(root, idx, max, event) {
                if (yyjson_is_obj(event)) {
                    process_event(event, rank, trace_file);
                }
            }
        } else if (yyjson_is_obj(root)) {
            // Single event
            process_event(root, rank, trace_file);
        }

        yyjson_doc_free(doc);
    }

    void add_trace(const std::string& trace_file, int rank) {
        TraceMetadata meta;
        meta.trace_file_path = trace_file;
        meta.rank = rank;
        traces_.push_back(meta);
    }

    void aggregate_all_traces(int num_threads = -1) {
        if (num_threads <= 0) {
            num_threads = std::thread::hardware_concurrency();
        }

        std::vector<std::thread> threads;
        threads.reserve(traces_.size());

        for (const auto& trace_meta : traces_) {
            threads.emplace_back([this, &trace_meta]() {
                process_trace_file(trace_meta.trace_file_path, trace_meta.rank);
            });

            // Limit number of concurrent threads
            if (threads.size() >= static_cast<std::size_t>(num_threads)) {
                for (auto& t : threads) {
                    if (t.joinable()) {
                        t.join();
                    }
                }
                threads.clear();
            }
        }

        // Wait for remaining threads
        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    std::unordered_map<AggregationKey, AggregationMetrics, AggregationKeyHash>
    merge_results() {
        std::unordered_map<AggregationKey, AggregationMetrics,
                           AggregationKeyHash>
            merged;

        for (const auto& thread_data : all_thread_data_) {
            for (const auto& [key, metrics] : thread_data->local_map) {
                if (merged.find(key) == merged.end()) {
                    merged[key] = metrics;
                } else {
                    merged[key].merge_from(metrics);
                }
            }
        }

        return merged;
    }

    std::size_t get_num_threads() const { return all_thread_data_.size(); }

    std::uint64_t get_total_events_processed() const {
        std::uint64_t total = 0;
        for (const auto& thread_data : all_thread_data_) {
            total += thread_data->events_processed;
        }
        return total;
    }

    const std::vector<TraceMetadata>& get_traces() const { return traces_; }
};

// Thread-local storage definition (define in cpp file)
thread_local ThreadLocalAggregator::ThreadLocalData*
    ThreadLocalAggregator::tls_data = nullptr;
