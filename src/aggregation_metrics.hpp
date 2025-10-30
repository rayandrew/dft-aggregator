#pragma once

#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>

struct AggregationMetrics {
    // Event count
    std::uint64_t count = 0;

    // Duration statistics (microseconds)
    std::uint64_t total_duration = 0;
    std::uint64_t min_duration = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t max_duration = 0;
    double mean_duration = 0.0;
    double m2_duration = 0.0;  // For Welford's algorithm

    // Size statistics (bytes)
    std::uint64_t total_size = 0;
    std::uint64_t min_size = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t max_size = 0;
    double mean_size = 0.0;
    double m2_size = 0.0;

    // Timestamp range
    std::uint64_t first_ts = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t last_ts = 0;

    // Contributing sources
    std::unordered_set<int> contributing_ranks;
    std::unordered_set<std::string> contributing_traces;

    // Association data
    std::unordered_map<std::string, std::string>
        boundary_associations;     // e.g., {"epoch": "1", "step": "5"}
    std::uint64_t parent_pid = 0;  // Parent process ID (0 if none)

    // Custom metrics (using unordered_map for O(1) access)
    std::unordered_map<std::string, std::uint64_t> custom_sum;
    std::unordered_map<std::string, std::uint64_t> custom_min;
    std::unordered_map<std::string, std::uint64_t> custom_max;
    std::unordered_map<std::string, double> custom_mean;
    std::unordered_map<std::string, double> custom_m2;

    // Update methods
    void update_duration(std::uint64_t dur) {
        count++;
        total_duration += dur;

        if (dur < min_duration) min_duration = dur;
        if (dur > max_duration) max_duration = dur;

        // Welford's online algorithm for mean and variance
        double delta = static_cast<double>(dur) - mean_duration;
        mean_duration += delta / count;
        double delta2 = static_cast<double>(dur) - mean_duration;
        m2_duration += delta * delta2;
    }

    void update_size(std::uint64_t size) {
        total_size += size;

        if (size < min_size) min_size = size;
        if (size > max_size) max_size = size;

        double delta = static_cast<double>(size) - mean_size;
        mean_size += delta / count;
        double delta2 = static_cast<double>(size) - mean_size;
        m2_size += delta * delta2;
    }

    void update_timestamp(std::uint64_t ts) {
        if (ts < first_ts) first_ts = ts;
        if (ts > last_ts) last_ts = ts;
    }

    void add_contributing_source(int rank, const std::string& trace_file) {
        contributing_ranks.insert(rank);
        contributing_traces.insert(trace_file);
    }

    void update_custom_metric(const std::string& name, std::uint64_t value) {
        custom_sum[name] += value;

        if (custom_min.find(name) == custom_min.end()) {
            custom_min[name] = std::numeric_limits<std::uint64_t>::max();
        }
        if (value < custom_min[name]) custom_min[name] = value;
        if (value > custom_max[name]) custom_max[name] = value;

        double delta = static_cast<double>(value) - custom_mean[name];
        custom_mean[name] += delta / count;
        double delta2 = static_cast<double>(value) - custom_mean[name];
        custom_m2[name] += delta * delta2;
    }

    // Computed statistics
    double get_stddev_duration() const {
        if (count < 2) return 0.0;
        return std::sqrt(m2_duration / (count - 1));
    }

    double get_stddev_size() const {
        if (count < 2) return 0.0;
        return std::sqrt(m2_size / (count - 1));
    }

    double get_custom_stddev(const std::string& name) const {
        if (count < 2) return 0.0;
        auto it = custom_m2.find(name);
        if (it == custom_m2.end()) return 0.0;
        return std::sqrt(it->second / (count - 1));
    }

    // Merge from another metrics object (for combining thread-local results)
    void merge_from(const AggregationMetrics& other) {
        std::uint64_t n1 = count;
        std::uint64_t n2 = other.count;
        std::uint64_t n = n1 + n2;

        // Merge counts and sums
        count = n;
        total_duration += other.total_duration;
        total_size += other.total_size;

        // Merge min/max
        min_duration = std::min(min_duration, other.min_duration);
        max_duration = std::max(max_duration, other.max_duration);
        min_size = std::min(min_size, other.min_size);
        max_size = std::max(max_size, other.max_size);

        // Merge timestamps
        first_ts = std::min(first_ts, other.first_ts);
        last_ts = std::max(last_ts, other.last_ts);

        // Merge contributing sources
        contributing_ranks.insert(other.contributing_ranks.begin(),
                                  other.contributing_ranks.end());
        contributing_traces.insert(other.contributing_traces.begin(),
                                   other.contributing_traces.end());

        // Merge mean and variance (parallel variance formula)
        if (n > 0) {
            double delta_duration = other.mean_duration - mean_duration;
            mean_duration = (n1 * mean_duration + n2 * other.mean_duration) / n;
            m2_duration = m2_duration + other.m2_duration +
                          delta_duration * delta_duration * n1 * n2 / n;

            double delta_size = other.mean_size - mean_size;
            mean_size = (n1 * mean_size + n2 * other.mean_size) / n;
            m2_size =
                m2_size + other.m2_size + delta_size * delta_size * n1 * n2 / n;
        }

        // Merge custom metrics
        for (const auto& [name, value] : other.custom_sum) {
            custom_sum[name] += value;

            if (custom_min.find(name) == custom_min.end()) {
                custom_min[name] = std::numeric_limits<std::uint64_t>::max();
            }
            custom_min[name] =
                std::min(custom_min[name], other.custom_min.at(name));
            custom_max[name] =
                std::max(custom_max[name], other.custom_max.at(name));

            double delta = other.custom_mean.at(name) - custom_mean[name];
            custom_mean[name] =
                (n1 * custom_mean[name] + n2 * other.custom_mean.at(name)) / n;
            custom_m2[name] = custom_m2[name] + other.custom_m2.at(name) +
                              delta * delta * n1 * n2 / n;
        }
    }
};
