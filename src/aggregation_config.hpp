#pragma once

#include <cstdint>
#include <string>
#include <vector>

// Configuration for boundary events (e.g., epoch markers)
struct BoundaryEventConfig {
    std::string event_name;  // Event name to look for (e.g., "epoch.block")
    std::string
        value_field;  // Arg field containing the value (e.g., "iter_count")
    std::string output_name;  // Name in output (e.g., "epoch")
};

struct AggregationConfig {
    // Time bucketing
    std::uint64_t time_interval_us = 1000000;  // Default: 1 second
    bool use_relative_time = false;
    std::uint64_t reference_timestamp = 0;

    // Grouping keys from args
    std::vector<std::string>
        extra_group_keys;  // e.g., {"epoch", "step", "level"}

    // Custom metric fields from args
    std::vector<std::string>
        custom_metric_fields;  // e.g., {"iter_count", "num_events"}

    // Filters
    std::vector<std::string> include_categories;
    std::vector<std::string> exclude_categories;
    std::vector<std::string> include_names;
    std::vector<std::string> exclude_names;

    // Options
    bool compute_statistics = true;
    bool include_trace_metadata = true;

    // Association tracking
    std::vector<BoundaryEventConfig>
        boundary_events;                // Epoch/step boundary events
    bool track_process_parents = true;  // Track fork/spawn parent relationships

    // Output
    std::string output_format = "json";  // "json", "csv", "parquet"

    // Check if event passes filters
    bool passes_filters(const std::string& cat, const std::string& name) const {
        // Check include categories
        if (!include_categories.empty()) {
            bool found = false;
            for (const auto& inc_cat : include_categories) {
                if (cat == inc_cat) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }

        // Check exclude categories
        for (const auto& exc_cat : exclude_categories) {
            if (cat == exc_cat) return false;
        }

        // Check include names
        if (!include_names.empty()) {
            bool found = false;
            for (const auto& inc_name : include_names) {
                if (name == inc_name) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }

        // Check exclude names
        for (const auto& exc_name : exclude_names) {
            if (name == exc_name) return false;
        }

        return true;
    }
};
