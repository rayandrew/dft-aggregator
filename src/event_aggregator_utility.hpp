#pragma once

#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/utilities/utility.h>

#include <unordered_set>

#include "aggregation_output.hpp"

using namespace dftracer::utils;

/**
 * @brief Input for merging chunk aggregation outputs.
 */
struct EventAggregatorUtilityInput {
    std::vector<ChunkAggregationOutput> chunk_outputs;
};

/**
 * @brief Utility for merging parallel chunk aggregation results.
 *
 * This utility takes multiple ChunkAggregationOutput results from parallel
 * chunk processing and merges them into a single EventAggregatorUtilityOutput
 * with combined metrics.
 */
class EventAggregatorUtility
    : public utilities::Utility<EventAggregatorUtilityInput,
                                EventAggregatorUtilityOutput> {
   public:
    EventAggregatorUtilityOutput process(
        const EventAggregatorUtilityInput& input) override {
        DFTRACER_UTILS_LOG_INFO("Merging %zu chunk aggregations...",
                                input.chunk_outputs.size());

        EventAggregatorUtilityOutput merged_output;

        // Track unique files
        std::unordered_set<std::string> unique_files;

        for (const auto& output : input.chunk_outputs) {
            if (!output.success) continue;

            merged_output.total_events_processed += output.events_processed;
            merged_output.total_bytes_processed += output.bytes_processed;
            unique_files.insert(output.file_path);

            // Merge aggregation maps
            for (const auto& [key, metrics] : output.aggregations) {
                auto& merged_metrics = merged_output.aggregations[key];

                // Merge counts and sums
                merged_metrics.count += metrics.count;
                merged_metrics.total_duration += metrics.total_duration;
                merged_metrics.total_size += metrics.total_size;

                // Update min/max
                merged_metrics.min_duration =
                    std::min(merged_metrics.min_duration, metrics.min_duration);
                merged_metrics.max_duration =
                    std::max(merged_metrics.max_duration, metrics.max_duration);
                merged_metrics.min_size =
                    std::min(merged_metrics.min_size, metrics.min_size);
                merged_metrics.max_size =
                    std::max(merged_metrics.max_size, metrics.max_size);

                // For mean, recompute after merge using total/count
                std::uint64_t new_count = merged_metrics.count;
                if (new_count > 0) {
                    merged_metrics.mean_duration =
                        merged_metrics.total_duration /
                        static_cast<double>(new_count);
                    merged_metrics.mean_size = merged_metrics.total_size /
                                               static_cast<double>(new_count);
                }

                // Merge custom metrics
                for (const auto& [field, value] : metrics.custom_sum) {
                    merged_metrics.custom_sum[field] += value;
                }

                // Merge association data (use first non-empty)
                if (merged_metrics.boundary_associations.empty() &&
                    !metrics.boundary_associations.empty()) {
                    merged_metrics.boundary_associations =
                        metrics.boundary_associations;
                }
                if (merged_metrics.parent_pid == 0 && metrics.parent_pid != 0) {
                    merged_metrics.parent_pid = metrics.parent_pid;
                }
            }
        }

        // Set unique file count
        merged_output.total_files_processed = unique_files.size();

        DFTRACER_UTILS_LOG_INFO(
            "Aggregation merge complete: %zu unique keys, %zu total events, "
            "%zu files",
            merged_output.aggregations.size(),
            merged_output.total_events_processed,
            merged_output.total_files_processed);

        merged_output.success = true;
        return merged_output;
    }
};
