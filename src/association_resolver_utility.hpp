#pragma once

#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/utilities/utility.h>

#include <memory>
#include <unordered_set>
#include <vector>

#include "aggregation_output.hpp"
#include "association_tracker.hpp"

using namespace dftracer::utils;

/**
 * @brief Input for association resolution.
 *
 * Takes merged aggregations and all local trackers from chunk processing.
 */
struct AssociationResolverInput {
    EventAggregatorUtilityOutput aggregations;
    std::vector<std::shared_ptr<AssociationTracker>> trackers;
    AggregationConfig
        config;  // Need config to know which associations to resolve
};

/**
 * @brief Output from association resolution.
 *
 * Returns the same aggregations but with globally-resolved associations,
 * plus trace-level metadata (durations, boundary durations).
 */
struct AssociationResolverOutput {
    EventAggregatorUtilityOutput aggregations;
    std::unordered_set<std::uint64_t> root_pids;
    std::uint64_t trace_duration = 0;  // Total trace duration (microseconds)
    BoundaryDurationsMap boundary_durations;  // boundary_name -> (value ->
                                              // duration in microseconds)
    bool success = true;
};

/**
 * @brief Utility that resolves associations globally after all chunks are
 * processed.
 *
 * This utility:
 * 1. Merges all local trackers into a global tracker
 * 2. For each metric, resolves boundary_associations and parent_pid using
 * global data
 * 3. Returns updated aggregations with correct cross-chunk associations
 *
 * This enables parent-child relationships and boundary events to work correctly
 * even when the parent and child are processed in different chunks.
 */
class AssociationResolverUtility
    : public utilities::Utility<AssociationResolverInput,
                                AssociationResolverOutput> {
   public:
    AssociationResolverOutput process(
        const AssociationResolverInput& input) override {
        DFTRACER_UTILS_LOG_INFO(
            "Resolving associations globally from %zu trackers...",
            input.trackers.size());

        AssociationResolverOutput output;
        output.aggregations = input.aggregations;

        // Skip if no associations to resolve
        if (input.trackers.empty() || (!input.config.track_process_parents &&
                                       input.config.boundary_events.empty())) {
            DFTRACER_UTILS_LOG_INFO(
                "No associations to resolve (trackers or config disabled)");
            return output;
        }

        // Step 1: Merge all trackers into a global tracker
        AssociationTracker global_tracker;
        for (const auto& tracker : input.trackers) {
            if (tracker) {
                global_tracker.merge(*tracker);
            }
        }

        DFTRACER_UTILS_LOG_INFO(
            "Global tracker merged: %s process relationships, %s boundary "
            "events",
            global_tracker.has_process_tree() ? "has" : "no",
            global_tracker.has_boundary_events() ? "has" : "no");

        // Identify root processes (PIDs with no parent)
        auto root_pids = global_tracker.get_root_pids();
        if (!root_pids.empty()) {
            DFTRACER_UTILS_LOG_INFO("Found %zu root process(es):",
                                    root_pids.size());
            std::size_t count = 0;
            for (std::uint64_t pid : root_pids) {
                if (count < 5) {  // Only log first 5 to avoid spam
                    DFTRACER_UTILS_LOG_INFO("  Root PID: %lu", pid);
                    count++;
                }
            }
            if (root_pids.size() > 5) {
                DFTRACER_UTILS_LOG_INFO("  ... and %zu more",
                                        root_pids.size() - 5);
            }
        }

        // Step 2: Resolve associations for each metric
        std::size_t metrics_updated = 0;

        for (auto& [key, metrics] : output.aggregations.aggregations) {
            bool updated = false;

            // First resolve parent PID
            if (input.config.track_process_parents &&
                global_tracker.has_process_tree()) {
                std::uint64_t parent = global_tracker.get_parent_pid(key.pid);
                if (parent != 0) {
                    metrics.parent_pid = parent;
                    updated = true;
                }
            }

            // Resolve boundary associations (epoch, step, etc.)
            // For worker events (with parent_pid), use parent's epoch
            // boundaries For main process events, use their own epoch
            // boundaries
            if (!input.config.boundary_events.empty() &&
                global_tracker.has_boundary_events()) {
                // Use the midpoint timestamp for more accurate boundary
                // assignment This reduces misclassification when buckets span
                // boundaries
                std::uint64_t representative_ts = (metrics.ts + metrics.te) / 2;

                // Determine which PID's boundaries to use:
                // - For worker events (has parent_pid), use parent's boundaries
                // - For main process events (no parent), use their own
                // boundaries
                std::uint64_t boundary_pid =
                    (metrics.parent_pid > 0) ? metrics.parent_pid : key.pid;

                auto associations = global_tracker.get_boundary_associations(
                    boundary_pid, representative_ts);
                if (!associations.empty()) {
                    metrics.boundary_associations = associations;
                    updated = true;
                }
            }

            if (updated) {
                metrics_updated++;
            }
        }

        DFTRACER_UTILS_LOG_INFO(
            "Association resolution complete: %zu/%zu metrics updated",
            metrics_updated, output.aggregations.aggregations.size());

        // Step 3: Compute trace duration and boundary durations
        compute_trace_metadata(global_tracker, output.aggregations, output);

        output.root_pids = root_pids;
        output.success = true;
        return output;
    }

   private:
    /**
     * @brief Computes trace-level metadata including trace duration and
     * boundary durations.
     *
     * @param tracker Global tracker with all boundary intervals
     * @param aggregations Aggregated metrics (unused currently, for future
     * extensions)
     * @param output Output structure to populate with metadata
     */
    void compute_trace_metadata(
        const AssociationTracker& tracker,
        const EventAggregatorUtilityOutput& aggregations,
        AssociationResolverOutput& output) {
        // Get all boundary intervals
        const auto& intervals = tracker.get_all_intervals();

        if (intervals.empty()) {
            DFTRACER_UTILS_LOG_INFO(
                "No boundary intervals found, skipping metadata computation");
            return;
        }

        // Track ranges for each boundary name+value combination
        // Map: boundary_name -> (boundary_value -> (min_ts, max_ts))
        std::unordered_map<
            std::string,
            std::unordered_map<std::string,
                               std::pair<std::uint64_t, std::uint64_t>>>
            ranges;

        std::uint64_t global_min = UINT64_MAX;
        std::uint64_t global_max = 0;

        for (const auto& interval : intervals) {
            // Update global trace duration
            global_min = std::min(global_min, interval.start_ts);
            global_max = std::max(global_max, interval.end_ts);

            // Update boundary-specific ranges
            auto& value_map = ranges[interval.name];
            auto& range = value_map[interval.value];

            if (range.first == 0 && range.second == 0) {
                // Initialize range
                range.first = interval.start_ts;
                range.second = interval.end_ts;
            } else {
                // Extend range
                range.first = std::min(range.first, interval.start_ts);
                range.second = std::max(range.second, interval.end_ts);
            }
        }

        // Set trace duration
        if (global_max > global_min) {
            output.trace_duration = global_max - global_min;
        }

        // Convert ranges to durations
        std::size_t total_boundaries = 0;
        for (const auto& [name, value_map] : ranges) {
            for (const auto& [value, range] : value_map) {
                output.boundary_durations[name][value] =
                    range.second - range.first;
                total_boundaries++;
            }
        }

        DFTRACER_UTILS_LOG_INFO(
            "Computed trace metadata: trace_duration=%lu us, %zu boundary "
            "types, %zu total boundaries",
            output.trace_duration, output.boundary_durations.size(),
            total_boundaries);
    }
};
