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
 * Returns the same aggregations but with globally-resolved associations.
 */
struct AssociationResolverOutput {
    EventAggregatorUtilityOutput aggregations;
    std::unordered_set<std::uint64_t> root_pids;
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

            // Resolve boundary associations (epoch, step, etc.)
            if (!input.config.boundary_events.empty() &&
                global_tracker.has_boundary_events()) {
                // Use the first timestamp in the metrics as representative
                // (all events with this key should be in the same boundary)
                std::uint64_t representative_ts = metrics.first_ts;

                auto associations = global_tracker.get_boundary_associations(
                    key.pid, representative_ts);
                if (!associations.empty()) {
                    metrics.boundary_associations = associations;
                    updated = true;
                }
            }

            // Resolve parent PID
            if (input.config.track_process_parents &&
                global_tracker.has_process_tree()) {
                std::uint64_t parent = global_tracker.get_parent_pid(key.pid);
                if (parent != 0) {
                    metrics.parent_pid = parent;
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

        output.root_pids = root_pids;
        output.success = true;
        return output;
    }
};
