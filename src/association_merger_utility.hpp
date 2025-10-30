#pragma once

#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/utilities/utility.h>

#include "association_tracker.hpp"
#include "chunk_association_extractor_utility.hpp"

using namespace dftracer::utils;

/**
 * @brief Input for merging association trackers from parallel chunks.
 */
struct AssociationMergerUtilityInput {
    std::vector<ChunkAssociationOutput> chunk_outputs;
};

/**
 * @brief Output from merging association trackers.
 */
struct AssociationMergerUtilityOutput {
    AssociationTracker merged_tracker;
    std::size_t total_events_processed = 0;
    bool success = true;
};

/**
 * @brief Utility for merging parallel chunk association extraction results.
 *
 * Takes multiple AssociationTracker objects from parallel chunk processing
 * and merges them into a single unified tracker containing all process
 * relationships and boundary event intervals.
 */
class AssociationMergerUtility
    : public utilities::Utility<AssociationMergerUtilityInput,
                                AssociationMergerUtilityOutput> {
   public:
    AssociationMergerUtilityOutput process(
        const AssociationMergerUtilityInput& input) override {
        DFTRACER_UTILS_LOG_INFO("Merging %zu association trackers...",
                                input.chunk_outputs.size());

        AssociationMergerUtilityOutput output;

        for (const auto& chunk_output : input.chunk_outputs) {
            if (chunk_output.success) {
                output.merged_tracker.merge(chunk_output.tracker);
                output.total_events_processed += chunk_output.events_processed;
            }
        }

        DFTRACER_UTILS_LOG_INFO(
            "Association merge complete: %zu events processed. "
            "Process tree: %s, Boundary events: %s",
            output.total_events_processed,
            output.merged_tracker.has_process_tree() ? "yes" : "no",
            output.merged_tracker.has_boundary_events() ? "yes" : "no");

        output.success = true;
        return output;
    }
};
