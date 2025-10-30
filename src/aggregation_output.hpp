#pragma once

#include <cstddef>
#include <unordered_map>

#include "aggregation_key.hpp"
#include "aggregation_metrics.hpp"

/**
 * @brief Output from processing a single chunk (byte range) of a file.
 */
struct ChunkAggregationOutput {
    std::unordered_map<AggregationKey, AggregationMetrics, AggregationKeyHash>
        aggregations;
    int chunk_index;
    std::size_t events_processed;
    bool success;
};

/**
 * @brief Output structure for event aggregation results.
 *
 * Contains aggregated metrics grouped by composite keys, along with
 * processing statistics.
 */
struct EventAggregatorUtilityOutput {
    std::unordered_map<AggregationKey, AggregationMetrics, AggregationKeyHash>
        aggregations;
    std::size_t total_events_processed = 0;
    std::size_t total_files_processed = 0;
    bool success = true;
};
