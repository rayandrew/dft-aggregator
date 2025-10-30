#pragma once

#include <dftracer/utils/core/utilities/utility.h>
#include <dftracer/utils/utilities/composites/dft/metadata_collector.h>

#include <cstdint>
#include <string>
#include <vector>

#include "aggregation_config.hpp"

using namespace dftracer::utils;

// Input: metadata collection results + config
struct ChunkMapperInput {
    std::vector<utilities::composites::dft::MetadataCollectorUtilityOutput>
        metadata;
    AggregationConfig config;
    std::size_t checkpoint_size;
    std::size_t target_chunk_size_mb;
    std::size_t batch_size = 4 * 1024 * 1024;

    static ChunkMapperInput from_metadata(
        const std::vector<
            utilities::composites::dft::MetadataCollectorUtilityOutput>& meta) {
        ChunkMapperInput input;
        input.metadata = meta;
        input.target_chunk_size_mb = 4;  // Default: 4MB chunks
        return input;
    }

    ChunkMapperInput& with_config(const AggregationConfig& cfg) {
        config = cfg;
        return *this;
    }

    ChunkMapperInput& with_checkpoint_size(std::size_t size) {
        checkpoint_size = size;
        return *this;
    }

    ChunkMapperInput& with_target_chunk_size(std::size_t size_mb) {
        target_chunk_size_mb = size_mb;
        return *this;
    }

    ChunkMapperInput& with_batch_size(std::size_t size_bytes) {
        batch_size = size_bytes;
        return *this;
    }
};

// Output: vector of chunk inputs ready for parallel processing
using ChunkMapperOutput = std::vector<ChunkAggregatorInput>;

// Utility that splits files into chunks based on byte ranges from metadata
class ChunkMapperUtility
    : public utilities::Utility<ChunkMapperInput, ChunkMapperOutput> {
   private:
    // Helper: Extract rank from filename
    int extract_rank_from_filename(const std::string& filename) const {
        std::size_t rank_pos = filename.find("rank_");
        if (rank_pos != std::string::npos) {
            std::size_t num_start = rank_pos + 5;
            std::size_t num_end =
                filename.find_first_not_of("0123456789", num_start);
            if (num_end != std::string::npos) {
                std::string rank_str =
                    filename.substr(num_start, num_end - num_start);
                return std::stoi(rank_str);
            }
        }
        return -1;
    }

   public:
    ChunkMapperOutput process(const ChunkMapperInput& input) override {
        ChunkMapperOutput chunks;
        int global_chunk_index = 0;

        std::size_t target_chunk_bytes =
            input.target_chunk_size_mb * 1024 * 1024;

        DFTRACER_UTILS_LOG_INFO(
            "Creating chunk mappings with target size: %zu MB (%zu bytes)",
            input.target_chunk_size_mb, target_chunk_bytes);

        for (const auto& meta : input.metadata) {
            if (!meta.success) {
                DFTRACER_UTILS_LOG_WARN("Skipping unsuccessful file: %s",
                                        meta.file_path.c_str());
                continue;
            }

            int rank = extract_rank_from_filename(meta.file_path);
            std::size_t file_size = meta.uncompressed_size;
            std::size_t num_lines = meta.valid_events;

            // Split file into chunks based on byte ranges
            std::size_t num_chunks =
                (file_size + target_chunk_bytes - 1) / target_chunk_bytes;

            // Ensure at least 1 chunk per file
            if (num_chunks == 0) num_chunks = 1;

            DFTRACER_UTILS_LOG_DEBUG(
                "Splitting file %s (%zu bytes, %zu lines) into %zu chunks",
                meta.file_path.c_str(), file_size, num_lines, num_chunks);

            for (std::size_t i = 0; i < num_chunks; i++) {
                std::size_t start_byte = i * target_chunk_bytes;
                // Ranges are [0, 4MB], [4MB, 8MB], etc. (inclusive at both
                // ends) LINE_BYTES handles finding line boundaries and
                // preventing overlaps
                std::size_t end_byte =
                    std::min((i + 1) * target_chunk_bytes, file_size);

                // Estimate line ranges
                std::size_t start_line = (num_lines * start_byte) / file_size;
                std::size_t end_line = (num_lines * end_byte) / file_size;

                // Ensure at least start_line is valid (1-based)
                if (start_line == 0) start_line = 1;
                if (end_line == 0) end_line = num_lines;

                auto chunk = ChunkAggregatorInput::from_metadata(
                    meta.file_path, meta.idx_path, start_byte, end_byte,
                    start_line, end_line, global_chunk_index, rank);

                chunk.with_config(input.config)
                    .with_checkpoint_size(input.checkpoint_size)
                    .with_batch_size(input.batch_size);

                chunks.push_back(chunk);
                global_chunk_index++;
            }
        }

        DFTRACER_UTILS_LOG_INFO(
            "Created %zu chunks from %zu files (avg %.1f chunks/file)",
            chunks.size(), input.metadata.size(),
            chunks.size() / static_cast<double>(input.metadata.size()));

        return chunks;
    }
};
