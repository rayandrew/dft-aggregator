#pragma once

#include <dftracer/utils/core/utilities/utility.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utilities/composites/composite_types.h>
#include <dftracer/utils/utilities/composites/indexed_file_reader.h>
#include <yyjson.h>

#include "association_tracker.hpp"
#include "chunk_aggregator_utility.hpp"

using namespace dftracer::utils;

// Utility that extracts associations from a single chunk (byte range)
class ChunkAssociationExtractorUtility
    : public utilities::Utility<ChunkAggregatorInput, ChunkAssociationOutput,
                                utilities::tags::Parallelizable> {
   public:
    ChunkAssociationOutput process(const ChunkAggregatorInput& input) override {
        ChunkAssociationOutput output;
        output.chunk_index = input.chunk_index;
        output.events_processed = 0;
        output.success = false;

        // Skip if no association tracking configured
        if (!input.config.track_process_parents &&
            input.config.boundary_events.empty()) {
            output.success = true;
            return output;
        }

        DFTRACER_UTILS_LOG_DEBUG(
            "Chunk %d: Extracting associations from %s [bytes %zu-%zu]",
            input.chunk_index, input.file_path.c_str(), input.start_byte,
            input.end_byte);

        // Create indexed file reader
        auto reader_input =
            utilities::composites::IndexedReadInput::from_file(input.file_path)
                .with_checkpoint_size(input.checkpoint_size)
                .with_index(input.idx_path);

        utilities::composites::IndexedFileReaderUtility reader_utility;
        auto reader = reader_utility.process(reader_input);

        if (!reader) {
            DFTRACER_UTILS_LOG_ERROR("Chunk %d: Failed to create reader for %s",
                                     input.chunk_index,
                                     input.file_path.c_str());
            return output;
        }

        // Create stream using LINE_BYTES for byte-range access
        auto stream =
            reader->stream(StreamType::LINE_BYTES, RangeType::BYTE_RANGE,
                           input.start_byte, input.end_byte);

        if (!stream) {
            DFTRACER_UTILS_LOG_ERROR("Chunk %d: Failed to create stream for %s",
                                     input.chunk_index,
                                     input.file_path.c_str());
            return output;
        }

        // Process each line (event) for association extraction
        constexpr std::size_t BUFFER_SIZE = 65536;  // 64KB buffer
        char buffer[BUFFER_SIZE];

        while (!stream->done()) {
            std::size_t bytes_read = stream->read(buffer, BUFFER_SIZE);
            if (bytes_read == 0) break;

            // Null-terminate for safety
            if (bytes_read < BUFFER_SIZE) {
                buffer[bytes_read] = '\0';
            }

            // Parse JSON line
            yyjson_doc* doc =
                yyjson_read(buffer, bytes_read, YYJSON_READ_NOFLAG);
            if (doc) {
                yyjson_val* event = yyjson_doc_get_root(doc);
                if (event && yyjson_is_obj(event)) {
                    output.tracker.extract_from_event(event, input.config);
                    output.events_processed++;
                }
                yyjson_doc_free(doc);
            }
        }

        // Finalize tracker (sort intervals)
        output.tracker.finalize();
        output.success = true;

        DFTRACER_UTILS_LOG_DEBUG(
            "Chunk %d: Extracted associations from %zu events. "
            "Process tree: %s, Boundary events: %s",
            input.chunk_index, output.events_processed,
            output.tracker.has_process_tree() ? "yes" : "no",
            output.tracker.has_boundary_events() ? "yes" : "no");

        return output;
    }
};
