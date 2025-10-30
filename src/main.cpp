#include <dftracer/utils/core/common/config.h>
#include <dftracer/utils/core/common/filesystem.h>
#include <dftracer/utils/core/pipeline/pipeline.h>
#include <dftracer/utils/core/pipeline/pipeline_config_manager.h>
#include <dftracer/utils/core/tasks/task.h>
#include <dftracer/utils/core/utilities/utility_adapter.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/utilities/composites/composites.h>

#include <argparse/argparse.hpp>
#include <chrono>
#include <thread>

#include "aggregation_config.hpp"
#include "chunk_aggregator_utility.hpp"
#include "chunk_mapper_utility.hpp"
#include "event_aggregator_utility.hpp"
#include "perfetto_writer.hpp"

using namespace dftracer::utils;

int main(int argc, char** argv) {
    DFTRACER_UTILS_LOGGER_INIT();

    auto default_checkpoint_size_str =
        std::to_string(Indexer::DEFAULT_CHECKPOINT_SIZE) + " B (" +
        std::to_string(Indexer::DEFAULT_CHECKPOINT_SIZE / (1024 * 1024)) +
        " MB)";

    argparse::ArgumentParser program("dftracer_aggregator",
                                     DFTRACER_UTILS_PACKAGE_VERSION);
    program.add_description(
        "Aggregate DFTracer events into time-series counters using explicit "
        "pipeline with maximum parallelism");

    program.add_argument("-d", "--directory")
        .help("Input directory containing .pfw or .pfw.gz files")
        .default_value<std::string>(".");

    program.add_argument("-o", "--output")
        .help("Output file path for aggregated counters")
        .default_value<std::string>("aggregated_output.json");

    program.add_argument("-t", "--time-interval")
        .help(
            "Time interval in microseconds for bucketing (default: 1000000 = "
            "1 second)")
        .scan<'d', std::uint64_t>()
        .default_value(static_cast<std::uint64_t>(1000000));

    program.add_argument("-g", "--group-keys")
        .help(
            "Comma-separated extra group keys from args (e.g., "
            "epoch,step,level)")
        .default_value<std::string>("");

    program.add_argument("-m", "--metric-fields")
        .help(
            "Comma-separated custom metric fields from args (e.g., "
            "iter_count,num_events)")
        .default_value<std::string>("");

    program.add_argument("-c", "--categories")
        .help("Include only these categories (comma-separated, empty = all)")
        .default_value<std::string>("");

    program.add_argument("-n", "--names")
        .help("Include only these event names (comma-separated, empty = all)")
        .default_value<std::string>("");

    program.add_argument("-f", "--force").help("Force index recreation").flag();

    program.add_argument("--checkpoint-size")
        .help("Checkpoint size for indexing in bytes (default: " +
              default_checkpoint_size_str + ")")
        .scan<'d', std::size_t>()
        .default_value(
            static_cast<std::size_t>(Indexer::DEFAULT_CHECKPOINT_SIZE));

    program.add_argument("--executor-threads")
        .help(
            "Number of executor threads for parallel processing (default: "
            "number of CPU cores)")
        .scan<'d', std::size_t>()
        .default_value(
            static_cast<std::size_t>(std::thread::hardware_concurrency()));

    program.add_argument("--scheduler-threads")
        .help("Number of scheduler threads (default: 1)")
        .scan<'d', std::size_t>()
        .default_value(static_cast<std::size_t>(1));

    program.add_argument("--index-dir")
        .help("Directory to store index files (default: system temp directory)")
        .default_value<std::string>("");

    program.add_argument("--compress")
        .help("Compress output using gzip")
        .default_value(false)
        .implicit_value(true);

    program.add_argument("--compression-level")
        .help("Gzip compression level (0-9, default: 6)")
        .scan<'d', int>()
        .default_value(6);

    program.add_argument("--boundary-events")
        .help(
            "Boundary event configuration: event_name:value_field:output_name "
            "(e.g., \"epoch.block:iter_count:epoch\")")
        .default_value<std::string>("");

    program.add_argument("--no-track-process-parents")
        .help(
            "Disable tracking of process parent relationships from fork/spawn")
        .default_value(false)
        .implicit_value(true);

    program.add_argument("--chunk-size")
        .help("Target chunk size in MB for parallel processing (default: 4)")
        .scan<'d', std::size_t>()
        .default_value(static_cast<std::size_t>(4));

    program.add_argument("--read-batch-size")
        .help(
            "Batch read size in MB for stream processing (default: 4, higher = "
            "faster but more memory)")
        .scan<'d', std::size_t>()
        .default_value(static_cast<std::size_t>(4));

    try {
        program.parse_args(argc, argv);
    } catch (const std::exception& err) {
        DFTRACER_UTILS_LOG_ERROR("Error occurred: %s", err.what());
        std::fprintf(stderr, "%s\n", program.help().str().c_str());
        return 1;
    }

    // Parse arguments
    std::string log_dir = program.get<std::string>("--directory");
    std::string output_file = program.get<std::string>("--output");
    std::uint64_t time_interval_us =
        program.get<std::uint64_t>("--time-interval");
    std::string group_keys_str = program.get<std::string>("--group-keys");
    std::string metric_fields_str = program.get<std::string>("--metric-fields");
    std::string categories_str = program.get<std::string>("--categories");
    std::string names_str = program.get<std::string>("--names");
    bool force_rebuild = program.get<bool>("--force");
    std::size_t checkpoint_size = program.get<std::size_t>("--checkpoint-size");
    std::size_t executor_threads =
        program.get<std::size_t>("--executor-threads");
    std::size_t scheduler_threads =
        program.get<std::size_t>("--scheduler-threads");
    std::string index_dir = program.get<std::string>("--index-dir");
    bool compress_output = program.get<bool>("--compress");
    int compression_level = program.get<int>("--compression-level");
    std::string boundary_events_str =
        program.get<std::string>("--boundary-events");
    bool no_track_parents = program.get<bool>("--no-track-process-parents");
    std::size_t chunk_size_mb = program.get<std::size_t>("--chunk-size");
    std::size_t batch_size_mb = program.get<std::size_t>("--read-batch-size");

    // Automatically add .gz extension if compressing and not already present
    if (compress_output) {
        if (output_file.size() < 3 ||
            output_file.substr(output_file.size() - 3) != ".gz") {
            output_file += ".gz";
        }
    }

    // Parse comma-separated lists
    auto split_string = [](const std::string& str) {
        std::vector<std::string> result;
        if (str.empty()) return result;
        std::stringstream ss(str);
        std::string item;
        while (std::getline(ss, item, ',')) {
            if (!item.empty()) {
                result.push_back(item);
            }
        }
        return result;
    };

    std::vector<std::string> group_keys = split_string(group_keys_str);
    std::vector<std::string> metric_fields = split_string(metric_fields_str);
    std::vector<std::string> include_categories = split_string(categories_str);
    std::vector<std::string> include_names = split_string(names_str);

    // Setup temp index directory
    std::string temp_index_dir;
    if (index_dir.empty()) {
        try {
            auto temp_path = fs::temp_directory_path();
            temp_path /= "dftracer_idx_" + std::to_string(std::time(nullptr));
            temp_index_dir = temp_path.string();
            fs::create_directories(temp_index_dir);
            index_dir = temp_index_dir;
            DFTRACER_UTILS_LOG_INFO("Created temporary index directory: %s",
                                    index_dir.c_str());
        } catch (const std::filesystem::filesystem_error& e) {
            // Fallback to /tmp if temp_directory_path() fails
            temp_index_dir =
                "/tmp/dftracer_idx_" + std::to_string(std::time(nullptr));
            fs::create_directories(temp_index_dir);
            index_dir = temp_index_dir;
            DFTRACER_UTILS_LOG_WARN(
                "Failed to get system temp directory, using /tmp: %s",
                e.what());
            DFTRACER_UTILS_LOG_INFO("Created temporary index directory: %s",
                                    index_dir.c_str());
        }
    }

    log_dir = fs::absolute(log_dir).string();
    output_file = fs::absolute(output_file).string();

    // Print configuration
    std::printf("==========================================\n");
    std::printf("DFTracer Aggregator (Pipeline)\n");
    std::printf("==========================================\n");
    std::printf("Arguments:\n");
    std::printf("  Input directory: %s\n", log_dir.c_str());
    std::printf("  Output file: %s\n", output_file.c_str());
    std::printf("  Time interval: %llu us (%.2f seconds)\n", time_interval_us,
                time_interval_us / 1000000.0);
    std::printf("  Force rebuild: %s\n", force_rebuild ? "true" : "false");
    std::printf("  Checkpoint size: %zu bytes (%.2f MB)\n", checkpoint_size,
                checkpoint_size / (1024.0 * 1024.0));
    std::printf("  Executor threads: %zu\n", executor_threads);
    std::printf("  Scheduler threads: %zu\n", scheduler_threads);

    if (!group_keys.empty()) {
        std::printf("  Extra group keys: ");
        for (std::size_t i = 0; i < group_keys.size(); ++i) {
            std::printf("%s%s", group_keys[i].c_str(),
                        i < group_keys.size() - 1 ? ", " : "\n");
        }
    }

    if (!metric_fields.empty()) {
        std::printf("  Custom metric fields: ");
        for (std::size_t i = 0; i < metric_fields.size(); ++i) {
            std::printf("%s%s", metric_fields[i].c_str(),
                        i < metric_fields.size() - 1 ? ", " : "\n");
        }
    }

    std::printf("==========================================\n\n");

    // Setup aggregation configuration
    // Parse boundary events configuration
    std::vector<BoundaryEventConfig> boundary_events;
    if (!boundary_events_str.empty()) {
        std::stringstream ss(boundary_events_str);
        std::string item;
        while (std::getline(ss, item, ',')) {
            // Parse format: event_name:value_field:output_name
            std::stringstream item_ss(item);
            std::string event_name, value_field, output_name;

            if (std::getline(item_ss, event_name, ':') &&
                std::getline(item_ss, value_field, ':') &&
                std::getline(item_ss, output_name, ':')) {
                BoundaryEventConfig config;
                config.event_name = event_name;
                config.value_field = value_field;
                config.output_name = output_name;
                boundary_events.push_back(config);
            }
        }
    }

    AggregationConfig agg_config;
    agg_config.time_interval_us = time_interval_us;
    agg_config.extra_group_keys = group_keys;
    agg_config.custom_metric_fields = metric_fields;
    agg_config.include_categories = include_categories;
    agg_config.include_names = include_names;
    agg_config.compute_statistics = true;
    agg_config.include_trace_metadata = true;
    agg_config.boundary_events = boundary_events;
    agg_config.track_process_parents = !no_track_parents;

    // Create pipeline with configuration
    auto pipeline_config = PipelineConfigManager()
                               .with_name("DFTracer Aggregator")
                               .with_executor_threads(executor_threads)
                               .with_scheduler_threads(scheduler_threads);

    Pipeline pipeline(pipeline_config);

    auto start_time = std::chrono::high_resolution_clock::now();

    // ========================================================================
    // Task 1: Build Indexes
    // ========================================================================
    DFTRACER_UTILS_LOG_INFO("%s", "Task 1: Building indexes...");

    auto index_dir_input =
        utilities::composites::DirectoryProcessInput::from_directory(log_dir)
            .with_extensions({".pfw", ".pfw.gz"});

    using IndexBuildOutput = utilities::composites::BatchFileProcessOutput<
        utilities::composites::dft::IndexBuildUtilityOutput>;

    auto index_builder_processor = [checkpoint_size, force_rebuild, &index_dir](
                                       TaskContext& /*ctx*/,
                                       const std::string& file_path)
        -> utilities::composites::dft::IndexBuildUtilityOutput {
        std::string idx_path = utilities::composites::dft::determine_index_path(
            file_path, index_dir);
        auto input =
            utilities::composites::dft::IndexBuildUtilityInput::from_file(
                file_path)
                .with_checkpoint_size(checkpoint_size)
                .with_force_rebuild(force_rebuild)
                .with_index(idx_path);
        return utilities::composites::dft::IndexBuilderUtility{}.process(input);
    };

    auto index_workflow =
        std::make_shared<utilities::composites::DirectoryFileProcessorUtility<
            utilities::composites::dft::IndexBuildUtilityOutput>>(
            index_builder_processor);

    auto task1_build_indexes = utilities::use(index_workflow).as_task();
    task1_build_indexes->with_name("BuildIndexes");

    // ========================================================================
    // Task 2: Collect Metadata
    // ========================================================================
    DFTRACER_UTILS_LOG_INFO("%s", "Task 2: Collecting metadata...");

    using MetadataCollectOutput = utilities::composites::BatchFileProcessOutput<
        utilities::composites::dft::MetadataCollectorUtilityOutput>;

    auto metadata_processor = [checkpoint_size, force_rebuild, &index_dir](
                                  TaskContext& /*ctx*/,
                                  const std::string& file_path)
        -> utilities::composites::dft::MetadataCollectorUtilityOutput {
        std::string idx_path = utilities::composites::dft::determine_index_path(
            file_path, index_dir);

        auto input = utilities::composites::dft::MetadataCollectorUtilityInput::
                         from_file(file_path)
                             .with_checkpoint_size(checkpoint_size)
                             .with_force_rebuild(force_rebuild)
                             .with_index(idx_path);

        return utilities::composites::dft::MetadataCollectorUtility{}.process(
            input);
    };

    auto metadata_workflow =
        std::make_shared<utilities::composites::DirectoryFileProcessorUtility<
            utilities::composites::dft::MetadataCollectorUtilityOutput>>(
            metadata_processor);

    auto task2_collect_metadata = utilities::use(metadata_workflow).as_task();
    task2_collect_metadata->with_name("CollectMetadata");

    task2_collect_metadata->with_combiner([&log_dir](const IndexBuildOutput&) {
        return utilities::composites::DirectoryProcessInput::from_directory(
                   log_dir)
            .with_extensions({".pfw", ".pfw.gz"});
    });

    // ========================================================================
    // Task 3: Create Chunk Mappings (NEW - Parallel Preprocessing)
    // ========================================================================
    DFTRACER_UTILS_LOG_INFO("%s", "Task 3: Creating chunk mappings...");

    using ChunkMappingInput = MetadataCollectOutput;

    auto create_chunk_mappings_func =
        [agg_config, checkpoint_size, chunk_size_mb, batch_size_mb](
            const ChunkMappingInput& metadata_output) -> ChunkMapperOutput {
        DFTRACER_UTILS_LOG_INFO("Creating chunk mappings from %zu files...",
                                metadata_output.results.size());

        ChunkMapperUtility mapper;
        auto mapper_input =
            ChunkMapperInput::from_metadata(metadata_output.results)
                .with_config(agg_config)
                .with_checkpoint_size(checkpoint_size)
                .with_target_chunk_size(chunk_size_mb)
                .with_batch_size(batch_size_mb * 1024 *
                                 1024);  // Convert MB to bytes

        auto chunks = mapper.process(mapper_input);

        DFTRACER_UTILS_LOG_INFO("Created %zu chunks from %zu files",
                                chunks.size(), metadata_output.results.size());
        return chunks;
    };

    auto task3_create_chunks =
        make_task(create_chunk_mappings_func, "CreateChunkMappings");

    // ========================================================================
    // Task 4: Aggregate Events (Single-pass: Extract associations while
    // aggregating)
    // ========================================================================
    DFTRACER_UTILS_LOG_INFO(
        "%s",
        "Task 4: Configuring parallel event aggregation (single-pass)...");

    // Create the aggregator (always single-pass now)
    auto aggregator_workflow = std::make_shared<ChunkAggregatorUtility>();

    auto aggregator_batch_processor =
        std::make_shared<utilities::composites::BatchProcessorUtility<
            ChunkAggregatorInput, ChunkAggregationOutput>>(aggregator_workflow);

    auto task4_aggregate_chunks =
        utilities::use(aggregator_batch_processor).as_task();
    task4_aggregate_chunks->with_name("AggregateChunks");

    // ========================================================================
    // Task 5: Merge Aggregations (and associations from single-pass)
    // ========================================================================
    DFTRACER_UTILS_LOG_INFO("%s", "Task 5: Configuring aggregation merge...");

    auto event_aggregator = std::make_shared<EventAggregatorUtility>();
    auto merge_aggregations_func =
        [event_aggregator](
            const std::vector<ChunkAggregationOutput>& chunk_outputs)
        -> EventAggregatorUtilityOutput {
        EventAggregatorUtilityInput input;
        input.chunk_outputs = chunk_outputs;
        return event_aggregator->process(input);
    };

    auto task5_merge_aggregations =
        make_task(merge_aggregations_func, "MergeAggregations");

    // ========================================================================
    // Task 6: Write Output
    // ========================================================================
    DFTRACER_UTILS_LOG_INFO("%s", "Task 6: Writing output...");

    auto write_output_func =
        [&output_file, &agg_config, compress_output, compression_level](
            const EventAggregatorUtilityOutput& agg_output) -> bool {
        DFTRACER_UTILS_LOG_INFO("Writing %zu aggregation keys to %s%s...",
                                agg_output.aggregations.size(),
                                output_file.c_str(),
                                compress_output ? " (compressed)" : "");

        if (agg_output.aggregations.empty()) {
            DFTRACER_UTILS_LOG_WARN("No aggregations to write!");
            return false;
        }

        PerfettoCounterWriter writer("aggregator_host");
        bool success = writer.write_aggregated_counters(
            output_file, agg_output.aggregations, agg_config.compute_statistics,
            compress_output, compression_level);

        if (success) {
            DFTRACER_UTILS_LOG_INFO("Output written successfully to: %s",
                                    output_file.c_str());
            // Verify file exists
            if (fs::exists(output_file)) {
                auto file_size = fs::file_size(output_file);
                DFTRACER_UTILS_LOG_INFO("File exists, size: %zu bytes",
                                        file_size);
            } else {
                DFTRACER_UTILS_LOG_ERROR("File does not exist after write!");
                success = false;
            }
        } else {
            DFTRACER_UTILS_LOG_ERROR("Failed to write output file");
        }
        return success;
    };

    auto task6_write_output = make_task(write_output_func, "WriteOutput");

    // ========================================================================
    // Execute Pipeline (SINGLE-PASS MODE)
    // ========================================================================

    // Define dependencies
    task2_collect_metadata->depends_on(task1_build_indexes);
    task3_create_chunks->depends_on(task2_collect_metadata);

    // Task 4 aggregates chunks
    task4_aggregate_chunks->depends_on(task3_create_chunks);

    // Task 5 merges aggregation results from Task 4
    task5_merge_aggregations->depends_on(task4_aggregate_chunks);

    // Task 6 writes output
    task6_write_output->depends_on(task5_merge_aggregations);

    // Set up pipeline
    pipeline.set_source(task1_build_indexes);
    pipeline.set_destination(task6_write_output);

    // Execute pipeline with initial input
    pipeline.execute(index_dir_input);

    // Get final results
    auto agg_results =
        task5_merge_aggregations->get<EventAggregatorUtilityOutput>();
    auto write_success = task6_write_output->get<bool>();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end_time - start_time;

    // ========================================================================
    // Print Results
    // ========================================================================

    std::printf("\n");
    std::printf("==========================================\n");
    std::printf("Aggregation Results\n");
    std::printf("==========================================\n");
    std::printf("  Execution time: %.2f seconds\n", duration.count() / 1000.0);
    std::printf("  Files processed: %zu\n", agg_results.total_files_processed);
    std::printf("  Events processed: %zu\n",
                agg_results.total_events_processed);
    std::printf("  Unique aggregation keys: %zu\n",
                agg_results.aggregations.size());
    std::printf("  Output file: %s\n", output_file.c_str());
    std::printf("  Write status: %s\n", write_success ? "SUCCESS" : "FAILED");
    std::printf("==========================================\n");

    // Print summary
    PerfettoCounterWriter writer2("aggregator_host");
    writer2.write_summary(agg_results.aggregations);

    // Cleanup temporary index directory if created
    if (!temp_index_dir.empty() && fs::exists(temp_index_dir)) {
        DFTRACER_UTILS_LOG_INFO("Cleaning up temporary index directory: %s",
                                temp_index_dir.c_str());
        fs::remove_all(temp_index_dir);
    }

    return agg_results.success && write_success ? 0 : 1;
}
