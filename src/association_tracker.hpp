#pragma once

#include <yyjson.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "aggregation_config.hpp"
#include "trace_parser.hpp"

// Represents a time interval for a boundary event (e.g., epoch)
struct BoundaryInterval {
    std::string name;   // Output name (e.g., "epoch")
    std::string value;  // Value (e.g., "1")
    std::uint64_t start_ts;
    std::uint64_t end_ts;
};

// Tracks process tree and boundary event intervals
class AssociationTracker {
   private:
    // Process tree: child_pid -> parent_pid
    std::unordered_map<std::uint64_t, std::uint64_t> process_parents_;

    // Boundary intervals per process: pid -> intervals
    std::unordered_map<std::uint64_t, std::vector<BoundaryInterval>>
        process_intervals_;

    // All boundary intervals sorted by start time (for fast lookup)
    std::vector<BoundaryInterval> all_intervals_;

   public:
    AssociationTracker() = default;

    // Extract process relationships and boundary intervals from a single event
    void extract_from_event(yyjson_val* event,
                            const AggregationConfig& config) {
        if (!event || !yyjson_is_obj(event)) return;

        std::string name = TraceParser::get_string(event, "name");
        std::uint64_t pid = TraceParser::get_uint64(event, "pid");

        // Track fork/spawn events
        if (config.track_process_parents &&
            (name == "fork" || name == "spawn")) {
            std::uint64_t child_pid = TraceParser::get_arg_uint64(event, "ret");
            if (child_pid > 0) {
                process_parents_[child_pid] = pid;
            }
        }

        // Track boundary events
        for (const auto& boundary_config : config.boundary_events) {
            if (name == boundary_config.event_name) {
                std::string value = TraceParser::get_arg_string(
                    event, boundary_config.value_field.c_str());
                if (!value.empty()) {
                    std::uint64_t ts = TraceParser::get_uint64(event, "ts");
                    std::uint64_t dur = TraceParser::get_uint64(event, "dur");

                    BoundaryInterval interval;
                    interval.name = boundary_config.output_name;
                    interval.value = value;
                    interval.start_ts = ts;
                    interval.end_ts = ts + dur;

                    process_intervals_[pid].push_back(interval);
                    all_intervals_.push_back(interval);
                }
            }
        }
    }

    // Finalize after all events have been extracted
    void finalize() {
        // Sort all intervals by start time for efficient lookup
        std::sort(all_intervals_.begin(), all_intervals_.end(),
                  [](const BoundaryInterval& a, const BoundaryInterval& b) {
                      return a.start_ts < b.start_ts;
                  });
    }

    // Get parent PID for a given process
    std::uint64_t get_parent_pid(std::uint64_t pid) const {
        auto it = process_parents_.find(pid);
        return (it != process_parents_.end()) ? it->second : 0;
    }

    // Get boundary associations for a given (pid, timestamp)
    std::unordered_map<std::string, std::string> get_boundary_associations(
        std::uint64_t pid, std::uint64_t ts) const {
        std::unordered_map<std::string, std::string> result;

        // First, check if this process has local boundary events
        auto it = process_intervals_.find(pid);
        if (it != process_intervals_.end()) {
            for (const auto& interval : it->second) {
                if (ts >= interval.start_ts && ts < interval.end_ts) {
                    result[interval.name] = interval.value;
                }
            }
        }

        // If no local boundaries found, try parent process recursively
        if (result.empty()) {
            std::uint64_t parent = get_parent_pid(pid);
            if (parent != 0) {
                return get_boundary_associations(parent, ts);
            }
        }

        return result;
    }

    // Get all intervals (for debugging/analysis)
    const std::vector<BoundaryInterval>& get_all_intervals() const {
        return all_intervals_;
    }

    // Check if we have any boundary events
    bool has_boundary_events() const { return !all_intervals_.empty(); }

    // Check if we have any process relationships
    bool has_process_tree() const { return !process_parents_.empty(); }
};
