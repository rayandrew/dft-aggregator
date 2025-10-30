#pragma once

#include <yyjson.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "aggregation_config.hpp"
#include "json_parser_utility.hpp"

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

        // Use JsonParserUtility (stack allocation for consistency)
        JsonParserUtility parser;
        auto json = parser.process(event);

        std::string_view name = json["name"].get<std::string_view>();
        std::uint64_t pid = json["pid"].get<std::uint64_t>();

        // Track fork/spawn events
        if (config.track_process_parents &&
            (name == "fork" || name == "spawn")) {
            std::uint64_t child_pid = json["args"]["ret"].get<std::uint64_t>();
            if (child_pid > 0) {
                process_parents_[child_pid] = pid;
            }
        }

        // Track boundary events
        for (const auto& boundary_config : config.boundary_events) {
            if (name == boundary_config.event_name) {
                std::string field_path = "args." + boundary_config.value_field;
                std::string_view value =
                    json.at(field_path).get<std::string_view>();
                if (!value.empty()) {
                    std::uint64_t ts = json["ts"].get<std::uint64_t>();
                    std::uint64_t dur = json["dur"].get<std::uint64_t>();

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
            // Intervals are sorted by start_ts, so we can use early termination
            for (const auto& interval : it->second) {
                // Early exit if we've passed the timestamp (sorted by start_ts)
                if (ts < interval.start_ts) break;

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

    // Merge another tracker into this one (for parallel processing)
    void merge(const AssociationTracker& other) {
        // Merge process parents (child -> parent mapping)
        for (const auto& [child_pid, parent_pid] : other.process_parents_) {
            process_parents_[child_pid] = parent_pid;
        }

        // Merge process intervals (pid -> intervals)
        for (const auto& [pid, intervals] : other.process_intervals_) {
            auto& my_intervals = process_intervals_[pid];
            my_intervals.insert(my_intervals.end(), intervals.begin(),
                                intervals.end());
        }

        // Merge all_intervals_ (will need re-sorting after merge)
        all_intervals_.insert(all_intervals_.end(),
                              other.all_intervals_.begin(),
                              other.all_intervals_.end());

        // Re-sort all intervals by start time after merge
        if (!all_intervals_.empty()) {
            std::sort(all_intervals_.begin(), all_intervals_.end(),
                      [](const BoundaryInterval& a, const BoundaryInterval& b) {
                          return a.start_ts < b.start_ts;
                      });
        }

        // Also need to re-sort per-process intervals
        for (auto& [pid, intervals] : process_intervals_) {
            std::sort(intervals.begin(), intervals.end(),
                      [](const BoundaryInterval& a, const BoundaryInterval& b) {
                          return a.start_ts < b.start_ts;
                      });
        }
    }
};
