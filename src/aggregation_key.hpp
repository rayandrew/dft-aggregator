#pragma once

#include <cstdint>
#include <map>
#include <string>

struct AggregationKey {
    // Core composite key fields
    std::string cat;       // category
    std::string name;      // event name
    uint64_t pid;          // process ID
    uint64_t tid;          // thread ID
    std::string hhash;     // hostname hash
    std::string fhash;     // file hash (optional)
    uint64_t time_bucket;  // time interval bucket

    // Extra runtime keys (e.g., epoch, step, level)
    std::map<std::string, std::string> extra_keys;

    bool operator==(const AggregationKey& other) const {
        return cat == other.cat && name == other.name && pid == other.pid &&
               tid == other.tid && hhash == other.hhash &&
               fhash == other.fhash && time_bucket == other.time_bucket &&
               extra_keys == other.extra_keys;
    }

    bool operator<(const AggregationKey& other) const {
        if (time_bucket != other.time_bucket)
            return time_bucket < other.time_bucket;
        if (cat != other.cat) return cat < other.cat;
        if (name != other.name) return name < other.name;
        if (pid != other.pid) return pid < other.pid;
        if (tid != other.tid) return tid < other.tid;
        if (hhash != other.hhash) return hhash < other.hhash;
        if (fhash != other.fhash) return fhash < other.fhash;
        return extra_keys < other.extra_keys;
    }
};

struct AggregationKeyHash {
    size_t operator()(const AggregationKey& key) const {
        size_t h = 0;

        // Hash using FNV-1a style algorithm
        auto hash_combine = [](size_t& seed, size_t value) {
            seed ^= value + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        };

        hash_combine(h, std::hash<std::string>{}(key.cat));
        hash_combine(h, std::hash<std::string>{}(key.name));
        hash_combine(h, std::hash<uint64_t>{}(key.pid));
        hash_combine(h, std::hash<uint64_t>{}(key.tid));
        hash_combine(h, std::hash<std::string>{}(key.hhash));
        hash_combine(h, std::hash<std::string>{}(key.fhash));
        hash_combine(h, std::hash<uint64_t>{}(key.time_bucket));

        // Hash extra keys in order
        for (const auto& [k, v] : key.extra_keys) {
            hash_combine(h, std::hash<std::string>{}(k));
            hash_combine(h, std::hash<std::string>{}(v));
        }

        return h;
    }
};
