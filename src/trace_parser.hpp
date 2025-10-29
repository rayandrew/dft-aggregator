#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "yyjson.h"

class TraceParser {
   public:
    // Helper: safely get string from yyjson value
    static std::string get_string(yyjson_val* val, const char* key,
                                  const std::string& default_val = "") {
        yyjson_val* v = yyjson_obj_get(val, key);
        if (v && yyjson_is_str(v)) {
            return yyjson_get_str(v);
        }
        return default_val;
    }

    // Helper: safely get uint64 from yyjson value
    static std::uint64_t get_uint64(yyjson_val* val, const char* key,
                                    std::uint64_t default_val = 0) {
        yyjson_val* v = yyjson_obj_get(val, key);
        if (v && yyjson_is_uint(v)) {
            return yyjson_get_uint(v);
        } else if (v && yyjson_is_int(v)) {
            return static_cast<std::uint64_t>(yyjson_get_int(v));
        }
        return default_val;
    }

    // Helper: safely get string from args
    static std::string get_arg_string(yyjson_val* event, const char* key,
                                      const std::string& default_val = "") {
        yyjson_val* args = yyjson_obj_get(event, "args");
        if (!args || !yyjson_is_obj(args)) return default_val;

        yyjson_val* v = yyjson_obj_get(args, key);
        if (v && yyjson_is_str(v)) {
            return yyjson_get_str(v);
        }
        return default_val;
    }

    // Helper: safely get uint64 from args
    static std::uint64_t get_arg_uint64(yyjson_val* event, const char* key,
                                        std::uint64_t default_val = 0) {
        yyjson_val* args = yyjson_obj_get(event, "args");
        if (!args || !yyjson_is_obj(args)) return default_val;

        yyjson_val* v = yyjson_obj_get(args, key);
        if (v && yyjson_is_uint(v)) {
            return yyjson_get_uint(v);
        } else if (v && yyjson_is_int(v)) {
            return static_cast<std::uint64_t>(yyjson_get_int(v));
        }
        return default_val;
    }

    // Helper: check if args contains key
    static bool has_arg(yyjson_val* event, const char* key) {
        yyjson_val* args = yyjson_obj_get(event, "args");
        if (!args || !yyjson_is_obj(args)) return false;
        return yyjson_obj_get(args, key) != nullptr;
    }
};
