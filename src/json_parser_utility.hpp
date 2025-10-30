#pragma once

#include <dftracer/utils/core/utilities/utility.h>
#include <dftracer/utils/utilities/filesystem/directory_scanner.h>
#include <dftracer/utils/utilities/io/file_reader.h>
#include <yyjson.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

using namespace dftracer::utils;

/**
 * @brief Lightweight zero-cost wrapper around yyjson_val* with convenient
 * accessors.
 *
 * Provides pure lazy evaluation:
 * - Fluent chaining: json["args"]["hhash"]
 * - Template get<T>() with auto-casting
 * - Default values for missing/null fields
 * - Zero overhead - just pointer navigation
 *
 * IMPORTANT: JsonValue is only valid while the yyjson_doc is alive.
 * Don't store JsonValue after calling yyjson_doc_free().
 *
 * Example usage:
 * @code
 *   JsonValue json(event);
 *   std::string cat = json["cat"].get<std::string>();
 *   uint64_t pid = json["pid"].get<uint64_t>();
 *   std::string hhash = json["args"]["hhash"].get<std::string>();
 * @endcode
 */
class JsonValue {
   private:
    yyjson_val* val_;

   public:
    // ========================================================================
    // Constructors
    // ========================================================================

    explicit JsonValue(yyjson_val* val = nullptr) : val_(val) {}

    // ========================================================================
    // Type checks
    // ========================================================================

    bool is_null() const { return !val_ || yyjson_is_null(val_); }
    bool is_bool() const { return val_ && yyjson_is_bool(val_); }
    bool is_string() const { return val_ && yyjson_is_str(val_); }
    bool is_uint() const { return val_ && yyjson_is_uint(val_); }
    bool is_int() const { return val_ && yyjson_is_int(val_); }
    bool is_number() const { return val_ && yyjson_is_num(val_); }
    bool is_object() const { return val_ && yyjson_is_obj(val_); }
    bool is_array() const { return val_ && yyjson_is_arr(val_); }
    bool exists() const { return val_ != nullptr; }

    // ========================================================================
    // Navigation - Pure Lazy (Zero Cost)
    // ========================================================================

    /**
     * @brief Navigate to a child field.
     * @param key Field name
     * @return JsonValue wrapping the child, or null JsonValue if not found
     *
     * Cost: O(1) - just pointer arithmetic, no parsing
     */
    JsonValue operator[](const char* key) const {
        return JsonValue(val_ ? yyjson_obj_get(val_, key) : nullptr);
    }

    JsonValue operator[](const std::string& key) const {
        return (*this)[key.c_str()];
    }

    JsonValue operator[](std::string_view key) const {
        // yyjson requires null-terminated string
        std::string key_str(key);
        return (*this)[key_str.c_str()];
    }

    // ========================================================================
    // Path-based navigation (for nested keys like "args.ret")
    // ========================================================================

    /**
     * @brief Navigate to a nested field using dot notation.
     * @param path Dot-separated path like "args.ret" or "data.stats.count"
     * @return JsonValue at the path, or null JsonValue if not found
     *
     * Example:
     * @code
     *   uint64_t ret = json.at("args.ret").get<uint64_t>();
     *   // Equivalent to: json["args"]["ret"].get<uint64_t>()
     * @endcode
     */
    JsonValue at(const char* path) const {
        if (!val_ || !path) return JsonValue(nullptr);

        JsonValue current(val_);
        const char* start = path;

        while (*start) {
            // Find next dot or end of string
            const char* end = start;
            while (*end && *end != '.') end++;

            // Extract key (need null-terminated string for yyjson)
            size_t key_len = end - start;
            if (key_len == 0) {
                start = (*end == '.') ? end + 1 : end;
                continue;
            }

            char key_buf[256];
            if (key_len >= sizeof(key_buf)) {
                // Key too long - fallback to heap allocation
                std::string key_str(start, key_len);
                current = current[key_str.c_str()];
            } else {
                // Fast path - stack buffer
                std::memcpy(key_buf, start, key_len);
                key_buf[key_len] = '\0';
                current = current[key_buf];
            }

            if (!current.exists()) {
                return JsonValue(nullptr);
            }

            start = (*end == '.') ? end + 1 : end;
        }

        return current;
    }

    JsonValue at(const std::string& path) const { return at(path.c_str()); }

    JsonValue at(std::string_view path) const {
        std::string path_str(path);
        return at(path_str.c_str());
    }

    // ========================================================================
    // Template get<T>() - Type-safe extraction with auto-casting
    // ========================================================================

    /**
     * @brief Extract value as type T with optional default.
     * @tparam T Target type (string, uint64_t, int64_t, double, bool, etc.)
     * @param default_val Value to return if field is missing or wrong type
     * @return Extracted value or default
     *
     * Supported types:
     * - std::string (copy)
     * - std::string_view (zero-copy view into yyjson buffer)
     * - const char* (zero-copy pointer to yyjson buffer)
     * - std::uint64_t, std::int64_t
     * - std::uint32_t, std::int32_t, etc. (any integral type)
     * - double, float
     * - bool
     */
    template <typename T>
    T get(const T& default_val = T{}) const {
        if constexpr (std::is_same_v<T, bool>) {
            return val_ && yyjson_is_bool(val_) ? yyjson_get_bool(val_)
                                                : default_val;
        } else if constexpr (std::is_same_v<T, std::string>) {
            return (val_ && yyjson_is_str(val_))
                       ? std::string(yyjson_get_str(val_))
                       : default_val;
        } else if constexpr (std::is_same_v<T, std::string_view>) {
            // Zero-copy - returns view into internal buffer
            if (val_ && yyjson_is_str(val_)) {
                const char* str = yyjson_get_str(val_);
                std::size_t len = yyjson_get_len(val_);
                return std::string_view(str, len);
            }
            return default_val;
        } else if constexpr (std::is_same_v<T, const char*>) {
            // Zero-copy - returns pointer to internal buffer
            return (val_ && yyjson_is_str(val_)) ? yyjson_get_str(val_)
                                                 : default_val;
        } else if constexpr (std::is_same_v<T, std::uint64_t>) {
            if (!val_) return default_val;
            if (yyjson_is_uint(val_)) return yyjson_get_uint(val_);
            if (yyjson_is_int(val_)) {
                auto v = yyjson_get_int(val_);
                return v >= 0 ? static_cast<std::uint64_t>(v) : default_val;
            }
            return default_val;
        } else if constexpr (std::is_same_v<T, std::int64_t>) {
            if (!val_) return default_val;
            if (yyjson_is_int(val_)) return yyjson_get_int(val_);
            if (yyjson_is_uint(val_)) {
                auto v = yyjson_get_uint(val_);
                return v <= static_cast<uint64_t>(
                                std::numeric_limits<int64_t>::max())
                           ? static_cast<std::int64_t>(v)
                           : default_val;
            }
            return default_val;
        } else if constexpr (std::is_same_v<T, double>) {
            if (!val_) return default_val;
            if (yyjson_is_real(val_)) return yyjson_get_real(val_);
            if (yyjson_is_int(val_))
                return static_cast<double>(yyjson_get_int(val_));
            if (yyjson_is_uint(val_))
                return static_cast<double>(yyjson_get_uint(val_));
            return default_val;
        } else if constexpr (std::is_same_v<T, float>) {
            return static_cast<float>(
                get<double>(static_cast<double>(default_val)));
        } else if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T>) {
            // Generic unsigned integers (uint32_t, uint16_t, etc.)
            return static_cast<T>(
                get<std::uint64_t>(static_cast<std::uint64_t>(default_val)));
        } else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
            // Generic signed integers (int32_t, int16_t, etc.)
            return static_cast<T>(
                get<std::int64_t>(static_cast<std::int64_t>(default_val)));
        } else {
            static_assert(!sizeof(T),
                          "Unsupported type for JsonValue::get<T>(). "
                          "Supported: string, integral types, double, bool");
        }
    }

    // ========================================================================
    // Optional-based get (returns std::nullopt if missing/wrong type)
    // ========================================================================

    /**
     * @brief Extract value as std::optional<T>.
     * @tparam T Target type
     * @return std::optional with value, or std::nullopt if missing/wrong type
     *
     * Use this when you need to distinguish between:
     * - Field missing/null
     * - Field has wrong type
     * - Field has value of 0 or ""
     */
    template <typename T>
    std::optional<T> get_optional() const {
        if (!val_) return std::nullopt;

        if constexpr (std::is_same_v<T, std::string>) {
            return yyjson_is_str(val_)
                       ? std::optional(std::string(yyjson_get_str(val_)))
                       : std::nullopt;
        } else if constexpr (std::is_same_v<T, std::string_view>) {
            if (yyjson_is_str(val_)) {
                const char* str = yyjson_get_str(val_);
                std::size_t len = yyjson_get_len(val_);
                return std::optional(std::string_view(str, len));
            }
            return std::nullopt;
        } else if constexpr (std::is_same_v<T, const char*>) {
            return yyjson_is_str(val_) ? std::optional(yyjson_get_str(val_))
                                       : std::nullopt;
        } else if constexpr (std::is_same_v<T, std::uint64_t>) {
            if (yyjson_is_uint(val_)) return yyjson_get_uint(val_);
            if (yyjson_is_int(val_)) {
                auto v = yyjson_get_int(val_);
                return v >= 0 ? std::optional(static_cast<std::uint64_t>(v))
                              : std::nullopt;
            }
            return std::nullopt;
        } else if constexpr (std::is_same_v<T, std::int64_t>) {
            if (yyjson_is_int(val_)) return yyjson_get_int(val_);
            return std::nullopt;
        } else if constexpr (std::is_same_v<T, double>) {
            if (yyjson_is_real(val_)) return yyjson_get_real(val_);
            if (yyjson_is_int(val_))
                return static_cast<double>(yyjson_get_int(val_));
            if (yyjson_is_uint(val_))
                return static_cast<double>(yyjson_get_uint(val_));
            return std::nullopt;
        } else if constexpr (std::is_same_v<T, bool>) {
            return yyjson_is_bool(val_) ? std::optional(yyjson_get_bool(val_))
                                        : std::nullopt;
        } else {
            static_assert(!sizeof(T),
                          "Unsupported type for JsonValue::get_optional<T>()");
        }
    }

    // ========================================================================
    // Advanced: Raw pointer access
    // ========================================================================

    /**
     * @brief Get raw yyjson_val pointer.
     * @return Pointer to underlying yyjson value, or nullptr if not exists
     *
     * Use this for advanced operations or passing to yyjson API directly.
     */
    yyjson_val* raw() const { return val_; }

    /**
     * @brief Implicit conversion to bool (for existence checks).
     * @return true if value exists, false otherwise
     *
     * Usage: if (json["field"]) { ... }
     */
    explicit operator bool() const { return exists(); }
};

// ============================================================================
// JsonParserUtility - Lightweight wrapper for JsonValue
// ============================================================================

/**
 * @brief Input for JsonParserUtility - just a yyjson_val pointer.
 */
using JsonParserInput = yyjson_val*;

/**
 * @brief Output from JsonParserUtility - wraps result as JsonValue.
 */
using JsonParserOutput = JsonValue;

/**
 * @brief Utility wrapper for JsonValue (for pipeline integration).
 *
 * This is a thin wrapper around JsonValue to make it compatible with
 * the dftracer-utils Utility pattern. Most users should just use
 * JsonValue directly for better performance.
 *
 * Use this when:
 * - You want to integrate JSON parsing into a pipeline
 * - You need the Utility interface for composability
 * - You already have a yyjson_val* from external parsing
 *
 * For direct usage in hot paths, prefer JsonValue directly.
 */
class JsonParserUtility
    : public utilities::Utility<JsonParserInput, JsonParserOutput> {
   public:
    JsonParserOutput process(const JsonParserInput& input) override {
        return JsonValue(input);
    }
};

// ============================================================================
// StringJsonParserUtility - JsonParserUtility with owned yyjson_doc
// ============================================================================

/**
 * @brief Input for StringJsonParserUtility.
 *
 * Keeps the text content alive (which the yyjson_doc will point into).
 */
struct StringJsonParserInput {
    utilities::text::Text content;  // Keep the string buffer alive

    // Factory: From file path (uses FileReaderUtility, keeps content alive)
    static StringJsonParserInput from_file(const std::string& file_path) {
        StringJsonParserInput input;

        utilities::io::FileReaderUtility file_reader;
        utilities::filesystem::FileEntry file_entry{file_path};
        input.content = file_reader.process(file_entry);

        return input;
    }

    // Factory: From string buffer (copies into Text)
    static StringJsonParserInput from_string(const std::string& json_str) {
        StringJsonParserInput input;
        input.content.content = json_str;
        return input;
    }
};

/**
 * @brief Utility that parses JSON from text content and keeps it alive.
 *
 * This ensures the text content stays alive as long as the utility exists,
 * allowing safe access to string_view and other zero-copy operations.
 *
 * Use this when:
 * - You need to parse from a file and keep the document alive
 * - You want to keep string_view references valid
 * - You're parsing from a string buffer that might be freed
 */
class StringJsonParserUtility
    : public utilities::Utility<StringJsonParserInput, JsonParserOutput> {
   private:
    utilities::text::Text content_;          // Keep the string buffer alive
    std::shared_ptr<yyjson_doc> owned_doc_;  // RAII for yyjson_doc

   public:
    JsonParserOutput process(const StringJsonParserInput& input) override {
        // Store the content to keep it alive
        content_ = input.content;

        // Parse JSON (points into content_.content buffer)
        yyjson_doc* doc =
            yyjson_read(content_.content.data(), content_.content.size(), 0);

        yyjson_val* json_object = nullptr;
        if (doc) {
            json_object = yyjson_doc_get_root(doc);
            // Wrap in shared_ptr with custom deleter for RAII
            owned_doc_ = std::shared_ptr<yyjson_doc>(doc, [](yyjson_doc* d) {
                if (d) yyjson_doc_free(d);
            });
        }

        // Return JsonValue
        return JsonValue(json_object);
    }

    // Keep the document alive
    void reset() {
        owned_doc_.reset();
        content_ = utilities::text::Text{};
    }
};
