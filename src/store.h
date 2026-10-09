// The keyspace: string keys → string values, with optional expiry times.
//
// Expiry works two ways, like Redis:
//  - Lazy: every read checks the key's deadline and deletes it if it's past.
//    Cheap, but a key nobody reads again would sit in memory forever.
//  - Active: a timer samples random keys that have deadlines and deletes the
//    expired ones; if many in the sample were expired, it samples again.
//    That bounds memory held by dead keys without scanning everything.
#pragma once

#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace kv {

using Millis = int64_t;  // milliseconds since the Unix epoch

Millis now_ms();

class Store {
public:
    // `clock` is injectable so tests can move time without sleeping.
    explicit Store(Millis (*clock)() = now_ms);

    std::optional<std::string> get(const std::string& key);
    void set(const std::string& key, std::string value, std::optional<Millis> expire_at = std::nullopt);
    bool del(const std::string& key);
    bool exists(const std::string& key);
    size_t size();  // live keys (expired-but-not-yet-collected keys are purged first)
    void clear();

    // Set or clear a deadline. Returns false if the key doesn't exist.
    bool expire_at(const std::string& key, Millis when);
    bool persist(const std::string& key);  // remove the deadline; false if none was set
    // Milliseconds left: -2 if the key doesn't exist, -1 if it has no deadline.
    int64_t ttl_ms(const std::string& key);
    std::optional<Millis> deadline(const std::string& key);  // for the append-only file

    std::vector<std::string> keys(const std::string& pattern);

    // One round of active expiry: sample up to `sample` keys with deadlines and
    // delete expired ones, repeating while more than 25% of a sample was expired
    // (capped by `max_rounds`). Returns how many keys were deleted.
    size_t expire_cycle(size_t sample = 20, int max_rounds = 16);

    size_t volatile_count() const { return volatile_keys_.size(); }
    size_t raw_size() const { return map_.size(); }  // includes expired keys not yet collected
    Millis now() const { return clock_(); }  // commands compute deadlines from the same clock

private:
    struct Entry {
        std::string value;
        Millis expire_at = 0;  // 0 = never
        size_t volatile_slot = SIZE_MAX;  // position in volatile_keys_ if it has a deadline
    };

    bool expired(const Entry& e) const;
    void untrack(Entry& e);
    void track(const std::string& key, Entry& e);
    // Look up a key, deleting it first if it has expired (lazy expiry).
    Entry* find_live(const std::string& key);

    std::unordered_map<std::string, Entry> map_;
    // Keys that have a deadline, in a vector so active expiry can pick random ones in O(1).
    // Removal swaps the last element into the hole and fixes its slot.
    std::vector<std::string> volatile_keys_;
    Millis (*clock_)();
    std::mt19937_64 rng_{12345};
};

// Redis-style glob matching for KEYS: * ? [abc] [a-z] and \ escapes.
bool glob_match(std::string_view pattern, std::string_view text);

}  // namespace kv
