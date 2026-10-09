#include "store.h"

#include <chrono>

namespace kv {

Millis now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

Store::Store(Millis (*clock)()) : clock_(clock) {}

bool Store::expired(const Entry& e) const { return e.expire_at != 0 && e.expire_at <= clock_(); }

void Store::track(const std::string& key, Entry& e) {
    if (e.volatile_slot != SIZE_MAX) return;
    e.volatile_slot = volatile_keys_.size();
    volatile_keys_.push_back(key);
}

void Store::untrack(Entry& e) {
    if (e.volatile_slot == SIZE_MAX) return;
    size_t slot = e.volatile_slot;
    if (slot != volatile_keys_.size() - 1) {
        // Move the last key into this slot and tell its entry where it went.
        volatile_keys_[slot] = std::move(volatile_keys_.back());
        map_[volatile_keys_[slot]].volatile_slot = slot;
    }
    volatile_keys_.pop_back();
    e.volatile_slot = SIZE_MAX;
}

Store::Entry* Store::find_live(const std::string& key) {
    auto it = map_.find(key);
    if (it == map_.end()) return nullptr;
    if (expired(it->second)) {
        untrack(it->second);
        map_.erase(it);
        return nullptr;
    }
    return &it->second;
}

std::optional<std::string> Store::get(const std::string& key) {
    Entry* e = find_live(key);
    if (!e) return std::nullopt;
    return e->value;
}

void Store::set(const std::string& key, std::string value, std::optional<Millis> expire_at) {
    Entry& e = map_[key];
    e.value = std::move(value);
    if (expire_at) {
        e.expire_at = *expire_at;
        track(key, e);
    } else {
        // A plain SET removes any previous deadline, as in Redis.
        e.expire_at = 0;
        untrack(e);
    }
}

bool Store::del(const std::string& key) {
    Entry* e = find_live(key);
    if (!e) return false;
    untrack(*e);
    map_.erase(key);
    return true;
}

bool Store::exists(const std::string& key) { return find_live(key) != nullptr; }

size_t Store::size() {
    // Exact count: sweep every key with a deadline (O(volatile keys)), unlike the sampled cycle.
    Millis now = clock_();
    for (size_t i = 0; i < volatile_keys_.size();) {
        auto it = map_.find(volatile_keys_[i]);
        if (it->second.expire_at <= now) {
            untrack(it->second);  // moves the last key into slot i, so don't advance
            map_.erase(it);
        } else {
            ++i;
        }
    }
    return map_.size();
}

void Store::clear() {
    map_.clear();
    volatile_keys_.clear();
}

bool Store::expire_at(const std::string& key, Millis when) {
    Entry* e = find_live(key);
    if (!e) return false;
    e->expire_at = when;
    track(key, *e);
    if (expired(*e)) del(key);  // a deadline in the past deletes the key now
    return true;
}

bool Store::persist(const std::string& key) {
    Entry* e = find_live(key);
    if (!e || e->expire_at == 0) return false;
    e->expire_at = 0;
    untrack(*e);
    return true;
}

int64_t Store::ttl_ms(const std::string& key) {
    Entry* e = find_live(key);
    if (!e) return -2;
    if (e->expire_at == 0) return -1;
    return e->expire_at - clock_();
}

std::optional<Millis> Store::deadline(const std::string& key) {
    Entry* e = find_live(key);
    if (!e || e->expire_at == 0) return std::nullopt;
    return e->expire_at;
}

std::vector<std::string> Store::keys(const std::string& pattern) {
    std::vector<std::string> out;
    Millis now = clock_();
    for (const auto& [key, e] : map_) {
        if (e.expire_at != 0 && e.expire_at <= now) continue;
        if (glob_match(pattern, key)) out.push_back(key);
    }
    return out;
}

size_t Store::expire_cycle(size_t sample, int max_rounds) {
    size_t deleted_total = 0;
    for (int round = 0; round < max_rounds && !volatile_keys_.empty(); ++round) {
        size_t n = std::min(sample, volatile_keys_.size());
        size_t deleted = 0;
        Millis now = clock_();
        for (size_t i = 0; i < n && !volatile_keys_.empty(); ++i) {
            size_t slot = std::uniform_int_distribution<size_t>(0, volatile_keys_.size() - 1)(rng_);
            std::string key = volatile_keys_[slot];  // copy: untrack may overwrite the slot
            auto it = map_.find(key);
            if (it->second.expire_at <= now) {
                untrack(it->second);
                map_.erase(it);
                ++deleted;
            }
        }
        deleted_total += deleted;
        if (deleted * 4 <= n) break;  // 25% or fewer expired: memory is under control
    }
    return deleted_total;
}

bool glob_match(std::string_view p, std::string_view s) {
    size_t pi = 0, si = 0, star_p = std::string_view::npos, star_s = 0;
    while (si < s.size()) {
        if (pi < p.size() && p[pi] == '*') {
            star_p = pi++;
            star_s = si;
            continue;
        }
        if (pi < p.size()) {
            bool match = false;
            size_t next = pi + 1;
            if (p[pi] == '?') {
                match = true;
            } else if (p[pi] == '[') {
                size_t j = pi + 1;
                bool negate = j < p.size() && p[j] == '^';
                if (negate) ++j;
                bool in = false;
                while (j < p.size() && p[j] != ']') {
                    if (p[j] == '\\' && j + 1 < p.size()) {
                        in |= p[j + 1] == s[si];
                        j += 2;
                    } else if (j + 2 < p.size() && p[j + 1] == '-' && p[j + 2] != ']') {
                        in |= p[j] <= s[si] && s[si] <= p[j + 2];
                        j += 3;
                    } else {
                        in |= p[j] == s[si];
                        ++j;
                    }
                }
                match = negate ? !in : in;
                next = j < p.size() ? j + 1 : j;
            } else if (p[pi] == '\\' && pi + 1 < p.size()) {
                match = p[pi + 1] == s[si];
                next = pi + 2;
            } else {
                match = p[pi] == s[si];
            }
            if (match) {
                pi = next;
                ++si;
                continue;
            }
        }
        if (star_p == std::string_view::npos) return false;
        pi = star_p + 1;  // backtrack: let the last * absorb one more character
        si = ++star_s;
    }
    while (pi < p.size() && p[pi] == '*') ++pi;
    return pi == p.size();
}

}  // namespace kv
