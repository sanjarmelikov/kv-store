#include "commands.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>
#include <optional>

namespace kv {

namespace {

std::string upper(std::string s) {
    for (auto& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

std::string lower(std::string s) {
    for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

std::optional<int64_t> to_int(const std::string& s) {
    int64_t v = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc() || ptr != s.data() + s.size() || s.empty()) return std::nullopt;
    return v;
}

void wrong_args(std::string& out, const std::string& name) {
    reply_error(out, "ERR wrong number of arguments for '" + lower(name) + "' command");
}

constexpr const char* kNotInt = "ERR value is not an integer or out of range";

}  // namespace

void Engine::log(Command cmd) {
    if (aof_ && !replaying_) aof_->append(cmd);
}

void Engine::replay(const Command& cmd) {
    replaying_ = true;
    std::string discard;
    execute(cmd, discard);
    replaying_ = false;
}

bool Engine::execute(const Command& cmd, std::string& out) {
    if (cmd.empty()) return true;  // blank inline line: ignore, like Redis
    const std::string name = upper(cmd[0]);
    const size_t argc = cmd.size();

    // ---- connection / server ----
    if (name == "PING") {
        if (argc == 1) reply_simple(out, "PONG");
        else if (argc == 2) reply_bulk(out, cmd[1]);
        else wrong_args(out, name);
    } else if (name == "ECHO") {
        if (argc != 2) return wrong_args(out, name), true;
        reply_bulk(out, cmd[1]);
    } else if (name == "QUIT") {
        reply_simple(out, "OK");
        return false;
    } else if (name == "SELECT") {
        // Only database 0 exists.
        if (argc != 2) return wrong_args(out, name), true;
        if (cmd[1] == "0") reply_simple(out, "OK");
        else reply_error(out, "ERR DB index is out of range");
    } else if (name == "COMMAND" || name == "CONFIG") {
        // Compatibility stubs: redis-cli asks for COMMAND DOCS on connect and
        // redis-benchmark asks for CONFIG GET; an empty list tells them "nothing to report".
        reply_array_header(out, 0);
    } else if (name == "CLIENT") {
        reply_simple(out, "OK");  // client libraries send CLIENT SETINFO / SETNAME on connect
    } else if (name == "INFO") {
        // Just the keyspace section. Counts are raw (expired-but-uncollected keys included),
        // which is what lets tests watch active expiry happen without reading any key.
        reply_bulk(out, "# Keyspace\r\ndb0:keys=" + std::to_string(store_.raw_size()) +
                            ",expires=" + std::to_string(store_.volatile_count()) + "\r\n");
    } else if (name == "DBSIZE") {
        reply_int(out, static_cast<int64_t>(store_.size()));
    } else if (name == "FLUSHALL" || name == "FLUSHDB") {
        store_.clear();
        log({"FLUSHALL"});
        reply_simple(out, "OK");

    // ---- strings ----
    } else if (name == "GET") {
        if (argc != 2) return wrong_args(out, name), true;
        auto v = store_.get(cmd[1]);
        v ? reply_bulk(out, *v) : reply_null(out);
    } else if (name == "SET") {
        if (argc < 3) return wrong_args(out, name), true;
        std::optional<Millis> deadline;
        bool nx = false, xx = false, keepttl = false;
        for (size_t i = 3; i < argc; ++i) {
            std::string opt = upper(cmd[i]);
            if (opt == "NX") nx = true;
            else if (opt == "XX") xx = true;
            else if (opt == "KEEPTTL") keepttl = true;
            else if ((opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT") && i + 1 < argc && !deadline) {
                auto n = to_int(cmd[++i]);
                if (!n || *n <= 0) return reply_error(out, "ERR invalid expire time in 'set' command"), true;
                if (opt == "EX") deadline = store_.now() + *n * 1000;
                else if (opt == "PX") deadline = store_.now() + *n;
                else if (opt == "EXAT") deadline = *n * 1000;
                else deadline = *n;
            } else {
                return reply_error(out, "ERR syntax error"), true;
            }
        }
        if ((nx && xx) || (keepttl && deadline)) return reply_error(out, "ERR syntax error"), true;
        bool exists = store_.exists(cmd[1]);
        if ((nx && exists) || (xx && !exists)) return reply_null(out), true;
        if (keepttl) deadline = store_.deadline(cmd[1]);
        store_.set(cmd[1], cmd[2], deadline);
        // Log an absolute deadline (PXAT) so replaying the file later doesn't restart the clock.
        if (deadline) log({"SET", cmd[1], cmd[2], "PXAT", std::to_string(*deadline)});
        else log({"SET", cmd[1], cmd[2]});
        reply_simple(out, "OK");
    } else if (name == "MGET") {
        if (argc < 2) return wrong_args(out, name), true;
        reply_array_header(out, argc - 1);
        for (size_t i = 1; i < argc; ++i) {
            auto v = store_.get(cmd[i]);
            v ? reply_bulk(out, *v) : reply_null(out);
        }
    } else if (name == "MSET") {
        if (argc < 3 || argc % 2 == 0) return wrong_args(out, name), true;
        for (size_t i = 1; i < argc; i += 2) store_.set(cmd[i], cmd[i + 1]);
        log(cmd);
        reply_simple(out, "OK");
    } else if (name == "DEL") {
        if (argc < 2) return wrong_args(out, name), true;
        Command logged{"DEL"};
        for (size_t i = 1; i < argc; ++i)
            if (store_.del(cmd[i])) logged.push_back(cmd[i]);
        if (logged.size() > 1) log(logged);
        reply_int(out, static_cast<int64_t>(logged.size() - 1));
    } else if (name == "EXISTS") {
        if (argc < 2) return wrong_args(out, name), true;
        int64_t n = 0;
        for (size_t i = 1; i < argc; ++i) n += store_.exists(cmd[i]);
        reply_int(out, n);
    } else if (name == "INCR" || name == "DECR" || name == "INCRBY" || name == "DECRBY") {
        bool by = name == "INCRBY" || name == "DECRBY";
        if (argc != (by ? 3u : 2u)) return wrong_args(out, name), true;
        int64_t delta = 1;
        if (by) {
            auto d = to_int(cmd[2]);
            if (!d) return reply_error(out, kNotInt), true;
            delta = *d;
        }
        if (name[0] == 'D') {
            if (delta == std::numeric_limits<int64_t>::min()) return reply_error(out, kNotInt), true;
            delta = -delta;
        }
        int64_t current = 0;
        if (auto v = store_.get(cmd[1])) {
            auto parsed = to_int(*v);
            if (!parsed) return reply_error(out, kNotInt), true;
            current = *parsed;
        }
        int64_t result;
        if (__builtin_add_overflow(current, delta, &result))
            return reply_error(out, "ERR increment or decrement would overflow"), true;
        store_.set(cmd[1], std::to_string(result), store_.deadline(cmd[1]));  // keeps any TTL
        log({"SET", cmd[1], std::to_string(result), "KEEPTTL"});
        reply_int(out, result);
    } else if (name == "APPEND") {
        if (argc != 3) return wrong_args(out, name), true;
        std::string value = store_.get(cmd[1]).value_or("") + cmd[2];
        store_.set(cmd[1], value, store_.deadline(cmd[1]));
        log({"SET", cmd[1], value, "KEEPTTL"});
        reply_int(out, static_cast<int64_t>(value.size()));
    } else if (name == "STRLEN") {
        if (argc != 2) return wrong_args(out, name), true;
        reply_int(out, static_cast<int64_t>(store_.get(cmd[1]).value_or("").size()));

    // ---- expiry ----
    } else if (name == "EXPIRE" || name == "PEXPIRE" || name == "PEXPIREAT" || name == "EXPIREAT") {
        if (argc != 3) return wrong_args(out, name), true;
        auto n = to_int(cmd[2]);
        if (!n) return reply_error(out, kNotInt), true;
        Millis when = name == "EXPIRE" ? store_.now() + *n * 1000
                    : name == "PEXPIRE" ? store_.now() + *n
                    : name == "EXPIREAT" ? *n * 1000
                    : *n;
        bool ok = store_.expire_at(cmd[1], when);
        if (ok) log({"PEXPIREAT", cmd[1], std::to_string(when)});
        reply_int(out, ok ? 1 : 0);
    } else if (name == "TTL" || name == "PTTL") {
        if (argc != 2) return wrong_args(out, name), true;
        int64_t ms = store_.ttl_ms(cmd[1]);
        if (ms < 0 || name == "PTTL") reply_int(out, ms);
        else reply_int(out, (ms + 999) / 1000);  // round up, like Redis
    } else if (name == "PERSIST") {
        if (argc != 2) return wrong_args(out, name), true;
        bool ok = store_.persist(cmd[1]);
        if (ok) log(cmd);
        reply_int(out, ok ? 1 : 0);
    } else if (name == "KEYS") {
        if (argc != 2) return wrong_args(out, name), true;
        auto keys = store_.keys(cmd[1]);
        std::sort(keys.begin(), keys.end());
        reply_array_header(out, keys.size());
        for (const auto& k : keys) reply_bulk(out, k);
    } else {
        reply_error(out, "ERR unknown command '" + cmd[0] + "'");
    }
    return true;
}

}  // namespace kv
