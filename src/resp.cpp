#include "resp.h"

#include <charconv>

namespace kv {

namespace {

constexpr size_t kMaxBulkLen = 512 * 1024 * 1024;  // Redis's limit for one string
constexpr size_t kMaxArgs = 1024 * 1024;
constexpr size_t kMaxInline = 64 * 1024;

// Parse a decimal integer that ends at the next \r\n starting at `pos`.
// Returns the number and moves `pos` past the \r\n, or nullopt if the line isn't complete yet.
std::optional<int64_t> read_int_line(std::string_view buf, size_t& pos, bool& bad) {
    size_t end = buf.find("\r\n", pos);
    if (end == std::string_view::npos) return std::nullopt;
    int64_t value = 0;
    auto [ptr, ec] = std::from_chars(buf.data() + pos, buf.data() + end, value);
    if (ec != std::errc() || ptr != buf.data() + end) {
        bad = true;
        return std::nullopt;
    }
    pos = end + 2;
    return value;
}

ParseResult parse_inline(std::string_view buf) {
    size_t end = buf.find('\n');
    if (end == std::string_view::npos) {
        if (buf.size() > kMaxInline) return {ParseStatus::Error, {}, 0, "inline command too long"};
        return {ParseStatus::Incomplete, {}, 0, {}};
    }
    std::string_view line = buf.substr(0, end);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    Command cmd;
    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && line[i] == ' ') ++i;
        size_t start = i;
        while (i < line.size() && line[i] != ' ') ++i;
        if (i > start) cmd.emplace_back(line.substr(start, i - start));
    }
    return {ParseStatus::Ok, std::move(cmd), end + 1, {}};
}

}  // namespace

ParseResult parse_command(std::string_view buf) {
    if (buf.empty()) return {ParseStatus::Incomplete, {}, 0, {}};
    if (buf[0] != '*') return parse_inline(buf);

    size_t pos = 1;
    bool bad = false;
    auto count = read_int_line(buf, pos, bad);
    if (bad) return {ParseStatus::Error, {}, 0, "invalid multibulk length"};
    if (!count) return {ParseStatus::Incomplete, {}, 0, {}};
    if (*count < 0 || static_cast<size_t>(*count) > kMaxArgs)
        return {ParseStatus::Error, {}, 0, "invalid multibulk length"};

    Command cmd;
    cmd.reserve(static_cast<size_t>(*count));
    for (int64_t i = 0; i < *count; ++i) {
        if (pos >= buf.size()) return {ParseStatus::Incomplete, {}, 0, {}};
        if (buf[pos] != '$') return {ParseStatus::Error, {}, 0, "expected '$'"};
        ++pos;
        auto len = read_int_line(buf, pos, bad);
        if (bad) return {ParseStatus::Error, {}, 0, "invalid bulk length"};
        if (!len) return {ParseStatus::Incomplete, {}, 0, {}};
        if (*len < 0 || static_cast<size_t>(*len) > kMaxBulkLen)
            return {ParseStatus::Error, {}, 0, "invalid bulk length"};
        size_t n = static_cast<size_t>(*len);
        if (buf.size() < pos + n + 2) return {ParseStatus::Incomplete, {}, 0, {}};
        if (buf.substr(pos + n, 2) != "\r\n") return {ParseStatus::Error, {}, 0, "bulk string not terminated"};
        cmd.emplace_back(buf.substr(pos, n));
        pos += n + 2;
    }
    return {ParseStatus::Ok, std::move(cmd), pos, {}};
}

void reply_simple(std::string& out, std::string_view s) {
    out += '+';
    out += s;
    out += "\r\n";
}

void reply_error(std::string& out, std::string_view msg) {
    out += '-';
    out += msg;
    out += "\r\n";
}

void reply_int(std::string& out, int64_t n) {
    out += ':';
    out += std::to_string(n);
    out += "\r\n";
}

void reply_bulk(std::string& out, std::string_view s) {
    out += '$';
    out += std::to_string(s.size());
    out += "\r\n";
    out += s;
    out += "\r\n";
}

void reply_null(std::string& out) { out += "$-1\r\n"; }

void reply_array_header(std::string& out, size_t n) {
    out += '*';
    out += std::to_string(n);
    out += "\r\n";
}

std::string encode_command(const Command& cmd) {
    std::string out;
    reply_array_header(out, cmd.size());
    for (const auto& arg : cmd) reply_bulk(out, arg);
    return out;
}

}  // namespace kv
