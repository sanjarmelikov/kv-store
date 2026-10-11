#include "resp.h"

#include <charconv>

namespace kv {

namespace {

// Append a decimal integer without allocating a temporary string.
void append_int(std::string& out, int64_t n) {
    char buf[24];
    auto [end, ec] = std::to_chars(buf, buf + sizeof buf, n);
    out.append(buf, static_cast<size_t>(end - buf));
}

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

ParseStatus parse_inline(std::string_view buf, Command& out, size_t& consumed, std::string& error) {
    size_t end = buf.find('\n');
    if (end == std::string_view::npos) {
        if (buf.size() > kMaxInline) return error = "inline command too long", ParseStatus::Error;
        return ParseStatus::Incomplete;
    }
    std::string_view line = buf.substr(0, end);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    out.clear();
    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && line[i] == ' ') ++i;
        size_t start = i;
        while (i < line.size() && line[i] != ' ') ++i;
        if (i > start) out.emplace_back(line.substr(start, i - start));
    }
    consumed = end + 1;
    return ParseStatus::Ok;
}

}  // namespace

ParseStatus parse_command_into(std::string_view buf, Command& out, size_t& consumed, std::string& error) {
    if (buf.empty()) return ParseStatus::Incomplete;
    if (buf[0] != '*') return parse_inline(buf, out, consumed, error);

    size_t pos = 1;
    bool bad = false;
    auto count = read_int_line(buf, pos, bad);
    if (bad) return error = "invalid multibulk length", ParseStatus::Error;
    if (!count) return ParseStatus::Incomplete;
    if (*count < 0 || static_cast<size_t>(*count) > kMaxArgs)
        return error = "invalid multibulk length", ParseStatus::Error;

    size_t n_args = static_cast<size_t>(*count);
    if (out.size() < n_args) out.resize(n_args);  // grow only; strings keep their capacity
    for (size_t i = 0; i < n_args; ++i) {
        if (pos >= buf.size()) return ParseStatus::Incomplete;
        if (buf[pos] != '$') return error = "expected '$'", ParseStatus::Error;
        ++pos;
        auto len = read_int_line(buf, pos, bad);
        if (bad) return error = "invalid bulk length", ParseStatus::Error;
        if (!len) return ParseStatus::Incomplete;
        if (*len < 0 || static_cast<size_t>(*len) > kMaxBulkLen)
            return error = "invalid bulk length", ParseStatus::Error;
        size_t n = static_cast<size_t>(*len);
        if (buf.size() < pos + n + 2) return ParseStatus::Incomplete;
        if (buf.substr(pos + n, 2) != "\r\n") return error = "bulk string not terminated", ParseStatus::Error;
        out[i].assign(buf.data() + pos, n);
        pos += n + 2;
    }
    out.resize(n_args);  // drop leftovers from a longer previous command
    consumed = pos;
    return ParseStatus::Ok;
}

ParseResult parse_command(std::string_view buf) {
    ParseResult r{ParseStatus::Incomplete, {}, 0, {}};
    r.status = parse_command_into(buf, r.command, r.consumed, r.error);
    if (r.status != ParseStatus::Ok) r.command.clear();
    return r;
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
    append_int(out, n);
    out += "\r\n";
}

void reply_bulk(std::string& out, std::string_view s) {
    out += '$';
    append_int(out, static_cast<int64_t>(s.size()));
    out += "\r\n";
    out += s;
    out += "\r\n";
}

void reply_null(std::string& out, int proto) { out += proto == 3 ? "_\r\n" : "$-1\r\n"; }

void reply_map_header(std::string& out, size_t n, int proto) {
    // RESP3 has a real map type; RESP2 sends the same pairs as a flat array.
    out += proto == 3 ? '%' : '*';
    append_int(out, static_cast<int64_t>(proto == 3 ? n : 2 * n));
    out += "\r\n";
}

void reply_array_header(std::string& out, size_t n) {
    out += '*';
    append_int(out, static_cast<int64_t>(n));
    out += "\r\n";
}

std::string encode_command(const Command& cmd) {
    std::string out;
    reply_array_header(out, cmd.size());
    for (const auto& arg : cmd) reply_bulk(out, arg);
    return out;
}

}  // namespace kv
