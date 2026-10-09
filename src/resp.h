// RESP (REdis Serialization Protocol): parsing client commands and encoding replies.
//
// Clients send each command as an array of bulk strings:
//   *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
// TCP delivers bytes, not messages: one read() can hold half a command or ten
// (pipelining). The parser is incremental: it consumes complete commands from
// the front of a buffer and leaves any partial one for the next read.
#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace kv {

using Command = std::vector<std::string>;

enum class ParseStatus { Ok, Incomplete, Error };

struct ParseResult {
    ParseStatus status;
    Command command;       // filled when status == Ok
    size_t consumed = 0;   // bytes used from the front of the buffer when Ok
    std::string error;     // filled when status == Error
};

// Parse one command from the front of `buf`. Accepts RESP arrays of bulk strings
// and "inline" commands (plain text lines like `PING\r\n`, which redis-cli and telnet send).
ParseResult parse_command(std::string_view buf);

// Same, but fills `out` in place so a connection can reuse one Command (and its strings'
// capacity) for every command it receives: no allocation per command in steady state.
// Returns Ok / Incomplete / Error; on Ok sets `consumed`, on Error sets `error`.
ParseStatus parse_command_into(std::string_view buf, Command& out, size_t& consumed, std::string& error);

// Reply encoders: each appends one RESP value to `out`.
void reply_simple(std::string& out, std::string_view s);        // +OK
void reply_error(std::string& out, std::string_view msg);       // -ERR ...
void reply_int(std::string& out, int64_t n);                    // :42
void reply_bulk(std::string& out, std::string_view s);          // $5\r\nhello
void reply_null(std::string& out);                              // $-1 (missing key)
void reply_array_header(std::string& out, size_t n);            // *n, followed by n values

// Encode a command as a RESP array (used for the append-only file).
std::string encode_command(const Command& cmd);

}  // namespace kv
