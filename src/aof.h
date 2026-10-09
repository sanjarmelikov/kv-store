// Append-only file (AOF): durability by logging every write command.
//
// Each write is appended in RESP form; on startup the file is replayed to
// rebuild the keyspace. Writes are buffered during an event-loop pass and
// written to the file *before* any reply is sent, so a write the client saw
// acknowledged is at least in the OS page cache. That survives the server
// process crashing (kill -9). Surviving a power loss needs fsync, which the
// policy controls:
//   always   - fsync before replying: nothing acknowledged is ever lost; slowest
//   everysec - fsync about once a second: lose at most ~1s of writes on power loss
//   no       - let the OS decide when to flush; fastest
#pragma once

#include <functional>
#include <string>

#include "resp.h"
#include "store.h"

namespace kv {

enum class FsyncPolicy { Always, EverySec, No };

FsyncPolicy parse_fsync_policy(const std::string& s);  // throws std::invalid_argument

class Aof {
public:
    Aof(std::string path, FsyncPolicy policy);
    ~Aof();
    Aof(const Aof&) = delete;
    Aof& operator=(const Aof&) = delete;

    // Replay the file, calling `apply` for each command. A torn final command
    // (the process died mid-write) is cut off so the file is valid again.
    // Returns the number of commands replayed. Throws on real corruption.
    size_t replay(const std::function<void(const Command&)>& apply);

    void append(const Command& cmd);  // buffer a command
    void flush();                     // write the buffer (and fsync if policy is Always)
    void tick();                      // called periodically: fsync if EverySec and a second has passed

    size_t bytes_written() const { return written_; }

private:
    std::string path_;
    FsyncPolicy policy_;
    int fd_ = -1;
    std::string buf_;
    size_t written_ = 0;
    Millis last_fsync_ = 0;
    bool dirty_ = false;  // written since the last fsync
};

}  // namespace kv
