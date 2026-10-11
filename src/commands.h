// Command execution: turns a parsed command into a reply, and logs writes to the AOF.
#pragma once

#include <string>

#include "aof.h"
#include "resp.h"
#include "store.h"

namespace kv {

class Engine {
public:
    explicit Engine(Store& store, Aof* aof = nullptr) : store_(store), aof_(aof) {}

    // Execute one command, appending its RESP reply to `out`.
    // Returns false if the client asked to close the connection (QUIT).
    bool execute(const Command& cmd, std::string& out);

    // Apply a command read back from the AOF (no reply, no re-logging).
    void replay(const Command& cmd);

    // Protocol version (2 or 3) of the connection whose command runs next; set by the
    // server per connection after HELLO. Only affects how "no value" is encoded.
    void set_protocol(int proto) { proto_ = proto; }

private:
    // True when writes must be appended to the AOF. Every log({...}) call is guarded
    // by this, so the command copy is only built when persistence is on.
    bool logging() const { return aof_ && !replaying_; }
    void log(const Command& cmd);
    Store& store_;
    Aof* aof_;
    bool replaying_ = false;
    int proto_ = 2;
};

}  // namespace kv
