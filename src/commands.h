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

private:
    void log(Command cmd);
    Store& store_;
    Aof* aof_;
    bool replaying_ = false;
};

}  // namespace kv
