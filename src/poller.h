// I/O readiness notification: "which of my sockets can I read or write without blocking?"
//
// One interface, two backends: epoll on Linux (the production target) and
// kqueue on macOS/BSD (so the server also runs natively on a Mac). Both are
// level-triggered here: a socket keeps being reported while it has data.
#pragma once

#include <memory>
#include <vector>

namespace kv {

struct Event {
    int fd;
    bool readable;
    bool writable;
    bool hangup;  // peer closed or socket error
};

class Poller {
public:
    virtual ~Poller() = default;
    virtual void add(int fd) = 0;                         // watch for readability
    virtual void want_write(int fd, bool enabled) = 0;    // also watch for writability (or stop)
    virtual void remove(int fd) = 0;
    // Wait up to timeout_ms for events; fills `out` and returns how many.
    virtual int wait(std::vector<Event>& out, int timeout_ms) = 0;
    virtual const char* name() const = 0;
};

std::unique_ptr<Poller> make_poller();

}  // namespace kv
