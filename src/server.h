// The single-threaded event loop: accept clients, read commands, run them, write replies.
//
// One thread serves every client. That works because each command is tiny
// (a hash-map lookup) and the thread never blocks: sockets are non-blocking and
// the poller says which ones are ready. No locks are needed, since only one
// command ever runs at a time.
#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "aof.h"
#include "commands.h"
#include "poller.h"
#include "store.h"

namespace kv {

struct ServerConfig {
    std::string bind = "127.0.0.1";
    int port = 6380;
    std::string aof_path;  // empty = no persistence
    FsyncPolicy fsync = FsyncPolicy::EverySec;
    size_t max_clients = 10000;
};

class Server {
public:
    explicit Server(ServerConfig config);
    ~Server();
    void run();   // blocks until stop() (or SIGINT/SIGTERM)
    void stop() { running_ = false; }
    size_t replayed() const { return replayed_; }

private:
    struct Conn {
        std::string in;
        std::string out;
        size_t out_pos = 0;
        bool want_write = false;
        bool close_after_write = false;
        Command cmd;  // reused for every command on this connection
        std::string parse_error;
    };

    void accept_clients();
    void on_readable(int fd);
    bool flush_conn(int fd, Conn& c);  // false if the connection should be closed
    void close_conn(int fd);

    ServerConfig config_;
    Store store_;
    std::unique_ptr<Aof> aof_;
    Engine engine_;
    std::unique_ptr<Poller> poller_;
    int listen_fd_ = -1;
    std::unordered_map<int, Conn> conns_;
    std::atomic<bool> running_{false};
    size_t replayed_ = 0;
};

}  // namespace kv
