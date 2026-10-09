#include "server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace kv {

namespace {

constexpr size_t kReadChunk = 16 * 1024;
constexpr size_t kMaxQueryBuffer = 1024 * 1024 * 1024;  // 1 GB, as in Redis
constexpr int kTickMs = 100;  // active expiry and everysec fsync run this often

void set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        throw std::runtime_error(std::string("fcntl: ") + std::strerror(errno));
}

}  // namespace

Server::Server(ServerConfig config)
    : config_(std::move(config)),
      aof_(config_.aof_path.empty() ? nullptr : std::make_unique<Aof>(config_.aof_path, config_.fsync)),
      engine_(store_, aof_.get()),
      poller_(make_poller()) {
    if (aof_) replayed_ = aof_->replay([this](const Command& cmd) { engine_.replay(cmd); });

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) throw std::runtime_error(std::string("socket: ") + std::strerror(errno));
    int yes = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(config_.port));
    if (::inet_pton(AF_INET, config_.bind.c_str(), &addr.sin_addr) != 1)
        throw std::runtime_error("bad bind address " + config_.bind);
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0)
        throw std::runtime_error(std::string("bind: ") + std::strerror(errno));
    if (::listen(listen_fd_, 511) < 0) throw std::runtime_error(std::string("listen: ") + std::strerror(errno));
    set_nonblocking(listen_fd_);
    poller_->add(listen_fd_);
}

Server::~Server() {
    for (auto& [fd, _] : conns_) ::close(fd);
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

void Server::run() {
    running_ = true;
    std::cerr << "kv-store listening on " << config_.bind << ":" << config_.port << " (" << poller_->name()
              << (aof_ ? ", AOF on" : ", no persistence") << ", " << replayed_ << " commands replayed)\n";
    std::vector<Event> events;
    std::vector<int> pending;  // connections with replies waiting to be sent
    Millis last_tick = now_ms();

    while (running_) {
        poller_->wait(events, kTickMs);
        pending.clear();
        for (const Event& ev : events) {
            if (ev.fd == listen_fd_) {
                accept_clients();
                continue;
            }
            auto it = conns_.find(ev.fd);
            if (it == conns_.end()) continue;
            if (ev.readable || ev.hangup) on_readable(ev.fd);  // a read returning 0 detects the close
            if (conns_.count(ev.fd)) pending.push_back(ev.fd);
        }

        // Durability rule: log this pass's writes *before* sending any reply,
        // so no client is ever told OK for a write that isn't in the AOF.
        if (aof_) aof_->flush();

        for (int fd : pending) {
            auto it = conns_.find(fd);
            if (it != conns_.end() && !flush_conn(fd, it->second)) close_conn(fd);
        }

        Millis now = now_ms();
        if (now - last_tick >= kTickMs) {
            store_.expire_cycle();
            if (aof_) aof_->tick();
            last_tick = now;
        }
    }
    if (aof_) aof_->flush();
}

void Server::accept_clients() {
    while (true) {
        int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
            std::cerr << "accept: " << std::strerror(errno) << "\n";
            return;
        }
        if (conns_.size() >= config_.max_clients) {
            const char* msg = "-ERR max number of clients reached\r\n";
            (void)!::write(fd, msg, std::strlen(msg));
            ::close(fd);
            continue;
        }
        set_nonblocking(fd);
        int yes = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof yes);  // send small replies immediately
#ifdef SO_NOSIGPIPE
        ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof yes);
#endif
        conns_.emplace(fd, Conn{});
        poller_->add(fd);
    }
}

void Server::on_readable(int fd) {
    Conn& c = conns_.at(fd);
    char buf[kReadChunk];
    while (true) {
        ssize_t n = ::read(fd, buf, sizeof buf);
        if (n > 0) {
            c.in.append(buf, static_cast<size_t>(n));
            if (c.in.size() > kMaxQueryBuffer) return close_conn(fd);
            continue;
        }
        if (n == 0) return close_conn(fd);  // client closed the connection
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        return close_conn(fd);
    }

    // Run every complete command in the buffer (clients may pipeline many per read).
    size_t pos = 0;
    while (pos < c.in.size() && !c.close_after_write) {
        size_t consumed = 0;
        ParseStatus st = parse_command_into(std::string_view(c.in).substr(pos), c.cmd, consumed, c.parse_error);
        if (st == ParseStatus::Incomplete) break;
        if (st == ParseStatus::Error) {
            reply_error(c.out, "ERR Protocol error: " + c.parse_error);
            c.close_after_write = true;
            break;
        }
        pos += consumed;
        if (!engine_.execute(c.cmd, c.out)) c.close_after_write = true;
    }
    c.in.erase(0, pos);  // keep any partial command for the next read
}

bool Server::flush_conn(int fd, Conn& c) {
    while (c.out_pos < c.out.size()) {
#ifdef MSG_NOSIGNAL
        ssize_t n = ::send(fd, c.out.data() + c.out_pos, c.out.size() - c.out_pos, MSG_NOSIGNAL);
#else
        ssize_t n = ::send(fd, c.out.data() + c.out_pos, c.out.size() - c.out_pos, 0);
#endif
        if (n > 0) {
            c.out_pos += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // Socket buffer full: ask to be told when it drains, and finish later.
            if (!c.want_write) poller_->want_write(fd, c.want_write = true);
            return true;
        }
        return false;
    }
    c.out.clear();
    c.out_pos = 0;
    if (c.want_write) poller_->want_write(fd, c.want_write = false);
    return !c.close_after_write;
}

void Server::close_conn(int fd) {
    poller_->remove(fd);
    ::close(fd);
    conns_.erase(fd);
}

}  // namespace kv
