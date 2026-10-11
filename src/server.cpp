#include "server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
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
        engine_.set_protocol(c.proto);
        if (!check_auth(c, fd) && !engine_.execute(c.cmd, c.out)) c.close_after_write = true;
        if (config_.max_output_buffer && c.out.size() - c.out_pos > config_.max_output_buffer) {
            // The client keeps sending but doesn't read its replies: drop it before it
            // exhausts the server's memory (same idea as Redis's client-output-buffer-limit).
            std::cerr << "closing client: output buffer over " << config_.max_output_buffer << " bytes\n";
            return close_conn(fd);
        }
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

namespace {

// Compare without exiting early, so response time doesn't reveal how many leading
// characters of a guessed password were right.
bool constant_time_equal(const std::string& a, const std::string& b) {
    unsigned char diff = static_cast<unsigned char>(a.size() != b.size());
    size_t n = std::max(a.size(), b.size());
    for (size_t i = 0; i < n; ++i) {
        unsigned char x = i < a.size() ? static_cast<unsigned char>(a[i]) : 0;
        unsigned char y = i < b.size() ? static_cast<unsigned char>(b[i]) : 0;
        diff |= static_cast<unsigned char>(x ^ y);
    }
    return diff == 0;
}

bool is_named(const Command& cmd, const char* name) {
    if (cmd.empty() || cmd[0].size() != std::strlen(name)) return false;
    for (size_t i = 0; i < cmd[0].size(); ++i)
        if (std::toupper(static_cast<unsigned char>(cmd[0][i])) != name[i]) return false;
    return true;
}

}  // namespace

void Server::hello(Conn& c, int fd) {
    // HELLO [protover [AUTH username password] [SETNAME name]]: the connection handshake
    // modern clients (redis-py 8, redis-cli -3) send first.
    const Command& cmd = c.cmd;
    int proto = c.proto;
    if (cmd.size() >= 2) {
        if (cmd[1] == "2" || cmd[1] == "3") {
            proto = cmd[1][0] - '0';
        } else {
            reply_error(c.out, "NOPROTO sorry, this protocol version is not supported.");
            return;
        }
    }
    for (size_t i = 2; i < cmd.size(); ++i) {
        std::string opt = cmd[i];
        for (auto& ch : opt) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
        if (opt == "AUTH" && i + 2 < cmd.size()) {
            if (config_.requirepass.empty() || cmd[i + 1] != "default" ||
                !constant_time_equal(cmd[i + 2], config_.requirepass)) {
                reply_error(c.out, "WRONGPASS invalid username-password pair or user is disabled.");
                return;
            }
            c.authed = true;
            i += 2;
        } else if (opt == "SETNAME" && i + 1 < cmd.size()) {
            ++i;  // client names aren't tracked
        } else {
            reply_error(c.out, "ERR syntax error in HELLO option '" + cmd[i] + "'");
            return;
        }
    }
    if (!config_.requirepass.empty() && !c.authed) {
        reply_error(c.out, "NOAUTH HELLO must be called with the client already authenticated, otherwise "
                           "the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client "
                           "and select the RESP protocol version at the same time");
        return;
    }
    c.proto = proto;
    reply_map_header(c.out, 7, proto);
    reply_bulk(c.out, "server");  reply_bulk(c.out, "kv-store");
    reply_bulk(c.out, "version"); reply_bulk(c.out, "7.0.0");
    reply_bulk(c.out, "proto");   reply_int(c.out, proto);
    reply_bulk(c.out, "id");      reply_int(c.out, fd);
    reply_bulk(c.out, "mode");    reply_bulk(c.out, "standalone");
    reply_bulk(c.out, "role");    reply_bulk(c.out, "master");
    reply_bulk(c.out, "modules"); reply_array_header(c.out, 0);
}

bool Server::check_auth(Conn& c, int fd) {
    const Command& cmd = c.cmd;
    if (is_named(cmd, "HELLO")) {
        hello(c, fd);
        return true;
    }
    if (is_named(cmd, "AUTH")) {
        if (config_.requirepass.empty()) {
            reply_error(c.out, "ERR AUTH <password> called without any password configured for the default user");
        } else if (cmd.size() < 2 || cmd.size() > 3) {
            reply_error(c.out, "ERR wrong number of arguments for 'auth' command");
        } else if ((cmd.size() == 3 && cmd[1] != "default") ||
                   !constant_time_equal(cmd.back(), config_.requirepass)) {
            reply_error(c.out, "WRONGPASS invalid username-password pair or user is disabled.");
        } else {
            c.authed = true;
            reply_simple(c.out, "OK");
        }
        return true;
    }
    if (config_.requirepass.empty() || c.authed || cmd.empty() || is_named(cmd, "QUIT")) return false;
    reply_error(c.out, "NOAUTH Authentication required.");
    return true;
}

void Server::close_conn(int fd) {
    poller_->remove(fd);
    ::close(fd);
    conns_.erase(fd);
}

}  // namespace kv
