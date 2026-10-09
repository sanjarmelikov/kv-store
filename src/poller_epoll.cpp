// epoll backend (Linux).
#if defined(__linux__)

#include <sys/epoll.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include "poller.h"

namespace kv {

class EpollPoller final : public Poller {
public:
    EpollPoller() : epfd_(::epoll_create1(EPOLL_CLOEXEC)) {
        if (epfd_ < 0) throw std::runtime_error(std::string("epoll_create1: ") + std::strerror(errno));
    }
    ~EpollPoller() override { ::close(epfd_); }

    void add(int fd) override { ctl(EPOLL_CTL_ADD, fd, EPOLLIN); }
    void want_write(int fd, bool enabled) override { ctl(EPOLL_CTL_MOD, fd, EPOLLIN | (enabled ? EPOLLOUT : 0)); }
    void remove(int fd) override { ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr); }

    int wait(std::vector<Event>& out, int timeout_ms) override {
        epoll_event evs[256];
        int n = ::epoll_wait(epfd_, evs, 256, timeout_ms);
        if (n < 0) {
            if (errno == EINTR) return 0;
            throw std::runtime_error(std::string("epoll_wait: ") + std::strerror(errno));
        }
        out.clear();
        for (int i = 0; i < n; ++i) {
            uint32_t e = evs[i].events;
            out.push_back({evs[i].data.fd, (e & EPOLLIN) != 0, (e & EPOLLOUT) != 0, (e & (EPOLLHUP | EPOLLERR)) != 0});
        }
        return n;
    }

    const char* name() const override { return "epoll"; }

private:
    void ctl(int op, int fd, uint32_t events) {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epfd_, op, fd, &ev) < 0)
            throw std::runtime_error(std::string("epoll_ctl: ") + std::strerror(errno));
    }
    int epfd_;
};

std::unique_ptr<Poller> make_poller() { return std::make_unique<EpollPoller>(); }

}  // namespace kv

#endif
