// kqueue backend (macOS / BSD), so the server runs natively on a Mac too.
#if defined(__APPLE__) || defined(__FreeBSD__)

#include <sys/event.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "poller.h"

namespace kv {

class KqueuePoller final : public Poller {
public:
    KqueuePoller() : kq_(::kqueue()) {
        if (kq_ < 0) throw std::runtime_error(std::string("kqueue: ") + std::strerror(errno));
    }
    ~KqueuePoller() override { ::close(kq_); }

    void add(int fd) override { change(fd, EVFILT_READ, EV_ADD | EV_ENABLE); }
    void want_write(int fd, bool enabled) override {
        // Unlike epoll, kqueue watches read and write as separate filters.
        change(fd, EVFILT_WRITE, enabled ? (EV_ADD | EV_ENABLE) : EV_DELETE, /*ignore_missing=*/!enabled);
    }
    void remove(int fd) override {
        // Closing the fd removes its filters automatically; nothing to do.
        (void)fd;
    }

    int wait(std::vector<Event>& out, int timeout_ms) override {
        struct kevent evs[256];
        timespec ts{timeout_ms / 1000, static_cast<long>(timeout_ms % 1000) * 1000000L};
        int n = ::kevent(kq_, nullptr, 0, evs, 256, &ts);
        if (n < 0) {
            if (errno == EINTR) return 0;
            throw std::runtime_error(std::string("kevent: ") + std::strerror(errno));
        }
        // kqueue reports read and write readiness as separate events; merge them per fd.
        std::unordered_map<int, size_t> index;
        out.clear();
        for (int i = 0; i < n; ++i) {
            int fd = static_cast<int>(evs[i].ident);
            auto [it, inserted] = index.emplace(fd, out.size());
            if (inserted) out.push_back({fd, false, false, false});
            Event& e = out[it->second];
            if (evs[i].filter == EVFILT_READ) e.readable = true;
            if (evs[i].filter == EVFILT_WRITE) e.writable = true;
            // EOF on the read side is left to read() (it returns 0 after any remaining data);
            // EOF/error on the write side means the peer is gone.
            if (evs[i].filter == EVFILT_WRITE && (evs[i].flags & (EV_EOF | EV_ERROR))) e.hangup = true;
        }
        return static_cast<int>(out.size());
    }

    const char* name() const override { return "kqueue"; }

private:
    void change(int fd, int16_t filter, uint16_t flags, bool ignore_missing = false) {
        struct kevent ev;
        EV_SET(&ev, fd, filter, flags, 0, 0, nullptr);
        if (::kevent(kq_, &ev, 1, nullptr, 0, nullptr) < 0 && !(ignore_missing && errno == ENOENT))
            throw std::runtime_error(std::string("kevent change: ") + std::strerror(errno));
    }
    int kq_;
};

std::unique_ptr<Poller> make_poller() { return std::make_unique<KqueuePoller>(); }

}  // namespace kv

#endif
