#include "aof.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>

namespace kv {

FsyncPolicy parse_fsync_policy(const std::string& s) {
    if (s == "always") return FsyncPolicy::Always;
    if (s == "everysec") return FsyncPolicy::EverySec;
    if (s == "no") return FsyncPolicy::No;
    throw std::invalid_argument("fsync policy must be always, everysec or no");
}

Aof::Aof(std::string path, FsyncPolicy policy) : path_(std::move(path)), policy_(policy) {
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) throw std::runtime_error("cannot open AOF " + path_ + ": " + std::strerror(errno));
    last_fsync_ = now_ms();
}

Aof::~Aof() {
    if (fd_ >= 0) {
        flush();
        ::fsync(fd_);
        ::close(fd_);
    }
}

size_t Aof::replay(const std::function<void(const Command&)>& apply) {
    std::string data;
    {
        struct stat st {};
        if (::fstat(fd_, &st) != 0) throw std::runtime_error("cannot stat AOF");
        data.resize(static_cast<size_t>(st.st_size));
        size_t got = 0;
        while (got < data.size()) {
            ssize_t n = ::pread(fd_, data.data() + got, data.size() - got, static_cast<off_t>(got));
            if (n <= 0) throw std::runtime_error("cannot read AOF");
            got += static_cast<size_t>(n);
        }
    }
    size_t pos = 0, count = 0;
    while (pos < data.size()) {
        ParseResult r = parse_command(std::string_view(data).substr(pos));
        if (r.status == ParseStatus::Incomplete) {
            // The process died partway through writing this command; it was never
            // acknowledged, so drop it and make the file valid again.
            std::cerr << "AOF: truncating " << data.size() - pos << " bytes of a torn final command\n";
            if (::ftruncate(fd_, static_cast<off_t>(pos)) != 0) throw std::runtime_error("cannot truncate AOF");
            break;
        }
        if (r.status == ParseStatus::Error || data[pos] != '*')
            throw std::runtime_error("AOF corrupt at byte " + std::to_string(pos) + ": " + r.error);
        apply(r.command);
        pos += r.consumed;
        ++count;
    }
    written_ = pos;
    return count;
}

void Aof::append(const Command& cmd) { buf_ += encode_command(cmd); }

void Aof::flush() {
    size_t off = 0;
    while (off < buf_.size()) {
        ssize_t n = ::write(fd_, buf_.data() + off, buf_.size() - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("AOF write failed: ") + std::strerror(errno));
        }
        off += static_cast<size_t>(n);
    }
    if (!buf_.empty()) {
        written_ += buf_.size();
        dirty_ = true;
        buf_.clear();
    }
    if (policy_ == FsyncPolicy::Always && dirty_) {
        ::fsync(fd_);
        dirty_ = false;
        last_fsync_ = now_ms();
    }
}

void Aof::tick() {
    if (policy_ != FsyncPolicy::EverySec || !dirty_) return;
    Millis now = now_ms();
    if (now - last_fsync_ >= 1000) {
        ::fsync(fd_);  // blocks the loop briefly; Redis moves this to a background thread
        dirty_ = false;
        last_fsync_ = now;
    }
}

}  // namespace kv
