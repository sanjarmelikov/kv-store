#include "aof.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
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
    base_size_ = pos;
    return count;
}

void Aof::append(const Command& cmd) {
    std::string encoded = encode_command(cmd);
    if (child_ > 0) rewrite_buf_ += encoded;  // the child's snapshot won't contain this write
    buf_ += encoded;
}

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

namespace kv {

namespace {

bool write_all(int fd, const std::string& data) {
    size_t off = 0;
    while (off < data.size()) {
        ssize_t n = ::write(fd, data.data() + off, data.size() - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

}  // namespace

bool Aof::begin_rewrite(Store& store) {
    if (child_ > 0) return false;
    flush();  // everything before the fork is on disk in the old AOF (and in the snapshot)
    rewrite_path_ = path_ + ".rewrite";
    rewrite_buf_.clear();
    pid_t pid = ::fork();
    if (pid < 0) throw std::runtime_error(std::string("fork: ") + std::strerror(errno));
    if (pid == 0) {
        // Child: a frozen copy-on-write view of memory. Write one SET per live key, with
        // its absolute deadline, then exit without running the parent's destructors.
        int fd = ::open(rewrite_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) ::_exit(1);
        std::string chunk;
        bool ok = true;
        store.for_each([&](const std::string& key, const std::string& value, Millis expire_at) {
            if (expire_at) chunk += encode_command({"SET", key, value, "PXAT", std::to_string(expire_at)});
            else chunk += encode_command({"SET", key, value});
            if (chunk.size() > (1 << 20)) {
                ok = ok && write_all(fd, chunk);
                chunk.clear();
            }
        });
        ok = ok && write_all(fd, chunk) && ::fsync(fd) == 0;
        ::close(fd);
        ::_exit(ok ? 0 : 1);
    }
    child_ = pid;
    return true;
}

void Aof::poll_rewrite() {
    if (child_ <= 0) return;
    int status = 0;
    pid_t done = ::waitpid(child_, &status, WNOHANG);
    if (done == 0) return;  // still running
    child_ = -1;
    if (done < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        std::cerr << "AOF rewrite failed; keeping the current AOF\n";
        ::unlink(rewrite_path_.c_str());
        rewrite_buf_.clear();
        return;
    }
    // Writes made while the child ran: append them, make it durable, then swap.
    flush();
    int fd = ::open(rewrite_path_.c_str(), O_WRONLY | O_APPEND);
    if (fd < 0 || !write_all(fd, rewrite_buf_) || ::fsync(fd) != 0) {
        if (fd >= 0) ::close(fd);
        std::cerr << "AOF rewrite: could not finish the new file; keeping the current AOF\n";
        ::unlink(rewrite_path_.c_str());
        rewrite_buf_.clear();
        return;
    }
    struct stat st {};
    ::fstat(fd, &st);
    // rename() is atomic: the AOF path names either the old file or the complete new one.
    if (::rename(rewrite_path_.c_str(), path_.c_str()) != 0) {
        ::close(fd);
        std::cerr << "AOF rewrite: rename failed: " << std::strerror(errno) << "\n";
        return;
    }
    ::close(fd_);
    fd_ = ::open(path_.c_str(), O_RDWR | O_APPEND);
    if (fd_ < 0) throw std::runtime_error("cannot reopen AOF after rewrite");
    written_ = base_size_ = static_cast<size_t>(st.st_size);
    rewrite_buf_.clear();
    std::cerr << "AOF rewritten: " << written_ << " bytes\n";
}

}  // namespace kv
