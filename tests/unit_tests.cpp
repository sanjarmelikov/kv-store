// Unit tests for the parser, store, commands and AOF. No framework: CHECK records
// failures and main() reports them, so the build has no dependencies.
#include <unistd.h>

#include <cstdio>
#include <iostream>
#include <string>

#include "aof.h"
#include "commands.h"
#include "resp.h"
#include "store.h"

static int failures = 0;
#define CHECK(cond)                                                                    \
    do {                                                                               \
        if (!(cond)) {                                                                 \
            ++failures;                                                                \
            std::cerr << __FILE__ << ":" << __LINE__ << ": CHECK failed: " #cond "\n"; \
        }                                                                              \
    } while (0)

using namespace kv;

static Millis fake_now = 1'000'000;
static Millis fake_clock() { return fake_now; }

static std::string run(Engine& e, Command cmd) {
    std::string out;
    e.execute(cmd, out);
    return out;
}

void test_parser() {
    auto r = parse_command("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    CHECK(r.status == ParseStatus::Ok && r.command == Command({"GET", "foo"}) && r.consumed == 22);

    // Every strict prefix of a valid command is "incomplete", never an error or a wrong parse.
    std::string full = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n";
    for (size_t i = 0; i < full.size(); ++i) CHECK(parse_command(full.substr(0, i)).status == ParseStatus::Incomplete);

    // Pipelining: two commands in one buffer parse one after the other.
    std::string two = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
    auto first = parse_command(two);
    CHECK(first.status == ParseStatus::Ok);
    CHECK(parse_command(std::string_view(two).substr(first.consumed)).status == ParseStatus::Ok);

    // Binary-safe: values may contain \r\n and NUL bytes.
    std::string bin("*2\r\n$4\r\nECHO\r\n$4\r\na\r\n\0\r\n", 24);  // 24 bytes incl. the NUL
    CHECK(parse_command(bin).command[1] == std::string("a\r\n\0", 4));

    CHECK(parse_command("PING\r\n").command == Command({"PING"}));      // inline
    CHECK(parse_command("set  a   b\n").command == Command({"set", "a", "b"}));
    CHECK(parse_command("*1\r\n$x\r\n").status == ParseStatus::Error);
    CHECK(parse_command("*1\r\n$3\r\nabcXY").status == ParseStatus::Error);  // bad terminator
    CHECK(parse_command("*-5\r\n").status == ParseStatus::Error);
    CHECK(encode_command({"SET", "a", "b"}) == "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n");
}

void test_store_ttl() {
    fake_now = 1'000'000;
    Store s(fake_clock);
    s.set("a", "1");
    s.set("b", "2", fake_now + 100);
    CHECK(s.ttl_ms("a") == -1 && s.ttl_ms("b") == 100 && s.ttl_ms("zzz") == -2);
    fake_now += 100;
    CHECK(!s.get("b"));  // lazy expiry on read
    CHECK(s.size() == 1);

    // Active expiry: 1000 keys expire, nobody reads them, the sampler still removes them.
    for (int i = 0; i < 1000; ++i) s.set("k" + std::to_string(i), "v", fake_now + 10);
    s.set("keep", "v", fake_now + 1'000'000);
    fake_now += 10;
    size_t removed = 0;
    for (int i = 0; i < 200 && s.volatile_count() > 1; ++i) removed += s.expire_cycle();
    CHECK(removed == 1000 && s.volatile_count() == 1);

    s.set("p", "v", fake_now + 50);
    CHECK(s.persist("p") && s.ttl_ms("p") == -1 && !s.persist("p"));
    s.set("p", "v", fake_now + 50);
    s.set("p", "w");  // plain SET clears the deadline
    CHECK(s.ttl_ms("p") == -1);
}

void test_glob() {
    CHECK(glob_match("*", "anything") && glob_match("user:*", "user:42") && !glob_match("user:*", "usr:1"));
    CHECK(glob_match("h?llo", "hello") && glob_match("h[ae]llo", "hallo") && !glob_match("h[^e]llo", "hello"));
    CHECK(glob_match("h[a-c]t", "hbt") && glob_match("a\\*b", "a*b") && !glob_match("a\\*b", "axb"));
    CHECK(glob_match("*a*b*", "xxaYYbzz") && !glob_match("*a*b", "xxaYYbzz"));
}

void test_commands() {
    fake_now = 5'000'000;
    Store s(fake_clock);
    Engine e(s);
    CHECK(run(e, {"PING"}) == "+PONG\r\n");
    CHECK(run(e, {"set", "k", "v"}) == "+OK\r\n");  // command names are case-insensitive
    CHECK(run(e, {"GET", "k"}) == "$1\r\nv\r\n");
    CHECK(run(e, {"GET", "nope"}) == "$-1\r\n");
    CHECK(run(e, {"SET", "k", "x", "NX"}) == "$-1\r\n");  // exists, NX refuses
    CHECK(run(e, {"SET", "new", "x", "XX"}) == "$-1\r\n");
    CHECK(run(e, {"SET", "t", "v", "EX", "10"}) == "+OK\r\n" && run(e, {"TTL", "t"}) == ":10\r\n");
    CHECK(run(e, {"SET", "t", "v", "EX", "0"}).rfind("-ERR invalid expire", 0) == 0);
    CHECK(run(e, {"INCR", "n"}) == ":1\r\n" && run(e, {"INCRBY", "n", "41"}) == ":42\r\n");
    CHECK(run(e, {"DECRBY", "n", "2"}) == ":40\r\n" && run(e, {"DECR", "n"}) == ":39\r\n");
    CHECK(run(e, {"INCR", "k"}).rfind("-ERR value is not an integer", 0) == 0);
    run(e, {"SET", "max", "9223372036854775807"});
    CHECK(run(e, {"INCR", "max"}).rfind("-ERR increment", 0) == 0);
    CHECK(run(e, {"MSET", "a", "1", "b", "2"}) == "+OK\r\n");
    CHECK(run(e, {"MGET", "a", "zz", "b"}) == "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n");
    CHECK(run(e, {"APPEND", "a", "23"}) == ":3\r\n" && run(e, {"STRLEN", "a"}) == ":3\r\n");
    CHECK(run(e, {"EXISTS", "a", "b", "zz"}) == ":2\r\n" && run(e, {"DEL", "a", "zz"}) == ":1\r\n");
    CHECK(run(e, {"EXPIRE", "b", "5"}) == ":1\r\n" && run(e, {"PTTL", "b"}) == ":5000\r\n");
    CHECK(run(e, {"PERSIST", "b"}) == ":1\r\n" && run(e, {"TTL", "b"}) == ":-1\r\n");
    run(e, {"SET", "ttl", "1", "PX", "100"});
    CHECK(run(e, {"INCR", "ttl"}) == ":2\r\n" && run(e, {"PTTL", "ttl"}) == ":100\r\n");  // INCR keeps TTL
    CHECK(run(e, {"KEYS", "m*"}) == "*1\r\n$3\r\nmax\r\n");
    CHECK(run(e, {"GET"}) == "-ERR wrong number of arguments for 'get' command\r\n");
    CHECK(run(e, {"BOGUS"}) == "-ERR unknown command 'BOGUS'\r\n");
    CHECK(run(e, {"FLUSHALL"}) == "+OK\r\n" && run(e, {"DBSIZE"}) == ":0\r\n");
}

void test_aof_round_trip_and_torn_tail() {
    char path[] = "/tmp/kvstore_aof_XXXXXX";
    int fd = mkstemp(path);
    close(fd);
    fake_now = 9'000'000;
    {
        Store s(fake_clock);
        Aof aof(path, FsyncPolicy::No);
        Engine e(s, &aof);
        run(e, {"SET", "a", "1"});
        run(e, {"SET", "ttl", "x", "EX", "100"});  // logged with an absolute deadline
        run(e, {"INCR", "a"});
        run(e, {"DEL", "missing"});  // deletes nothing: not logged
        run(e, {"SET", "gone", "1"});
        run(e, {"DEL", "gone"});
        aof.flush();
    }
    // Simulate a crash mid-write: half a command at the end of the file.
    FILE* f = fopen(path, "a");
    fputs("*3\r\n$3\r\nSET\r\n$1\r\nz", f);
    fclose(f);

    fake_now += 50'000;  // restart 50 seconds later
    Store s(fake_clock);
    Aof aof(path, FsyncPolicy::No);
    Engine e(s, &aof);
    size_t n = aof.replay([&](const Command& c) { e.replay(c); });
    CHECK(n == 5);  // SET, SET PXAT, SET KEEPTTL (from INCR), SET gone, DEL gone
    CHECK(s.get("a") == std::optional<std::string>("2"));
    CHECK(s.ttl_ms("ttl") == 50'000);  // 100s deadline, 50s later: the clock didn't restart
    CHECK(!s.exists("gone") && !s.exists("z"));
    unlink(path);
}

int main() {
    test_parser();
    test_store_ttl();
    test_glob();
    test_commands();
    test_aof_round_trip_and_torn_tail();
    if (failures) {
        std::cerr << failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all unit tests passed\n";
    return 0;
}
