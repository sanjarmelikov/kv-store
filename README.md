# kv-store

A Redis-compatible in-memory key-value server in C++17: a single-threaded event loop (epoll on Linux, kqueue on macOS), the RESP wire protocol, key expiry, and append-only-file persistence. It works with the standard `redis-cli`, `redis-benchmark`, and client libraries such as redis-py.

```console
$ ./build/kv-store --port 6380 --aof data/appendonly.aof --appendfsync everysec
kv-store listening on 127.0.0.1:6380 (kqueue, AOF on, 0 commands replayed)

$ redis-cli -p 6380 SET greeting hello EX 100
OK
$ redis-cli -p 6380 TTL greeting
(integer) 100
```

## Design

| Piece | File | How it works |
|---|---|---|
| Event loop | `server.cpp`, `poller_*.cpp` | One thread, non-blocking sockets, level-triggered readiness from **epoll** (Linux) or **kqueue** (macOS) behind one `Poller` interface. Commands never block, so no locks are needed. |
| Protocol | `resp.cpp` | Incremental RESP parser: handles partial reads, pipelining (many commands per read), binary-safe values, and inline commands. Parses into a per-connection buffer that's reused, so steady-state parsing doesn't allocate. |
| Keyspace + TTL | `store.cpp` | Hash map plus expiry deadlines. **Lazy** expiry on access, and **active** expiry: every 100 ms, sample 20 keys that have deadlines and delete the expired ones, repeating while more than 25% of a sample was expired (Redis's algorithm). |
| Persistence | `aof.cpp` | Every write is appended as RESP and **written before any reply is sent**, so an acknowledged write is in the OS page cache. `--appendfsync always\|everysec\|no` controls fsync. Relative TTLs are logged as absolute deadlines, so a restart doesn't reset them. A torn last command (crash mid-write) is cut off on startup. |
| Commands | `commands.cpp` | `PING ECHO GET SET (EX PX EXAT PXAT NX XX KEEPTTL) MGET MSET DEL EXISTS INCR DECR INCRBY DECRBY APPEND STRLEN EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST KEYS DBSIZE FLUSHALL FLUSHDB INFO SELECT QUIT`, plus `COMMAND`/`CONFIG`/`CLIENT` stubs that real clients send on connect. |
| AOF rewrite | `aof.cpp` | `BGREWRITEAOF` (and automatically when the AOF doubles past 64 MB): `fork()` a child that writes the current state from a copy-on-write snapshot while the parent keeps serving; writes made meanwhile are appended before an atomic rename. 20,000 overwrites compact to about a dozen entries (tested). |
| Handshake + auth | `server.cpp` | `HELLO 2\|3 [AUTH user pass]` negotiates RESP2 or **RESP3** per connection (redis-py 8 defaults to RESP3). `--requirepass` / `KV_REQUIREPASS` enables `AUTH`; passwords are compared in constant time. |
| Resource limits | `server.cpp` | A client whose unread replies exceed `--max-output-buffer` (default 512 MB) is disconnected, so one slow client can't exhaust memory. Query buffers are capped at 1 GB, as in Redis. |

## Testing

```bash
cmake -S . -B build && cmake --build build && ./build/unit_tests
python3 -m venv .venv && .venv/bin/pip install redis pytest && .venv/bin/pytest tests/
```

- **Unit tests** (C++): parser edge cases (every prefix of a command is "incomplete", binary values, pipelining), TTL with an injected clock, active expiry of 1,000 unread keys, glob matching, every command's replies, and AOF replay including a torn tail. They also pass under AddressSanitizer + UBSan.
- **Integration tests** (redis-py against the real binary): commands, a 1 MB binary value, 10,000 pipelined commands, 50 clients incrementing one counter (exactly 10,000), active expiry observed through `INFO`, restart with AOF replay (TTLs preserved), and protocol errors.
- **Crash test:** a client writes as fast as it can, and after at least 5,000 acknowledged writes the server is killed with `SIGKILL` (no cleanup). After a restart, **every acknowledged write is present: 0 lost, under each of `always`, `everysec` and `no`.** That's expected for a process crash, since acknowledged writes are already in the OS page cache. Surviving a *power* loss is what `always` (fsync before replying) is for.
- **Edge cases** (`tests/test_edge_cases.py`): a command sent one byte at a time, a client vanishing mid-command, every kind of malformed RESP, a 64 KB inline line, 500 simultaneous connections, a client that never reads, interactive `redis-cli`, awkward binary keys, a corrupt AOF, a port already in use.
- **Security** (`tests/test_security.py`): AUTH required with a password, wrong passwords rejected, `redis-cli -a`, the password from the environment, RESP2 and RESP3 clients, the HELLO handshake, the output-buffer cut-off.
- **Linux:** `docker build .` compiles with GCC and runs the unit tests; the container's server reports `epoll` and replays its AOF after `docker restart`.

## Performance

Measured on an Apple M2 (8 GB), both servers on the same machine with persistence off, `redis-benchmark` with 50 clients and 1M random keys, median of 3 runs. Redis 8.10.2 from Homebrew. Commit `808347b`; raw rows in `results/benchmark.jsonl`.

| | kv-store | Redis | kv-store / Redis |
|---|---:|---:|---:|
| SET, pipelined ×16 | 1,481,482/s | 1,137,656/s | 1.30 |
| GET, pipelined ×16 | 1,518,603/s | 1,520,913/s | 1.00 |
| SET, no pipelining | 183,486/s | 194,553/s | 0.94 |
| GET, no pipelining | 172,712/s | 143,609/s | 1.20 |

**Caveats before reading too much into this:**
- **Redis does far more per command:** memory accounting, eviction, keyspace notifications, replication hooks, many data types. kv-store does strings only.
- **Homebrew's Redis on macOS uses the system `malloc`**, not jemalloc as official Linux builds do.
- **Without pipelining, both are limited by network round trips and the benchmark client** (about 180K/s each), so those ratios mostly reflect noise.

### Profiling and removing per-command allocations

`sample` (macOS's profiler) on the server under pipelined load showed about 14% of samples in `malloc`/`free` and about 7% copying a command vector. The AOF entry for every write was being built **even with persistence off**. Fixes (commit `41f4e88`): build AOF entries only when an AOF is open; GET replies straight from the stored string instead of copying it; parse into a reused per-connection buffer; format integers on the stack. A/B against the previous build, alternating 5 runs each under identical conditions (`bench/ab.sh`, `results/ab.jsonl`):

| | Before | After | Change |
|---|---:|---:|---:|
| SET | 1,386,963/s | 1,542,020/s | **+11.2%** |
| GET | 1,488,095/s | 1,568,628/s | **+5.4%** |

GET gained less because benchmark values are 3 bytes, so the removed copy was already cheap (short strings are stored inline, without a heap allocation).

## Limitations

Strings only (no lists, hashes or sets). No replication, eviction, AOF rewrite/compaction, or `MULTI`. `KEYS` scans everything. `everysec` fsyncs on the event-loop thread (Redis uses a background thread).
