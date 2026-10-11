"""End-to-end tests: the real server binary, driven by redis-py (the standard Python Redis client).

    cmake --build build && .venv/bin/pytest tests/
"""

import os
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
import redis

from conftest import Server

def test_commands_through_redis_py(server):
    r = server.client(decode_responses=True)
    assert r.ping() is True
    assert r.set("k", "v") and r.get("k") == "v"
    assert r.set("k", "x", nx=True) is None and r.get("k") == "v"
    assert r.incr("n") == 1 and r.incrby("n", 41) == 42 and r.decr("n") == 41
    assert r.mset({"a": "1", "b": "2"}) and r.mget("a", "nope", "b") == ["1", None, "2"]
    assert r.exists("a", "b", "nope") == 2 and r.delete("a", "nope") == 1
    assert r.set("t", "v", ex=100) and r.ttl("t") == 100 and r.persist("t") and r.ttl("t") == -1
    assert r.append("k", "!!") == 3 and r.strlen("k") == 3
    assert sorted(r.keys("*")) == ["b", "k", "n", "t"] and r.dbsize() == 4
    with pytest.raises(redis.ResponseError, match="not an integer"):
        r.incr("k")
    assert r.flushall() and r.dbsize() == 0


def test_binary_safe_and_large_values(server):
    r = server.client()
    blob = os.urandom(1024 * 1024) + b"\r\n\x00tail"  # 1 MB of random bytes, CRLF and NUL inside
    assert r.set(b"bin\x00key", blob) and r.get(b"bin\x00key") == blob


def test_pipelining(server):
    r = server.client()
    pipe = r.pipeline(transaction=False)
    for i in range(10_000):
        pipe.set(f"p{i}", i)
    pipe.get("p9999")
    replies = pipe.execute()  # 10,001 commands sent back to back, replies read after
    assert replies[-1] == b"9999" and all(replies[:-1]) and r.dbsize() == 10_000


def test_many_clients_increment_atomically(server):
    # 50 clients x 200 INCRs each: the single-threaded loop runs commands one at a
    # time, so no increment can be lost to a race.
    def work():
        r = server.client()
        for _ in range(200):
            r.incr("counter")

    threads = [threading.Thread(target=work) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert int(server.client().get("counter")) == 10_000


def test_active_expiry_removes_keys_nobody_reads(server):
    r = server.client(decode_responses=True)
    pipe = r.pipeline(transaction=False)
    for i in range(2000):
        pipe.set(f"e{i}", "v", px=100)
    pipe.set("keep", "v")
    pipe.execute()
    assert r.info()["db0"]["keys"] == 2001  # redis-py parses our INFO reply like real Redis's
    time.sleep(1.5)  # no reads: only the background sampler can remove them
    keys_left = r.info()["db0"]["keys"]
    assert keys_left == 1, keys_left


def test_restart_replays_aof_and_keeps_deadlines(server, tmp_path):
    r = server.client(decode_responses=True)
    for i in range(1000):
        r.set(f"k{i}", i)
    r.set("ttl", "v", ex=100)
    r.incr("k0")
    r.delete("k1")
    server.stop()  # graceful: flushes and fsyncs the AOF

    again = Server(server.aof).start()
    try:
        r2 = again.client(decode_responses=True)
        assert r2.dbsize() == 1000  # 1000 keys + ttl - deleted k1
        assert r2.get("k0") == "1" and r2.get("k1") is None and r2.get("k999") == "999"
        assert 98 <= r2.ttl("ttl") <= 100  # absolute deadline: the clock didn't restart
    finally:
        again.stop()


@pytest.mark.parametrize("fsync", ["always", "everysec", "no"])
def test_kill_9_loses_no_acknowledged_write(tmp_path, fsync):
    """Crash test: kill -9 the server while a client is writing; every write that got
    an OK before the crash must be there after a restart."""
    server = Server(tmp_path / "crash.aof", fsync).start()
    acked = []
    stop = threading.Event()

    def writer():
        r = server.client()
        i = 0
        while not stop.is_set():
            try:
                r.set(f"key{i}", i)
            except redis.ConnectionError:
                return  # the server died; this write was never acknowledged
            acked.append(i)
            i += 1

    t = threading.Thread(target=writer)
    t.start()
    while len(acked) < 2000:
        time.sleep(0.01)
    server.proc.send_signal(signal.SIGKILL)  # no cleanup, no final flush
    server.proc.wait()
    stop.set()
    t.join()

    again = Server(server.aof, fsync).start()
    try:
        r = again.client()
        values = r.mget([f"key{i}" for i in acked])
        lost = [i for i, v in zip(acked, values) if v != str(i).encode()]
        assert not lost, f"{len(lost)} of {len(acked)} acknowledged writes lost"
    finally:
        again.stop()


def test_protocol_error_closes_connection(server):
    with socket.create_connection(("127.0.0.1", server.port)) as s:
        s.sendall(b"*1\r\n$abc\r\n")
        assert s.recv(100).startswith(b"-ERR Protocol error")
        assert s.recv(100) == b""  # server closed the connection
