"""AOF rewrite (compaction): fork, snapshot, merge concurrent writes, atomic swap."""

import time

import pytest
import redis

from conftest import Server


def wait_for_size_drop(path, before, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.stat().st_size < before:
            return path.stat().st_size
        time.sleep(0.05)
    raise AssertionError("AOF did not shrink")


def test_rewrite_compacts_and_keeps_concurrent_writes(tmp_path):
    s = Server(tmp_path / "c.aof", extra_args=["--auto-aof-rewrite-percentage", "0"]).start()
    r = s.client()
    pipe = r.pipeline(transaction=False)
    for i in range(20_000):
        pipe.set(f"k{i % 10}", i)  # 10 keys, each overwritten 2,000 times
    pipe.execute()
    r.set("ttl", "v", ex=100)
    before = s.aof.stat().st_size
    assert r.bgrewriteaof()
    for i in range(50):  # writes racing the rewrite must not be lost
        r.set(f"during{i}", i)
    after = wait_for_size_drop(s.aof, before)
    assert after < before / 50  # ~20,000 SETs collapse to about a dozen
    r.set("after", "yes")  # appended to the new file
    s.stop()

    again = Server(s.aof).start()
    try:
        r2 = again.client()
        assert r2.get("k9") == b"19999" and r2.get("during49") == b"49" and r2.get("after") == b"yes"
        assert 95 <= r2.ttl("ttl") <= 100  # deadline survived the rewrite
        assert r2.dbsize() == 10 + 1 + 50 + 1
    finally:
        again.stop()


def test_second_rewrite_while_running_is_refused_or_fine(tmp_path):
    s = Server(tmp_path / "d.aof", extra_args=["--auto-aof-rewrite-percentage", "0"]).start()
    r = s.client()
    r.set("a", "1")
    assert r.bgrewriteaof()
    try:
        r.bgrewriteaof()  # may still be running (refused) or already done (started again)
    except redis.ResponseError as e:
        assert "already in progress" in str(e)
    s.stop()


def test_auto_rewrite_when_the_aof_doubles(tmp_path):
    s = Server(tmp_path / "e.aof", extra_args=["--auto-aof-rewrite-min-size", "200000"]).start()
    r = s.client()
    pipe = r.pipeline(transaction=False)
    for i in range(20_000):  # ~600 KB of log for one key
        pipe.set("same", i)
    pipe.execute()
    time.sleep(1.0)  # the loop notices the growth, forks, and swaps
    assert s.aof.stat().st_size < 1000
    assert r.get("same") == b"19999"
    s.stop()


def test_rewrite_without_aof(tmp_path):
    from conftest import BINARY, free_port
    import subprocess
    port = free_port()
    proc = subprocess.Popen([str(BINARY), "--port", str(port)], stderr=subprocess.DEVNULL)
    try:
        time.sleep(0.3)
        with pytest.raises(redis.ResponseError, match="AOF is not enabled"):
            redis.Redis(port=port).bgrewriteaof()
    finally:
        proc.terminate()
        proc.wait()
