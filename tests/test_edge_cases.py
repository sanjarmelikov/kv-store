"""Edge cases and hostile inputs: the server must stay correct and responsive."""

import socket
import subprocess
import time

import pytest
import redis

from conftest import BINARY, free_port


def raw(port: int) -> socket.socket:
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    return s


def recv_until(s: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = s.recv(65536)
        if not chunk:
            break
        data += chunk
    return data


def still_alive(server) -> bool:
    return server.client().ping() is True


# ---- input arriving in odd shapes ----

def test_command_split_into_single_bytes(server):
    s = raw(server.port)
    for b in b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n":
        s.sendall(bytes([b]))
        time.sleep(0.001)
    assert recv_until(s, 5) == b"+OK\r\n"
    assert server.client().get("k") == b"hello"


def test_client_disconnects_mid_command(server):
    s = raw(server.port)
    s.sendall(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100\r\nonly part of the val")
    s.close()
    time.sleep(0.1)
    assert still_alive(server) and server.client().get("k") is None


def test_pipeline_with_protocol_error_in_the_middle(server):
    s = raw(server.port)
    s.sendall(b"*1\r\n$4\r\nPING\r\n*1\r\n$x\r\n*1\r\n$4\r\nPING\r\n")
    data = recv_until(s, 100)
    assert data.startswith(b"+PONG\r\n-ERR Protocol error")  # first reply kept, then error + close
    assert b"PONG" not in data[7:]
    assert still_alive(server)


@pytest.mark.parametrize("payload", [
    b"*1\r\n$-5\r\n",            # negative bulk length
    b"*abc\r\n",                  # non-numeric array length
    b"*1\r\n$99999999999\r\n",    # bulk longer than the 512 MB limit
    b"*99999999\r\n",             # more arguments than allowed
    b"*1\r\n:5\r\n",              # integer where a bulk string must be
])
def test_malformed_input_gets_an_error_not_a_crash(server, payload):
    s = raw(server.port)
    s.sendall(payload)
    assert recv_until(s, 5).startswith(b"-ERR Protocol error")
    assert still_alive(server)


def test_endless_inline_line_is_cut_off(server):
    s = raw(server.port)
    s.sendall(b"A" * 70_000)  # no newline, over the 64 KB inline limit
    assert recv_until(s, 5).startswith(b"-ERR Protocol error")
    assert still_alive(server)


def test_blank_lines_and_inline_commands(server):
    s = raw(server.port)
    s.sendall(b"\r\n\r\nping\r\nECHO hi\r\n")
    assert recv_until(s, 15) == b"+PONG\r\n$2\r\nhi\r\n"


# ---- arguments ----

def test_argument_validation(server):
    r = server.client()
    for bad in (["SET", "k"], ["GET"], ["MSET", "a"], ["DEL"], ["INCRBY", "k"], ["EXPIRE", "k"]):
        with pytest.raises(redis.ResponseError, match="wrong number of arguments"):
            r.execute_command(*bad)
    with pytest.raises(redis.ResponseError, match="not an integer"):
        r.execute_command("INCRBY", "n", "1.5")
    with pytest.raises(redis.ResponseError, match="invalid expire"):
        r.execute_command("SET", "k", "v", "EX", "-1")
    with pytest.raises(redis.ResponseError, match="syntax error"):
        r.execute_command("SET", "k", "v", "NX", "XX")
    with pytest.raises(redis.ResponseError, match="syntax error"):
        r.execute_command("SET", "k", "v", "EX", "10", "PX", "100")  # two expiry options
    with pytest.raises(redis.ResponseError, match="out of range"):
        r.execute_command("SELECT", "1")
    with pytest.raises(redis.ResponseError, match="unknown command"):
        r.execute_command("HSET", "h", "f", "v")


def test_options_are_case_insensitive(server):
    r = server.client()
    assert r.execute_command("set", "k", "v", "ex", "100") is True  # redis-py turns +OK into True
    assert r.execute_command("tTl", "k") == 100


def test_negative_expire_deletes_the_key(server):
    r = server.client()
    r.set("k", "v")
    assert r.expire("k", -1) is True and r.exists("k") == 0  # same as Redis


def test_keys_with_awkward_bytes(server):
    r = server.client()
    weird = [b"", b" ", b"\r\n", b"\x00\xff", "ключ".encode(), b"*?[]"]
    for i, k in enumerate(weird):
        r.set(k, i)
    for i, k in enumerate(weird):
        assert r.get(k) == str(i).encode()
    assert r.keys("\\*\\?\\[\\]") == [b"*?[]"]  # escaped glob metacharacters match literally


def test_quit_closes_after_reply(server):
    s = raw(server.port)
    s.sendall(b"*1\r\n$4\r\nQUIT\r\n*1\r\n$4\r\nPING\r\n")
    assert recv_until(s, 100) == b"+OK\r\n"  # nothing after QUIT is executed


# ---- many clients, slow clients ----

def test_hundreds_of_simultaneous_connections(server):
    socks = [raw(server.port) for _ in range(500)]
    for i, s in enumerate(socks):
        s.sendall(b"*2\r\n$4\r\nINCR\r\n$1\r\nc\r\n")
    replies = [recv_until(s, 4) for s in socks]
    assert all(r.startswith(b":") for r in replies)
    assert server.client().get("c") == b"500"
    for s in socks:
        s.close()


def test_client_that_never_reads_does_not_block_others(server):
    r = server.client()
    r.set("big", b"x" * 1_000_000)
    hog = raw(server.port)
    hog.sendall(b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n" * 200)  # 200 MB of replies it never reads
    time.sleep(0.5)
    start = time.perf_counter()
    assert still_alive(server)
    assert time.perf_counter() - start < 1.0  # other clients are still served promptly
    hog.close()


# ---- interactive redis-cli ----

def test_interactive_redis_cli(server):
    # redis-cli in interactive mode: it sends COMMAND DOCS on connect, then each line.
    # (QUIT is handled by redis-cli itself and prints nothing, exactly as with real Redis.)
    script = "SET greeting hello\nGET greeting\nINCR n\nQUIT\n"
    out = subprocess.run(["redis-cli", "-p", str(server.port)], input=script, capture_output=True,
                         text=True, timeout=10)
    assert out.returncode == 0 and out.stdout.split() == ["OK", "hello", "1"]


# ---- startup problems ----

def test_corrupt_aof_refuses_to_start(tmp_path):
    aof = tmp_path / "bad.aof"
    aof.write_bytes(b"*1\r\n$4\r\nPING\r\nGARBAGE\r\n")
    proc = subprocess.run([str(BINARY), "--port", str(free_port()), "--aof", str(aof)],
                          capture_output=True, text=True, timeout=10)
    assert proc.returncode == 1 and "AOF corrupt" in proc.stderr


def test_port_in_use_and_bad_aof_directory_fail_cleanly(tmp_path):
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        proc = subprocess.run([str(BINARY), "--port", str(port)], capture_output=True, text=True, timeout=10)
        assert proc.returncode == 1 and "bind" in proc.stderr
    proc = subprocess.run([str(BINARY), "--port", str(free_port()), "--aof", "/nonexistent/dir/x.aof"],
                          capture_output=True, text=True, timeout=10)
    assert proc.returncode == 1 and "cannot open AOF" in proc.stderr


def test_bad_command_line(tmp_path):
    proc = subprocess.run([str(BINARY), "--appendfsync", "sometimes"], capture_output=True, text=True, timeout=10)
    assert proc.returncode != 0
