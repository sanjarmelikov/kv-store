"""Authentication and resource limits."""

import socket
import subprocess
import time

import pytest
import redis

from conftest import BINARY, Server, free_port


@pytest.fixture
def secured(tmp_path):
    s = Server(tmp_path / "a.aof", extra_args=["--requirepass", "s3cret"]).start()
    yield s
    s.stop()


def test_commands_require_auth(secured):
    with pytest.raises(redis.AuthenticationError):
        secured.client().get("k")
    with pytest.raises(redis.AuthenticationError):
        secured.client(password="wrong").ping()
    r = secured.client(password="s3cret")
    assert r.set("k", "v") and r.get("k") == b"v"
    assert secured.client(username="default", password="s3cret").ping()


def test_redis_cli_with_password(secured):
    out = subprocess.run(["redis-cli", "-p", str(secured.port), "--no-auth-warning", "-a", "s3cret", "PING"],
                         capture_output=True, text=True, timeout=10)
    assert out.stdout.strip() == "PONG"
    out = subprocess.run(["redis-cli", "-p", str(secured.port), "PING"], capture_output=True, text=True, timeout=10)
    assert "NOAUTH" in out.stdout


def test_password_from_environment(tmp_path):
    port = free_port()
    proc = subprocess.Popen([str(BINARY), "--port", str(port)], env={"KV_REQUIREPASS": "envpass"},
                            stderr=subprocess.PIPE)
    try:
        time.sleep(0.3)
        assert redis.Redis(port=port, password="envpass").ping()
        with pytest.raises(redis.AuthenticationError):
            redis.Redis(port=port).ping()
    finally:
        proc.terminate()
        proc.wait()


def test_auth_without_a_configured_password(server):
    with pytest.raises(redis.ResponseError, match="without any password configured"):
        server.client().execute_command("AUTH", "x")


def test_client_that_never_reads_is_disconnected_at_the_limit(tmp_path):
    s = Server(tmp_path / "b.aof", extra_args=["--max-output-buffer", str(4 * 1024 * 1024)]).start()
    try:
        r = s.client()
        r.set("big", b"x" * 1_000_000)
        hog = socket.create_connection(("127.0.0.1", s.port))
        hog.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        hog.sendall(b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n" * 50)  # 50 MB of replies, never read
        time.sleep(0.5)
        assert r.ping()  # everyone else is fine
        hog.settimeout(5)
        got = 0
        while True:  # drain what was sent before the cut-off; the server closes the connection
            chunk = hog.recv(1 << 20)
            if not chunk:
                break
            got += len(chunk)
        assert got < 50_000_000  # cut off well before all 50 replies
    finally:
        s.stop()


def test_bad_fsync_policy_prints_usage_not_a_crash():
    proc = subprocess.run([str(BINARY), "--appendfsync", "sometimes"], capture_output=True, text=True, timeout=10)
    assert proc.returncode == 2 and "fsync policy" in proc.stderr


@pytest.mark.parametrize("protocol", [2, 3])
def test_both_protocol_versions(server, protocol):
    r = server.client(protocol=protocol)  # redis-py 8 negotiates with HELLO
    assert r.set("k", "v") and r.get("k") == b"v"
    assert r.get("missing") is None and r.mget("k", "missing") == [b"v", None]


def test_hello_handshake_and_errors(server):
    s = socket.create_connection(("127.0.0.1", server.port))
    s.sendall(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n*2\r\n$3\r\nGET\r\n$1\r\nx\r\n*2\r\n$5\r\nHELLO\r\n$1\r\n9\r\n")
    time.sleep(0.2)
    data = s.recv(4096)
    assert data.startswith(b"%7\r\n") and b"$5\r\nproto\r\n:3\r\n" in data
    assert b"\r\n_\r\n" in data  # RESP3 null for the missing key
    assert data.endswith(b"-NOPROTO sorry, this protocol version is not supported.\r\n")


def test_redis_cli_resp3(server):
    out = subprocess.run(["redis-cli", "-3", "-p", str(server.port), "GET", "nope"], capture_output=True,
                         text=True, timeout=10)
    assert out.returncode == 0 and out.stdout.strip() in ("", "(nil)")  # nil prints blank when piped
