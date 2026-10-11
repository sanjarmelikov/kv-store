"""Shared helpers: start the real server binary on a free port."""

import signal
import socket
import subprocess
import time
from pathlib import Path

import pytest
import redis

BINARY = Path(__file__).resolve().parents[1] / "build" / "kv-store"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, aof: Path, fsync: str = "everysec", extra_args: list[str] | None = None):
        self.aof, self.fsync, self.port, self.proc = aof, fsync, free_port(), None
        self.extra_args = extra_args or []

    def start(self) -> "Server":
        self.proc = subprocess.Popen(
            [str(BINARY), "--port", str(self.port), "--aof", str(self.aof), "--appendfsync", self.fsync,
             *self.extra_args],
            stderr=subprocess.PIPE, text=True,
        )
        deadline = time.time() + 5
        while time.time() < deadline:  # wait until it accepts connections
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return self
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("server did not start")

    def client(self, **kw) -> redis.Redis:
        return redis.Redis(port=self.port, **kw)

    def stop(self, sig=signal.SIGTERM) -> str:
        self.proc.send_signal(sig)
        _, err = self.proc.communicate(timeout=10)
        return err


@pytest.fixture
def server(tmp_path):
    s = Server(tmp_path / "test.aof").start()
    yield s
    if s.proc.poll() is None:
        s.stop()
