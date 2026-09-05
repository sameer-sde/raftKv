"""
RPC transport layer, server side.

Uses Python's stdlib http.server -- no external dependencies. Each Raft
node runs one of these, listening for RequestVote and AppendEntries calls
from its peers. The actual Raft logic lives elsewhere (raftkv.node.raft);
this class just deserializes requests, calls a handler callback, and
serializes the response.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable


class RPCHandler(BaseHTTPRequestHandler):
    # Set by RPCServer before serving -- avoids needing a custom __init__
    # (BaseHTTPRequestHandler's constructor signature is fixed by http.server).
    request_vote_callback: Callable[[dict], dict] = None
    append_entries_callback: Callable[[dict], dict] = None
    get_state_callback: Callable[[], dict] = None
    submit_callback: Callable[[dict], dict] = None
    get_value_callback: Callable[[dict], dict] = None

    def log_message(self, format, *args):
        pass  # silence default access-log spam; the Tracer handles our logging

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            # Expected under fault injection: a client (e.g. RPCClient with a
            # short timeout, or a peer we're deliberately killing) can close
            # the connection before we finish writing the response. This is
            # normal in a system designed to tolerate unreachable peers, not
            # a bug -- so we swallow it instead of dumping a traceback.
            pass

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw)

    def _send_json(self, data: dict, status: int = 200) -> None:
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        try:
            body = self._read_json_body()
            if self.path == "/request_vote":
                response = self.request_vote_callback(body)
            elif self.path == "/append_entries":
                response = self.append_entries_callback(body)
            elif self.path == "/submit" and self.submit_callback:
                response = self.submit_callback(body)
            elif self.path == "/get" and self.get_value_callback:
                response = self.get_value_callback(body)
            else:
                self._send_json({"error": "unknown endpoint"}, status=404)
                return
            self._send_json(response)
        except Exception as exc:  # noqa: BLE001
            self._send_json({"error": str(exc)}, status=500)

    def do_GET(self):
        if self.path == "/state" and self.get_state_callback:
            self._send_json(self.get_state_callback())
        else:
            self._send_json({"error": "unknown endpoint"}, status=404)


class RPCServer:
    """Wraps ThreadingHTTPServer, running it on a background thread."""

    def __init__(
        self,
        host: str,
        port: int,
        request_vote_callback: Callable[[dict], dict],
        append_entries_callback: Callable[[dict], dict],
        get_state_callback: Callable[[], dict] | None = None,
        submit_callback: Callable[[dict], dict] | None = None,
        get_value_callback: Callable[[dict], dict] | None = None,
    ):
        handler = type(
            "BoundRPCHandler",
            (RPCHandler,),
            {
                "request_vote_callback": staticmethod(request_vote_callback),
                "append_entries_callback": staticmethod(append_entries_callback),
                "get_state_callback": staticmethod(get_state_callback or (lambda: {})),
                "submit_callback": staticmethod(submit_callback or (lambda body: {"accepted": False})),
                "get_value_callback": staticmethod(get_value_callback or (lambda body: {"value": None})),
            },
        )
        self._httpd = ThreadingHTTPServer((host, port), handler)
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
