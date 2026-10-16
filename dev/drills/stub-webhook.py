# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Stub HTTP webhook receiver for `leeway watch` e2e tests and drills.

Implements the two webhook endpoints (`POST /incidents` and
`POST /incidents/<id>/events`) as well as the core-agent attach endpoints
(`POST /sessions` and `POST /sessions/<sid>/inject`) and logs every request
body to stdout as single greppable lines:

    INCIDENT-OPEN id=stub-inc-0001 kind=<payload kind> token=<present|MISSING> body=<full JSON body>
    INCIDENT-EVENT id=stub-inc-0001 kind=<payload kind> token=<present|MISSING> body=<full JSON body>
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

counter = 0
INJECT_MAX_BYTES = 8 * 1024


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _reply_text(self, code, text):
        body = (text + "\n").encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/healthz":
            self._reply(200, {"status": "ok"})
            return
        self._reply(404, {"error": "unknown path"})

    def do_POST(self):
        global counter
        length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(length)
        raw = body_bytes.decode(errors="replace")
        token = "present" if self.headers.get("Authorization") else "MISSING"

        if self.path == "/incidents":
            counter += 1
            inc_id = "stub-inc-%04d" % counter
            kind = ""
            try:
                kind = json.loads(raw).get("kind", "")
            except (ValueError, AttributeError):
                pass
            if len(body_bytes) > INJECT_MAX_BYTES:
                print(
                    "REJECT id=%s bytes=%d limit=%d kind_rejected=%s head=%s"
                    % (inc_id, len(body_bytes), INJECT_MAX_BYTES, kind, raw[:200]),
                    flush=True,
                )
                self._reply_text(400, "request body too large (max %d bytes)" % INJECT_MAX_BYTES)
                return
            print(
                "INCIDENT-OPEN id=%s kind=%s token=%s body=%s"
                % (inc_id, kind, token, raw),
                flush=True,
            )
            self._reply(201, {"id": inc_id})
            return

        if self.path.startswith("/incidents/") and self.path.endswith("/events"):
            inc_id = self.path.split("/")[2]
            kind = ""
            try:
                kind = json.loads(raw).get("kind", "")
            except (ValueError, AttributeError):
                pass
            if len(body_bytes) > INJECT_MAX_BYTES:
                print(
                    "REJECT id=%s bytes=%d limit=%d kind_rejected=%s head=%s"
                    % (inc_id, len(body_bytes), INJECT_MAX_BYTES, kind, raw[:200]),
                    flush=True,
                )
                self._reply_text(400, "request body too large (max %d bytes)" % INJECT_MAX_BYTES)
                return
            print(
                "INCIDENT-EVENT id=%s kind=%s token=%s body=%s"
                % (inc_id, kind, token, raw),
                flush=True,
            )
            self._reply(200, {})
            return

        if self.path == "/sessions":
            counter += 1
            sid = "stub-sess-%04d" % counter
            caller = self.headers.get("X-Asserted-Caller", "")
            print("SESSION-CREATE sid=%s caller=%s token=%s" % (sid, caller, token), flush=True)
            self._reply(201, {"sessionID": sid})
            return

        if self.path.startswith("/sessions/") and self.path.endswith("/inject"):
            sid = self.path.split("/")[2]
            kind = ""
            try:
                msg = json.loads(raw).get("message", "")
                kind = json.loads(msg).get("kind", "")
            except (ValueError, AttributeError):
                pass
            print("INJECT sid=%s kind=%s token=%s body=%s" % (sid, kind, token, raw), flush=True)
            self._reply(200, {})
            return

        print("UNEXPECTED %s %s body=%s" % (self.command, self.path, raw), flush=True)
        self._reply(404, {"error": "unknown path"})


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7777
    print("stub-webhook listening on :%d" % port, flush=True)
    HTTPServer(("", port), Handler).serve_forever()
