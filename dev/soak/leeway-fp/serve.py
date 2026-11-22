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

"""Serve dev/drills/stub-webhook.py's handler on a threaded server.

The drill stub runs a single-threaded HTTPServer with HTTP/1.1 keep-alive,
so one idle watcher connection can block concurrent requests during bursts.
A soak needs every POST /incidents and POST /incidents/<id>/events on the
wire, so this serves the same Handler, unmodified, one thread per connection.
POSTs are serialized under a lock because the stub's incident counter and
printed lines are not thread-safe.

Usage: serve.py <stub-webhook.py> <port>
"""

import importlib.util
import sys
import threading
from http.server import ThreadingHTTPServer

spec = importlib.util.spec_from_file_location("stub_webhook", sys.argv[1])
stub = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stub)

lock = threading.Lock()


class Handler(stub.Handler):
    def do_POST(self):
        with lock:
            super().do_POST()


if __name__ == "__main__":
    port = int(sys.argv[2])
    print("stub webhook listening on :%d (threaded)" % port, flush=True)
    ThreadingHTTPServer(("", port), Handler).serve_forever()
