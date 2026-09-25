"""Gateway in front of multiple firefox-auto-turnstile worker containers.

Client-facing API (port 8081), drop-in compatible with a single worker:
  POST /solve   {url, sitekey, timeout?, proxy?}  -> long-poll, returns token
                plus a "solver" field naming the worker that handled it.
  GET  /status  -> aggregated worker statuses
  GET  /coords  -> merged calibration coords from all workers
  GET  /stats   -> HTML page: per-solver request count and average solve time
  GET  /healthz -> liveness probe

Scheduling: on each /solve, workers are probed via GET /status and an idle
one (current == null) is chosen; if all are busy, the request is forwarded
to the worker whose in-flight task started most recently (likely to finish
first), and its native 409 semantics apply: the gateway retries the next
worker on 409, and returns the last 409 if none accepts.

Workers run the upstream image unmodified. Calibration coords are shared
across workers by the gateway: a background thread periodically reads
GET /coords from every worker, merges the entries (newest ts per hostname
wins), and pushes the merge into every worker's /config/relay/coords.json
via the bind-mounted shared directory -- the upstream API reads coords.json
on every task, so the sync only needs to keep the file fresh, not notify
anyone. The upstream writer uses atomic replace, and the sync writer does
too, so a file observed by the API is always complete JSON.

Config (env): WORKERS="name1:host:port,name2:host:port,..." (default
worker-1..worker-3 on port 8081), COORDS_SYNC_DIR (default /shared-coords,
set to "" to disable syncing), SYNC_INTERVAL (seconds, default 3).
"""

import html
import json
import os
import threading
import time
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8081
MAX_TIMEOUT = 300
SYNC_INTERVAL = float(os.environ.get("SYNC_INTERVAL", "3"))
COORDS_SYNC_DIR = os.environ.get("COORDS_SYNC_DIR", "/shared-coords")


def _parse_workers():
    raw = os.environ.get(
        "WORKERS", "worker-1:worker-1:8081,worker-2:worker-2:8081,"
                   "worker-3:worker-3:8081")
    workers = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) == 2:  # "name:port" with host == name (compose DNS)
            name, host, port = parts[0], parts[0], int(parts[1])
        elif len(parts) == 3:
            name, host, port = parts[0], parts[1], int(parts[2])
        else:
            raise ValueError("bad WORKERS entry: %r" % item)
        workers.append({"name": name, "host": host, "port": port})
    if not workers:
        raise ValueError("WORKERS is empty")
    return workers


WORKERS = _parse_workers()

# stats[solver] = {"count": int, "total_elapsed": float, "last": {...}}
_stats = {}
_stats_lock = threading.Lock()
_sync_lock = threading.Lock()  # one coords sync pass at a time


# --------------------------------------------------------------------------
# HTTP helpers (stdlib http.client; requests is not available in the image)
# --------------------------------------------------------------------------

def _request(worker, method, path, body=None, timeout=10):
    """One HTTP round-trip to a worker. Returns (status, json-or-None)."""
    conn = http.client.HTTPConnection(worker["host"], worker["port"],
                                      timeout=timeout)
    try:
        payload = None
        headers = {}
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        conn.request(method, path, payload, headers)
        resp = conn.getresponse()
        data = resp.read()
        try:
            obj = json.loads(data.decode("utf-8")) if data else None
        except ValueError:
            obj = None
        return resp.status, obj
    finally:
        conn.close()


def _probe(worker):
    """GET /status; returns the status dict or None if unreachable."""
    try:
        code, obj = _request(worker, "GET", "/status")
    except (OSError, http.client.HTTPException):
        return None
    if code == 200 and isinstance(obj, dict):
        return obj
    return None


def _pick_worker():
    """Choose the worker for a new /solve.

    Prefer an idle worker (current == null). Among busy ones, prefer the
    one whose task started most recently (smallest "created"): it is the
    most likely to free up soon, and if it 409s we move to the next.
    Returns a list of all workers ordered by preference (the caller
    retries down the list on 409/503).
    """
    scored = []
    for w in WORKERS:
        st = _probe(w)
        if st is None:
            continue  # unreachable: don't even try to send the task there
        cur = st.get("current")
        if cur is None:
            scored.append((0, 0.0, w))  # idle: always first
        else:
            try:
                created = float(cur.get("created", 0))
            except (TypeError, ValueError):
                created = 0.0
            scored.append((1, created, w))  # busy: newest task first
    scored.sort(key=lambda t: (t[0], t[1]))
    return [t[2] for t in scored]


# --------------------------------------------------------------------------
# Coords sharing: merge every worker's coords.json, write the merge into
# the shared dir, and (re)install it into workers whose file differs.
# --------------------------------------------------------------------------

def _merge_coords(all_coords):
    """Newest calibration per hostname wins."""
    merged = {}
    for coords in all_coords:
        if not isinstance(coords, dict):
            continue
        for hostname, entry in coords.items():
            if not isinstance(entry, dict):
                continue
            try:
                ts = int(entry.get("ts", 0))
            except (TypeError, ValueError):
                ts = 0
            if hostname not in merged or ts >= merged[hostname].get("_ts", 0):
                merged[hostname] = dict(entry, _ts=ts)
    for entry in merged.values():
        entry.pop("_ts", None)
    return merged


def _atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def _read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _sync_coords():
    """One sync pass. Runs in a daemon thread every SYNC_INTERVAL seconds.

    Skips a worker whose task is in flight AND whose file already equals
    the merge, so a stale (task-start-time) read never replaces a newer
    local calibration written mid-task.
    """
    if not COORDS_SYNC_DIR:
        return
    with _sync_lock:
        local = []
        for w in WORKERS:
            try:
                code, obj = _request(w, "GET", "/coords", timeout=5)
            except (OSError, http.client.HTTPException):
                continue
            if code == 200 and isinstance(obj, dict):
                local.append(obj.get("coords"))
        if not local:
            return  # nobody reachable; keep whatever we have
        merged = _merge_coords(local)
        for w in WORKERS:
            path = os.path.join(COORDS_SYNC_DIR, w["name"], "coords.json")
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                if _read_json(path) == merged:
                    continue
                _atomic_write_json(path, merged)
            except OSError as e:
                print("[gw] coords write failed for %s: %s" % (w["name"], e),
                      flush=True)


def _sync_loop():
    while True:
        try:
            _sync_coords()
        except Exception as e:  # never let the thread die
            print("[gw] coords sync error: %s" % e, flush=True)
        time.sleep(SYNC_INTERVAL)


# --------------------------------------------------------------------------
# Stats
# --------------------------------------------------------------------------

def _record_stat(solver, code, obj):
    with _stats_lock:
        st = _stats.setdefault(
            solver, {"count": 0, "success": 0, "total_elapsed": 0.0,
                     "last_ts": 0, "last_elapsed": None, "last_error": None})
        st["count"] += 1
        st["last_ts"] = int(time.time())
        if code == 200 and isinstance(obj, dict) and obj.get("ok"):
            st["success"] += 1
            try:
                elapsed = float(obj.get("elapsed", 0))
            except (TypeError, ValueError):
                elapsed = 0.0
            st["total_elapsed"] += elapsed
            st["last_elapsed"] = elapsed
            st["last_error"] = None
        else:
            err = "HTTP %d" % code
            if isinstance(obj, dict) and obj.get("error"):
                err = str(obj["error"])[:120]
            st["last_error"] = err


def _stats_snapshot():
    with _stats_lock:
        snap = {name: dict(st) for name, st in _stats.items()}
    return snap


def _stats_page():
    rows = []
    for w in WORKERS:
        st = _stats_snapshot().get(w["name"], {})
        count = st.get("count", 0)
        success = st.get("success", 0)
        total = st.get("total_elapsed", 0.0)
        avg = (total / success) if success else None
        busy = _probe(w)
        state = "idle" if (busy and busy.get("current") is None) \
            else ("busy" if busy else "unreachable")
        fmt = lambda v: ("%.2fs" % v) if v is not None else "-"
        rows.append(
            "<tr><td>%s</td><td>%s</td><td>%d</td><td>%d</td><td>%s</td>"
            "<td>%s</td><td>%s</td></tr>"
            % (html.escape(w["name"]), state, count, success, fmt(avg),
               fmt(st.get("last_elapsed")),
               html.escape(str(st.get("last_error") or "-"))))
    if not rows:
        rows.append("<tr><td colspan='7'>no workers configured</td></tr>")
    return """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>solver stats</title>
<style>
body{font-family:system-ui,sans-serif;margin:2em;background:#f6f7f9}
table{border-collapse:collapse;background:#fff;box-shadow:0 1px 3px #0002}
th,td{padding:8px 14px;border:1px solid #e2e4e8;text-align:left;font-size:14px}
th{background:#f0f1f3}td:nth-child(n+3){text-align:right}
</style></head><body>
<h2>solver stats</h2>
<p>in-memory since gateway start; refresh to update</p>
<table>
<tr><th>solver</th><th>state</th><th>requests</th><th>success</th>
<th>avg solve</th><th>last solve</th><th>last error</th></tr>
%s
</table></body></html>""" % "".join(rows)


# --------------------------------------------------------------------------
# HTTP handler
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "turnstile-gw/1.0"

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, code, obj):
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"))

    def log_message(self, fmt, *args):
        print("[gw] %s %s" % (self.address_string(), fmt % args), flush=True)

    def _read_body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > 64 * 1024:
            return None
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None

    def do_POST(self):
        if self.path.split("?", 1)[0] != "/solve":
            self._send_json(404, {"ok": False, "error": "not found"})
            return
        payload = self._read_body()
        if not isinstance(payload, dict):
            self._send_json(400, {"ok": False,
                                  "error": "body must be a JSON object"})
            return
        try:
            timeout = int(payload.get("timeout", 180))
        except (TypeError, ValueError):
            timeout = 180
        timeout = max(5, min(timeout, MAX_TIMEOUT))

        candidates = _pick_worker()
        if not candidates:
            self._send_json(503, {"ok": False,
                                  "error": "no reachable worker"})
            return

        # Buffer the client body: forward the identical JSON downstream.
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        last_code, last_obj = 503, {"ok": False, "error": "no worker accepted the task"}
        for w in candidates:
            try:
                code, obj = _request(w, "POST", "/solve", body=payload,
                                     timeout=timeout + 180)
            except (OSError, http.client.HTTPException) as e:
                print("[gw] forward to %s failed: %s" % (w["name"], e),
                      flush=True)
                code, obj = 502, None
            if code == 409 or code == 502:
                last_code, last_obj = code, obj
                print("[gw] %s refused (%d), trying next" % (w["name"], code),
                      flush=True)
                continue
            # Accepted (even if it ran and failed with 4xx/5xx): the task
            # belongs to this worker now, answer with its response.
            last_code, last_obj = code, obj
            if isinstance(obj, dict):
                obj["solver"] = w["name"]
            _record_stat(w["name"], code, obj)
            print("[gw] %s -> %d" % (w["name"], code), flush=True)
            break
        if isinstance(last_obj, dict) and "solver" not in last_obj \
                and last_obj.get("error"):
            last_obj = dict(last_obj)
            last_obj.setdefault("error", "")
        self._send_json(last_code, last_obj
                        if last_obj is not None
                        else {"ok": False, "error": "upstream error"})

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/healthz":
            self._send_json(200, {"ok": True})
        elif path == "/stats":
            self._send(200, _stats_page().encode("utf-8"), "text/html; charset=utf-8")
        elif path == "/status":
            workers = []
            for w in WORKERS:
                st = _probe(w)
                workers.append({
                    "name": w["name"],
                    "reachable": st is not None,
                    "current": (st or {}).get("current"),
                    "last_result": (st or {}).get("last_result"),
                })
            self._send_json(200, {"ok": True, "workers": workers})
        elif path == "/coords":
            local = []
            for w in WORKERS:
                try:
                    code, obj = _request(w, "GET", "/coords", timeout=5)
                except (OSError, http.client.HTTPException):
                    continue
                if code == 200 and isinstance(obj, dict):
                    local.append(obj.get("coords"))
            self._send_json(200, {"ok": True, "coords": _merge_coords(local)})
        else:
            self._send_json(404, {"ok": False, "error": "not found"})


def main():
    if COORDS_SYNC_DIR:
        threading.Thread(target=_sync_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print("[gw] listening on 0.0.0.0:%d, workers: %s"
          % (PORT, ", ".join(w["name"] for w in WORKERS)), flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
