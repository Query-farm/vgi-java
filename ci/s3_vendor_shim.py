"""S3 accounting shim: lets the extension's blob-vendor lane .test files run unchanged
against a worker on the direct S3 backend.

It sits in front of an S3-compatible store (RustFS) as the worker's S3 endpoint and the
public endpoint of every presigned URL. Requests are forwarded with their headers verbatim
(the Host header included, so SigV4 signatures still verify). It answers the fake vendor's
test routes on the same port, with the same counter names:

  GET /__stats__                      counters (fake vendor keys)
  GET /__admin__/reset                zero counters and faults
  GET /__admin__/fault?purpose=fs|rpc&action=corrupt|missing&count=N
  GET /__admin__/fault?purpose=upload&action=corrupt&count=N
  GET /__admin__/delete?origin=client_upload

Classification (keys as the vgi direct backend writes them; function storage under
<prefix>fs/):
  SDK PUT  *.arrow           worker result         puts, puts_rpc, storage_puts
  SDK PUT  fs/*.bin          function storage      puts, puts_fs, storage_puts
  SDK PUT  other *.bin       StateRef object       puts, puts_rpc, puts_state_ref, storage_puts
  presigned PUT              client upload grant   puts, puts_upload, storage_puts, client_uploads | refusals
  presigned GET              extension fetch       storage_gets (fault purpose rpc)
  SDK GET  fs/*.bin          function storage read gets, storage_gets (fault purpose fs)
  SDK GET  other *.bin       StateRef read         gets, gets_state_ref, storage_gets (fault rpc)
  SDK GET  *.arrow           argument-ref read     gets, gets_upload, storage_gets (fault rpc)

Usage: s3_vendor_shim.py <listen-port> <upstream-host:port> <bucket> <access-key> <secret-key>
Prints URL:http://127.0.0.1:<port> when ready.
"""
import http.client
import http.server
import json
import sys
import threading
import urllib.parse

import boto3

PORT, UPSTREAM, BUCKET, AK, SK = sys.argv[1:6]
UP_HOST, UP_PORT = UPSTREAM.rsplit(":", 1)
KEYS = ["puts", "gets", "deletes", "delete_scopes", "upload_urls", "storage_puts", "storage_gets", "objects",
        "bytes", "bytes_uploaded", "bytes_downloaded", "upload_refusals", "upload_bad_digest",
        "upload_incomplete_body", "upload_signature_mismatch", "uploads_corrupted", "puts_rpc", "puts_fs",
        "puts_upload", "client_uploads", "client_upload_bytes", "client_upload_refusals", "puts_state_ref",
        "gets_state_ref", "gets_upload"]
lock = threading.Lock()
stats = dict.fromkeys(KEYS, 0)
faults = {}  # purpose -> [action, remaining]
client_keys = []
admin = boto3.client("s3", endpoint_url=f"http://{UPSTREAM}", aws_access_key_id=AK, aws_secret_access_key=SK,
                     region_name="us-east-1")


def bump(*names, by=1):
    with lock:
        for n in names:
            stats[n] += by


def take_fault(purpose):
    with lock:
        f = faults.get(purpose)
        if not f or f[1] <= 0:
            return None
        f[1] -= 1
        return f[0]


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def reply(self, status, body, ctype="application/json", headers=()):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def admin_route(self, url):
        q = urllib.parse.parse_qs(url.query)
        if url.path == "/__stats__":
            with lock:
                return self.reply(200, json.dumps(stats).encode())
        if url.path == "/__admin__/reset":
            with lock:
                for k in stats:
                    stats[k] = 0
                faults.clear()
            return self.reply(200, b'{"ok": true}')
        if url.path == "/__admin__/fault":
            purpose = q.get("purpose", [""])[0]
            action = q.get("action", [""])[0]
            count = int(q.get("count", ["1"])[0])
            with lock:
                faults[purpose] = [action, count]
            return self.reply(200, json.dumps({"ok": True, "purpose": purpose, "action": action,
                                               "count": count}).encode())
        if url.path == "/__admin__/delete":
            with lock:
                keys, client_keys[:] = list(client_keys), []
            for k in keys:
                admin.delete_object(Bucket=BUCKET, Key=k)
            bump("deletes", by=len(keys))
            return self.reply(200, json.dumps({"ok": True, "deleted": len(keys)}).encode())
        return self.reply(404, b'{"error": "not_found"}')

    def proxy(self):
        url = urllib.parse.urlsplit(self.path)
        if url.path.startswith("/__"):
            return self.admin_route(url)
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else b""
        presigned = "X-Amz-Signature=" in (url.query or "")
        prefix = "/" + BUCKET + "/"
        key = urllib.parse.unquote(url.path[len(prefix):]) if url.path.startswith(prefix) else ""
        kind = None
        if self.command == "PUT" and key:
            if presigned:
                kind = "client_upload"
                bump("puts", "puts_upload", "storage_puts")
                if take_fault("upload") == "corrupt" and body:
                    body = bytes([body[0] ^ 0xFF]) + body[1:]
                    bump("uploads_corrupted")
            elif key.endswith(".arrow"):
                bump("puts", "puts_rpc", "storage_puts")
            elif "/fs/" in "/" + key or key.startswith("fs/"):
                bump("puts", "puts_fs", "storage_puts")
            else:
                bump("puts", "puts_rpc", "puts_state_ref", "storage_puts")
        fault = None
        if self.command == "GET" and key:
            if presigned:
                bump("storage_gets")
                fault = take_fault("rpc")
            elif "/fs/" in "/" + key or key.startswith("fs/"):
                bump("gets", "storage_gets")
                fault = take_fault("fs")
            elif key.endswith(".arrow"):
                bump("gets", "gets_upload", "storage_gets")
                fault = take_fault("rpc")
            else:
                bump("gets", "gets_state_ref", "storage_gets")
                fault = take_fault("rpc")
        if fault == "missing":
            return self.reply(404, b"<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code>"
                                   b"<Message>fault</Message></Error>", "application/xml")
        conn = http.client.HTTPConnection(UP_HOST, int(UP_PORT), timeout=120)
        conn.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
        for k, v in self.headers.items():
            if k.lower() in ("expect", "connection"):
                continue
            conn.putheader(k, v)
        conn.endheaders(body if body else None)
        resp = conn.getresponse()
        data = resp.read() if self.command != "HEAD" else b""
        status = resp.status
        if kind == "client_upload":
            if 200 <= status < 300:
                bump("client_uploads")
                bump("client_upload_bytes", "bytes_uploaded", by=len(body))
                with lock:
                    client_keys.append(key)
            else:
                bump("client_upload_refusals", "upload_refusals")
                if b"BadDigest" in data or b"Checksum" in data:
                    bump("upload_bad_digest")
        elif self.command == "PUT" and 200 <= status < 300:
            bump("bytes_uploaded", by=len(body))
        if self.command == "GET" and 200 <= status < 300:
            bump("bytes_downloaded", by=len(data))
            if fault == "corrupt" and data:
                data = bytes([data[0] ^ 0xFF]) + data[1:]
        self.send_response(status)
        for k, v in resp.getheaders():
            if k.lower() in ("transfer-encoding", "content-length", "connection"):
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data) if self.command != "HEAD"
                                                else int(resp.getheader("Content-Length") or 0)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)
        conn.close()

    do_GET = do_PUT = do_HEAD = do_DELETE = do_POST = proxy


server = http.server.ThreadingHTTPServer(("127.0.0.1", int(PORT)), Handler)
server.daemon_threads = True
print(f"URL:http://127.0.0.1:{server.server_address[1]}", flush=True)
server.serve_forever()
