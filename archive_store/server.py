"""Standard-library HTTP interface for the object store."""

from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit

from .core import DEFAULT_IMAGE, ObjectStore
from .devices import HttpDeviceGateway, LocalCluster, fixture_object_reader
from .model import StoreError, UnavailableBytesError
from .scenario import ScenarioRunner


class ArchiveHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], store: ObjectStore):
        super().__init__(address, ArchiveRequestHandler)
        self.store = store


def create_server(host: str = "127.0.0.1", port: int = 8080, *,
                  store: Optional[ObjectStore] = None,
                  fixture_dir: Optional[str] = None,
                  device_base_url: Optional[str] = None) -> ArchiveHTTPServer:
    if store is None:
        object_reader = fixture_object_reader(fixture_dir) if fixture_dir else None
        if device_base_url:
            gateway: Any = HttpDeviceGateway(device_base_url)
            store = ObjectStore(object_reader=object_reader, gateway=gateway, seed=False)
        else:
            cluster = LocalCluster(object_reader=object_reader)
            store = ObjectStore(cluster=cluster, object_reader=object_reader)
        if fixture_dir:
            from .devices import load_events_record

            record = load_events_record(f"{fixture_dir}/events.json")
            store.load_policy(record["policy"])
    return ArchiveHTTPServer((host, port), store)


class ArchiveRequestHandler(BaseHTTPRequestHandler):
    server: ArchiveHTTPServer

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet by default
        return

    @property
    def store(self) -> ObjectStore:
        return self.server.store

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, status: int, data: bytes, content_type: str = "application/octet-stream") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _query(self) -> dict[str, str]:
        parsed = urlsplit(self.path)
        return {k: v[-1] for k, v in parse_qs(parsed.query).items()}

    def _body_bytes(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0") or 0)
        return self.rfile.read(length)

    def _query_int(self, q: dict[str, str], *names: str) -> Optional[int]:
        for name in names:
            if name in q and q[name] != "":
                return int(q[name])
        return None

    def do_GET(self) -> None:  # noqa: N802
        try:
            path = urlsplit(self.path).path.rstrip("/") or "/"
            q = self._query()
            if path in {"/", "/health", "/healthz"}:
                self._send_json(200, {"service": "archive-store", "ok": True})
                return
            if path == "/v1/images":
                self._send_json(200, {"images": sorted(self.store.images), "current": self.store.current_versions})
                return
            if path in {"/v1/capacity", "/v1/query/capacity", "/v1/queries/capacity"}:
                self._send_json(200, self.store.capacity_report())
                return
            if path in {"/v1/reclaimable", "/v1/query/reclaimable", "/v1/queries/reclaimable",
                        "/v1/space/reclaimable"}:
                self._send_json(200, self.store.reclaimable_space())
                return
            if path in {"/v1/queries/rebuildable", "/v1/query/rebuildable", "/v1/rebuildable",
                        "/v1/areas/rebuildable"}:
                self._send_json(200, self.store.rebuildable_areas(q.get("version"), q.get("image")))
                return
            if path in {"/v1/queries/fault-domains", "/v1/query/fault-domains", "/v1/fault-domains",
                        "/v1/domains/missing"}:
                self._send_json(200, self.store.fault_domain_query(q.get("version"), q.get("image")))
                return
            if path in {"/v1/durability", "/v1/queries/durability"}:
                self._send_json(200, self.store.durability_status(q.get("version"), q.get("image")))
                return
            if path in {"/v1/dashboard", "/v1/state", "/v1/ops"}:
                self._send_json(200, _json_safe(self.store.diagnose(q.get("version"), q.get("image"))))
                return
            if path in {"/v1/versions/current", "/v1/current-version"}:
                image = q.get("image", DEFAULT_IMAGE)
                self._send_json(200, {"image": image, "version": self.store.current_versions.get(image)})
                return

            m = re.fullmatch(r"/v1/handles/([^/]+)/read", path)
            if m:
                offset = self._query_int(q, "offset", "start") or 0
                length = self._query_int(q, "length", "len", "end")
                if "end" in q and length is not None:
                    length = int(q["end"]) - offset
                report = self.store.read_range(handle=m.group(1), offset=offset, length=length)
                self._send_json(200, report.to_dict())
                return

            m = re.fullmatch(r"/v1/images/([^/]+)/versions/([^/]+)(?:/(read|range|report|content|download|bytes))?", path)
            if m:
                image, version, kind = m.groups()
                self._version_read(image, version, q, kind or "read")
                return
            m = re.fullmatch(r"/v1/versions/([^/]+)(?:/(read|range|report|content|download|bytes))?", path)
            if m:
                version, kind = m.groups()
                self._version_read(q.get("image", DEFAULT_IMAGE), version, q, kind or "read")
                return
            m = re.fullmatch(r"/objects/([^/]+)(?:/(read|range|report|content|download|bytes))?", path)
            if m:
                version, kind = m.groups()
                self._version_read(q.get("image", DEFAULT_IMAGE), version, q, kind or "read")
                return
            self._send_json(404, {"error": "not_found", "path": path})
        except UnavailableBytesError as exc:
            self._send_json(409, exc.report.to_dict())
        except StoreError as exc:  # noqa: PERF203
            status = 404 if type(exc).__name__.startswith("Unknown") else 400
            self._send_json(status, {"error": type(exc).__name__, "message": str(exc)})
        except Exception as exc:
            self._send_json(500, {"error": type(exc).__name__, "message": str(exc)})

    def _version_read(self, image: str, version: str, q: dict[str, str], kind: str) -> None:
        offset = self._query_int(q, "offset", "start") or 0
        length = self._query_int(q, "length", "len")
        if "end" in q:
            end = int(q["end"])
            length = max(0, end - offset)
        if kind in {"content", "download", "bytes"} or q.get("format") == "bytes":
            try:
                data = self.store.read_bytes(version, offset, length, image=image,
                                             handle=q.get("handle"))
                self._send_bytes(200, data)
            except UnavailableBytesError as exc:
                self._send_json(409, exc.report.to_dict())
            return
        report = self.store.read_range(version, offset, length, image=image, handle=q.get("handle"))
        # The report itself carries direct/degraded/unavailable intervals; use
        # 200 so schedulers can consume that classification normally.
        self._send_json(200, report.to_dict())

    def do_POST(self) -> None:  # noqa: N802
        try:
            path = urlsplit(self.path).path.rstrip("/") or "/"
            if path == "/v1/policy":
                payload = self._read_json()
                self.store.load_policy(payload.get("policy", payload))
                self._send_json(201, {"ok": True})
                return
            if path in {"/v1/fixture/replay", "/v1/admin/replay"}:
                payload = self._read_json()
                runner = ScenarioRunner(payload["directory"])
                result = runner.run()
                self.server.store = runner.store  # type: ignore[attr-defined]
                self._send_json(200, _json_safe(result))
                return
            if path == "/v1/handles":
                payload = self._read_json()
                handle = self.store.open_read(payload.get("version"), payload.get("handle") or payload.get("id"),
                                              payload.get("image", DEFAULT_IMAGE))
                self._send_json(201, {"handle": handle.id, "version": handle.snapshot.version,
                                      "image": handle.snapshot.image})
                return
            m = re.fullmatch(r"/v1/handles/([^/]+)/close", path)
            if m:
                self.store.close_read(m.group(1))
                self._send_json(200, {"closed": m.group(1)})
                return
            if path in {"/v1/devices/offline", "/v1/faults/offline"}:
                p = self._read_json()
                self.store.report_offline(p["device"], reason=p.get("reason", "offline"))
                self._send_json(200, {"ok": True})
                return
            m = re.fullmatch(r"/v1/devices/([^/]+)/offline", path)
            if m:
                p = self._read_json()
                self.store.report_offline(m.group(1), reason=p.get("reason", "offline"))
                self._send_json(200, {"ok": True})
                return
            if path in {"/v1/devices/online", "/v1/faults/online"}:
                p = self._read_json()
                self.store.report_online(p["device"])
                self._send_json(200, {"ok": True})
                return
            if path in {"/v1/faults/corrupt", "/v1/devices/corrupt"}:
                p = self._read_json()
                self.store.report_corrupt(**{k: v for k, v in p.items() if k in {
                    "device", "stripe", "fragment", "role", "fragment_id", "file", "sha256",
                    "actual_sha256", "version"}})
                self._send_json(200, {"ok": True})
                return
            if path in {"/v1/repairs", "/v1/repair"}:
                p = self._read_json()
                attempt = self.store.start_repair(
                    p["version"], p["stripe"], p.get("fragment") or p.get("role"),
                    p.get("target"), image=p.get("image"), attempt=p.get("attempt"),
                    write=bool(p.get("write", True)), coalesce=bool(p.get("coalesce", False)),
                )
                self._send_json(202 if attempt.state == "writing" else 200, _attempt_dict(attempt))
                return
            if path in {"/v1/repairs/receipt", "/v1/repair/receipt"}:
                p = self._read_json()
                attempt = self.store.report_repair_receipt(**p)
                self._send_json(200, _attempt_dict(attempt))
                return
            if path in {"/v1/space/release", "/v1/reclaimable/release"}:
                p = self._read_json()
                self._send_json(200, self.store.release_reclaimable(only_online=bool(p.get("only_online", True))))
                return
            if path in {"/v1/publish", "/v1/versions"}:
                p = self._read_json()
                import base64

                data = base64.b64decode(p["data_b64"]) if p.get("data_b64") else (p.get("data", "").encode() if p.get("data") else b"")
                result = self.store.publish_object(p["version"], data, image=p.get("image", DEFAULT_IMAGE),
                                                   fragment_bytes=p.get("fragment_bytes"), targets=p.get("targets"))
                self._send_json(201, result)
                return
            m = re.fullmatch(r"/v1/images/([^/]+)/versions", path)
            if m:
                p = self._read_json()
                import base64

                data = base64.b64decode(p["data_b64"]) if p.get("data_b64") else p.get("data", b"")
                result = self.store.publish_object(p["version"], data, image=m.group(1),
                                                   fragment_bytes=p.get("fragment_bytes"), targets=p.get("targets"))
                self._send_json(201, result)
                return
            self._send_json(404, {"error": "not_found", "path": path})
        except StoreError as exc:
            self._send_json(400, {"error": type(exc).__name__, "message": str(exc)})
        except Exception as exc:
            self._send_json(500, {"error": type(exc).__name__, "message": str(exc)})

    def do_PUT(self) -> None:  # noqa: N802
        try:
            path = urlsplit(self.path).path.rstrip("/") or "/"
            q = self._query()
            m = re.fullmatch(r"/objects/([^/]+)", path)
            if m:
                version = m.group(1)
                data = self._body_bytes()
                result = self.store.publish_object(version, data, image=q.get("image", DEFAULT_IMAGE),
                                                   fragment_bytes=self._query_int(q, "fragment_bytes"))
                self._send_json(201, result)
                return
            self._send_json(404, {"error": "not_found", "path": path})
        except Exception as exc:
            self._send_json(400, {"error": type(exc).__name__, "message": str(exc)})


def _attempt_dict(attempt: Any) -> dict[str, Any]:
    return {
        "id": attempt.id,
        "state": attempt.state,
        "target": attempt.target,
        "fragment_id": attempt.fragment_id,
        "base_epoch": attempt.base_epoch,
        "replica": attempt.replica_id,
        "error": attempt.error,
        "receipt": attempt.receipt.raw if attempt.receipt else None,
    }


def _json_safe(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, bytes):
        import base64

        return {"data_b64": base64.b64encode(value).decode()}
    return value


def serve(host: str = "127.0.0.1", port: int = 8080, **kwargs: Any) -> None:
    server = create_server(host, port, **kwargs)
    print(f"http://{host}:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
