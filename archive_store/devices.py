"""Device transports and in-process device doubles."""

from __future__ import annotations

import base64
import http.client
import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote, urlsplit

from .model import FragmentUnavailable, sha256_hex


@dataclass
class DeviceReply:
    data: bytes
    sha256: str
    generation: Optional[int] = None
    file: Optional[str] = None
    raw: dict[str, Any] = field(default_factory=dict)
    device: Optional[str] = None
    fragment_id: Optional[str] = None


@dataclass
class PutReceipt:
    device: str
    fragment_id: str
    sha256: str
    generation: Optional[int] = None
    file: Optional[str] = None
    raw: dict[str, Any] = field(default_factory=dict)


class DeviceGateway:
    """Transport interface used by the repair/read logic."""

    def fetch(self, device: str, fragment_id: str) -> DeviceReply:
        raise NotImplementedError

    def put(self, device: str, fragment_id: str, data: bytes, attempt: Optional[str] = None) -> PutReceipt:
        raise NotImplementedError

    def delete(self, device: str, fragment_id: str, generation: Optional[int] = None) -> dict[str, Any]:
        raise NotImplementedError


class HttpDeviceGateway(DeviceGateway):
    """Talks the repository's JSON-media protocol and fixture blob endpoint."""

    def __init__(
        self,
        base_url: str | int = "http://127.0.0.1:8090",
        *,
        port: Optional[int] = None,
        device_urls: Optional[dict[str, str]] = None,
        object_url: Optional[str] = None,
        timeout: float = 5.0,
        authorization: Optional[str] = None,
    ):
        if isinstance(base_url, int):
            port = base_url
            base_url = f"http://127.0.0.1:{port}"
        elif port is not None:
            base_url = f"http://127.0.0.1:{port}"
        self.base_url = str(base_url).rstrip("/")
        self.device_urls = dict(device_urls or {})
        # Default is the fixture server's raw object endpoint.
        self.object_url = object_url or (self.base_url + "/fixture/objects/{file}")
        self.timeout = timeout
        self.authorization = authorization

    def get_fragment(self, device: str, fragment_id: str) -> DeviceReply:
        return self.fetch(device, fragment_id)

    def put_fragment(self, device: str, fragment_id: str, data: bytes,
                     attempt: Optional[str] = None) -> PutReceipt:
        return self.put(device, fragment_id, data, attempt)

    def delete_fragment(self, device: str, fragment_id: str,
                        generation: Optional[int] = None) -> dict[str, Any]:
        return self.delete(device, fragment_id, generation)

    def _connection(self, base_url: str):
        parts = urlsplit(base_url)
        if parts.scheme != "http":
            raise ValueError("only http device endpoints are supported by the stdlib gateway")
        return http.client.HTTPConnection(parts.hostname, parts.port or 80, timeout=self.timeout)

    def _request(
        self,
        base_url: str,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> tuple[int, bytes, dict[str, str]]:
        conn = self._connection(base_url)
        try:
            hdrs = dict(headers or {})
            if body is not None:
                hdrs.setdefault("Content-Length", str(len(body)))
                hdrs.setdefault("Content-Type", "application/octet-stream")
            if self.authorization:
                hdrs.setdefault("Authorization", self.authorization)
            conn.request(method, path, body=body, headers=hdrs)
            response = conn.getresponse()
            data = response.read()
            return response.status, data, {k.lower(): v for k, v in response.getheaders()}
        finally:
            conn.close()

    def _device_base(self, device: str) -> str:
        return self.device_urls.get(device, self.base_url).rstrip("/")

    @staticmethod
    def _json_or_error(status: int, data: bytes) -> dict[str, Any]:
        try:
            reply = json.loads(data.decode("utf-8")) if data else {}
        except Exception:
            reply = {"state": "bad_json", "body": data[:200].decode("latin1", "replace")}
        if status >= 400:
            state = reply.get("state") or f"http_{status}"
            raise FragmentUnavailable("device", "fragment", str(state))
        return reply

    def _fetch_named_object(self, file_ref: str) -> bytes:
        if file_ref.startswith(("http://", "https://")):
            parts = urlsplit(file_ref)
            path = parts.path + (("?" + parts.query) if parts.query else "")
            base = f"{parts.scheme}://{parts.netloc}"
            status, data, _ = self._request(base, "GET", path)
            if status >= 400:
                raise FragmentUnavailable("blob", file_ref, f"http_{status}")
            return data
        if self.object_url.startswith("http"):
            url = self.object_url.format(file=quote(file_ref))
            parts = urlsplit(url)
            status, data, _ = self._request(f"{parts.scheme}://{parts.netloc}", "GET", parts.path + "?" + parts.query if parts.query else parts.path)
            if status >= 400:
                raise FragmentUnavailable("blob", file_ref, f"http_{status}")
            return data
        path = Path(self.object_url.format(file=file_ref))
        return path.read_bytes()

    def fetch(self, device: str, fragment_id: str) -> DeviceReply:
        base = self._device_base(device)
        path = f"/media/{quote(device)}/fragments/{quote(fragment_id)}"
        status, data, _ = self._request(base, "GET", path)
        try:
            reply = self._json_or_error(status, data)
        except FragmentUnavailable as exc:
            exc.device = device
            exc.fragment_id = fragment_id
            raise
        raw_bytes: Optional[bytes] = None
        file_ref = reply.get("file")
        if reply.get("data_b64") is not None:
            raw_bytes = base64.b64decode(reply["data_b64"])
        elif reply.get("data") is not None:
            raw_bytes = base64.b64decode(reply["data"]) if isinstance(reply["data"], str) else bytes(reply["data"])
        elif file_ref:
            raw_bytes = self._fetch_named_object(str(file_ref))
        elif reply.get("sha256") and reply.get("bytes") is not None:
            # A metadata-only reply is not enough to read bytes.
            raise FragmentUnavailable(device, fragment_id, "metadata_without_body")
        if raw_bytes is None:
            raise FragmentUnavailable(device, fragment_id, "empty_device_reply")
        digest = str(reply.get("sha256") or sha256_hex(raw_bytes))
        return DeviceReply(
            data=raw_bytes,
            sha256=digest,
            generation=reply.get("generation"),
            file=file_ref,
            raw=reply,
            device=device,
            fragment_id=fragment_id,
        )

    def put(self, device: str, fragment_id: str, data: bytes, attempt: Optional[str] = None) -> PutReceipt:
        base = self._device_base(device)
        path = f"/media/{quote(device)}/fragments/{quote(fragment_id)}"
        headers = {"X-Attempt-Id": attempt} if attempt else None
        status, body, _ = self._request(base, "PUT", path, data, headers)
        reply = self._json_or_error(status, body)
        digest = str(reply.get("sha256") or sha256_hex(data))
        return PutReceipt(
            device=device,
            fragment_id=fragment_id,
            sha256=digest,
            generation=reply.get("generation"),
            file=reply.get("file"),
            raw=reply,
        )

    def delete(self, device: str, fragment_id: str, generation: Optional[int] = None) -> dict[str, Any]:
        base = self._device_base(device)
        path = f"/media/{quote(device)}/fragments/{quote(fragment_id)}"
        if generation is not None:
            path += f"?generation={int(generation)}"
        status, body, _ = self._request(base, "DELETE", path)
        return self._json_or_error(status, body)


@dataclass
class StoredFragment:
    data: bytes
    generation: int = 1
    file: Optional[str] = None
    sha256: Optional[str] = None

    @property
    def digest(self) -> str:
        return self.sha256 or sha256_hex(self.data)


class LocalDevice:
    def __init__(self, device_id: str, *, online: bool = True, capacity: int = 10**18):
        self.id = device_id
        self.online = online
        self.capacity = capacity
        self.fragments: dict[str, StoredFragment] = {}
        self.lock = threading.RLock()
        self.put_failures = 0
        self.drop_next_put = False
        self.deleted: list[str] = []

    def fetch(self, fragment_id: str) -> DeviceReply:
        with self.lock:
            if not self.online:
                raise FragmentUnavailable(self.id, fragment_id, "offline")
            fragment = self.fragments.get(fragment_id)
            if fragment is None:
                raise FragmentUnavailable(self.id, fragment_id, "absent")
            return DeviceReply(
                data=fragment.data,
                sha256=fragment.digest,
                generation=fragment.generation,
                file=fragment.file,
                device=self.id,
                fragment_id=fragment_id,
            )

    def put(self, fragment_id: str, data: bytes, attempt: Optional[str] = None,
            generation: Optional[int] = None) -> PutReceipt:
        with self.lock:
            if not self.online:
                raise FragmentUnavailable(self.id, fragment_id, "offline")
            if self.put_failures > 0:
                self.put_failures -= 1
                raise FragmentUnavailable(self.id, fragment_id, "put_failed")
            if self.drop_next_put:
                self.drop_next_put = False
                raise ConnectionResetError("device dropped after receiving bytes")
            old = self.fragments.get(fragment_id)
            next_generation = generation if generation is not None else ((old.generation + 1) if old else 1)
            stored = StoredFragment(data=bytes(data), generation=int(next_generation), sha256=sha256_hex(data))
            self.fragments[fragment_id] = stored
            return PutReceipt(self.id, fragment_id, stored.digest, stored.generation, raw={"state": "stored"})

    def delete(self, fragment_id: str, generation: Optional[int] = None) -> dict[str, Any]:
        with self.lock:
            fragment = self.fragments.get(fragment_id)
            if fragment is None:
                return {"state": "absent"}
            if generation is not None and fragment.generation != generation:
                return {"state": "generation_conflict", "generation": fragment.generation}
            self.fragments.pop(fragment_id, None)
            self.deleted.append(fragment_id)
            return {"state": "deleted"}

    def corrupt(self, fragment_id: str, data: bytes = b"CORRUPT") -> None:
        with self.lock:
            self.fragments[fragment_id] = StoredFragment(data=bytes(data), generation=(
                self.fragments[fragment_id].generation if fragment_id in self.fragments else 1
            ), sha256=sha256_hex(data))


class LocalCluster(DeviceGateway):
    """In-process :class:`DeviceGateway` with explicit fault injection."""

    def __init__(
        self,
        devices: Optional[dict[str, LocalDevice]] = None,
        *,
        domains: Optional[dict[str, str]] = None,
        capacity: Optional[dict[str, int]] = None,
        object_reader: Optional[Callable[[str], bytes]] = None,
    ):
        self.devices = dict(devices or {})
        self.domains = dict(domains or {})
        self.capacity = dict(capacity or {})
        self.object_reader = object_reader
        self.lock = threading.RLock()
        self.requests: list[dict[str, Any]] = []

    def device(self, device_id: str) -> LocalDevice:
        if device_id not in self.devices:
            cap = int(self.capacity.get(device_id, 10**18))
            self.devices[device_id] = LocalDevice(device_id, capacity=cap)
        return self.devices[device_id]

    def add_device(self, device_id: str, *, domain: Optional[str] = None,
                   capacity: Optional[int] = None, online: bool = True) -> LocalDevice:
        if domain is not None:
            self.domains[device_id] = domain
        if capacity is not None:
            self.capacity[device_id] = int(capacity)
        device = LocalDevice(device_id, online=online,
                             capacity=int(capacity if capacity is not None else self.capacity.get(device_id, 10**18)))
        self.devices[device_id] = device
        return device

    def read_named_object(self, file_name: str) -> bytes:
        if self.object_reader is None:
            raise FileNotFoundError(file_name)
        return self.object_reader(file_name)

    def seed_triple(self, device_id: str, fragment_id: str, file_name: Optional[str],
                    expected_sha256: Optional[str] = None, generation: int = 1) -> DeviceReply:
        if file_name is None or self.object_reader is None:
            raise FileNotFoundError(file_name)
        data = self.read_named_object(file_name)
        if expected_sha256 and sha256_hex(data) != expected_sha256:
            # Seed the bytes anyway: tests need to observe and report corruption.
            pass
        dev = self.device(device_id)
        with dev.lock:
            dev.fragments[fragment_id] = StoredFragment(data, generation=generation, file=file_name)
        return DeviceReply(data, sha256_hex(data), generation, file_name, device=device_id, fragment_id=fragment_id)

    def set_online(self, device_id: str, online: bool = True) -> None:
        self.device(device_id).online = online

    def offline(self, device_id: str) -> None:
        self.set_online(device_id, False)

    def online(self, device_id: str) -> None:
        self.set_online(device_id, True)

    def corrupt(self, device_id: str, fragment_id: str, data: bytes = b"CORRUPT!",
                file_name: Optional[str] = None) -> None:
        if file_name and self.object_reader:
            try:
                data = self.read_named_object(file_name)
            except FileNotFoundError:
                pass
        self.device(device_id).corrupt(fragment_id, data)

    def fetch(self, device: str, fragment_id: str) -> DeviceReply:
        with self.lock:
            self.requests.append({"op": "GET", "device": device, "fragment_id": fragment_id})
            return self.device(device).fetch(fragment_id)

    def put(self, device: str, fragment_id: str, data: bytes, attempt: Optional[str] = None) -> PutReceipt:
        with self.lock:
            self.requests.append({"op": "PUT", "device": device, "fragment_id": fragment_id,
                                  "attempt": attempt, "bytes": len(data)})
            return self.device(device).put(fragment_id, data, attempt)

    def put_receipt(self, device: str, fragment_id: str, data: bytes, generation: int,
                    attempt: Optional[str] = None, file: Optional[str] = None) -> PutReceipt:
        """Inject an asynchronous/device-reported receipt without another PUT."""

        digest = sha256_hex(data)
        dev = self.device(device)
        with dev.lock:
            dev.fragments[fragment_id] = StoredFragment(data, generation=int(generation), file=file)
        return PutReceipt(device, fragment_id, digest, int(generation), file,
                          {"state": "stored", "injected": True})

    def delete(self, device: str, fragment_id: str, generation: Optional[int] = None) -> dict[str, Any]:
        with self.lock:
            self.requests.append({"op": "DELETE", "device": device, "fragment_id": fragment_id})
            return self.device(device).delete(fragment_id, generation)


class AdaptiveGateway(DeviceGateway):
    """Adapt common fake/backend method-name conventions to DeviceGateway."""

    _GET_NAMES = ("fetch", "get_fragment", "read_fragment", "get", "read", "load_fragment", "load")
    _PUT_NAMES = ("put", "put_fragment", "write_fragment", "write", "save_fragment", "save", "store")
    _DELETE_NAMES = ("delete", "delete_fragment", "remove_fragment", "remove")

    def __init__(self, backend: Any):
        self.backend = backend

    def _find(self, names: tuple[str, ...]) -> Optional[Callable[..., Any]]:
        for name in names:
            candidate = getattr(self.backend, name, None)
            if callable(candidate):
                return candidate
        return None

    def fetch(self, device: str, fragment_id: str) -> DeviceReply:
        fn = self._find(self._GET_NAMES)
        if fn is None:
            raise AttributeError("backend has no supported fragment read method")
        result = fn(device, fragment_id)
        if isinstance(result, DeviceReply):
            return result
        if isinstance(result, (bytes, bytearray)):
            data = bytes(result)
            return DeviceReply(data, sha256_hex(data), device=device, fragment_id=fragment_id)
        if isinstance(result, dict):
            data = result.get("data") or result.get("bytes") or b""
            if isinstance(data, str):
                data = base64.b64decode(data)
            return DeviceReply(
                bytes(data), str(result.get("sha256") or sha256_hex(bytes(data))),
                result.get("generation"), result.get("file"), result, device, fragment_id,
            )
        data = getattr(result, "data", None) or getattr(result, "bytes", b"")
        return DeviceReply(
            bytes(data),
            str(getattr(result, "sha256", None) or sha256_hex(bytes(data))),
            getattr(result, "generation", None), getattr(result, "file", None),
            {"value": result}, device, fragment_id,
        )

    def put(self, device: str, fragment_id: str, data: bytes, attempt: Optional[str] = None) -> PutReceipt:
        fn = self._find(self._PUT_NAMES)
        if fn is None:
            raise AttributeError("backend has no supported fragment write method")
        try:
            result = fn(device, fragment_id, data, attempt) if attempt else fn(device, fragment_id, data)
        except TypeError:
            result = fn(device, fragment_id, data)
        if isinstance(result, PutReceipt):
            return result
        digest = sha256_hex(data)
        if result is None:
            return PutReceipt(device, fragment_id, digest, None, None, {})
        if isinstance(result, int):
            return PutReceipt(device, fragment_id, digest, result, None, {"generation": result})
        if isinstance(result, dict):
            return PutReceipt(
                device, fragment_id,
                str(result.get("sha256") or result.get("digest") or digest),
                result.get("generation"), result.get("file"), result,
            )
        return PutReceipt(
            device, fragment_id,
            str(getattr(result, "sha256", None) or getattr(result, "digest", None) or digest),
            getattr(result, "generation", None), getattr(result, "file", None),
            {"value": result},
        )

    def delete(self, device: str, fragment_id: str, generation: Optional[int] = None) -> dict[str, Any]:
        fn = self._find(self._DELETE_NAMES)
        if fn is None:
            return {"state": "unsupported"}
        try:
            result = fn(device, fragment_id, generation) if generation is not None else fn(device, fragment_id)
        except TypeError:
            result = fn(device, fragment_id)
        return result if isinstance(result, dict) else {"state": "deleted", "result": str(result)}


def fixture_object_reader(fixture_dir: str | Path) -> Callable[[str], bytes]:
    objects = Path(fixture_dir) / "objects"

    def _read(file_name: str) -> bytes:
        # Match the fixture server's conservative filename grammar.
        if "/" in file_name or "\\" in file_name or ".." in file_name:
            raise FileNotFoundError(file_name)
        return (objects / file_name).read_bytes()

    return _read


def load_events_record(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


# Common client-name aliases.
DeviceClient = HttpDeviceGateway
HttpClient = HttpDeviceGateway
LocalCluster.get_fragment = LocalCluster.fetch  # type: ignore[attr-defined]
LocalCluster.put_fragment = LocalCluster.put  # type: ignore[attr-defined]
LocalDevice.get_fragment = LocalDevice.fetch  # type: ignore[attr-defined]
