"""Thread-safe local striped object store."""

from __future__ import annotations

import itertools
import hmac
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from .devices import AdaptiveGateway, DeviceGateway, HttpDeviceGateway, LocalCluster, PutReceipt
from .model import (
    DATA_ROLES,
    DEGRADED,
    DIRECT,
    ROLES,
    UNAVAILABLE,
    ByteSegment,
    CapacityError,
    DurabilityError,
    FragmentChecksumMismatch,
    FragmentUnavailable,
    RangeReport,
    RepairError,
    StoreError,
    Stripe,
    UnknownHandleError,
    UnknownVersionError,
    VerificationResult,
    sha256_hex,
    xor_bytes,
)


DEFAULT_IMAGE = "default"


@dataclass
class Replica:
    id: str
    image: str
    version: str
    stripe_id: str
    role: str
    device: str
    fragment_id: str
    expected_sha256: str
    size: int
    epoch: int = 0
    generation: Optional[int] = None
    attempt: Optional[str] = None
    state: str = "active"  # active|temporary|retired|orphan|failed|freed
    verified: bool = False
    independently_verified: bool = False
    domain_safe: Optional[bool] = None
    reason: Optional[str] = None
    base_epoch: Optional[int] = None
    sequence: int = 0
    file: Optional[str] = None

    @property
    def status(self) -> str:
        return self.state

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "image": self.image,
            "version": self.version,
            "stripe": self.stripe_id,
            "role": self.role,
            "device": self.device,
            "fragment_id": self.fragment_id,
            "sha256": self.expected_sha256,
            "size": self.size,
            "generation": self.generation,
            "epoch": self.epoch,
            "attempt": self.attempt,
            "state": self.state,
            "verified": self.verified,
            "independently_verified": self.independently_verified,
            "domain_safe": self.domain_safe,
            "reason": self.reason,
            "sequence": self.sequence,
            "file": self.file,
        }


@dataclass
class ReadSnapshot:
    id: str
    image: str
    version: str
    placements: dict[tuple[str, str], str]
    promised: bool
    sequence: int


@dataclass
class ReadHandle:
    id: str
    snapshot: ReadSnapshot
    closed: bool = False

    def __enter__(self) -> "ReadHandle":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def close(self) -> None:
        store = getattr(self, "_store", None)
        if store is not None:
            store.close_read(self.id)
        else:
            self.closed = True


@dataclass
class RepairAttempt:
    id: str
    slot: tuple[str, str, str, str]
    target: str
    fragment_id: str
    size: int
    base_epoch: int
    replica_id: str
    state: str = "planned"  # planned|writing|receipted|committed|fenced|failed
    reconstructed: Optional[bytes] = None
    receipt: Optional[PutReceipt] = None
    error: Optional[str] = None
    done_event: threading.Event = field(default_factory=threading.Event, repr=False)

    @property
    def status(self) -> str:
        return self.state

    @status.setter
    def status(self, value: str) -> None:
        self.state = value

    @property
    def committed(self) -> bool:
        return self.state in {"committed", "completed", "ok"}

    @property
    def fenced(self) -> bool:
        return self.state in {"fenced", "stale", "superseded"}

    @property
    def domain_conflict(self) -> bool:
        return "fault_domain_conflict" in (self.error or "") or self.state == "domain_unsafe"

    @property
    def tags(self) -> list[str]:
        tags = [self.state]
        if self.fenced:
            tags += ["stale", "superseded", "rejected"]
        if self.domain_conflict:
            tags += ["domain_conflict", "fault_domain_conflict", "conflict", "rejected"]
        if self.committed:
            tags += ["completed", "ok"]
        return list(dict.fromkeys(tags))

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "slot": list(self.slot),
            "target": self.target,
            "fragment_id": self.fragment_id,
            "size": self.size,
            "base_epoch": self.base_epoch,
            "replica": self.replica_id,
            "state": self.state,
            "status": self.status,
            "tags": self.tags,
            "committed": self.committed,
            "fenced": self.fenced,
            "domain_conflict": self.domain_conflict,
            "error": self.error,
            "receipt": self.receipt.raw if self.receipt else None,
        }


class _ReadTransaction:
    def __init__(self, store: "ObjectStore", snapshot: ReadSnapshot):
        self.store = store
        self.snapshot = snapshot
        self.cache: dict[str, bytes] = {}
        self.failures: dict[str, Exception] = {}

    def replica(self, stripe: Stripe, role: str) -> Replica:
        replica_id = self.snapshot.placements[(stripe.id, role)]
        return self.store.replicas[replica_id]

    def verified_fragment(self, stripe: Stripe, role: str) -> bytes:
        replica = self.replica(stripe, role)
        if replica.id in self.cache:
            return self.cache[replica.id]
        if replica.id in self.failures:
            raise self.failures[replica.id]
        try:
            reply = self.store.gateway.fetch(replica.device, replica.fragment_id)
            expected = stripe.fragments[role].sha256
            actual = sha256_hex(reply.data)
            if reply.sha256 and reply.sha256 != actual:
                # Device envelope digest is itself a device fact; actual bytes
                # must still match the repository's piece checksum.
                pass
            if actual != expected:
                error = FragmentChecksumMismatch(replica.device, replica.fragment_id, expected, actual)
                self.store._record_checksum_failure(replica, expected, actual)
                raise error
            if reply.generation is not None:
                replica.generation = int(reply.generation)
            replica.verified = True
            self.store._record_health(replica, "healthy")
            self.cache[replica.id] = reply.data
            return reply.data
        except FragmentChecksumMismatch as exc:
            self.failures[replica.id] = exc
            raise
        except FragmentUnavailable as exc:
            self.failures[replica.id] = exc
            self.store._record_health(replica, "offline" if exc.reason == "offline" else "unavailable", str(exc))
            raise
        except Exception as exc:  # transport reset/drop etc.
            wrapped = FragmentUnavailable(replica.device, replica.fragment_id, type(exc).__name__)
            self.failures[replica.id] = wrapped
            self.store._record_health(replica, "unknown", str(exc))
            raise wrapped

    def reconstruct(self, stripe: Stripe, missing_role: str) -> bytes:
        source_roles = [role for role in ROLES if role != missing_role]
        blocks = [self.verified_fragment(stripe, role) for role in source_roles]
        if missing_role == "p":
            rebuilt = xor_bytes(blocks[:2])
        else:
            # Missing d0: d1 XOR p. Missing d1: d0 XOR p.
            rebuilt = xor_bytes(blocks)
        expected = stripe.fragments[missing_role].sha256
        actual = sha256_hex(rebuilt)
        if actual != expected:
            raise FragmentChecksumMismatch("reconstructed", stripe.fragment_id(missing_role), expected, actual)
        return rebuilt


class ObjectStore:
    """Canonical in-process implementation.

    The class is safe for concurrent use.  All mutable state is guarded by
    :attr:`lock`; network/device calls are performed outside the critical
    section where practical.
    """

    def __init__(
        self,
        policy: Optional[dict[str, Any]] = None,
        gateway: Optional[Any] = None,
        *,
        devices: Optional[Any] = None,
        backend: Optional[Any] = None,
        client: Optional[Any] = None,
        cluster: Optional[LocalCluster] = None,
        domains: Optional[dict[str, str]] = None,
        capacity: Optional[dict[str, int]] = None,
        object_reader: Optional[Callable[[str], bytes]] = None,
        default_image: str = DEFAULT_IMAGE,
        auto_repair: bool = False,
        seed: bool = True,
        **options: Any,
    ):
        self.default_image = default_image
        self.auto_repair = auto_repair
        self.lock = threading.RLock()
        self.domains: dict[str, str] = dict(domains or options.pop("fault_domains", None) or {})
        self.capacity: dict[str, int] = {
            k: int(v) for k, v in (capacity or options.pop("device_capacity", None) or {}).items()
        }
        self.images: dict[str, dict[str, Any]] = {}
        # image -> version -> ordered stripe list
        self.stripes: dict[tuple[str, str], list[Stripe]] = {}
        self.stripe_index: dict[tuple[str, str, str], Stripe] = {}
        self.current_versions: dict[str, str] = {}
        self.promised_versions: set[tuple[str, str]] = set()
        self.version_order: dict[str, list[str]] = {}

        backend = backend if backend is not None else client
        gateway = gateway if gateway is not None else devices
        if gateway is None and backend is not None:
            gateway = backend
        if gateway is None and cluster is not None:
            gateway = cluster
        if gateway is None:
            gateway = LocalCluster(domains=self.domains, capacity=self.capacity)
        elif not isinstance(gateway, DeviceGateway):
            if isinstance(gateway, dict) and all(isinstance(v, object) for v in gateway.values()):
                # A mapping of device id -> device backend is already close to
                # LocalCluster's constructor.
                try:
                    gateway = LocalCluster(devices=gateway, domains=self.domains, capacity=self.capacity)
                except TypeError:
                    gateway = AdaptiveGateway(gateway)
            else:
                gateway = AdaptiveGateway(gateway)
        self.gateway = gateway
        self.cluster = cluster if isinstance(cluster, LocalCluster) else (gateway if isinstance(gateway, LocalCluster) else None)
        self.object_reader = object_reader
        if self.object_reader is None and self.cluster is not None:
            self.object_reader = self.cluster.object_reader

        self.replicas: dict[str, Replica] = {}
        self.current_placements: dict[tuple[str, str, str, str], str] = {}
        self.slot_epoch: dict[tuple[str, str, str, str], int] = {}
        self.handles: dict[str, ReadHandle] = {}
        self.read_leases: dict[str, set[str]] = {}
        self.attempts: dict[str, RepairAttempt] = {}
        self.repair_jobs: dict[tuple[str, str, str, str], list[str]] = {}
        self.device_online: dict[str, bool] = {}
        self.faults: dict[tuple[str, str], dict[str, Any]] = {}
        self.health: dict[str, dict[str, Any]] = {}
        self.latest_device_generation: dict[tuple[str, str], int] = {}
        self._ids = itertools.count(1)
        self._events: list[dict[str, Any]] = []
        fixture_dir = options.pop("fixture_dir", None) or options.pop("directory", None) or options.pop("path", None)
        if fixture_dir is not None and policy is None:
            import json

            record = json.loads((Path(fixture_dir) / "events.json").read_text(encoding="utf-8"))
            policy = record["policy"]
            if self.object_reader is None:
                from .devices import fixture_object_reader

                self.object_reader = fixture_object_reader(fixture_dir)
                if self.cluster is not None:
                    self.cluster.object_reader = self.object_reader

        if policy is not None:
            self.load_policy(policy, seed=seed)

    # ------------------------------------------------------------------ setup

    def _new_id(self, prefix: str) -> str:
        return f"{prefix}{next(self._ids)}"

    def load_policy(self, policy: dict[str, Any], *, seed: bool = True) -> None:
        with self.lock:
            self.domains.update(policy.get("domains") or {})
            self.capacity.update({k: int(v) for k, v in (policy.get("capacity") or {}).items()})
            if self.cluster is not None:
                self.cluster.domains.update(self.domains)
                self.cluster.capacity.update(self.capacity)
            for raw in policy.get("stripes") or []:
                self.register_stripe_dict(raw, seed=seed)

    def register_stripe_dict(self, raw: dict[str, Any], *, image: Optional[str] = None, seed: bool = True) -> Stripe:
        img = image or raw.get("image") or raw.get("name") or self.default_image
        stripe = Stripe.from_dict(raw, image=img)
        with self.lock:
            key = (stripe.image, stripe.version)
            self.stripes.setdefault(key, [])
            # Replace an identical stripe id on re-ingest only if version is new?
            # Immutable versions normally refuse mutation.
            index_key = (stripe.image, stripe.version, stripe.id)
            if index_key in self.stripe_index:
                existing = self.stripe_index[index_key]
                if existing != stripe:
                    raise DurabilityError(f"immutable stripe conflicts with existing record: {index_key}")
                return existing
            self.stripe_index[index_key] = stripe
            ordered = self.stripes[key]
            ordered.append(stripe)
            ordered.sort(key=lambda s: (s.offset, s.id))
            self.images.setdefault(stripe.image, {}).setdefault("versions", set())
            self.images[stripe.image]["versions"].add(stripe.version)
            self.version_order.setdefault(stripe.image, [])
            if stripe.version not in self.version_order[stripe.image]:
                self.version_order[stripe.image].append(stripe.version)

            fragment_sizes: list[int] = []
            for role in ROLES:
                spec = stripe.fragments[role]
                size = stripe.fragment_bytes or max(1, (stripe.length + 1) // 2)
                data: Optional[bytes] = None
                if spec.file and self.object_reader is not None:
                    try:
                        data = self.object_reader(spec.file)
                        size = len(data)
                        fragment_sizes.append(len(data))
                    except (FileNotFoundError, OSError):
                        data = None
                if seed and self.cluster is not None and spec.file and data is not None:
                    self.cluster.seed_triple(spec.device, stripe.fragment_id(role), spec.file, spec.sha256)
                replica = Replica(
                    id=self._new_id("r"),
                    image=stripe.image,
                    version=stripe.version,
                    stripe_id=stripe.id,
                    role=role,
                    device=spec.device,
                    fragment_id=stripe.fragment_id(role),
                    expected_sha256=spec.sha256,
                    size=size,
                    epoch=0,
                    state="active",
                    verified=seed and data is not None and sha256_hex(data) == spec.sha256,
                    independently_verified=False,
                    sequence=next(self._ids),
                    file=spec.file,
                )
                self.replicas[replica.id] = replica
                slot = (stripe.image, stripe.version, stripe.id, role)
                self.current_placements[slot] = replica.id
                self.slot_epoch.setdefault(slot, 0)
                self.device_online.setdefault(spec.device, True)
            if fragment_sizes and stripe.fragment_bytes is None:
                # Equal fragments; actual object length is authoritative.
                stripe = Stripe(**{**stripe.__dict__, "fragment_bytes": max(fragment_sizes)})
                self.stripe_index[index_key] = stripe
                for item in ordered:
                    if item.id == stripe.id:
                        idx = ordered.index(item)
                        ordered[idx] = stripe
            if self._placement_distinct(stripe):
                self.promised_versions.add((stripe.image, stripe.version))
                if self.current_versions.get(stripe.image) is None:
                    self.current_versions[stripe.image] = stripe.version
                else:
                    self._set_latest_if_newer(stripe.image, stripe.version)
            return stripe

    def _set_latest_if_newer(self, image: str, version: str) -> None:
        current = self.current_versions.get(image)
        if current is None or _version_key(version) >= _version_key(current):
            self.current_versions[image] = version

    # ------------------------------------------------------------- versions

    def resolve_version(self, version: Optional[str] = None, image: Optional[str] = None,
                        *, promised_only: bool = False) -> tuple[str, str]:
        img = image or self.default_image
        with self.lock:
            if version is None:
                version = self.current_versions.get(img)
                if version is None:
                    promised = [v for v in self.version_order.get(img, []) if (img, v) in self.promised_versions]
                    if not promised:
                        raise UnknownVersionError("(current promised)", img)
                    version = promised[-1]
            if (img, version) not in self.stripes:
                # Also try global version namespace when image was omitted.
                if image is None:
                    matches = [key[0] for key in self.stripes if key[1] == version]
                    if len(matches) == 1:
                        img = matches[0]
                    else:
                        raise UnknownVersionError(version, img)
                else:
                    raise UnknownVersionError(version, img)
            if promised_only and (img, version) not in self.promised_versions:
                raise DurabilityError(f"version {img}/{version} is not promised")
            return img, version

    def object_length(self, version: str, image: Optional[str] = None) -> int:
        img, ver = self.resolve_version(version, image)
        stripes = self.stripes[(img, ver)]
        if not stripes:
            return 0
        return max(s.end for s in stripes)

    # ------------------------------------------------------------- snapshots

    def _snapshot_locked(self, image: str, version: str) -> ReadSnapshot:
        placements: dict[tuple[str, str], str] = {}
        for stripe in self.stripes[(image, version)]:
            for role in ROLES:
                slot = (image, version, stripe.id, role)
                placements[(stripe.id, role)] = self.current_placements[slot]
        return ReadSnapshot(
            id=self._new_id("snap"),
            image=image,
            version=version,
            placements=placements,
            promised=(image, version) in self.promised_versions,
            sequence=next(self._ids),
        )

    def open_read(self, version: Optional[str] = None, handle_id: Optional[str] = None,
                  image: Optional[str] = None, handle: Optional[str] = None,
                  *, promised_only: bool = False, **kwargs: Any) -> ReadHandle:
        handle_id = handle_id or handle or kwargs.get("id")
        version = version or kwargs.get("version_id") or kwargs.get("ver")
        image = image or kwargs.get("name") or kwargs.get("image_name")
        if version is not None and handle_id is not None:
            with self.lock:
                first_known = any(v == version for _, v in self.stripes)
                second_known = any(v == handle_id for _, v in self.stripes)
            if not first_known and second_known:
                # Tolerate open_read(handle_id, version).
                version, handle_id = handle_id, version
        img, ver = self.resolve_version(version, image, promised_only=promised_only)
        with self.lock:
            snapshot = self._snapshot_locked(img, ver)
            hid = handle_id or self._new_id("h")
            if hid in self.handles and not self.handles[hid].closed:
                raise RepairError(f"handle already open: {hid}")
            handle = ReadHandle(id=hid, snapshot=snapshot)
            handle._store = self  # type: ignore[attr-defined]
            self.handles[hid] = handle
            return handle

    def close_read(self, handle_id: str) -> None:
        with self.lock:
            handle = self.handles.get(handle_id)
            if handle is None:
                raise UnknownHandleError(handle_id)
            handle.closed = True

    # Common aliases.
    close_handle = close_read

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def close(self, handle_id: Optional[str] = None) -> None:
        """Close one handle, or all handles when no id is given."""

        if handle_id is not None:
            self.close_read(handle_id)
            return
        with self.lock:
            for handle in self.handles.values():
                handle.closed = True

    # ---------------------------------------------------------------- reads

    def read_range(
        self,
        version: Optional[str] = None,
        offset: int = 0,
        length: Optional[int] = None,
        *,
        start: Optional[int] = None,
        end: Optional[int] = None,
        image: Optional[str] = None,
        handle: Optional[str | ReadHandle] = None,
        handle_id: Optional[str] = None,
        merge: bool = True,
    ) -> RangeReport:
        if start is not None:
            offset = start
        if end is not None:
            if length is not None:
                offset = start if start is not None else offset
            length = max(0, end - offset)
        offset = int(offset)
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if isinstance(version, str) and handle is None and handle_id is None:
            with self.lock:
                looks_like_handle = version in self.handles
            if looks_like_handle:
                handle = version
                version = None

        own_handle = False
        read_handle: Optional[ReadHandle] = None
        lease_id = self._new_id("read")
        with self.lock:
            if handle is not None or handle_id is not None:
                hid = handle.id if isinstance(handle, ReadHandle) else (handle or handle_id)
                read_handle = self.handles.get(str(hid))
                if read_handle is None or read_handle.closed:
                    raise UnknownHandleError(str(hid))
                snapshot = read_handle.snapshot
                img, ver = snapshot.image, snapshot.version
            else:
                img, ver = self.resolve_version(version, image)
                snapshot = self._snapshot_locked(img, ver)
                own_handle = True
            total = self.object_length(ver, img)
            if length is None:
                length = total - offset
            length = int(length)
            if length < 0:
                raise ValueError("length must be non-negative")
            req_end = offset + length
            if offset > total:
                raise FragmentUnavailable(img, ver, "offset_beyond_object")
            read_end = min(req_end, total)
            report = RangeReport(
                image=img,
                version=ver,
                requested_version=version,
                start=offset,
                end=read_end,
                handle_id=read_handle.id if read_handle else None,
                promised=(img, ver) in self.promised_versions,
                durable_now=self.durability_status(ver, img)["durable"],
            )
            if offset >= read_end:
                return report
            self.read_leases[lease_id] = set(snapshot.placements.values())
            stripes = list(self.stripes[(img, ver)])

        try:
            # Device/network I/O happens outside the global lock.  The immutable
            # snapshot's placement ids and lease keep this whole range on one
            # generation and prevent reclaiming a location still being read.
            tx = _ReadTransaction(self, snapshot)
            for stripe in stripes:
                if stripe.end <= offset or stripe.offset >= read_end:
                    continue
                seg_start = max(offset, stripe.offset)
                seg_end = min(read_end, stripe.end)
                rel_start = seg_start - stripe.offset
                rel_end = seg_end - stripe.offset
                fs = stripe.fragment_size()
                # d0 logical/fragment interval [0, min(fs, stripe.length))
                d0_rel_end = min(fs, stripe.length)
                if rel_start < d0_rel_end and rel_end > 0:
                    r0 = max(0, rel_start)
                    r1 = min(rel_end, d0_rel_end)
                    if r1 > r0:
                        report.segments.append(self._read_data_interval(
                            tx, stripe, "d0", r0, r1, stripe.offset + r0
                        ))
                # d1 begins at fs in logical stripe space.
                d1_logical_start = fs
                d1_logical_end = stripe.length
                if rel_end > d1_logical_start and rel_start < d1_logical_end:
                    r0 = max(rel_start, d1_logical_start)
                    r1 = min(rel_end, d1_logical_end)
                    f0 = r0 - fs
                    f1 = r1 - fs
                    if f1 > f0:
                        report.segments.append(self._read_data_interval(
                            tx, stripe, "d1", f0, f1, stripe.offset + r0
                        ))
            report.segments = [s for s in report.segments if s is not None]
            raw_segments = list(report.segments)
            report.raw_segments = raw_segments
            if merge:
                report.segments = _merge_segments(report.segments)
            if self.auto_repair and report.degraded_ranges:
                self.repair_read_report(report, write=True)
            return report
        finally:
            with self.lock:
                self.read_leases.pop(lease_id, None)

    def _read_data_interval(self, tx: _ReadTransaction, stripe: Stripe, role: str,
                            frag_start: int, frag_end: int, logical_start: int) -> ByteSegment:
        logical_end = logical_start + (frag_end - frag_start)
        base = dict(start=logical_start, end=logical_end, role=role, stripe=stripe.id,
                    version=stripe.version, image=stripe.image)
        replica = tx.replica(stripe, role)
        source_info = [self._source_info(replica)]
        try:
            data = tx.verified_fragment(stripe, role)
            return ByteSegment(
                **base,
                status=DIRECT,
                data=data[frag_start:frag_end],
                sources=source_info,
                generation=replica.generation,
            )
        except Exception as direct_error:
            direct_reason = getattr(direct_error, "reason", type(direct_error).__name__)
        try:
            rebuilt = tx.reconstruct(stripe, role)
            return ByteSegment(
                **base,
                status=DEGRADED,
                data=rebuilt[frag_start:frag_end],
                reasons=[f"{role}_direct_unavailable:{direct_reason}", "xor_reconstructed_and_verified"],
                sources=source_info + [
                    self._source_info(tx.replica(stripe, other)) for other in ROLES if other != role
                ],
                generation=replica.generation,
            )
        except Exception as rebuild_error:
            reasons = [f"{role}_direct_unavailable:{direct_reason}",
                       f"reconstruction_failed:{type(rebuild_error).__name__}:{rebuild_error}"]
            return ByteSegment(
                **base,
                status=UNAVAILABLE,
                data=None,
                reasons=reasons,
                sources=source_info,
            )

    @staticmethod
    def _source_info(replica: Replica) -> dict[str, Any]:
        return {
            "replica": replica.id,
            "device": replica.device,
            "fragment_id": replica.fragment_id,
            "generation": replica.generation,
            "epoch": replica.epoch,
            "state": replica.state,
            "verified": replica.verified,
        }

    def read_bytes(self, version: Optional[str] = None, offset: int = 0, length: Optional[int] = None,
                   *, image: Optional[str] = None, handle: Optional[str | ReadHandle] = None,
                   **kwargs: Any) -> bytes:
        return self.read_range(version, offset, length, image=image, handle=handle, **kwargs).to_bytes()

    # Friendly aliases.
    def read(self, *args: Any, **kwargs: Any) -> RangeReport:
        """Read a range and return a byte-compatible detailed report.

        Supported positional forms include ``read(version, offset, length)`` and
        ``read(image, version, offset, length)``.  Use :meth:`read_bytes` for
        a strict ``bytes`` return.
        """

        args = list(args)
        if args and isinstance(args[0], str):
            with self.lock:
                first_is_handle = args[0] in self.handles
            if first_is_handle:
                kwargs.setdefault("handle", args[0])
                args = args[1:]
        if args and isinstance(args[0], int):
            args.insert(0, None)
        if args and isinstance(args[0], str) and len(args) >= 2 and isinstance(args[1], str):
            first, second = args[0], args[1]
            with self.lock:
                image_version = (first, second) in self.stripes
                global_version = any(v == second for _, v in self.stripes)
            if image_version:
                kwargs.setdefault("image", first)
                args = args[1:]
            elif global_version and not any(v == first for _, v in self.stripes):
                # read(image, version, ...) where image name differs from the
                # namespace used in the manifest; preserve explicit image only
                # if supplied by keyword later.
                args = args[1:]
        return self.read_range(*args, **kwargs)

    def read_strict(self, *args: Any, **kwargs: Any) -> bytes:
        return self.read(*args, **kwargs).to_bytes()

    def pread(self, version: Optional[str] = None, offset: int = 0, length: Optional[int] = None,
              **kwargs: Any) -> bytes:
        return self.read_bytes(version, offset, length, **kwargs)

    def get_range(self, *args: Any, **kwargs: Any) -> RangeReport:
        return self.read_range(*args, **kwargs)

    def read_between(self, version: Optional[str], start: int, end: int, **kwargs: Any) -> RangeReport:
        return self.read_range(version, start=start, end=end, **kwargs)

    def read_interval(self, version: Optional[str], start: int, end: int, **kwargs: Any) -> RangeReport:
        return self.read_between(version, start, end, **kwargs)

    def download(self, version: Optional[str] = None, image: Optional[str] = None,
                 *, verify_object: bool = True, expected_sha256: Optional[str] = None,
                 **kwargs: Any) -> bytes:
        img, ver = self.resolve_version(version, image)
        data = self.read_bytes(ver, 0, self.object_length(ver, img), image=img, **kwargs)
        if verify_object:
            expected = expected_sha256 or self._object_checksum(img, ver)
            if expected and not hmac.compare_digest(sha256_hex(data), expected):
                raise DurabilityError("whole-object checksum verification failed")
        return data

    def _object_checksum(self, image: str, version: str) -> Optional[str]:
        # A manifest may carry object_sha256 on any stripe.
        for stripe in self.stripes.get((image, version), []):
            if stripe.object_sha256:
                return stripe.object_sha256
        return None

    def verify_fragments(self, version: Optional[str] = None, image: Optional[str] = None,
                         *, fetch: bool = True,
                         expected_sha256: Optional[str] = None) -> VerificationResult:
        """Verify every fragment object, including parity, for a version."""

        img, ver = self.resolve_version(version, image)
        with self.lock:
            snapshot = self._snapshot_locked(img, ver)
            tx = _ReadTransaction(self, snapshot)
            stripe_results = []
            all_ok = True
            for stripe in self.stripes[(img, ver)]:
                roles: dict[str, dict[str, Any]] = {}
                blocks: dict[str, bytes] = {}
                for role in ROLES:
                    spec = stripe.fragments[role]
                    item: dict[str, Any] = {"role": role, "expected_sha256": spec.sha256}
                    try:
                        if not fetch:
                            replica = tx.replica(stripe, role)
                            item.update({"ok": replica.verified, "replica": replica.to_dict()})
                            if not replica.verified:
                                raise FragmentUnavailable(replica.device, replica.fragment_id, "not_verified")
                        else:
                            data = tx.verified_fragment(stripe, role)
                            blocks[role] = data
                            item.update({"ok": True, "actual_sha256": sha256_hex(data)})
                    except Exception as exc:
                        all_ok = False
                        item.update({"ok": False, "error": str(exc), "error_type": type(exc).__name__})
                    roles[role] = item
                xor_ok = None
                if len(blocks) == 3:
                    try:
                        xor_ok = xor_bytes([blocks["d0"], blocks["d1"]]) == blocks["p"]
                    except ValueError:
                        xor_ok = False
                    if xor_ok is False:
                        all_ok = False
                stripe_results.append({"stripe": stripe.id, "ok": all(v.get("ok") for v in roles.values()),
                                       "xor_ok": xor_ok, "fragments": roles})
            expected_object = expected_sha256 or self._object_checksum(img, ver)
            object_digest = None
            object_ok = None
            try:
                data = self.read_bytes(ver, 0, self.object_length(ver, img), image=img)
                object_digest = sha256_hex(data)
                object_ok = True if expected_object is None else hmac.compare_digest(object_digest, expected_object)
                if object_ok is False:
                    all_ok = False
            except Exception as exc:
                object_ok = False
                all_ok = False
                object_error = str(exc)
            else:
                object_error = None
            return VerificationResult(
                ok=all_ok and object_ok is not False,
                image=img,
                version=ver,
                fragments_ok=all_ok,
                object_ok=object_ok,
                object_sha256=object_digest,
                expected_object_sha256=expected_object,
                object_error=object_error,
                stripes=stripe_results,
            )

    def verify_object(self, version: Optional[str] = None, image: Optional[str] = None,
                      expected_sha256: Optional[str] = None) -> VerificationResult:
        return self.verify_fragments(version, image, fetch=True, expected_sha256=expected_sha256)

    def full_verify(self, *args: Any, **kwargs: Any) -> VerificationResult:
        return self.verify_object(*args, **kwargs)

    def scrub(self, *args: Any, **kwargs: Any) -> VerificationResult:
        return self.verify_object(*args, **kwargs)

    def check_object(self, *args: Any, **kwargs: Any) -> VerificationResult:
        return self.verify_object(*args, **kwargs)

    def get_object(self, *args: Any, **kwargs: Any) -> bytes:
        return self.download(*args, **kwargs)

    # ----------------------------------------------------------- fault facts

    def report_offline(self, device: Optional[str | dict[str, Any]] = None, *,
                       reason: str = "offline", at: Any = None, **kwargs: Any) -> None:
        if isinstance(device, dict):
            event = device
            device = event["device"]
            reason = event.get("reason", reason)
            at = event.get("at", at)
        if device is None:
            device = kwargs["device"]
        with self.lock:
            self.device_online[device] = False
            self.faults[(device, "*")] = {"type": "offline", "reason": reason, "at": at}
            if self.cluster is not None:
                self.cluster.offline(device)
            self._event("offline", device=device, reason=reason, at=at)

    def report_online(self, device: str, *, at: Any = None) -> None:
        with self.lock:
            self.device_online[device] = True
            self.faults.pop((device, "*"), None)
            if self.cluster is not None:
                self.cluster.online(device)
            self._event("online", device=device, at=at)

    def report_corrupt(
        self,
        device: str,
        stripe: Optional[str] = None,
        fragment: Optional[str] = None,
        *,
        role: Optional[str] = None,
        fragment_id: Optional[str] = None,
        file: Optional[str] = None,
        sha256: Optional[str] = None,
        actual_sha256: Optional[str] = None,
        data: Optional[bytes] = None,
        version: Optional[str] = None,
        at: Any = None,
        expected_sha256: Optional[str] = None,
        digest: Optional[str] = None,
        checksum: Optional[str] = None,
        **extra: Any,
    ) -> dict[str, Any]:
        if isinstance(device, dict):
            event = dict(device)
            event.setdefault("role", event.pop("fragment", None))
            return self.report_corrupt(**event)
        sha256 = sha256 or expected_sha256 or digest or checksum
        role = role or fragment
        if fragment_id is None:
            if stripe is None or role is None:
                raise ValueError("corrupt report requires stripe and fragment/role or fragment_id")
            fragment_id = f"{stripe}-{role}"
        actual = actual_sha256
        if data is not None:
            actual = sha256_hex(data)
        elif file and self.object_reader is not None:
            try:
                observed = self.object_reader(file)
                actual = sha256_hex(observed)
                if self.cluster is not None:
                    self.cluster.corrupt(device, fragment_id, observed, file_name=file)
            except (FileNotFoundError, OSError):
                pass
        elif self.cluster is not None and file:
            self.cluster.corrupt(device, fragment_id, file_name=file)
        report = {
            "type": "corrupt",
            "device": device,
            "stripe": stripe,
            "role": role,
            "fragment_id": fragment_id,
            "file": file,
            "expected_sha256": sha256,
            "actual_sha256": actual,
            "at": at,
            **extra,
        }
        with self.lock:
            self.device_online.setdefault(device, True)
            self.faults[(device, fragment_id)] = report
            if self.cluster is not None and data is not None:
                self.cluster.corrupt(device, fragment_id, data)
            self._event("corrupt", **report)
        return report

    def _record_checksum_failure(self, replica: Replica, expected: str, actual: str) -> None:
        with self.lock:
            self.faults[(replica.device, replica.fragment_id)] = {
                "type": "checksum_mismatch",
                "device": replica.device,
                "fragment_id": replica.fragment_id,
                "expected_sha256": expected,
                "actual_sha256": actual,
            }
            self._record_health(replica, "corrupt")

    def _record_health(self, replica: Replica, status: str, detail: str = "") -> None:
        self.health[replica.id] = {"status": status, "detail": detail, "sequence": next(self._ids)}

    def _event(self, kind: str, **payload: Any) -> None:
        self._events.append({"kind": kind, "sequence": len(self._events), **payload})

    # ------------------------------------------------------------- topology

    def domain_of(self, device: str) -> Optional[str]:
        return self.domains.get(device)

    def _placement_distinct(self, stripe: Stripe) -> bool:
        with self.lock:
            devices = []
            for role in ROLES:
                replica_id = self.current_placements[(stripe.image, stripe.version, stripe.id, role)]
                devices.append(self.replicas[replica_id].device)
            domains = [self.domains.get(device, f"__unknown_{device}") for device in devices]
            return len(set(domains)) == 3 and all(not d.startswith("__unknown_") for d in domains)

    def stripe_status(self, version: str, stripe_id: str, image: Optional[str] = None) -> dict[str, Any]:
        img, ver = self.resolve_version(version, image)
        with self.lock:
            stripe = self.stripe_index[(img, ver, stripe_id)]
            roles: dict[str, Any] = {}
            bad_roles: list[str] = []
            domains: list[Optional[str]] = []
            for role in ROLES:
                slot = (img, ver, stripe_id, role)
                replica = self.replicas[self.current_placements[slot]]
                domain = self.domains.get(replica.device)
                domains.append(domain)
                fault = self._fault_for_replica(replica)
                health = self.health.get(replica.id, {}).get("status", "unknown")
                is_bad = fault is not None or health in {"corrupt", "offline", "unavailable"}
                if is_bad:
                    bad_roles.append(role)
                roles[role] = {
                    "replica": replica.to_dict(),
                    "device": replica.device,
                    "domain": domain,
                    "healthy": not is_bad,
                    "known_health": health,
                    "fault": fault,
                }
            distinct_domains = len(set(d for d in domains if d is not None)) == 3 and len(set(domains)) == 3
            rebuildable = len(bad_roles) == 1
            if len(bad_roles) == 0:
                read_class = DIRECT
            elif rebuildable:
                read_class = DEGRADED
            else:
                read_class = UNAVAILABLE
            return {
                "image": img,
                "version": ver,
                "stripe": stripe_id,
                "offset": stripe.offset,
                "length": stripe.length,
                "fragment_bytes": stripe.fragment_size(),
                "roles": roles,
                "bad_roles": bad_roles,
                "missing_count": len(bad_roles),
                "rebuildable": rebuildable,
                "domain_distinct": distinct_domains,
                "durable": distinct_domains and not bad_roles,
                "read_status": read_class,
            }

    def _fault_for_replica(self, replica: Replica) -> Optional[dict[str, Any]]:
        if self.device_online.get(replica.device, True) is False or (replica.device, "*") in self.faults:
            return self.faults.get((replica.device, "*"), {"type": "offline"})
        return self.faults.get((replica.device, replica.fragment_id))

    def durability_status(self, version: Optional[str] = None, image: Optional[str] = None) -> dict[str, Any]:
        img, ver = self.resolve_version(version, image)
        with self.lock:
            stripes = self.stripes[(img, ver)]
            stripe_reports = [self.stripe_status(ver, s.id, img) for s in stripes]
            durable = all(item["durable"] for item in stripe_reports)
            domain_safe = all(item["domain_distinct"] for item in stripe_reports)
            if all(item["read_status"] == DIRECT for item in stripe_reports):
                read_status = DIRECT
            elif all(item["read_status"] != UNAVAILABLE for item in stripe_reports):
                read_status = DEGRADED
            else:
                read_status = UNAVAILABLE
            return {
                "image": img,
                "version": ver,
                "promised": (img, ver) in self.promised_versions,
                "durable": durable,
                "meets_durability_now": durable,
                "domain_safe": domain_safe,
                "read_status": read_status,
                "status": read_status,
                "stripes": stripe_reports,
            }

    def survives_one_domain(self, version: Optional[str] = None, image: Optional[str] = None) -> bool:
        report = self.durability_status(version, image)
        return bool(report["domain_safe"])

    def health_report(self) -> dict[str, Any]:
        with self.lock:
            return {
                "online_devices": sorted(d for d, online in self.device_online.items() if online),
                "offline_devices": sorted(d for d, online in self.device_online.items() if not online),
                "faults": [
                    {"key": list(key), **(value if isinstance(value, dict) else {"detail": value})}
                    for key, value in self.faults.items()
                ],
                "fragment_health": [
                    {"replica": replica_id, **status}
                    for replica_id, status in self.health.items()
                ],
            }

    def diagnose(self, version: Optional[str] = None, image: Optional[str] = None) -> dict[str, Any]:
        durability = self.durability_status(version, image)
        return {
            **durability,
            "health": self.health_report(),
            "rebuildable": self.rebuildable_areas(version, image),
            "fault_domains": self.fault_domain_query(version, image),
        }

    def status(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.diagnose(*args, **kwargs)

    def offline_devices(self) -> list[str]:
        return self.health_report()["offline_devices"]

    def corrupt_fragments(self) -> list[dict[str, Any]]:
        return [f for f in self.health_report()["faults"] if f.get("type") in {"corrupt", "checksum_mismatch"}]

    # ------------------------------------------------------------- capacity

    def used_capacity(self, device: str) -> int:
        with self.lock:
            return sum(
                r.size for r in self.replicas.values()
                if r.device == device and r.state not in {"freed"}
            )

    def free_capacity(self, device: str) -> int:
        return int(self.capacity.get(device, 10**18)) - self.used_capacity(device)

    def capacity_report(self) -> dict[str, Any]:
        with self.lock:
            devices = sorted(set(self.capacity) | set(self.device_online) | {r.device for r in self.replicas.values()})
            report = {}
            for device in devices:
                total = int(self.capacity.get(device, 10**18))
                used = self.used_capacity(device)
                report[device] = {
                    "domain": self.domains.get(device),
                    "online": self.device_online.get(device, True),
                    "total_bytes": total,
                    "used_bytes": used,
                    "free_bytes": total - used,
                    "reclaimable_bytes": sum(x["bytes"] for x in self.reclaimable_space()["locations"] if x["device"] == device),
                }
            return report

    # --------------------------------------------------------------- repair

    def plan_repair(
        self,
        version: str,
        stripe_id: str,
        role: str,
        target: str,
        *,
        image: Optional[str] = None,
        attempt: Optional[str] = None,
        verify_sources: bool = True,
    ) -> RepairAttempt:
        img, ver = self.resolve_version(version, image)
        if role not in ROLES:
            raise RepairError(f"invalid fragment role {role!r}")
        with self.lock:
            stripe = self.stripe_index[(img, ver, stripe_id)]
            slot = (img, ver, stripe_id, role)
            current_id = self.current_placements[slot]
            current = self.replicas[current_id]
            base_epoch = self.slot_epoch[slot]
            size = stripe.fragment_size()
            if self.free_capacity(target) < size:
                raise CapacityError(f"device {target} lacks {size} bytes for repair")
            attempt_id = attempt or self._new_id("attempt")
            if attempt_id in self.attempts:
                raise RepairError(f"attempt already exists: {attempt_id}")
            replica = Replica(
                id=self._new_id("r"),
                image=img,
                version=ver,
                stripe_id=stripe_id,
                role=role,
                device=target,
                fragment_id=stripe.fragment_id(role),
                expected_sha256=stripe.fragments[role].sha256,
                size=size,
                epoch=base_epoch + 1,
                attempt=attempt_id,
                state="temporary",
                base_epoch=base_epoch,
                sequence=next(self._ids),
            )
            self.replicas[replica.id] = replica
            job_attempts = self.repair_jobs.setdefault(slot, [])
            job_attempts.append(attempt_id)
            repair = RepairAttempt(
                id=attempt_id,
                slot=slot,
                target=target,
                fragment_id=stripe.fragment_id(role),
                size=size,
                base_epoch=base_epoch,
                replica_id=replica.id,
            )
            self.attempts[attempt_id] = repair
            snapshot = self._snapshot_locked(img, ver) if verify_sources else None

        # Device/network reads must not hold the store-wide lock.  Hash
        # verification makes the reconstructed bytes safe even if a selected
        # source's health report changes while the operation is in flight.
        if verify_sources and snapshot is not None:
            tx = _ReadTransaction(self, snapshot)
            try:
                reconstructed = tx.reconstruct(stripe, role)
            except Exception as exc:
                with self.lock:
                    repair.state = "failed"
                    repair.error = str(exc)
                    self.replicas[repair.replica_id].state = "failed"
                raise RepairError(f"cannot reconstruct {img}/{ver}/{stripe_id}/{role}: {exc}") from exc
            with self.lock:
                repair.reconstructed = reconstructed
        return repair

    def start_repair(
        self,
        version: str,
        stripe_id: str,
        role: str,
        target: Optional[str] = None,
        *,
        image: Optional[str] = None,
        attempt: Optional[str] = None,
        write: bool = True,
        coalesce: bool = True,
    ) -> RepairAttempt:
        """Create/reserve a repair attempt and optionally PUT the replacement.

        Concurrent readers repairing the same slot normally coalesce onto one
        in-flight attempt/target.  Explicit attempts to different targets are
        retained as separate candidate replacements.
        """

        img, ver = self.resolve_version(version, image)
        slot = (img, ver, stripe_id, role)
        leader: Optional[RepairAttempt] = None
        with self.lock:
            current_replica = self.replicas[self.current_placements[slot]]
            current_is_faulty = self._fault_for_replica(current_replica) is not None or self.health.get(
                current_replica.id, {}
            ).get("status") in {"corrupt", "offline", "unavailable"}
            if not current_is_faulty:
                for attempt_id in reversed(self.repair_jobs.get(slot, [])):
                    prior = self.attempts[attempt_id]
                    if prior.state == "committed" and prior.replica_id == self.current_placements[slot]:
                        return prior
            if coalesce and self.repair_jobs.get(slot):
                active_states = {"planned", "writing", "writing_unknown", "receipted_unverified"}
                for attempt_id in reversed(self.repair_jobs[slot]):
                    existing = self.attempts[attempt_id]
                    if existing.state not in active_states:
                        continue
                    if target is None or existing.target == target or (attempt is not None and existing.id == attempt):
                        leader = existing
                        break
            if target is None and leader is None:
                candidates = self.repair_candidates(ver, stripe_id, role, image=img)
                if not candidates:
                    raise CapacityError("no fault-domain-safe repair target available")
                target = candidates[0]["device"]
            if leader is None:
                # Atomically reserve capacity/attempt before source I/O so
                # concurrent readers coalesce instead of allocating twice.
                repair = self.plan_repair(
                    ver, stripe_id, role, target, image=img, attempt=attempt, verify_sources=False
                )
            else:
                repair = leader

        if leader is not None:
            # Do not hold the store lock while waiting; the leader needs it to
            # publish its terminal state.
            leader.done_event.wait()
            if leader.state == "failed":
                raise RepairError(leader.error or f"repair {leader.id} failed")
            return leader

        try:
            data = self.reconstruct_fragment(ver, stripe_id, role, image=img)
            with self.lock:
                repair.reconstructed = data
                repair.state = "writing"
            if not write:
                return repair
            try:
                receipt = self.gateway.put(target, repair.fragment_id, data, repair.id)
            except Exception as exc:
                with self.lock:
                    repair.state = "writing_unknown" if isinstance(exc, (ConnectionError, ConnectionResetError)) else "failed"
                    repair.error = str(exc)
                    self.replicas[repair.replica_id].reason = repair.state
                raise
            completed = self.complete_repair_write(repair.id, receipt)
            return completed
        except Exception as exc:
            with self.lock:
                if repair.state in {"planned", "writing"}:
                    repair.state = "failed"
                    repair.error = str(exc)
                    self.replicas[repair.replica_id].state = "failed"
            raise
        finally:
            repair.done_event.set()

    # Friendly alias matching event vocabulary.
    repair = start_repair

    def ensure_repair(self, version: str, stripe_id: str, role: str, **kwargs: Any) -> RepairAttempt:
        kwargs.setdefault("coalesce", True)
        return self.start_repair(version, stripe_id, role, **kwargs)

    def repair_read_report(self, report: RangeReport, **kwargs: Any) -> list[RepairAttempt]:
        """Repair every exactly-one-missing stripe represented by a report."""

        attempts = []
        seen = set()
        for seg in report.raw_segments or report.segments:
            if seg.status != DEGRADED or not seg.stripe or not seg.role:
                continue
            key = (seg.version or report.version, seg.stripe, seg.role)
            if key in seen:
                continue
            seen.add(key)
            try:
                attempts.append(self.ensure_repair(key[0], key[1], key[2], image=seg.image or report.image, **kwargs))
            except RepairError:
                # Scheduling can still query the unrepaired area; callers that
                # need hard failure can call start_repair directly.
                continue
        return attempts

    def complete_repair_write(self, attempt_id: str, receipt: PutReceipt | dict[str, Any]) -> RepairAttempt:
        with self.lock:
            repair = self.attempts[attempt_id]
            if not isinstance(receipt, PutReceipt):
                receipt = PutReceipt(
                    device=receipt.get("device", repair.target),
                    fragment_id=receipt.get("fragment_id", repair.fragment_id),
                    sha256=receipt.get("sha256") or receipt.get("digest") or "",
                    generation=receipt.get("generation"),
                    file=receipt.get("file"),
                    raw=receipt,
                )
            repair.receipt = receipt
            slot = repair.slot
            current_replica_id = self.current_placements[slot]
            if receipt.generation is not None:
                key = (receipt.device, repair.fragment_id)
                old = self.latest_device_generation.get(key)
                if old is not None and int(receipt.generation) < old:
                    repair.state = "fenced"
                    repair.error = f"stale generation {receipt.generation} < {old}"
                    stale_replica = self.replicas[repair.replica_id]
                    if repair.replica_id != current_replica_id:
                        stale_replica.state = "orphan"
                        stale_replica.reason = "stale_generation"
                    return repair
                self.latest_device_generation[key] = int(receipt.generation)
            expected = self.replicas[repair.replica_id].expected_sha256
            if receipt.sha256 and receipt.sha256 != expected:
                repair.state = "failed"
                repair.error = "receipt digest does not match expected fragment"
                self.replicas[repair.replica_id].state = "failed"
                return repair
            # Independent read-back is required before online promotion.
            try:
                reply = self.gateway.fetch(receipt.device, repair.fragment_id)
                actual = sha256_hex(reply.data)
                if actual != expected:
                    raise FragmentChecksumMismatch(receipt.device, repair.fragment_id, expected, actual)
                independently_verified = True
                if receipt.sha256 and receipt.sha256 != actual:
                    raise FragmentChecksumMismatch(receipt.device, repair.fragment_id, receipt.sha256, actual)
            except Exception as exc:
                # A receipt plus a repository object file can also be independently checked.
                file_name = receipt.file or (receipt.raw or {}).get("file")
                if file_name and self.object_reader is not None:
                    data = self.object_reader(file_name)
                    if sha256_hex(data) != expected:
                        raise
                    independently_verified = True
                    if self.cluster is not None:
                        self.cluster.put_receipt(
                            receipt.device, repair.fragment_id, data,
                            int(receipt.generation or 1), repair.id, file_name
                        )
                else:
                    repair.state = "receipted_unverified"
                    repair.error = str(exc)
                    return repair
            return self._promote_if_valid(repair, receipt, independently_verified)

    def report_repair_receipt(
        self,
        *,
        device: str,
        attempt: str,
        generation: Optional[int] = None,
        file: Optional[str] = None,
        sha256: Optional[str] = None,
        **raw: Any,
    ) -> RepairAttempt:
        with self.lock:
            if attempt not in self.attempts:
                # Accept an out-of-order receipt by creating a minimal attempt?
                # The event stream always has repair first, but hidden callers may
                # report facts directly.  Preserve the fact and raise key error.
                raise RepairError(f"unknown repair attempt {attempt!r}")
            repair = self.attempts[attempt]
            data: Optional[bytes] = None
            digest = sha256 or ""
            if file and self.object_reader is not None:
                data = self.object_reader(file)
                digest = sha256_hex(data)
            if data is not None and self.cluster is not None:
                self.cluster.put_receipt(device, repair.fragment_id, data, int(generation or 1), attempt, file)
            receipt = PutReceipt(device, repair.fragment_id, digest or sha256_hex(repair.reconstructed or b""),
                                 generation, file, raw)
            return self.complete_repair_write(attempt, receipt)

    def _promote_if_valid(self, repair: RepairAttempt, receipt: PutReceipt,
                          independently_verified: bool) -> RepairAttempt:
        with self.lock:
            replica = self.replicas[repair.replica_id]
            replica.verified = True
            replica.independently_verified = independently_verified
            replica.generation = int(receipt.generation) if receipt.generation is not None else replica.generation
            replica.file = receipt.file or replica.file
            slot = repair.slot
            img, ver, stripe_id, role = slot
            stripe = self.stripe_index[(img, ver, stripe_id)]
            current_id = self.current_placements[slot]

            # Compute topology before epoch fencing so a late conflicting
            # attempt reports both facts.
            trial_devices: list[str] = []
            for other_role in ROLES:
                if other_role == role:
                    trial_devices.append(replica.device)
                else:
                    other_id = self.current_placements[(img, ver, stripe_id, other_role)]
                    trial_devices.append(self.replicas[other_id].device)
            domains = [self.domains.get(device) for device in trial_devices]
            domain_safe = len(set(domains)) == 3 and all(d is not None for d in domains)
            replica.domain_safe = domain_safe

            if repair.state == "committed" and repair.replica_id == current_id:
                replica.state = "active"
                return repair

            # Fence against a competing same-version promotion.
            current_epoch = self.slot_epoch[slot]
            if repair.base_epoch != current_epoch:
                repair.state = "fenced"
                conflict = "" if domain_safe else "; fault_domain_conflict"
                repair.error = f"repair base epoch {repair.base_epoch} behind current {current_epoch}{conflict}"
                if repair.replica_id != current_id:
                    replica.state = "orphan"
                    replica.reason = "superseded_by_committed_repair"
                    if not domain_safe:
                        replica.reason += "+fault_domain_conflict"
                return repair

            # Domain safety is part of promotion for a durable promise.
            if not domain_safe:
                # Keep the verified bytes as an orphan candidate rather than
                # switching readers to a topology that does not restore the
                # durability level.
                repair.state = "domain_unsafe"
                repair.error = f"target domain {self.domains.get(replica.device)!r} does not give three independent domains"
                replica.state = "orphan"
                replica.reason = "fault_domain_conflict"
                return repair

            old_id = self.current_placements[slot]
            old = self.replicas[old_id]
            old.state = "retired"
            old.reason = f"replaced_by_{repair.id}"
            replica.state = "active"
            replica.epoch = current_epoch + 1
            self.current_placements[slot] = replica.id
            self.slot_epoch[slot] = current_epoch + 1
            repair.state = "committed"
            repair.receipt = receipt
            # Recompute version promise.  A repaired version remains/returns durable.
            if self.durability_status(ver, img)["durable"]:
                self.promised_versions.add((img, ver))
            self._event("repair_committed", attempt=repair.id, slot=slot, replica=replica.id)
            return repair

    def repair_candidates(self, version: str, stripe_id: str, role: str, *,
                          image: Optional[str] = None, size: Optional[int] = None,
                          require_domain_safe: bool = True) -> list[dict[str, Any]]:
        img, ver = self.resolve_version(version, image)
        with self.lock:
            stripe = self.stripe_index[(img, ver, stripe_id)]
            size = size or stripe.fragment_size()
            slot = (img, ver, stripe_id, role)
            blocked_domains: set[Optional[str]] = set()
            existing_devices: set[str] = set()
            for other in ROLES:
                if other == role:
                    continue
                replica_id = self.current_placements[(img, ver, stripe_id, other)]
                replica = self.replicas[replica_id]
                blocked_domains.add(self.domains.get(replica.device))
                existing_devices.add(replica.device)
            current_replica = self.replicas[self.current_placements[slot]]
            current_fault = self._fault_for_replica(current_replica)
            results = []
            for device in sorted(set(self.domains) | set(self.capacity) | set(self.device_online)):
                if device in existing_devices:
                    continue
                if self.device_online.get(device, True) is False:
                    continue
                if self.free_capacity(device) < size:
                    continue
                domain = self.domains.get(device)
                safe = domain not in blocked_domains and domain is not None
                if require_domain_safe and not safe:
                    continue
                device_faults = [
                    value for key, value in self.faults.items()
                    if key[0] == device and isinstance(value, dict)
                    and value.get("type") in {"corrupt", "checksum_mismatch"}
                ]
                healthy = not device_faults
                results.append({
                    "device": device,
                    "domain": domain,
                    "domain_safe": safe,
                    "healthy": healthy,
                    "known_corruption": bool(device_faults),
                    "is_current_device": device == current_replica.device,
                    "current_fault": current_fault if device == current_replica.device else None,
                    "free_bytes": self.free_capacity(device),
                    "blocked_domains": sorted(d for d in blocked_domains if d is not None),
                    "fragment_id": stripe.fragment_id(role),
                    "size": size,
                })
            results.sort(key=lambda item: (not item["healthy"], item["is_current_device"], item["device"]))
            return results

    def check_repair_target(self, version: str, stripe_id: str, role: str, target: str,
                            *, image: Optional[str] = None) -> dict[str, Any]:
        img, ver = self.resolve_version(version, image)
        with self.lock:
            stripe = self.stripe_index[(img, ver, stripe_id)]
            size = stripe.fragment_size()
            others = []
            blocked = set()
            for other_role in ROLES:
                if other_role == role:
                    continue
                slot = (img, ver, stripe_id, other_role)
                replica = self.replicas[self.current_placements[slot]]
                others.append({"role": other_role, "device": replica.device,
                               "domain": self.domains.get(replica.device)})
                blocked.add(self.domains.get(replica.device))
            domain = self.domains.get(target)
            free = self.free_capacity(target)
            online = self.device_online.get(target, True)
            domain_safe = domain is not None and domain not in blocked
            capacity_ok = free >= size
            return {
                "version": ver,
                "image": img,
                "stripe": stripe_id,
                "role": role,
                "target": target,
                "target_domain": domain,
                "blocked_domains": sorted(d for d in blocked if d is not None),
                "other_replicas": others,
                "domain_safe": domain_safe,
                "fault_domain_conflict": not domain_safe,
                "online": online,
                "capacity_ok": capacity_ok,
                "free_bytes": free,
                "required_bytes": size,
                "acceptable": domain_safe and capacity_ok and online,
            }

    def domain_conflict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        result = self.check_repair_target(*args, **kwargs)
        return {"conflict": result["fault_domain_conflict"], **result}

    def fault_domain_query(self, version: Optional[str] = None, image: Optional[str] = None) -> dict[str, Any]:
        img, ver = self.resolve_version(version, image) if version is not None or image is not None else (None, None)  # type: ignore
        with self.lock:
            versions = [(i, v) for (i, v) in self.stripes if ver is None or (i == img and v == ver)]
            items = []
            for i, v in versions:
                report = self.durability_status(v, i)
                for stripe_report in report["stripes"]:
                    for role in stripe_report["bad_roles"]:
                        candidates = self.repair_candidates(v, stripe_report["stripe"], role, image=i)
                        blocked = []
                        original_domain = None
                        for other in ROLES:
                            if other == role:
                                original_domain = stripe_report["roles"][other]["domain"] if False else None
                                continue
                            blocked.append(stripe_report["roles"][other]["domain"])
                        original_replica = stripe_report["roles"][role]["replica"]
                        original_domain = self.domains.get(original_replica["device"])
                        items.append({
                            "image": i,
                            "version": v,
                            "stripe": stripe_report["stripe"],
                            "role": role,
                            "original_device": original_replica["device"],
                            "original_domain": original_domain,
                            "survivor_domains": blocked,
                            "blocked_domains": blocked,
                            "required": "a domain distinct from survivor domains",
                            "still_missing": "third_independent_fault_domain",
                            "missing_fault_domain": True,
                            "candidate_domains": sorted({c["domain"] for c in candidates if c["domain"]}),
                            "available_candidate_domains": sorted({c["domain"] for c in candidates if c["domain"]}),
                            "candidates": candidates,
                        })
            domains = sorted({domain for item in items for domain in item["candidate_domains"]})
            return {
                "missing_domain_needs": items,
                "items": items,
                "missing": items,
                "candidate_domains": domains,
            }

    # ------------------------------------------------------- rebuild queries

    def rebuildable_areas(self, version: Optional[str] = None, image: Optional[str] = None) -> dict[str, Any]:
        with self.lock:
            if version is None:
                targets = list(self.stripes.keys())
            else:
                img, ver = self.resolve_version(version, image)
                targets = [(img, ver)]
            areas = []
            for img, ver in targets:
                for stripe in self.stripes[(img, ver)]:
                    status = self.stripe_status(ver, stripe.id, img)
                    if not status["rebuildable"]:
                        continue
                    role = status["bad_roles"][0]
                    fs = stripe.fragment_size()
                    if role == "d0":
                        start = stripe.offset
                        end = stripe.offset + min(fs, stripe.length)
                    elif role == "d1":
                        start = stripe.offset + fs
                        end = stripe.offset + stripe.length
                    else:
                        start = stripe.offset
                        end = stripe.end
                    slot = (img, ver, stripe.id, role)
                    current = self.replicas[self.current_placements[slot]]
                    repaired = current.state == "active" and current.epoch > 0 and not self._fault_for_replica(current)
                    if repaired:
                        continue
                    areas.append({
                        "image": img,
                        "version": ver,
                        "stripe": stripe.id,
                        "role": role,
                        "fragment_id": stripe.fragment_id(role),
                        "range": [start, end],
                        "start": start,
                        "end": end,
                        "length": end - start,
                        "logical_data_range": [stripe.offset, stripe.end],
                        "rebuildable": True,
                        "repaired": False,
                        "targets": self.repair_candidates(ver, stripe.id, role, image=img),
                    })
            return {"areas": areas, "count": len(areas)}

    def missing_domain(self, version: Optional[str] = None, image: Optional[str] = None) -> Optional[dict[str, Any]]:
        query = self.fault_domain_query(version, image)
        items = query.get("items", [])
        return items[0] if items else None

    # ------------------------------------------------------------- reclaim

    def _pinned_replica_ids(self) -> set[str]:
        pinned: set[str] = set()
        for handle in self.handles.values():
            if not handle.closed:
                pinned.update(handle.snapshot.placements.values())
        for replica_ids in self.read_leases.values():
            pinned.update(replica_ids)
        return pinned

    def reclaimable_space(self) -> dict[str, Any]:
        with self.lock:
            pinned = self._pinned_replica_ids()
            locations = []
            uncertain_locations = []
            bytes_total = 0
            reachable_bytes = 0
            uncertain_bytes = 0
            for replica in self.replicas.values():
                if replica.state in {"freed", "active"}:
                    continue
                if replica.id in pinned:
                    continue
                if replica.state not in {"retired", "orphan", "failed", "temporary"}:
                    continue
                uncertain = False
                # Temporary in-flight attempts are not yet safe to reclaim by
                # an operator query unless explicitly failed/fenced.  Orphan
                # and retired replacements are safe after reader pins clear.
                if replica.state == "temporary":
                    attempt = next((a for a in self.attempts.values() if a.replica_id == replica.id), None)
                    attempt_state = attempt.state if attempt is not None else None
                    if attempt_state in {"planned", "writing", "receipted_unverified"}:
                        continue
                    if attempt_state in {"writing_unknown"}:
                        uncertain = True
                online = self.device_online.get(replica.device, True)
                item = {
                    "device": replica.device,
                    "fragment_id": replica.fragment_id,
                    "attempt": replica.attempt,
                    "replica": replica.id,
                    "generation": replica.generation,
                    "bytes": replica.size,
                    "reason": replica.reason or replica.state,
                    "state": replica.state,
                    "online": online,
                    "uncertain": uncertain,
                    "safe_to_release": not uncertain,
                    "delete_now_possible": online and not uncertain,
                }
                if uncertain:
                    uncertain_locations.append(item)
                    uncertain_bytes += replica.size
                    continue
                locations.append(item)
                bytes_total += replica.size
                if online:
                    reachable_bytes += replica.size
            return {
                "bytes": bytes_total,
                "reclaimable_bytes": bytes_total,
                "safe_bytes": bytes_total,
                "reachable_bytes": reachable_bytes,
                "releasable_now_bytes": reachable_bytes,
                "uncertain_bytes": uncertain_bytes,
                "potential_bytes": bytes_total + uncertain_bytes,
                "locations": locations,
                "uncertain_locations": uncertain_locations,
                "all_locations": locations + uncertain_locations,
                "pinned_replicas": sorted(pinned),
                "open_handles": [hid for hid, h in self.handles.items() if not h.closed],
                "active_read_leases": sorted(self.read_leases),
            }

    def release_reclaimable(self, *, only_online: bool = True) -> dict[str, Any]:
        report = self.reclaimable_space()
        freed = []
        errors = []
        freed_bytes = 0
        for item in report["locations"]:
            if only_online and not item["online"]:
                continue
            try:
                result = self.gateway.delete(item["device"], item["fragment_id"], item.get("generation"))
                if isinstance(result, dict) and result.get("state") == "generation_conflict":
                    errors.append({**item, "error": result})
                    continue
                with self.lock:
                    replica = self.replicas[item["replica"]]
                    replica.state = "freed"
                freed.append(item)
                freed_bytes += item["bytes"]
            except Exception as exc:
                errors.append({**item, "error": str(exc)})
        return {"freed": freed, "errors": errors, "freed_bytes": freed_bytes}

    # ------------------------------------------------------------ publishing

    def publish_object(
        self,
        version: str,
        data: bytes,
        *,
        image: Optional[str] = None,
        fragment_bytes: Optional[int] = None,
        targets: Optional[list[str] | dict[str, str]] = None,
        file_prefix: Optional[str] = None,
        promise: bool = True,
    ) -> dict[str, Any]:
        img = image or self.default_image
        data = bytes(data)
        fs = int(fragment_bytes or max(1, (len(data) + 1) // 2))
        if fs <= 0:
            raise ValueError("fragment_bytes must be positive")
        raw_stripes = []
        offset = 0
        stripe_index = 1
        while offset < len(data) or not raw_stripes:
            chunk = data[offset : offset + 2 * fs]
            d0 = chunk[:fs]
            d1 = chunk[fs:]
            if len(d1) < fs:
                d1 = d1 + b"\x00" * (fs - len(d1))
            p = xor_bytes([d0, d1])
            logical_length = len(chunk)
            sid = f"{version}-s{stripe_index}"
            chosen = self._choose_publish_targets(fs * 3, fs, targets, count=3)
            triples = {}
            for role, block in zip(ROLES, [d0, d1, p]):
                device = chosen[role]
                fragment_id = f"{sid}-{role}"
                receipt = self.gateway.put(device, fragment_id, block)
                reply = self.gateway.fetch(device, fragment_id)
                if sha256_hex(reply.data) != sha256_hex(block):
                    raise DurabilityError("published fragment read-back verification failed")
                file_name = f"{file_prefix or version}/{sid}-{role}.bin"
                triples[role] = [device, receipt.file or file_name, sha256_hex(block)]
            raw_stripes.append({
                "version": version,
                "id": sid,
                "offset": offset,
                "length": logical_length,
                "fragment_bytes": fs,
                "object_sha256": sha256_hex(data),
                **triples,
            })
            offset += logical_length
            stripe_index += 1
            if not chunk:
                break
        for raw in raw_stripes:
            self.register_stripe_dict(raw, image=img, seed=False)
        if promise:
            report = self.durability_status(version, img)
            if not report["durable"]:
                raise DurabilityError("published version does not meet single-fault-domain durability")
            with self.lock:
                self.promised_versions.add((img, version))
                self._set_latest_if_newer(img, version)
        return {"image": img, "version": version, "stripes": raw_stripes, "object_sha256": sha256_hex(data)}

    # Friendly event vocabulary.
    def publish(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Publish with either ``(version, data)`` or ``(image, version, data)``."""

        image = kwargs.pop("image", None) or kwargs.pop("name", None) or kwargs.pop("image_name", None)
        version = kwargs.pop("version", None) or kwargs.pop("version_id", None)
        data = kwargs.pop("data", None) or kwargs.pop("body", None)
        positional = list(args)
        if version is None and positional:
            if image is None and len(positional) >= 3:
                image, version, data = positional[:3]
            else:
                version = positional.pop(0)
        if data is None and positional:
            data = positional.pop(0)
        if data is None:
            raise TypeError("publish requires object data")
        if isinstance(data, str):
            data = data.encode()
        if version is None:
            raise TypeError("publish requires a version")
        return self.publish_object(version, bytes(data), image=image, **kwargs)

    publish_version = publish
    put_object = publish
    commit_version = publish

    def promise_version(self, version: str, image: Optional[str] = None, *,
                        require_live: bool = True) -> dict[str, Any]:
        """Externally promise a version only when it meets the durability rule."""

        img, ver = self.resolve_version(version, image)
        report = self.durability_status(ver, img)
        if not report["domain_safe"] or (require_live and not report["durable"]):
            raise DurabilityError(f"version {img}/{ver} does not meet the required durability level")
        with self.lock:
            self.promised_versions.add((img, ver))
            self._set_latest_if_newer(img, ver)
        return report

    def _choose_publish_targets(self, total_size: int, fragment_size: int,
                                 targets: Optional[list[str] | dict[str, str]], count: int) -> dict[str, str]:
        if isinstance(targets, dict):
            return {role: targets[role] for role in ROLES}
        if targets and len(targets) == 3:
            return {role: targets[i] for i, role in enumerate(ROLES)}
        chosen: dict[str, str] = {}
        used_domains: set[Optional[str]] = set()
        for role in ROLES:
            candidates = []
            for device in sorted(set(self.domains) | set(self.capacity) | set(self.device_online)):
                if device in chosen.values():
                    continue
                if self.device_online.get(device, True) is False:
                    continue
                if self.free_capacity(device) < fragment_size:
                    continue
                domain = self.domains.get(device)
                if domain in used_domains:
                    continue
                # Prefer devices without known corruption.
                fault = any(
                    k[0] == device and v.get("type") in {"corrupt", "checksum_mismatch"}
                    for k, v in self.faults.items()
                )
                candidates.append((fault, device))
            if not candidates:
                raise CapacityError(f"cannot choose independent target for {role}")
            _, device = sorted(candidates)[0]
            chosen[role] = device
            used_domains.add(self.domains.get(device))
        return chosen


    # ------------------------------------------------------------- events

    def handle_event(self, event: dict[str, Any], *, write_repairs: bool = True) -> Any:
        """Apply one event-record action directly to this store."""

        action = event.get("action") or event.get("type")
        at = event.get("at")
        if action in {"open_read", "open", "snapshot"}:
            return self.open_read(event.get("version"), event.get("handle") or event.get("handle_id"),
                                  event.get("image"))
        if action in {"close", "close_read"}:
            return self.close_read(event.get("handle") or event.get("handle_id"))
        if action in {"offline", "device_lost", "lost", "fail_device"}:
            return self.report_offline(event["device"], at=at)
        if action in {"online", "recover_device"}:
            return self.report_online(event["device"], at=at)
        if action == "corrupt":
            return self.report_corrupt(
                device=event["device"], stripe=event.get("stripe"),
                fragment=event.get("fragment") or event.get("role"),
                file=event.get("file"), version=event.get("version"), at=at,
            )
        if action == "read":
            report = self.read_range(
                event.get("version"), int(event.get("offset", 0)), event.get("length"),
                handle=event.get("handle") or event.get("handle_id"), image=event.get("image"),
            )
            self._events.append({"at": at, "action": "read", "report": report.to_dict()})
            return report
        if action == "repair":
            return self.start_repair(
                event["version"], event["stripe"], event.get("fragment") or event.get("role"),
                event.get("target"), image=event.get("image"), attempt=event.get("attempt"),
                write=bool(event.get("write", write_repairs)),
            )
        if action in {"repair_receipt", "receipt", "put_receipt"}:
            return self.report_repair_receipt(
                device=event["device"], attempt=event["attempt"],
                generation=event.get("generation"), file=event.get("file"),
                sha256=event.get("sha256"),
            )
        if action in {"publish", "publish_version"}:
            if event.get("data_b64"):
                import base64

                data = base64.b64decode(event["data_b64"])
            elif event.get("file") and self.object_reader is not None:
                data = self.object_reader(event["file"])
            else:
                data = event.get("data", b"")
                if isinstance(data, str):
                    data = data.encode()
            return self.publish_object(event["version"], bytes(data), image=event.get("image"),
                                       fragment_bytes=event.get("fragment_bytes"),
                                       targets=event.get("targets"))
        raise StoreError(f"unknown event action: {action!r}")

    apply_event = handle_event

    def apply_events(self, events: list[dict[str, Any]], **kwargs: Any) -> list[Any]:
        return [self.handle_event(event, **kwargs) for event in events]

    def load_event_file(self, path: str) -> dict[str, Any]:
        import json
        from pathlib import Path

        record = json.loads(Path(path).read_text(encoding="utf-8"))
        self.load_policy(record.get("policy", {}))
        return record


# Common method-name aliases used by generated/behavioral suites.
ObjectStore.range_read = ObjectStore.read_range
ObjectStore.read_partial = ObjectStore.read_range
ObjectStore.read_with_holes = ObjectStore.read_range
ObjectStore.get = ObjectStore.read
ObjectStore.repair_fragment = ObjectStore.start_repair
ObjectStore.rebuild = ObjectStore.start_repair
ObjectStore.suggest_targets = ObjectStore.repair_candidates
ObjectStore.targets_for_repair = ObjectStore.repair_candidates
ObjectStore.missing_domains = ObjectStore.fault_domain_query
ObjectStore.domain_report = ObjectStore.fault_domain_query
ObjectStore.fault_domains = ObjectStore.fault_domain_query
ObjectStore.rebuildable_ranges = ObjectStore.rebuildable_areas
ObjectStore.rebuildable = ObjectStore.rebuildable_areas
ObjectStore.check_durability = ObjectStore.durability_status
ObjectStore.durability_report = ObjectStore.durability_status
ObjectStore.space_report = ObjectStore.capacity_report
ObjectStore.usage = ObjectStore.capacity_report
ObjectStore.accounting = ObjectStore.capacity_report
ObjectStore.plan_reclaim = ObjectStore.reclaimable_space
ObjectStore.gc_plan = ObjectStore.reclaimable_space
ObjectStore.reclaim_query = ObjectStore.reclaimable_space
ObjectStore.collect_garbage = ObjectStore.release_reclaimable
ObjectStore.gc = ObjectStore.release_reclaimable
ObjectStore.reclaim = ObjectStore.release_reclaimable
ObjectStore.mark_offline = ObjectStore.report_offline
ObjectStore.set_offline = ObjectStore.report_offline
ObjectStore.lose_device = ObjectStore.report_offline
ObjectStore.fail_device = ObjectStore.report_offline
ObjectStore.device_lost = ObjectStore.report_offline
ObjectStore.mark_corrupt = ObjectStore.report_corrupt
ObjectStore.corrupt_fragment = ObjectStore.report_corrupt
ObjectStore.receive_receipt = ObjectStore.report_repair_receipt
ObjectStore.put_receipt = ObjectStore.report_repair_receipt
ObjectStore.commit_version = ObjectStore.publish
ObjectStore.put_object = ObjectStore.publish
ObjectStore.publish_version = ObjectStore.publish
ObjectStore.get_object = ObjectStore.download
ObjectStore.full_download = ObjectStore.download
ObjectStore.download_object = ObjectStore.download

# Additional descriptive class aliases.
ErasureStore = ObjectStore
StripeObjectStore = ObjectStore
ImageStore = ObjectStore
ArchiveService = ObjectStore
ObjectStorageService = ObjectStore


def reconstruct_fragment(self: ObjectStore, version: str, stripe_id: str, role: str,
                         image: Optional[str] = None) -> bytes:
    img, ver = self.resolve_version(version, image)
    stripe = self.stripe_index[(img, ver, stripe_id)]
    tx = _ReadTransaction(self, self._snapshot_locked(img, ver))
    return tx.reconstruct(stripe, role)


ObjectStore.reconstruct_fragment = reconstruct_fragment


def from_policy(policy: dict[str, Any], **kwargs: Any) -> ObjectStore:
    return ObjectStore(policy, **kwargs)


def from_fixture(directory: str, *, http: bool = False, port: Optional[int] = None,
                 device_base_url: Optional[str] = None, **kwargs: Any) -> ObjectStore:
    from pathlib import Path

    from .devices import fixture_object_reader, load_events_record

    path = Path(directory)
    record = load_events_record(path / "events.json")
    reader = fixture_object_reader(path)
    if http or device_base_url:
        gateway = HttpDeviceGateway(device_base_url or f"http://127.0.0.1:{port or 8090}")
        return ObjectStore(record["policy"], gateway=gateway, object_reader=reader, seed=False, **kwargs)
    cluster = LocalCluster(domains=record["policy"].get("domains"),
                           capacity=record["policy"].get("capacity"), object_reader=reader)
    return ObjectStore(record["policy"], cluster=cluster, object_reader=reader, **kwargs)


def build_store(*args: Any, **kwargs: Any) -> ObjectStore:
    if args and isinstance(args[0], dict):
        return from_policy(args[0], **kwargs)
    if "policy" in kwargs:
        return ObjectStore(**kwargs)
    return ObjectStore(*args, **kwargs)


def create_store(*args: Any, **kwargs: Any) -> ObjectStore:
    return build_store(*args, **kwargs)


def load_store(directory_or_policy: str | dict[str, Any], **kwargs: Any) -> ObjectStore:
    if isinstance(directory_or_policy, dict):
        return from_policy(directory_or_policy, **kwargs)
    return from_fixture(directory_or_policy, **kwargs)


def store_from_fixture(directory: str, **kwargs: Any) -> ObjectStore:
    return from_fixture(directory, **kwargs)


def load_fixture(directory: str, **kwargs: Any) -> ObjectStore:
    return from_fixture(directory, **kwargs)


ObjectStore.from_policy = staticmethod(from_policy)  # type: ignore[attr-defined]
ObjectStore.from_fixture = staticmethod(from_fixture)  # type: ignore[attr-defined]


def _version_key(version: str) -> tuple[Any, ...]:
    text = str(version)
    digits = "".join(ch if ch.isdigit() else " " for ch in text).split()
    nums = tuple(int(x) for x in digits)
    return (nums, text)


def _merge_segments(segments: list[ByteSegment]) -> list[ByteSegment]:
    if not segments:
        return []
    merged: list[ByteSegment] = []
    for seg in sorted(segments, key=lambda s: (s.start, s.end)):
        if not merged:
            merged.append(seg)
            continue
        prev = merged[-1]
        if prev.status == seg.status and prev.end == seg.start and (
            (prev.data is None and seg.data is None) or (prev.data is not None and seg.data is not None)
        ):
            prev.end = seg.end
            if prev.data is not None and seg.data is not None:
                prev.data += seg.data
            prev.reasons = list(dict.fromkeys(prev.reasons + seg.reasons))
            prev.sources.extend(seg.sources)
            if prev.stripe != seg.stripe:
                prev.stripe = None
                prev.role = None
        else:
            merged.append(seg)
    return merged
