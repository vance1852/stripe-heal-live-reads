"""Data types and errors for the striped archive store."""

from __future__ import annotations

import base64
import enum
import hashlib
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional


ROLES = ("d0", "d1", "p")
DATA_ROLES = ("d0", "d1")


class ReadStatus(str, enum.Enum):
    """Classification of a byte interval for the scheduling service."""

    DIRECT = "direct"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"

    # Common synonyms used by callers/tests.
    OK = "direct"
    HEALTHY = "direct"
    RECONSTRUCTED = "degraded"
    REBUILT = "degraded"
    MISSING = "unavailable"
    FAILED = "unavailable"


DIRECT = ReadStatus.DIRECT.value
DEGRADED = ReadStatus.DEGRADED.value
UNAVAILABLE = ReadStatus.UNAVAILABLE.value


class StoreError(Exception):
    """Base class for store failures."""


class UnknownVersionError(StoreError, KeyError):
    def __init__(self, version: str, image: Optional[str] = None):
        self.version = version
        self.image = image
        super().__init__(f"unknown version {version!r}" + (f" for image {image!r}" if image else ""))


class UnknownHandleError(StoreError, KeyError):
    def __init__(self, handle_id: str):
        self.handle_id = handle_id
        super().__init__(f"unknown or closed read handle {handle_id!r}")


class FragmentError(StoreError):
    pass


class FragmentUnavailable(FragmentError):
    def __init__(self, device: str, fragment_id: str, reason: str = "unavailable"):
        self.device = device
        self.fragment_id = fragment_id
        self.reason = reason
        super().__init__(f"{device}/{fragment_id}: {reason}")


class FragmentChecksumMismatch(FragmentError):
    def __init__(self, device: str, fragment_id: str, expected: str, actual: str):
        self.device = device
        self.fragment_id = fragment_id
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"checksum mismatch for {device}/{fragment_id}: expected {expected}, got {actual}"
        )


class UnavailableBytesError(StoreError):
    """Raised by strict reads when one or more requested intervals are missing."""

    def __init__(self, report: "RangeReport"):
        self.report = report
        self.unavailable_ranges = list(report.unavailable_ranges)
        super().__init__(f"range contains unavailable bytes: {self.unavailable_ranges!r}")


class DurabilityError(StoreError):
    pass


class CapacityError(StoreError):
    pass


class RepairError(StoreError):
    pass


class FencedRepairError(RepairError):
    pass


class VerificationResult(dict):
    """Dict-like verification report whose boolean value is success."""

    def __init__(self, *args: Any, ok: bool = False, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.ok = ok
        self["ok"] = ok

    def __bool__(self) -> bool:
        return bool(self.ok)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def xor_bytes(blocks: Iterable[bytes]) -> bytes:
    """XOR equally-sized blocks."""

    result: Optional[bytearray] = None
    count = 0
    for block in blocks:
        if result is None:
            result = bytearray(block)
            count = len(block)
        else:
            if len(block) != count:
                raise ValueError("XOR blocks must have equal length")
            for i, value in enumerate(block):
                result[i] ^= value
    if result is None:
        raise ValueError("at least one block is required")
    return bytes(result)


@dataclass(frozen=True)
class FragmentSpec:
    """One ``[device, file, sha256]`` triple from the repository policy."""

    role: str
    device: str
    file: Optional[str]
    sha256: str

    @classmethod
    def from_triple(cls, role: str, value: Any) -> "FragmentSpec":
        if isinstance(value, FragmentSpec):
            return value
        if isinstance(value, dict):
            return cls(
                role=role,
                device=value.get("device") or value[0],
                file=value.get("file") if "file" in value else value[1],
                sha256=value.get("sha256") or value.get("sha") or value[2],
            )
        device, file, digest = value
        return cls(role=role, device=str(device), file=file, sha256=str(digest))

    def to_triple(self) -> list[str]:
        return [self.device, self.file or "", self.sha256]


@dataclass(frozen=True)
class Stripe:
    version: str
    id: str
    offset: int
    length: int
    fragments: dict[str, FragmentSpec]
    image: str = "default"
    fragment_bytes: Optional[int] = None
    object_sha256: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any], image: str = "default") -> "Stripe":
        version = str(raw["version"])
        stripe_id = str(raw.get("id") or raw.get("stripe") or raw["stripe_id"])
        fragments = {role: FragmentSpec.from_triple(role, raw[role]) for role in ROLES if role in raw}
        missing = [role for role in ROLES if role not in fragments]
        if missing:
            raise ValueError(f"stripe {stripe_id} is missing fragments: {missing}")
        return cls(
            version=version,
            id=stripe_id,
            offset=int(raw["offset"]),
            length=int(raw["length"]),
            fragments=fragments,
            image=str(raw.get("image") or raw.get("name") or image),
            fragment_bytes=raw.get("fragment_bytes") or raw.get("fragment_length") or raw.get("chunk_size"),
            object_sha256=raw.get("object_sha256") or raw.get("sha256") if not raw.get("d0") else raw.get("object_sha256"),
            metadata={k: v for k, v in raw.items() if k not in {"version", "id", "offset", "length", "d0", "d1", "p"}},
        )

    @property
    def end(self) -> int:
        return self.offset + self.length

    def fragment_size(self, known_size: Optional[int] = None) -> int:
        if self.fragment_bytes is not None:
            return int(self.fragment_bytes)
        if known_size is not None:
            return int(known_size)
        # The two data fragments are equal and logical length may omit padding.
        return (int(self.length) + 1) // 2

    def fragment_id(self, role: str) -> str:
        return f"{self.id}-{role}"


@dataclass
class ByteSegment:
    """A classified interval in the requested logical object range."""

    start: int
    end: int
    status: str
    data: Optional[bytes] = None
    role: Optional[str] = None
    stripe: Optional[str] = None
    version: Optional[str] = None
    image: Optional[str] = None
    reasons: list[str] = field(default_factory=list)
    sources: list[dict[str, Any]] = field(default_factory=list)
    generation: Optional[int] = None

    @property
    def length(self) -> int:
        return self.end - self.start

    # Common alternate name.
    @property
    def size(self) -> int:
        return self.length

    @property
    def readable(self) -> bool:
        return self.status != UNAVAILABLE and self.data is not None and len(self.data) == self.length

    def interval(self) -> tuple[int, int]:
        return self.start, self.end

    def __getitem__(self, key: str) -> Any:
        if isinstance(key, str):
            if key == "range":
                return [self.start, self.end]
            if key in {"data", "bytes"}:
                return self.data
            if hasattr(self, key):
                return getattr(self, key)
            raise KeyError(key)
        raise TypeError("segment keys must be strings")

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self) -> list[str]:
        return ["start", "end", "status", "data", "role", "stripe", "version", "image", "reasons", "sources"]

    def to_dict(self, include_data: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "start": self.start,
            "end": self.end,
            "offset": self.start,
            "length": self.length,
            "status": self.status,
            "classification": self.status,
            "readable": self.readable,
            "role": self.role,
            "stripe": self.stripe,
            "version": self.version,
            "image": self.image,
            "reasons": list(self.reasons),
            "source": list(self.sources),
            "sources": list(self.sources),
            "generation": self.generation,
        }
        if include_data and self.data is not None:
            result["data_b64"] = base64.b64encode(self.data).decode("ascii")
            result["sha256"] = sha256_hex(self.data)
        return result


@dataclass(eq=False)
class RangeReport:
    image: str
    version: str
    start: int
    end: int
    segments: list[ByteSegment] = field(default_factory=list)
    raw_segments: list[ByteSegment] = field(default_factory=list)
    requested_version: Optional[str] = None
    handle_id: Optional[str] = None
    promised: bool = False
    durable_now: bool = False

    @property
    def length(self) -> int:
        return self.end - self.start

    def __len__(self) -> int:
        return self.length

    @property
    def requested_length(self) -> int:
        return self.length

    def _ranges(self, status: str) -> list[tuple[int, int]]:
        return [(seg.start, seg.end) for seg in self.segments if seg.status == status]

    @property
    def direct_ranges(self) -> list[tuple[int, int]]:
        return self._ranges(DIRECT)

    @property
    def degraded_ranges(self) -> list[tuple[int, int]]:
        return self._ranges(DEGRADED)

    @property
    def unavailable_ranges(self) -> list[tuple[int, int]]:
        return self._ranges(UNAVAILABLE)

    @property
    def missing_ranges(self) -> list[tuple[int, int]]:
        return self.unavailable_ranges

    @property
    def holes(self) -> list[tuple[int, int]]:
        return self.unavailable_ranges

    @property
    def direct_bytes(self) -> int:
        return sum(e - s for s, e in self.direct_ranges)

    @property
    def degraded_bytes(self) -> int:
        return sum(e - s for s, e in self.degraded_ranges)

    @property
    def unavailable_bytes(self) -> int:
        return sum(e - s for s, e in self.unavailable_ranges)

    @property
    def available_bytes(self) -> int:
        return self.direct_bytes + self.degraded_bytes

    @property
    def complete(self) -> bool:
        return not self.unavailable_ranges and self.available_bytes == self.length

    @property
    def fully_readable(self) -> bool:
        return self.complete

    @property
    def ok(self) -> bool:
        """True when every requested byte is available, even if reconstructed."""

        return self.complete

    @property
    def fragment_segments(self) -> list["ByteSegment"]:
        return self.raw_segments or self.segments

    @property
    def chunks(self) -> list["ByteSegment"]:
        return self.segments

    @property
    def parts(self) -> list["ByteSegment"]:
        return self.segments

    @property
    def fragment_ranges(self) -> list[dict[str, Any]]:
        return [
            {"start": seg.start, "end": seg.end, "status": seg.status,
             "stripe": seg.stripe, "role": seg.role}
            for seg in self.fragment_segments
        ]

    @property
    def status(self) -> str:
        if self.unavailable_ranges:
            return UNAVAILABLE
        if self.degraded_ranges:
            return DEGRADED
        return DIRECT

    classification = status

    def data_parts(self) -> list[tuple[int, bytes]]:
        return [(seg.start, seg.data or b"") for seg in self.segments if seg.data is not None]

    def to_bytes(self) -> bytes:
        """Return all bytes or raise :class:`UnavailableBytesError`."""

        if not self.complete:
            raise UnavailableBytesError(self)
        result = bytearray(self.length)
        for seg in self.segments:
            if seg.data is not None:
                result[seg.start - self.start : seg.end - self.start] = seg.data
        return bytes(result)

    def to_strict_bytes(self) -> bytes:
        return self.to_bytes()

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RangeReport):
            return self.to_dict() == other.to_dict()
        if isinstance(other, (bytes, bytearray)) and self.complete:
            return self.to_bytes() == bytes(other)
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.image, self.version, self.start, self.end, self.status))

    def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        return self.to_bytes().decode(encoding, errors)

    def hex(self) -> str:
        return self.to_bytes().hex()

    def __getitem__(self, item: Any) -> Any:
        if isinstance(item, str):
            if item in {"data", "bytes"}:
                return self.to_bytes()
            if item == "ranges":
                return {
                    "direct": self.direct_ranges,
                    "degraded": self.degraded_ranges,
                    "unavailable": self.unavailable_ranges,
                }
            if hasattr(self, item):
                return getattr(self, item)
            data = self.to_dict(include_data=True)
            if item in data:
                return data[item]
            raise KeyError(item)
        # A complete report is byte-compatible with the range it represents.
        data = self.to_bytes()
        return data[item]

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except (KeyError, UnavailableBytesError):
            return default

    def to_dict(self, include_data: bool = True) -> dict[str, Any]:
        return {
            "image": self.image,
            "version": self.version,
            "requested_version": self.requested_version or self.version,
            "handle": self.handle_id,
            "start": self.start,
            "end": self.end,
            "offset": self.start,
            "length": self.length,
            "status": self.status,
            "classification": self.status,
            "complete": self.complete,
            "fully_readable": self.fully_readable,
            "ok": self.ok,
            "promised": self.promised,
            "durable_now": self.durable_now,
            "direct_ranges": [[a, b] for a, b in self.direct_ranges],
            "degraded_ranges": [[a, b] for a, b in self.degraded_ranges],
            "unavailable_ranges": [[a, b] for a, b in self.unavailable_ranges],
            "missing_ranges": [[a, b] for a, b in self.unavailable_ranges],
            "counts": {
                "direct": self.direct_bytes,
                "degraded": self.degraded_bytes,
                "unavailable": self.unavailable_bytes,
                "available": self.available_bytes,
                "total": self.length,
            },
            "segments": [seg.to_dict(include_data=include_data) for seg in self.segments],
            "fragment_segments": [seg.to_dict(include_data=include_data) for seg in self.fragment_segments],
            "fragment_ranges": self.fragment_ranges,
        }
