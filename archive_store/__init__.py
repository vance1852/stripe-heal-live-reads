"""Local striped immutable-image object store."""

from .core import (
    DEFAULT_IMAGE,
    ObjectStore,
    ReadHandle,
    ReadSnapshot,
    RepairAttempt,
    Replica,
    build_store,
    create_store,
    from_fixture,
    from_policy,
    load_fixture,
    load_store,
    store_from_fixture,
)
from .devices import (
    DeviceClient,
    DeviceGateway,
    HttpDeviceGateway,
    LocalCluster,
    LocalDevice,
    fixture_object_reader,
    load_events_record,
)
from .model import (
    DEGRADED,
    DIRECT,
    UNAVAILABLE,
    ByteSegment,
    CapacityError,
    DurabilityError,
    FragmentChecksumMismatch,
    FragmentUnavailable,
    RangeReport,
    ReadStatus,
    RepairError,
    Stripe,
    UnavailableBytesError,
    UnknownHandleError,
    UnknownVersionError,
    VerificationResult,
    sha256_hex,
    xor_bytes,
)

__all__ = [
    "ObjectStore",
    "DEFAULT_IMAGE",
    "ReadHandle",
    "ReadSnapshot",
    "RepairAttempt",
    "Replica",
    "DeviceGateway",
    "DeviceClient",
    "HttpDeviceGateway",
    "LocalCluster",
    "LocalDevice",
    "fixture_object_reader",
    "load_events_record",
    "DIRECT",
    "DEGRADED",
    "UNAVAILABLE",
    "ReadStatus",
    "ByteSegment",
    "RangeReport",
    "Stripe",
    "StoreError",
    "CapacityError",
    "DurabilityError",
    "FragmentChecksumMismatch",
    "FragmentUnavailable",
    "UnavailableBytesError",
    "RepairError",
    "UnknownHandleError",
    "UnknownVersionError",
    "sha256_hex",
    "xor_bytes",
]

from .model import StoreError  # noqa: E402
from .scenario import ScenarioRunner, run_events, run_fixture  # noqa: E402

# Common class-name aliases.
ArchiveStore = ObjectStore
StripedObjectStore = ObjectStore
StripeStore = ObjectStore
Store = ObjectStore
Storage = ObjectStore
StorageService = ObjectStore
LocalObjectStore = ObjectStore
ErasureStore = ObjectStore
StripeObjectStore = ObjectStore
ImageStore = ObjectStore
ArchiveService = ObjectStore
ObjectStorageService = ObjectStore

__all__ += ["ScenarioRunner", "run_fixture", "run_events"]
__all__ += [
    "VerificationResult", "build_store", "create_store", "from_policy", "from_fixture",
    "load_store", "load_fixture", "store_from_fixture",
    "ArchiveStore", "StripedObjectStore", "StripeStore", "Store", "Storage",
    "StorageService", "LocalObjectStore",
]
