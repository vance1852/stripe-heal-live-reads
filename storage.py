"""Compatibility entry point for ``import storage``."""

from archive_store import *  # noqa: F403
from archive_store.core import DEFAULT_IMAGE, ObjectStore

Storage = ObjectStore
Store = ObjectStore
ArchiveStore = ObjectStore
StripeStore = ObjectStore
ObjectStorage = ObjectStore
LocalObjectStore = ObjectStore
Service = ObjectStore

__all__ = [name for name in globals() if not name.startswith("_")]
