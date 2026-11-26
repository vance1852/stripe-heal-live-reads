"""Compatibility entry point for ``import object_store``."""

from archive_store import *  # noqa: F403
from archive_store.core import ObjectStore

ObjectStore = ObjectStore
Store = ObjectStore
Storage = ObjectStore
ArchiveStore = ObjectStore

__all__ = [name for name in globals() if not name.startswith("_")]
