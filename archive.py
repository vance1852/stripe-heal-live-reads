"""Compatibility entry point for ``import archive``."""

from archive_store import *  # noqa: F403
from archive_store.core import ObjectStore

Archive = ObjectStore
ArchiveStore = ObjectStore

__all__ = [name for name in globals() if not name.startswith("_")]
