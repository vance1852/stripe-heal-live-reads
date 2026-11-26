"""Compatibility entry point for ``import striped_store``."""

from archive_store import *  # noqa: F403
from archive_store.core import ObjectStore

StripedObjectStore = ObjectStore
StripedStore = ObjectStore
Store = ObjectStore

__all__ = [name for name in globals() if not name.startswith("_")]
