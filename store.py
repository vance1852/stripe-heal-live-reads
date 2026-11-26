"""Compatibility entry point for ``import store``."""

from archive_store import *  # noqa: F403
from archive_store.core import ObjectStore

Store = ObjectStore
ObjectStore = ObjectStore
Storage = ObjectStore

__all__ = [name for name in globals() if not name.startswith("_")]
