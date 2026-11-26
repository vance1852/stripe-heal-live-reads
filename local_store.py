"""Compatibility entry point for ``import local_store``."""

from archive_store import *  # noqa: F403
from archive_store.core import ObjectStore

LocalStore = ObjectStore
LocalObjectStore = ObjectStore

__all__ = [name for name in globals() if not name.startswith("_")]
