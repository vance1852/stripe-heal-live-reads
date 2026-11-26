"""Compatibility entry point for ``import core``."""

from archive_store.core import *  # noqa: F403
from archive_store.core import ObjectStore

__all__ = [name for name in globals() if not name.startswith("_")]
