"""Name-compatible re-export."""
from .core import ObjectStore

Storage = ObjectStore
__all__ = ["ObjectStore", "Storage"]
