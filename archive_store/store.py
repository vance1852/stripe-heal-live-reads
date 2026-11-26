"""Name-compatible re-export."""
from .core import ObjectStore

Store = ObjectStore
StorageService = ObjectStore
__all__ = ["ObjectStore", "Store", "StorageService"]
