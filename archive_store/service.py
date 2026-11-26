"""Name-compatible service re-export."""
from .server import ArchiveHTTPServer, create_server, serve
from .core import ObjectStore

StorageService = ObjectStore
__all__ = ["ArchiveHTTPServer", "create_server", "serve", "StorageService", "ObjectStore"]
