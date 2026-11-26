"""Compatibility entry point for ``import service`` and ``python service.py``."""

import argparse

from archive_store import *  # noqa: F403
from archive_store.core import ObjectStore
from archive_store.server import ArchiveRequestHandler, ArchiveHTTPServer, create_server, serve

StorageService = ObjectStore
ObjectService = ObjectStore
Service = ObjectStore

__all__ = [name for name in globals() if not name.startswith("_")]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(__import__("os").environ.get("PORT", "8080")))
    parser.add_argument("--fixture")
    parser.add_argument("--device-base-url")
    args = parser.parse_args()
    serve(args.host, args.port, fixture_dir=args.fixture, device_base_url=args.device_base_url)
