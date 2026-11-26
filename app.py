"""Root application alias."""

from archive_store.core import ObjectStore
from archive_store.server import create_server, serve

App = ObjectStore
Application = ObjectStore

if __name__ == "__main__":
    import sys

    sys.argv[0] = "archive_store"
    from archive_store.__main__ import main

    raise SystemExit(main())
