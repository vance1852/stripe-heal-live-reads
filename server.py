"""Root HTTP server entry point."""

from archive_store.server import ArchiveHTTPServer, ArchiveRequestHandler, create_server, serve

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--fixture")
    parser.add_argument("--device-base-url")
    args = parser.parse_args()
    serve(args.host, args.port, fixture_dir=args.fixture, device_base_url=args.device_base_url)
