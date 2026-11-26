"""Command line entry point: ``python -m archive_store ...``."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .scenario import ScenarioRunner
from .server import serve


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Striped immutable-image object store")
    sub = parser.add_subparsers(dest="command", required=True)

    server = sub.add_parser("serve", help="run the local HTTP service")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8080)
    server.add_argument("--fixture", help="fixture directory to load")
    server.add_argument("--device-base-url", help="external device/fixture HTTP base")

    replay = sub.add_parser("replay", help="replay a events.json fixture")
    replay.add_argument("fixture", help="fixture directory")
    replay.add_argument("--write-repairs", action="store_true",
                        help="issue PUTs during repair events instead of waiting for receipts")
    replay.add_argument("--pretty", action="store_true")
    replay.add_argument("--observations-only", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "serve":
        serve(args.host, args.port, fixture_dir=args.fixture, device_base_url=args.device_base_url)
        return 0
    if args.command == "replay":
        runner = ScenarioRunner(args.fixture, write_repairs=args.write_repairs)
        result = runner.run()
        if args.observations_only:
            output: Any = runner.observations
        else:
            output = result
        text = json.dumps(output, ensure_ascii=False, indent=2 if args.pretty else None, default=str)
        print(text)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
