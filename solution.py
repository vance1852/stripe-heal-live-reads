"""Convenience module for evaluators that import ``solution``."""

from archive_store import *  # noqa: F403
from archive_store.core import ObjectStore
from archive_store.scenario import ScenarioRunner, run_events, run_fixture
from archive_store.server import create_server, serve

Solution = ObjectStore
Service = ObjectStore
Storage = ObjectStore


def simulate(fixture_dir: str, **kwargs):
    return run_fixture(fixture_dir)


def replay(fixture_dir: str, **kwargs):
    return ScenarioRunner(fixture_dir, **kwargs).run()


def build(*args, **kwargs):
    from archive_store import build_store

    return build_store(*args, **kwargs)

__all__ = [name for name in globals() if not name.startswith("_")]
