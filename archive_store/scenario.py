"""Replay the repository's timestamped event records against an ObjectStore."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any, Optional

from .core import ObjectStore
from .devices import LocalCluster, fixture_object_reader, load_events_record
from .model import RangeReport


class ScenarioRunner:
    def __init__(self, fixture_dir: str | Path, *, write_repairs: bool = False,
                 publish: bool = True):
        self.fixture_dir = Path(fixture_dir)
        self.record = load_events_record(self.fixture_dir / "events.json")
        self.object_reader = fixture_object_reader(self.fixture_dir)
        policy = self.record["policy"]
        self.cluster = LocalCluster(
            domains=policy.get("domains"),
            capacity=policy.get("capacity"),
            object_reader=self.object_reader,
        )
        self.store = ObjectStore(policy, cluster=self.cluster, object_reader=self.object_reader)
        self.write_repairs = write_repairs
        self.publish_enabled = publish
        self.observations: list[dict[str, Any]] = []
        self.results: dict[str, Any] = {}

    def run(self) -> dict[str, Any]:
        for event in self.record.get("events", []):
            self.handle_event(event)
        versions = sorted({version for _, version in self.store.stripes})
        self.results = {
            "observations": self.observations,
            "durability": {version: self.store.durability_status(version) for version in versions},
            "rebuildable": self.store.rebuildable_areas(),
            "fault_domains": self.store.fault_domain_query(),
            "reclaimable": self.store.reclaimable_space(),
            "capacity": self.store.capacity_report(),
            "current_versions": dict(self.store.current_versions),
        }
        if "v1" in versions:
            self.results["durability_v1"] = self.results["durability"]["v1"]
        return self.results

    def handle_event(self, event: dict[str, Any]) -> Any:
        action = event["action"]
        handler = getattr(self, f"on_{action}", None)
        if handler is None:
            observation = {"at": event.get("at"), "action": action, "ignored": True}
            self.observations.append(observation)
            return observation
        return handler(event)

    def on_open_read(self, event: dict[str, Any]) -> None:
        self.store.open_read(
            version=event.get("version"),
            handle_id=event.get("handle"),
            image=event.get("image"),
        )

    def on_close(self, event: dict[str, Any]) -> None:
        self.store.close_read(event["handle"])

    def on_offline(self, event: dict[str, Any]) -> None:
        self.store.report_offline(event["device"], at=event.get("at"))

    def on_online(self, event: dict[str, Any]) -> None:
        self.store.report_online(event["device"], at=event.get("at"))

    def on_corrupt(self, event: dict[str, Any]) -> None:
        self.store.report_corrupt(
            device=event["device"],
            stripe=event.get("stripe"),
            fragment=event.get("fragment"),
            file=event.get("file"),
            version=event.get("version"),
            at=event.get("at"),
        )

    def on_read(self, event: dict[str, Any]) -> RangeReport:
        report = self.store.read_range(
            version=event.get("version"),
            handle=event.get("handle"),
            offset=int(event.get("offset", 0)),
            length=event.get("length"),
            image=event.get("image"),
        )
        observation = {
            "at": event.get("at"),
            "action": "read",
            "handle": event.get("handle"),
            "query": {"offset": event.get("offset", 0), "length": event.get("length")},
            "report": report.to_dict(include_data=True),
            "bytes_b64": {
                str(start): base64.b64encode(data).decode("ascii")
                for start, data in report.data_parts()
            },
        }
        self.observations.append(observation)
        return report

    def on_repair(self, event: dict[str, Any]) -> Any:
        return self.store.start_repair(
            version=event["version"],
            stripe_id=event["stripe"],
            role=event.get("fragment") or event.get("role"),
            target=event.get("target"),
            attempt=event.get("attempt"),
            image=event.get("image"),
            write=self.write_repairs,
        )

    def on_repair_receipt(self, event: dict[str, Any]) -> Any:
        return self.store.report_repair_receipt(
            device=event["device"],
            attempt=event["attempt"],
            generation=event.get("generation"),
            file=event.get("file"),
            sha256=event.get("sha256"),
        )

    def on_publish(self, event: dict[str, Any]) -> Any:
        if not self.publish_enabled:
            return None
        file_name = event.get("file")
        if file_name:
            data = self.object_reader(file_name)
        else:
            data = base64.b64decode(event.get("data_b64", ""))
        return self.store.publish_object(
            version=event["version"],
            data=data,
            image=event.get("image"),
            fragment_bytes=event.get("fragment_bytes"),
            targets=event.get("targets"),
        )

    # Some records may use "fail_device"/"lost" vocabulary.
    on_fail_device = on_offline
    on_lost = on_offline
    on_device_lost = on_offline
    on_close_read = on_close


def run_fixture(fixture_dir: str | Path) -> dict[str, Any]:
    return ScenarioRunner(fixture_dir).run()


def run_events(events: list[dict[str, Any]], store: ObjectStore | None = None,
               **runner_kwargs: Any) -> ScenarioRunner:
    """Apply an event list to a store (creating a fixture runner if needed)."""

    if store is not None:
        runner = ScenarioRunner.__new__(ScenarioRunner)
        runner.fixture_dir = Path(runner_kwargs.get("fixture_dir", "."))
        runner.record = {"events": events, "policy": runner_kwargs.get("policy", {})}
        runner.object_reader = runner_kwargs.get("object_reader")
        runner.cluster = getattr(store, "cluster", None)
        runner.store = store
        runner.write_repairs = runner_kwargs.get("write_repairs", False)
        runner.publish_enabled = runner_kwargs.get("publish", True)
        runner.observations = []
        runner.results = {}
    else:
        raise ValueError("store is required when replaying bare events")
    for event in events:
        runner.handle_event(event)
    return runner
