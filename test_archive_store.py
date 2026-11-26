from __future__ import annotations

import json
import threading
import unittest
from pathlib import Path
import threading

from device_fixture import FixtureServer

from archive_store import (
    DEGRADED,
    DIRECT,
    UNAVAILABLE,
    HttpDeviceGateway,
    LocalCluster,
    ObjectStore,
    UnavailableBytesError,
    fixture_object_reader,
)
from archive_store.scenario import ScenarioRunner

ROOT = Path(__file__).parent
CONFLICT = ROOT / "fixtures" / "repair-target-conflicts-with-fault-domain"
NORMAL = ROOT / "fixtures" / "verified-normal-range"


class NormalFixtureTests(unittest.TestCase):
    def test_exact_verified_range(self):
        store = ObjectStore.from_fixture(str(NORMAL))
        report = store.read_range("v3", 2, 4)
        self.assertEqual(report.status, DIRECT)
        self.assertEqual(report.direct_ranges, [(2, 6)])
        self.assertEqual(report.to_bytes(), b"4833")
        self.assertEqual(store.read("v3", 2, 4), b"4833")
        self.assertEqual(store.download("v3"), b"12483399")
        verification = store.verify_object("v3")
        self.assertTrue(verification)

    def test_runner_observation(self):
        runner = ScenarioRunner(NORMAL)
        result = runner.run()
        report = runner.observations[0]["report"]
        self.assertEqual(report["direct_ranges"], [[2, 6]])
        self.assertEqual(result["durability"]["v3"]["durable"], True)


class ConflictScenarioTests(unittest.TestCase):
    def make_runner_until(self, at: int):
        runner = ScenarioRunner(CONFLICT)
        reports = []
        for event in runner.record["events"]:
            if event.get("at", 0) > at:
                break
            result = runner.handle_event(event)
            if event["action"] == "read":
                reports.append(result)
        return runner, reports

    def test_first_read_is_degraded_but_complete(self):
        runner, reports = self.make_runner_until(3)
        report = reports[0]
        self.assertTrue(report.complete)
        self.assertEqual(report.unavailable_ranges, [])
        self.assertEqual(report.direct_ranges, [(8, 16), (24, 30)])
        self.assertEqual(report.degraded_ranges, [(4, 8), (16, 24)])
        self.assertEqual(report.to_bytes(), b"1234EFGH5678NPQR0123TUVW45")

    def test_corruption_creates_exact_hole_without_zeros(self):
        runner, reports = self.make_runner_until(7)
        report = reports[-1]
        self.assertEqual(report.direct_ranges, [(8, 16), (24, 30)])
        self.assertEqual(report.degraded_ranges, [(4, 8)])
        self.assertEqual(report.unavailable_ranges, [(16, 24)])
        with self.assertRaises(UnavailableBytesError) as ctx:
            report.to_bytes()
        self.assertEqual(ctx.exception.unavailable_ranges, [(16, 24)])
        # The hole is not represented by eight zero bytes.
        self.assertNotIn(b"\x00" * 8, b"".join(seg.data or b"" for seg in report.segments))

    def test_domain_check_and_repair_fencing_and_space(self):
        runner = ScenarioRunner(CONFLICT)
        runner.run()
        store = runner.store

        bad_d = store.check_repair_target("v1", "s1", "d0", "D")
        good_e = store.check_repair_target("v1", "s1", "d0", "E")
        self.assertFalse(bad_d["domain_safe"])
        self.assertTrue(bad_d["fault_domain_conflict"])
        self.assertTrue(good_e["domain_safe"])

        slot = ("default", "v1", "s1", "d0")
        active = store.replicas[store.current_placements[slot]]
        self.assertEqual(active.device, "E")
        self.assertEqual(active.generation, 2)

        d_attempt = store.attempts["d1"]
        e_attempt = store.attempts["e1"]
        self.assertEqual(e_attempt.state, "committed")
        self.assertEqual(d_attempt.state, "fenced")
        self.assertIn("fault_domain_conflict", d_attempt.error or "")

        reclaim = store.reclaimable_space()
        locations = {(item["device"], item["fragment_id"]): item for item in reclaim["locations"]}
        # After h1 closes in run(), both old A and stale D are reclaimable.
        self.assertIn(("A", "s1-d0"), locations)
        self.assertIn(("D", "s1-d0"), locations)
        self.assertEqual(reclaim["bytes"], 16)
        self.assertEqual(reclaim["releasable_now_bytes"], 8)  # A is offline.

        released = store.release_reclaimable()
        self.assertEqual(released["freed_bytes"], 8)
        self.assertTrue(any(item["device"] == "D" for item in released["freed"]))

    def test_open_handle_pins_old_location(self):
        runner = ScenarioRunner(CONFLICT)
        # Process through both receipts but before publish/close.
        for event in runner.record["events"]:
            if event.get("at", 0) > 9:
                break
            runner.handle_event(event)
        reclaim = runner.store.reclaimable_space()
        devices = [item["device"] for item in reclaim["locations"]]
        self.assertIn("D", devices)
        self.assertNotIn("A", devices)

    def test_publish_becomes_current_but_old_handle_stays_v1(self):
        runner = ScenarioRunner(CONFLICT)
        runner.run()
        store = runner.store
        self.assertEqual(store.current_versions["default"], "v2")
        # h1 is closed by the fixture; a newly explicit v1 read still works
        # where enough fragments remain, but s2 has the recorded hole.
        report = store.read_range("v1", 4, 12)
        self.assertEqual(report.version, "v1")
        self.assertTrue(report.complete)

    def test_concurrent_identical_repairs_coalesce(self):
        store = ObjectStore.from_fixture(str(CONFLICT))
        store.report_offline("A")
        first = store.start_repair("v1", "s1", "d0", "E", attempt="a", write=False)
        second = store.start_repair("v1", "s1", "d0", "E", write=False)
        self.assertIs(first, second)
        self.assertEqual(len(store.repair_jobs[("default", "v1", "s1", "d0")]), 1)

        other = store.start_repair("v1", "s1", "d0", "F", attempt="f", write=False)
        self.assertIsNot(first, other)
        self.assertEqual(len(store.repair_jobs[("default", "v1", "s1", "d0")]), 2)


class HttpProtocolTests(unittest.TestCase):
    def test_two_step_media_and_blob_protocol(self):
        server = FixtureServer(("127.0.0.1", 0), CONFLICT)
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
        worker.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            gateway = HttpDeviceGateway(base, object_url=base + "/fixture/objects/{file}")
            policy = json.loads((CONFLICT / "events.json").read_text())["policy"]
            store = ObjectStore(
                policy, gateway=gateway,
                object_reader=fixture_object_reader(CONFLICT), seed=False,
            )
            report = store.read_range("v1", 4, 12)
            self.assertEqual(report.to_bytes(), b"1234EFGH5678")
            paths = [entry["path"] for entry in server.journal]
            self.assertEqual(paths, [
                "/media/A/fragments/s1-d0",
                "/media/B/fragments/s1-d1",
                "/media/C/fragments/s1-p",
            ])
        finally:
            server.shutdown()
            worker.join(timeout=3)
            server.server_close()


class VersionIsolationTests(unittest.TestCase):
    def test_never_uses_another_versions_bytes(self):
        reader = fixture_object_reader(CONFLICT)
        policy = json.loads((CONFLICT / "events.json").read_text())["policy"]
        cluster = LocalCluster(object_reader=reader)
        store = ObjectStore(policy, cluster=cluster, object_reader=reader)
        # Add a fake other version whose stripe/id/file names resemble v1 but
        # have incompatible checksums.  Reconstruction must still use v1 only.
        other = {
            "version": "vX", "id": "sx", "offset": 0, "length": 16,
            "d0": ["D", "new.bin", "f41ec9f928f772f05f1ea593444c642cebde6e36360bafc94c07f6a029ac938b"],
            "d1": ["E", "new.bin", "f41ec9f928f772f05f1ea593444c642cebde6e36360bafc94c07f6a029ac938b"],
            "p": ["F", "new.bin", "f41ec9f928f772f05f1ea593444c642cebde6e36360bafc94c07f6a029ac938b"],
        }
        store.register_stripe_dict(other)
        report = store.read_range("v1", 0, 16)
        self.assertTrue(report.complete)
        self.assertEqual(report.to_bytes(), b"ABCD1234EFGH5678")


if __name__ == "__main__":
    unittest.main()
