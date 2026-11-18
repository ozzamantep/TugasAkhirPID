import unittest
from datetime import datetime, timedelta, timezone

from src.simulator.generator import TelemetrySimulator
from src.simulator.topology import CAMPUS_DEVICES, get_building_ids


class TestTopology(unittest.TestCase):
    def test_device_counts_and_buildings(self) -> None:
        self.assertEqual(len(CAMPUS_DEVICES), 24)
        self.assertGreaterEqual(len(get_building_ids()), 3)

        switches = [d for d in CAMPUS_DEVICES if d.device_type == "switch"]
        aps = [d for d in CAMPUS_DEVICES if d.device_type == "access_point"]
        probes = [d for d in CAMPUS_DEVICES if d.device_type == "latency_probe"]
        self.assertEqual(len(switches), 8)
        self.assertEqual(len(aps), 12)
        self.assertEqual(len(probes), 4)


class TestTelemetrySimulator(unittest.TestCase):
    def test_message_schema_and_determinism(self) -> None:
        sim1 = TelemetrySimulator(seed=42, dirty_rate=0.0)
        sim2 = TelemetrySimulator(seed=42, dirty_rate=0.0)
        ts = datetime(2026, 11, 18, 3, 0, 0, tzinfo=timezone.utc)  # 10:00 WIB

        events1, ledger1 = sim1.generate_tick(ts)
        events2, ledger2 = sim2.generate_tick(ts)

        self.assertEqual(len(events1), 24)
        self.assertEqual(len(ledger1), 0)
        self.assertEqual(events1, events2)
        self.assertEqual(ledger1, ledger2)

        for ev in events1:
            self.assertIn("device_id", ev)
            self.assertIn("device_type", ev)
            self.assertIn("building_id", ev)
            self.assertIn("timestamp", ev)
            self.assertTrue(ev["timestamp"].endswith("Z"))
            self.assertIsInstance(ev["data"], dict)

    def test_diurnal_and_flash_crowd_load_profiles(self) -> None:
        sim = TelemetrySimulator(seed=7, dirty_rate=0.0)
        lecture_ts = datetime(2026, 11, 18, 3, 0, 0, tzinfo=timezone.utc)  # 10:00 WIB
        night_ts = datetime(2026, 11, 18, 19, 0, 0, tzinfo=timezone.utc)   # 02:00 WIB

        lecture_events, _ = sim.generate_tick(lecture_ts)
        night_events, _ = sim.generate_tick(night_ts)
        flash_events, _ = sim.generate_tick(lecture_ts, flash_crowd=True)

        avg_lecture_in = sum(
            e["data"]["port_in_mbps"] for e in lecture_events if e["device_type"] == "switch"
        ) / 8
        avg_night_in = sum(
            e["data"]["port_in_mbps"] for e in night_events if e["device_type"] == "switch"
        ) / 8
        avg_flash_lat = sum(
            e["data"]["latency_ms"] for e in flash_events if e["device_type"] == "latency_probe"
        ) / 4

        self.assertGreater(avg_lecture_in, avg_night_in)
        self.assertGreater(avg_flash_lat, 100.0)

    def test_all_five_dirty_types_injected_and_recorded_in_ledger(self) -> None:
        sim = TelemetrySimulator(seed=123, dirty_rate=0.25)
        start = datetime(2026, 11, 18, 2, 0, 0, tzinfo=timezone.utc)

        all_events = []
        all_ledger = []
        for i in range(60):
            events, ledger = sim.generate_tick(start + timedelta(minutes=i))
            all_events.extend(events)
            all_ledger.extend(ledger)

        injected_types = {entry["anomaly_type"] for entry in all_ledger}
        expected_types = {
            "counter_reset",
            "inconsistent_unit",
            "out_of_range",
            "missing_heartbeat",
            "duplicate_message",
        }
        self.assertEqual(injected_types, expected_types)
        self.assertGreater(len(all_events), 0)


if __name__ == "__main__":
    unittest.main()
