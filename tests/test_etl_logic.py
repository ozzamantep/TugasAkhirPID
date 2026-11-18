import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from src.dags.etl_logic import clean_and_audit_telemetry, compute_sla_windows, reconcile_with_ledger
from src.simulator.generator import TelemetrySimulator


class TestEtlLogic(unittest.TestCase):
    def test_clean_and_audit_catches_all_five_dirty_types(self) -> None:
        sim = TelemetrySimulator(seed=99, dirty_rate=0.2)
        start = datetime(2026, 11, 18, 2, 0, 0, tzinfo=timezone.utc)
        raw_docs = []
        ledger = []
        for i in range(40):
            evs, ldg = sim.generate_tick(start + timedelta(minutes=i), mode="backfill")
            raw_docs.extend(evs)
            ledger.extend(ldg)

        cleaned_df, dq_counts, hb_alerts = clean_and_audit_telemetry(raw_docs)
        reconciled = reconcile_with_ledger(ledger, dq_counts)

        self.assertGreater(len(cleaned_df), 0)
        for anomaly_type in (
            "counter_reset",
            "inconsistent_unit",
            "out_of_range",
            "missing_heartbeat",
            "duplicate_message",
        ):
            row = reconciled[reconciled["anomaly_type"] == anomaly_type].iloc[0]
            self.assertGreater(row["disisipkan_count"], 0, f"Expected injected {anomaly_type}")
            self.assertGreater(row["tertangkap_count"], 0, f"Expected caught {anomaly_type}")
            self.assertEqual(row["tertangkap_count"], row["ditangani_count"])

        # Verify no out-of-range or bps values remain in cleaned_df
        self.assertTrue((cleaned_df["port_in_mbps"].dropna() <= 10_000.0).all())
        self.assertTrue((cleaned_df["packet_loss_pct"].dropna() <= 100.0).all())
        self.assertTrue((cleaned_df["latency_ms"].dropna() >= 0.0).all())
        self.assertGreater(len(hb_alerts), 0)

    def test_compute_sla_windows_flags_violations(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "device_id": "PRB-FIK-01",
                    "device_type": "latency_probe",
                    "building_id": "BLD-FIK",
                    "ts_utc": pd.Timestamp("2026-11-18T03:00:00Z"),
                    "latency_ms": 130.0,
                    "packet_loss_pct": 1.0,
                    "channel_util_pct": None,
                },
                {
                    "device_id": "PRB-FT-01",
                    "device_type": "latency_probe",
                    "building_id": "BLD-FT",
                    "ts_utc": pd.Timestamp("2026-11-18T03:00:00Z"),
                    "latency_ms": 25.0,
                    "packet_loss_pct": 0.2,
                    "channel_util_pct": None,
                },
            ]
        )
        sla_df = compute_sla_windows(df)
        fik_row = sla_df[sla_df["building_id"] == "BLD-FIK"].iloc[0]
        ft_row = sla_df[sla_df["building_id"] == "BLD-FT"].iloc[0]
        self.assertFalse(bool(fik_row["is_sla_compliant"]))
        self.assertTrue(bool(ft_row["is_sla_compliant"]))


if __name__ == "__main__":
    unittest.main()
