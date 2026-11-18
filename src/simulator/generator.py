import copy
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from src.simulator.topology import CAMPUS_DEVICES, NetworkDevice

WIB_OFFSET = timedelta(hours=7)


class TelemetrySimulator:
    """Deterministic campus network telemetry generator with controlled dirty data injection."""

    def __init__(self, seed: int = 42, dirty_rate: float = 0.05) -> None:
        self.rng = random.Random(seed)
        self.dirty_rate = dirty_rate
        self.switch_errors: Dict[str, int] = {
            d.device_id: self.rng.randint(10, 50)
            for d in CAMPUS_DEVICES
            if d.device_type == "switch"
        }
        self.switch_uptime: Dict[str, int] = {
            d.device_id: self.rng.randint(36000, 86400)
            for d in CAMPUS_DEVICES
            if d.device_type == "switch"
        }
        self.missing_until: Dict[str, datetime] = {}

    @staticmethod
    def _diurnal_factor(ts_utc: datetime) -> float:
        """Compute traffic multiplier based on WIB hour (high during lecture hours, low at night)."""
        wib_hour = (ts_utc + WIB_OFFSET).hour
        if 8 <= wib_hour <= 17:
            return 0.75 + 0.15 * (1.0 if 9 <= wib_hour <= 15 else 0.5)
        if 18 <= wib_hour <= 21:
            return 0.45
        return 0.15

    @staticmethod
    def _building_stress_bias(building_id: str) -> float:
        """Give BLD-FEB and BLD-RECT slightly higher baseline congestion for realistic analytics."""
        biases = {
            "BLD-FIK": 1.0,
            "BLD-FT": 1.05,
            "BLD-FEB": 1.25,
            "BLD-RECT": 1.15,
        }
        return biases.get(building_id, 1.0)

    def _build_normal_data(
        self, device: NetworkDevice, load: float, flash_crowd: bool
    ) -> Dict[str, Any]:
        bias = self._building_stress_bias(device.building_id)
        effective_load = min(1.35 if flash_crowd else 0.95, load * bias * (1.8 if flash_crowd else 1.0))

        if device.device_type == "switch":
            cap = float(device.max_bandwidth_mbps or 1000)
            in_mbps = round(cap * 0.45 * effective_load * self.rng.uniform(0.85, 1.15), 2)
            out_mbps = round(in_mbps * self.rng.uniform(0.75, 0.95), 2)
            self.switch_errors[device.device_id] += self.rng.randint(0, 3 if flash_crowd else 1)
            self.switch_uptime[device.device_id] += 1
            return {
                "port_in_mbps": in_mbps,
                "port_out_mbps": out_mbps,
                "errors": self.switch_errors[device.device_id],
                "uptime_s": self.switch_uptime[device.device_id],
            }

        if device.device_type == "access_point":
            base_clients = 45 if "RECT" in device.device_id else 32
            clients = max(1, int(base_clients * effective_load * self.rng.uniform(0.8, 1.2)))
            rssi = round(self.rng.uniform(-68.0, -48.0) - (5.0 if flash_crowd else 0.0), 1)
            ch_util = round(min(96.0, 55.0 * effective_load * self.rng.uniform(0.85, 1.15)), 1)
            return {
                "clients": clients,
                "rssi_avg_dbm": rssi,
                "channel_util_pct": ch_util,
            }

        # latency_probe
        if flash_crowd:
            latency = round(self.rng.uniform(105.0, 195.0), 2)
            loss = round(self.rng.uniform(2.2, 5.5), 2)
        else:
            latency = round(14.0 + 28.0 * effective_load + self.rng.uniform(-3.0, 8.0), 2)
            loss = round(max(0.0, self.rng.uniform(0.0, 0.65) * bias), 2)
        return {
            "latency_ms": latency,
            "packet_loss_pct": loss,
            "target": device.target_host or "core-dc.campus.ac.id",
        }

    def _maybe_inject_dirty(
        self,
        event: Dict[str, Any],
        ts_utc: datetime,
        ts_str: str,
        mode: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Apply controlled dirty data anomalies and return (emitted_events, ledger_entries)."""
        if self.dirty_rate <= 0 or self.rng.random() >= self.dirty_rate:
            return [event], []

        dtype = event["device_type"]
        did = event["device_id"]
        candidates = ["duplicate_message", "missing_heartbeat"]
        if dtype == "switch":
            candidates.extend(["counter_reset", "inconsistent_unit"])
        else:
            candidates.append("out_of_range")

        anomaly = self.rng.choice(candidates)
        ledger_base = {
            "timestamp": ts_str,
            "device_id": did,
            "building_id": event["building_id"],
            "anomaly_type": anomaly,
            "mode": mode,
        }

        if anomaly == "missing_heartbeat":
            self.missing_until[did] = ts_utc + timedelta(minutes=3)
            return [], [ledger_base]

        if anomaly == "counter_reset":
            self.switch_errors[did] = 0
            self.switch_uptime[did] = 0
            event["data"]["errors"] = 0
            event["data"]["uptime_s"] = 0
            return [event], [ledger_base]

        if anomaly == "inconsistent_unit":
            event["data"]["port_in_mbps"] = round(event["data"]["port_in_mbps"] * 1_000_000, 2)
            event["data"]["port_out_mbps"] = round(event["data"]["port_out_mbps"] * 1_000_000, 2)
            return [event], [ledger_base]

        if anomaly == "out_of_range":
            if dtype == "latency_probe":
                if self.rng.random() < 0.5:
                    event["data"]["packet_loss_pct"] = round(self.rng.uniform(105.0, 145.0), 2)
                else:
                    event["data"]["latency_ms"] = round(-self.rng.uniform(5.0, 30.0), 2)
            else:
                event["data"]["channel_util_pct"] = round(self.rng.uniform(105.0, 135.0), 1)
            return [event], [ledger_base]

        # duplicate_message
        dup_event = copy.deepcopy(event)
        return [event, dup_event], [ledger_base]

    def generate_tick(
        self,
        ts_utc: datetime,
        flash_crowd: bool = False,
        mode: str = "stream",
        stressed_buildings: Tuple[str, ...] = (),
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Generate one timestamp tick across all 24 devices."""
        if ts_utc.tzinfo is None:
            ts_utc = ts_utc.replace(tzinfo=timezone.utc)
        ts_str = ts_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        load = self._diurnal_factor(ts_utc)

        all_events: List[Dict[str, Any]] = []
        all_ledger: List[Dict[str, Any]] = []

        for device in CAMPUS_DEVICES:
            mute_until = self.missing_until.get(device.device_id)
            if mute_until is not None:
                if ts_utc < mute_until:
                    continue
                del self.missing_until[device.device_id]

            is_stressed = flash_crowd or (device.building_id in stressed_buildings)
            event: Dict[str, Any] = {
                "device_id": device.device_id,
                "device_type": device.device_type,
                "building_id": device.building_id,
                "timestamp": ts_str,
                "data": self._build_normal_data(device, load, is_stressed),
            }
            emitted, ledger = self._maybe_inject_dirty(event, ts_utc, ts_str, mode)
            all_events.extend(emitted)
            all_ledger.extend(ledger)

        return all_events, all_ledger
