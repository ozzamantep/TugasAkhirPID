from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class NetworkDevice:
    """Represents a simulated campus network endpoint."""

    device_id: str
    device_type: str
    building_id: str
    max_bandwidth_mbps: Optional[int] = None
    target_host: Optional[str] = None


CAMPUS_DEVICES: List[NetworkDevice] = [
    # BLD-FIK (6 devices: 2 switches, 3 APs, 1 probe)
    NetworkDevice("SW-FIK-01", "switch", "BLD-FIK", max_bandwidth_mbps=1000),
    NetworkDevice("SW-FIK-02", "switch", "BLD-FIK", max_bandwidth_mbps=1000),
    NetworkDevice("AP-FIK-01", "access_point", "BLD-FIK", max_bandwidth_mbps=300),
    NetworkDevice("AP-FIK-02", "access_point", "BLD-FIK", max_bandwidth_mbps=300),
    NetworkDevice("AP-FIK-03", "access_point", "BLD-FIK", max_bandwidth_mbps=300),
    NetworkDevice("PRB-FIK-01", "latency_probe", "BLD-FIK", target_host="core-dc.campus.ac.id"),
    # BLD-FT (6 devices)
    NetworkDevice("SW-FT-01", "switch", "BLD-FT", max_bandwidth_mbps=1000),
    NetworkDevice("SW-FT-02", "switch", "BLD-FT", max_bandwidth_mbps=1000),
    NetworkDevice("AP-FT-01", "access_point", "BLD-FT", max_bandwidth_mbps=300),
    NetworkDevice("AP-FT-02", "access_point", "BLD-FT", max_bandwidth_mbps=300),
    NetworkDevice("AP-FT-03", "access_point", "BLD-FT", max_bandwidth_mbps=300),
    NetworkDevice("PRB-FT-01", "latency_probe", "BLD-FT", target_host="core-dc.campus.ac.id"),
    # BLD-FEB (6 devices)
    NetworkDevice("SW-FEB-01", "switch", "BLD-FEB", max_bandwidth_mbps=1000),
    NetworkDevice("SW-FEB-02", "switch", "BLD-FEB", max_bandwidth_mbps=1000),
    NetworkDevice("AP-FEB-01", "access_point", "BLD-FEB", max_bandwidth_mbps=300),
    NetworkDevice("AP-FEB-02", "access_point", "BLD-FEB", max_bandwidth_mbps=300),
    NetworkDevice("AP-FEB-03", "access_point", "BLD-FEB", max_bandwidth_mbps=300),
    NetworkDevice("PRB-FEB-01", "latency_probe", "BLD-FEB", target_host="sia-krs.campus.ac.id"),
    # BLD-RECT (6 devices)
    NetworkDevice("SW-RECT-01", "switch", "BLD-RECT", max_bandwidth_mbps=10000),
    NetworkDevice("SW-RECT-02", "switch", "BLD-RECT", max_bandwidth_mbps=10000),
    NetworkDevice("AP-RECT-01", "access_point", "BLD-RECT", max_bandwidth_mbps=600),
    NetworkDevice("AP-RECT-02", "access_point", "BLD-RECT", max_bandwidth_mbps=600),
    NetworkDevice("AP-RECT-03", "access_point", "BLD-RECT", max_bandwidth_mbps=600),
    NetworkDevice("PRB-RECT-01", "latency_probe", "BLD-RECT", target_host="sia-krs.campus.ac.id"),
]


def get_building_ids() -> List[str]:
    """Return sorted unique building identifiers."""
    return sorted({d.building_id for d in CAMPUS_DEVICES})
