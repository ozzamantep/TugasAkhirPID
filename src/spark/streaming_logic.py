import math
from typing import Dict, Optional, Sequence


def normalize_bandwidth_mbps(value: Optional[float]) -> Optional[float]:
    """Convert bps values (> 10,000) to Mbps; return None if input is None."""
    if value is None:
        return None
    if value > 10_000.0:
        return round(value / 1_000_000.0, 4)
    return float(value)


def sanitize_metric(
    value: Optional[float], min_val: float, max_val: float
) -> Optional[float]:
    """Return value if within [min_val, max_val], else None."""
    if value is None:
        return None
    if value < min_val or value > max_val:
        return None
    return float(value)


def detect_moving_avg_anomaly(
    current_value: float,
    history: Sequence[float],
    min_samples: int = 10,
    sigma_multiplier: float = 3.0,
) -> Optional[Dict[str, float]]:
    """Flag current_value if outside moving-average +/- 3 std band."""
    if len(history) < min_samples:
        return None
    mean = sum(history) / len(history)
    variance = sum((x - mean) ** 2 for x in history) / len(history)
    std = math.sqrt(variance)
    effective_std = max(std, 1.0)
    upper = mean + sigma_multiplier * effective_std
    lower = max(0.0, mean - sigma_multiplier * effective_std)

    if current_value > upper or current_value < lower:
        z_score = (current_value - mean) / effective_std
        return {
            "port_in_mbps": round(current_value, 2),
            "moving_avg_1h": round(mean, 2),
            "moving_std_1h": round(std, 2),
            "upper_band": round(upper, 2),
            "lower_band": round(lower, 2),
            "z_score": round(z_score, 2),
        }
    return None
