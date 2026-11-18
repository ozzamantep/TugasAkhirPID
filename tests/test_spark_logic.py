import unittest

from src.spark.streaming_logic import (
    detect_moving_avg_anomaly,
    normalize_bandwidth_mbps,
    sanitize_metric,
)


class TestSparkStreamingLogic(unittest.TestCase):
    def test_normalize_bandwidth_mbps(self) -> None:
        self.assertAlmostEqual(normalize_bandwidth_mbps(420.5), 420.5)
        self.assertAlmostEqual(normalize_bandwidth_mbps(420_500_000.0), 420.5)
        self.assertIsNone(normalize_bandwidth_mbps(None))

    def test_sanitize_metric_bounds(self) -> None:
        self.assertEqual(sanitize_metric(1.5, 0.0, 100.0), 1.5)
        self.assertIsNone(sanitize_metric(125.0, 0.0, 100.0))
        self.assertIsNone(sanitize_metric(-12.0, 0.0, 10000.0))

    def test_detect_moving_avg_anomaly(self) -> None:
        history = [100.0, 102.0, 98.0, 101.0, 99.0, 100.0, 103.0, 97.0, 101.0, 99.0]
        # Normal value within +/- 3 std
        self.assertIsNone(detect_moving_avg_anomaly(103.0, history))
        # Flash crowd / spike well outside +3 std
        anomaly = detect_moving_avg_anomaly(250.0, history)
        self.assertIsNotNone(anomaly)
        assert anomaly is not None
        self.assertGreater(anomaly["z_score"], 3.0)
        self.assertGreater(anomaly["port_in_mbps"], anomaly["upper_band"])


if __name__ == "__main__":
    unittest.main()
