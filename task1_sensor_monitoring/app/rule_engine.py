"""
Task 1: Campus Sensor Monitoring and Alerts
High-Performance In-Memory Rule Evaluation Engine
Supports single-reading and sliding time-window rules (avg, max, min, count)
"""

import operator
from collections import deque
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
import uuid
import threading

# Comparison operator mapping
OPERATORS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}


class RuleEngine:
    """
    Evaluates streaming sensor readings against rules with sub-microsecond latency.
    Keeps sliding windows in memory for O(1) aggregations.
    """

    def __init__(self, alert_cooldown_seconds: float = 10.0):
        self.rules: List[Dict[str, Any]] = []
        # Key: (sensor_id, metric_type) -> deque of (epoch_timestamp, value)
        self.windows: Dict[Tuple[str, str], deque] = {}
        # Key: (metric_type,) -> campus-wide deque of (epoch_timestamp, value)
        self.campus_windows: Dict[str, deque] = {}
        # Cooldown tracker to prevent duplicate OPEN alert storms:
        # Key: (rule_id, sensor_id) -> last_alert_epoch
        self.cooldown_tracker: Dict[Tuple[str, str], float] = {}
        self.alert_cooldown_seconds = alert_cooldown_seconds
        self._lock = threading.Lock()

    def sync_rules(self, rules: List[Dict[str, Any]]):
        """Synchronizes active rules into fast in-memory cache."""
        with self._lock:
            # Only keep enabled rules in the hot path
            self.rules = [r for r in rules if r.get("enabled")]

    def _evaluate_condition(self, val: float, op_str: str, threshold: float) -> bool:
        """Compares value against threshold using operator string."""
        op_func = OPERATORS.get(op_str)
        if op_func is None:
            return False
        return op_func(val, threshold)

    def process_reading(self, reading: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Processes a single reading: updates sliding window and evaluates rules.
        Returns a list of newly generated alert dictionaries (if any).
        """
        sensor_id = reading["sensorId"]
        metric_type = reading["type"]
        val = float(reading["value"])
        
        # Parse timestamp to float epoch
        ts = reading["timestamp"]
        if isinstance(ts, str):
            clean_ts = ts.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_ts)
        elif isinstance(dt_obj := ts, datetime):
            dt = dt_obj
        else:
            dt = datetime.now(timezone.utc)
        epoch = dt.timestamp()
        iso_timestamp = dt.isoformat()

        generated_alerts = []

        with self._lock:
            # 1. Update sliding window for this (sensor_id, metric_type)
            key = (sensor_id, metric_type)
            if key not in self.windows:
                self.windows[key] = deque()
            dq = self.windows[key]
            dq.append((epoch, val))

            # Also update campus-wide window
            if metric_type not in self.campus_windows:
                self.campus_windows[metric_type] = deque()
            cdq = self.campus_windows[metric_type]
            cdq.append((epoch, val))

            # Prune windows older than 60 minutes
            max_retention_sec = 3600.0
            cutoff = epoch - max_retention_sec
            while dq and dq[0][0] < cutoff:
                dq.popleft()
            while cdq and cdq[0][0] < cutoff:
                cdq.popleft()

            # 2. Evaluate all active rules
            for rule in self.rules:
                # Check if rule matches this metric
                if rule["metric_type"] != metric_type:
                    continue

                # Check sensor filter: if rule specifies a sensor, it must match
                rule_sensor = rule.get("sensor_id")
                if rule_sensor and rule_sensor != "*" and rule_sensor != sensor_id:
                    continue

                rule_type = rule.get("rule_type", "single")

                # Single reading evaluation
                if rule_type == "single":
                    if self._evaluate_condition(val, rule["operator"], rule["threshold"]):
                        alert = self._create_alert_if_eligible(
                            rule=rule,
                            sensor_id=sensor_id,
                            metric_type=metric_type,
                            triggered_value=val,
                            iso_timestamp=iso_timestamp,
                            epoch=epoch,
                            summary=f"{metric_type} {val} {rule['operator']} threshold {rule['threshold']}"
                        )
                        if alert:
                            generated_alerts.append(alert)

                # Time window evaluation
                elif rule_type == "time_window":
                    window_minutes = rule.get("window_minutes") or 10
                    window_seconds = window_minutes * 60.0
                    aggregation = rule.get("aggregation") or "avg"
                    window_cutoff = epoch - window_seconds

                    # Choose appropriate window (specific sensor vs campus)
                    target_dq = dq if rule_sensor else cdq
                    matching_values = [v for (t, v) in target_dq if t >= window_cutoff]

                    if not matching_values:
                        continue

                    # Compute aggregation
                    if aggregation == "avg":
                        agg_val = sum(matching_values) / len(matching_values)
                    elif aggregation == "max":
                        agg_val = max(matching_values)
                    elif aggregation == "min":
                        agg_val = min(matching_values)
                    elif aggregation == "count":
                        agg_val = float(len(matching_values))
                    else:
                        continue

                    if self._evaluate_condition(agg_val, rule["operator"], rule["threshold"]):
                        summary = (
                            f"{window_minutes}m {aggregation.upper()} {metric_type} is "
                            f"{round(agg_val, 2)} {rule['operator']} threshold {rule['threshold']} "
                            f"(samples: {len(matching_values)})"
                        )
                        alert = self._create_alert_if_eligible(
                            rule=rule,
                            sensor_id=sensor_id if rule_sensor else "campus-wide",
                            metric_type=metric_type,
                            triggered_value=round(agg_val, 2),
                            iso_timestamp=iso_timestamp,
                            epoch=epoch,
                            summary=summary
                        )
                        if alert:
                            generated_alerts.append(alert)

        return generated_alerts

    def _create_alert_if_eligible(
        self,
        rule: Dict[str, Any],
        sensor_id: str,
        metric_type: str,
        triggered_value: float,
        iso_timestamp: str,
        epoch: float,
        summary: str
    ) -> Optional[Dict[str, Any]]:
        """Checks cooldown de-duplication to prevent flooding alerts."""
        cooldown_key = (rule["id"], sensor_id)
        last_alert_time = self.cooldown_tracker.get(cooldown_key, 0.0)

        # Enforce cooldown period per rule and sensor
        if epoch - last_alert_time < self.alert_cooldown_seconds:
            return None

        self.cooldown_tracker[cooldown_key] = epoch

        return {
            "id": f"alert-{uuid.uuid4().hex[:10]}",
            "rule_id": rule["id"],
            "rule_name": rule["name"],
            "sensor_id": sensor_id,
            "metric_type": metric_type,
            "triggered_value": triggered_value,
            "threshold": rule["threshold"],
            "condition_summary": summary,
            "timestamp": iso_timestamp,
            "status": "OPEN",
            "acknowledged_at": None,
            "resolved_at": None,
            "notes": None
        }

    def process_batch(self, readings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Processes a batch of readings efficiently."""
        all_alerts = []
        for r in readings:
            alerts = self.process_reading(r)
            if alerts:
                all_alerts.extend(alerts)
        return all_alerts
