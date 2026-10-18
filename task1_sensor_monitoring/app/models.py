"""
Task 1: Campus Sensor Monitoring and Alerts
Models and Pydantic Schemas
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Literal
from datetime import datetime, timezone
import uuid


class SensorReading(BaseModel):
    sensorId: str = Field(..., description="Unique identifier of the sensor, e.g. library-floor-2")
    type: str = Field(..., description="Metric type: temperature, air_quality, occupancy, humidity, etc.")
    value: float = Field(..., description="Numeric sensor reading value")
    timestamp: Optional[datetime] = Field(default=None, description="ISO-8601 timestamp of reading")

    @field_validator("timestamp", mode="before")
    def ensure_timestamp(cls, v):
        if v is None:
            return datetime.now(timezone.utc)
        if isinstance(v, str):
            try:
                # Handle ISO strings with Z or offsets
                clean_str = v.replace("Z", "+00:00")
                return datetime.fromisoformat(clean_str)
            except Exception:
                return datetime.now(timezone.utc)
        return v


class BatchReadings(BaseModel):
    readings: List[SensorReading] = Field(..., description="List of sensor readings")


class RuleCreate(BaseModel):
    name: str = Field(..., description="Human-readable rule name, e.g. High Library Temperature")
    description: Optional[str] = Field(default="", description="Optional description of the rule")
    rule_type: Literal["single", "time_window"] = Field(
        default="single", 
        description="'single' evaluates per reading; 'time_window' evaluates an aggregation over a sliding window"
    )
    sensor_id: Optional[str] = Field(
        default=None, 
        description="Specific sensor ID to monitor, or null/* to monitor all sensors reporting this metric"
    )
    metric_type: str = Field(..., description="Metric type to match: temperature, air_quality, occupancy, etc.")
    operator: Literal[">", ">=", "<", "<=", "==", "!="] = Field(..., description="Comparison operator")
    threshold: float = Field(..., description="Numerical threshold")
    window_minutes: Optional[int] = Field(
        default=None, 
        description="Window size in minutes for time_window rules (e.g. 10)"
    )
    aggregation: Optional[Literal["avg", "max", "min", "count"]] = Field(
        default=None, 
        description="Aggregation function for time_window rules ('avg', 'max', 'min', 'count')"
    )
    enabled: bool = Field(default=True, description="Whether rule is actively evaluated")


class RuleUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    operator: Optional[Literal[">", ">=", "<", "<=", "==", "!="]] = None
    threshold: Optional[float] = None
    window_minutes: Optional[int] = None
    aggregation: Optional[Literal["avg", "max", "min", "count"]] = None
    enabled: Optional[bool] = None


class RuleResponse(BaseModel):
    id: str
    name: str
    description: str
    rule_type: str
    sensor_id: Optional[str]
    metric_type: str
    operator: str
    threshold: float
    window_minutes: Optional[int]
    aggregation: Optional[str]
    enabled: bool
    created_at: str
    updated_at: str


class AlertResponse(BaseModel):
    id: str
    rule_id: str
    rule_name: str
    sensor_id: str
    metric_type: str
    triggered_value: float
    threshold: float
    condition_summary: str
    timestamp: str
    status: Literal["OPEN", "ACKNOWLEDGED", "RESOLVED"]
    acknowledged_at: Optional[str] = None
    resolved_at: Optional[str] = None
    notes: Optional[str] = None


class AlertUpdateNotes(BaseModel):
    notes: str = Field(..., description="Organizer notes or remarks on the alert")


class IngestResponse(BaseModel):
    accepted: int
    alerts_triggered: int
    duration_ms: float
    message: str
