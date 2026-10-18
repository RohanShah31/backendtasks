"""
Task 2: Workshop Registration and Notifications
Pydantic Schemas and Data Models
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Literal
from datetime import datetime
import re

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class WorkshopCreate(BaseModel):
    title: str = Field(..., description="Title of the workshop")
    description: Optional[str] = Field(default="", description="Workshop description")
    capacity: int = Field(default=50, ge=1, description="Maximum participant capacity")
    organizer_email: str = Field(..., description="Email of the organizing lead")

    @field_validator("organizer_email")
    def validate_organizer_email(cls, v):
        clean = v.strip().lower()
        if not EMAIL_REGEX.match(clean):
            raise ValueError("Invalid email format")
        return clean


class WorkshopResponse(BaseModel):
    id: str
    title: str
    description: str
    capacity: int
    organizer_email: str
    status: Literal["ACTIVE", "CANCELLED"]
    registered_count: int
    created_at: str
    cancelled_at: Optional[str] = None


class MemberRegisterRequest(BaseModel):
    name: str = Field(..., description="Full name of the registrant")
    email: str = Field(..., description="Email address (must be unique per workshop)")

    @field_validator("email")
    def validate_member_email(cls, v):
        clean = v.strip().lower()
        if not EMAIL_REGEX.match(clean):
            raise ValueError("Invalid email format")
        return clean


class RegistrationResponse(BaseModel):
    registration_id: str
    workshop_id: str
    workshop_title: str
    member_name: str
    member_email: str
    status: str
    message: str
    registered_at: str
    notification_job_id: str


class NotificationJobResponse(BaseModel):
    id: str
    idempotency_key: str
    workshop_id: str
    member_email: str
    member_name: str
    event_type: str
    payload: dict
    status: Literal["PENDING", "PROCESSING", "RETRY", "DELIVERED", "FAILED"]
    attempts: int
    max_attempts: int
    last_error: Optional[str] = None
    next_retry_at: str
    created_at: str
    delivered_at: Optional[str] = None


class MockConfig(BaseModel):
    failure_rate: float = Field(default=0.0, ge=0.0, le=1.0, description="Chance (0.0 to 1.0) of throwing HTTP 500")
    timeout_rate: float = Field(default=0.0, ge=0.0, le=1.0, description="Chance (0.0 to 1.0) of timing out")
    latency_seconds: float = Field(default=0.05, ge=0.0, description="Artificial network delay in seconds")
    offline: bool = Field(default=False, description="If True, service is completely unreachable (HTTP 503)")
