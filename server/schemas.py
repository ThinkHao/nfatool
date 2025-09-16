from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, ConfigDict


class TaskBase(BaseModel):
    name: str
    active: bool = True
    kind: Literal["one_off", "periodic"] = "one_off"

    schedule_type: Optional[Literal["cron", "interval", "weekly_preset"]] = None
    schedule_expr: Optional[str] = None
    schedule_time_of_day: Optional[str] = None  # HH:MM:SS
    timezone: str = "Asia/Shanghai"

    window_selector: Literal["custom", "last_week", "last_n_days"] = "custom"
    window_params: Optional[dict[str, Any]] = None

    params: dict[str, Any] = Field(default_factory=dict)
    export_formats: list[Literal["csv", "xlsx"]] = Field(default_factory=lambda: ["csv"])
    output_filename_template: Optional[str] = None


class TaskCreate(TaskBase):
    pass


class TaskUpdate(BaseModel):
    name: Optional[str] = None
    active: Optional[bool] = None
    kind: Optional[Literal["one_off", "periodic"]] = None
    schedule_type: Optional[Literal["cron", "interval", "weekly_preset"]] = None
    schedule_expr: Optional[str] = None
    schedule_time_of_day: Optional[str] = None
    timezone: Optional[str] = None
    window_selector: Optional[Literal["custom", "last_week", "last_n_days"]] = None
    window_params: Optional[dict[str, Any]] = None
    params: Optional[dict[str, Any]] = None
    export_formats: Optional[list[Literal["csv", "xlsx"]]] = None
    output_filename_template: Optional[str] = None


class TaskOut(TaskBase):
    id: int
    created_at: datetime
    updated_at: datetime
    next_run_time: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class JobRunCreate(BaseModel):
    # For ad-hoc run
    window_selector: Optional[Literal["custom", "last_week", "last_n_days"]] = None
    window_params: Optional[dict[str, Any]] = None
    params: Optional[dict[str, Any]] = None
    export_formats: Optional[list[Literal["csv", "xlsx"]]] = None
    output_filename_template: Optional[str] = None


class JobRunOut(BaseModel):
    id: str
    task_id: Optional[int]
    status: Literal["pending", "running", "succeeded", "failed"]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    resolved_window: Optional[dict[str, Any]] = None
    resolved_params: Optional[dict[str, Any]] = None
    artifacts: list[dict[str, Any]] = Field(default_factory=list)
    log_path: Optional[str] = None
    error_message: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
