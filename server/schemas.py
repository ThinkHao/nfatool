from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, ConfigDict


class TaskBase(BaseModel):
    name: str
    group_name: Optional[str] = None
    active: bool = True
    kind: Literal["one_off", "periodic"] = "one_off"
    data_source_type: Literal["nfa", "edc"] = "nfa"
    data_source_instance: Optional[str] = "default"

    schedule_type: Optional[Literal["cron", "interval", "weekly_preset"]] = None
    schedule_expr: Optional[str] = None
    schedule_time_of_day: Optional[str] = None  # HH:MM:SS
    timezone: str = "Asia/Shanghai"

    window_selector: Literal["custom", "last_week", "last_month", "last_n_days"] = "custom"
    window_params: Optional[dict[str, Any]] = None

    params: dict[str, Any] = Field(default_factory=dict)
    export_formats: list[Literal["csv", "xlsx"]] = Field(default_factory=lambda: ["csv"])
    output_filename_template: Optional[str] = None


class TaskCreate(TaskBase):
    pass


class TaskUpdate(BaseModel):
    name: Optional[str] = None
    group_name: Optional[str] = None
    active: Optional[bool] = None
    kind: Optional[Literal["one_off", "periodic"]] = None
    data_source_type: Optional[Literal["nfa", "edc"]] = None
    data_source_instance: Optional[str] = None
    schedule_type: Optional[Literal["cron", "interval", "weekly_preset"]] = None
    schedule_expr: Optional[str] = None
    schedule_time_of_day: Optional[str] = None
    timezone: Optional[str] = None
    window_selector: Optional[Literal["custom", "last_week", "last_month", "last_n_days"]] = None
    window_params: Optional[dict[str, Any]] = None
    params: Optional[dict[str, Any]] = None
    export_formats: Optional[list[Literal["csv", "xlsx"]]] = None
    output_filename_template: Optional[str] = None


class TaskGroupCreate(BaseModel):
    name: str


class TaskGroupRename(BaseModel):
    old_name: str
    new_name: str
    merge: bool = False


class TaskGroupAssign(BaseModel):
    group_name: Optional[str] = None


class TaskOut(TaskBase):
    id: int
    created_at: datetime
    updated_at: datetime
    next_run_time: Optional[datetime] = None
    latest_budget_summary: Optional[dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)


class JobRunCreate(BaseModel):
    # For ad-hoc run
    data_source_type: Optional[Literal["nfa", "edc"]] = None
    data_source_instance: Optional[str] = None
    window_selector: Optional[Literal["custom", "last_week", "last_month", "last_n_days"]] = None
    window_params: Optional[dict[str, Any]] = None
    params: Optional[dict[str, Any]] = None
    export_formats: Optional[list[Literal["csv", "xlsx"]]] = None
    output_filename_template: Optional[str] = None


class JobRunOut(BaseModel):
    id: str
    task_id: Optional[int]
    task_kind: Optional[Literal["one_off", "periodic"]] = None
    status: Literal["pending", "running", "succeeded", "failed"]
    progress_pct: int = 0
    progress_stage: Optional[str] = None
    progress_events: list[dict[str, Any]] = Field(default_factory=list)
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    resolved_window: Optional[dict[str, Any]] = None
    resolved_params: Optional[dict[str, Any]] = None
    artifacts: list[dict[str, Any]] = Field(default_factory=list)
    log_path: Optional[str] = None
    error_message: Optional[str] = None
    edc_match: Optional[dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)


class TaskPageOut(BaseModel):
    items: list[TaskOut]
    total: int
    page: int
    page_size: int


class JobRunPageOut(BaseModel):
    items: list[JobRunOut]
    total: int
    page: int
    page_size: int


class EdcMatchPreviewPayload(BaseModel):
    data_source_instance: str = "default"
    start_time: str
    end_time: str
    edc_name: str
    edc_match_mode: Optional[Literal["exact", "prefix"]] = "prefix"
    edc_exclude_like: Optional[str] = "%-backup"


class TaskBatchDelete(BaseModel):
    ids: list[int] = Field(default_factory=list)


class JobBatchDelete(BaseModel):
    ids: list[str] = Field(default_factory=list)


class JobBatchDownload(BaseModel):
    run_ids: Optional[list[str]] = None
    month: Optional[str] = None  # YYYY-MM
    task_kind: Literal["all", "periodic", "one_off"] = "all"
    status: Literal["all", "pending", "running", "succeeded", "failed"] = "succeeded"
    file_format: Literal["all", "csv", "xlsx"] = "csv"


class DataSourceInstancePayload(BaseModel):
    source_type: Literal["nfa", "edc"]
    instance: str
    config: dict[str, Any] = Field(default_factory=dict)


class DataSourceTestPayload(BaseModel):
    source_type: Literal["nfa", "edc"]
    instance: Optional[str] = None
    config: Optional[dict[str, Any]] = None


class DataSourceRotateKeyPayload(BaseModel):
    old_seed: str
    new_seed: str


class DataSourceRotatePolicyPayload(BaseModel):
    enabled: bool = True
    interval_days: int = 30


class UpdateApplyPayload(BaseModel):
    restart_after_update: bool = True
