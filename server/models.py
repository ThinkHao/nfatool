from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import String, Integer, Boolean, Text, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


class TaskGroup(Base):
    __tablename__ = "task_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    group_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    kind: Mapped[Optional[str]] = mapped_column(String(20), default="one_off")
    data_source_type: Mapped[str] = mapped_column(String(20), default="nfa")
    data_source_instance: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # schedule
    schedule_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # cron | interval | weekly_preset
    schedule_expr: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)  # e.g. cron expr or seconds
    schedule_time_of_day: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # HH:MM:SS for weekly/daily
    timezone: Mapped[str] = mapped_column(String(64), default="Asia/Shanghai")

    # time window
    window_selector: Mapped[str] = mapped_column(String(50), default="custom")  # last_week | last_n_days | custom
    window_params: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON string

    # script params
    params: Mapped[str] = mapped_column(Text)  # JSON string of script params

    # export
    export_formats: Mapped[str] = mapped_column(Text, default='["csv"]')  # JSON array string
    output_filename_template: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    latest_budget_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON object

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    runs: Mapped[list[JobRun]] = relationship("JobRun", back_populates="task", cascade="all, delete-orphan")


class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # UUID string
    task_id: Mapped[Optional[int]] = mapped_column(ForeignKey("tasks.id"), nullable=True)

    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending|running|succeeded|failed
    progress_pct: Mapped[int] = mapped_column(Integer, default=0)
    progress_stage: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    progress_events: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON array
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # snapshots
    resolved_window: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON
    resolved_params: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON

    artifacts: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON array
    log_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    task: Mapped[Optional[Task]] = relationship("Task", back_populates="runs")


class EdcMatchSnapshot(Base):
    """Frozen EDC object set used by one run."""

    __tablename__ = "edc_match_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_run_id: Mapped[str] = mapped_column(String(36), ForeignKey("job_runs.id"), unique=True, index=True)
    task_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    task_name_snapshot: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    data_source_instance: Mapped[str] = mapped_column(String(100), default="default")
    window_start: Mapped[str] = mapped_column(String(32))
    window_end: Mapped[str] = mapped_column(String(32))
    edc_name_expression: Mapped[str] = mapped_column(Text)
    match_mode: Mapped[str] = mapped_column(String(20), default="prefix")
    exclude_like: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    matched_count: Mapped[int] = mapped_column(Integer, default=0)
    match_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="resolved")
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_config_fingerprint: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    resolved_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    items: Mapped[list[EdcMatchSnapshotItem]] = relationship(
        "EdcMatchSnapshotItem", back_populates="snapshot", cascade="all, delete-orphan", order_by="EdcMatchSnapshotItem.edc_name"
    )


class EdcMatchSnapshotItem(Base):
    __tablename__ = "edc_match_snapshot_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("edc_match_snapshots.id"), index=True)
    edc_name: Mapped[str] = mapped_column(String(255))
    matched_by: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    match_operator: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    effective_pattern: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    snapshot: Mapped[EdcMatchSnapshot] = relationship("EdcMatchSnapshot", back_populates="items")

    __table_args__ = (UniqueConstraint("snapshot_id", "edc_name", name="uq_edc_snapshot_item_name"),)
