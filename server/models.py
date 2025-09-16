from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import String, Integer, Boolean, Text, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    kind: Mapped[Optional[str]] = mapped_column(String(20), default="one_off")

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

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    runs: Mapped[list[JobRun]] = relationship("JobRun", back_populates="task", cascade="all, delete-orphan")


class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # UUID string
    task_id: Mapped[Optional[int]] = mapped_column(ForeignKey("tasks.id"), nullable=True)

    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending|running|succeeded|failed
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # snapshots
    resolved_window: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON
    resolved_params: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON

    artifacts: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON array
    log_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    task: Mapped[Optional[Task]] = relationship("Task", back_populates="runs")
