"""Local Collector task and run state for AxData.

This module is intentionally storage-first: it defines serializable task/run
records and a small JSON store that can be shared by API, CLI, and later the
single-process scheduler queue. It does not import provider or downloader
runtime at module import time.
"""

from __future__ import annotations

import json
import os
import re
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from socket import timeout as SocketTimeout
from pathlib import Path
from threading import Condition, Lock, RLock, Thread
from time import sleep
from typing import Any, Iterable, Mapping
from uuid import uuid4

SCHEDULER_STORE_VERSION = 1
COLLECTOR_SCHEDULER_FILE_NAME = "collector_scheduler.json"
LOCAL_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
RUN_EVENT_DETAIL_LIMIT = 20
RUN_EVENT_HISTORY_LIMIT = 100
RUN_EVENT_STRING_LIMIT = 500

RUN_ERROR_CATEGORIES = frozenset(
    {
        "provider_missing",
        "plugin_disabled",
        "plugin_failed",
        "dependency_missing",
        "invalid_params",
        "network_error",
        "upstream_empty",
        "upstream_error",
        "schema_mismatch",
        "quality_failed",
        "write_failed",
        "storage_missing",
        "storage_permission",
        "duplicate_skipped",
        "backoff_blocked",
        "resource_waiting",
        "scheduler_interrupted",
        "unknown",
    }
)

RUN_TIMING_FIELDS = (
    "queue_wait_ms",
    "params_resolve_ms",
    "provider_resolve_ms",
    "download_ms",
    "write_ms",
    "quality_ms",
    "total_ms",
)


class CollectorSchedulerError(ValueError):
    """Raised when collector scheduler state cannot be parsed or updated."""


class TaskStatus(str, Enum):
    """Run status values used by the local collector scheduler."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class TriggerType(str, Enum):
    """Supported MVP collector trigger types."""

    MANUAL = "manual"
    INTERVAL = "interval"
    DAILY = "daily"
    STARTUP = "startup"


ACTIVE_RUN_STATUSES = frozenset({TaskStatus.PENDING.value, TaskStatus.QUEUED.value, TaskStatus.RUNNING.value})
FINAL_RUN_STATUSES = frozenset(
    {
        TaskStatus.SUCCESS.value,
        TaskStatus.FAILED.value,
        TaskStatus.SKIPPED.value,
        TaskStatus.CANCELLED.value,
    }
)


@dataclass(frozen=True)
class CollectorTask:
    """Persisted definition for one collector task."""

    task_id: str
    collector_name: str
    name: str = ""
    template_id: str | None = None
    created_by: str = "user"
    enabled: bool = True
    trigger_type: str = TriggerType.MANUAL.value
    interval_seconds: int | None = None
    daily_time: str | None = None
    interface_name: str | None = None
    provider_id: str | None = None
    downloader_profile: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    fields: list[str] | None = None
    formats: list[str] | None = None
    resource_group: str = "default"
    output_root: str | None = None
    output_dir: str | None = None
    collect_mode: str | None = None
    connection_mode: str | None = None
    concurrency_mode: str | None = None
    connection_count: int | None = None
    source_server_count: int | None = None
    connections_per_server: int | None = None
    max_concurrent_tasks: int | None = None
    batch_size: int | None = None
    request_interval_ms: int | None = None
    retry_count: int | None = None
    max_retries: int | None = None
    backoff_seconds: int | None = None
    timeout_ms: int | None = None
    expected_layer: str | None = None
    schedule_hint: str | None = None
    write_mode: str | None = None
    partition_by: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    date_field: str | None = None
    required_datasets: list[str] = field(default_factory=list)
    required_plugin: str | None = None
    dependency: dict[str, Any] = field(default_factory=dict)
    dependency_status: str | None = None
    dependency_message: str | None = None
    dependency_errors: list[dict[str, Any]] = field(default_factory=list)
    category: str | None = None
    tags: list[str] = field(default_factory=list)
    next_run_at: str | None = None
    backoff_until: str | None = None
    last_run_id: str | None = None
    last_status: str | None = None
    last_success_at: str | None = None
    last_failure_at: str | None = None
    last_error: str | None = None
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        _validate_identifier(self.task_id, "task_id", allow_dot=True)
        if not str(self.collector_name or "").strip():
            raise CollectorSchedulerError("collector_name is required.")
        _normalize_status(self.last_status, allow_none=True)
        trigger_type = _normalize_trigger(self.trigger_type)
        object.__setattr__(self, "trigger_type", trigger_type)
        if trigger_type == TriggerType.INTERVAL.value:
            if self.interval_seconds is None or int(self.interval_seconds) <= 0:
                raise CollectorSchedulerError("interval_seconds must be a positive integer for interval tasks.")
        if trigger_type == TriggerType.DAILY.value:
            _normalize_daily_time(self.daily_time)
        if not self.resource_group:
            object.__setattr__(self, "resource_group", "default")
        if not self.name:
            object.__setattr__(self, "name", self.collector_name)

    def to_dict(self, *, include_guidance: bool = True) -> dict[str, Any]:
        payload = {
            "task_id": self.task_id,
            "collector_name": self.collector_name,
            "name": self.name,
            "template_id": self.template_id,
            "created_by": self.created_by,
            "enabled": bool(self.enabled),
            "trigger_type": self.trigger_type,
            "interval_seconds": self.interval_seconds,
            "daily_time": self.daily_time,
            "interface_name": self.interface_name,
            "provider_id": self.provider_id,
            "downloader_profile": self.downloader_profile,
            "params": _jsonable_mapping(self.params),
            "fields": list(self.fields) if self.fields is not None else None,
            "formats": list(self.formats) if self.formats is not None else None,
            "resource_group": self.resource_group,
            "output_root": self.output_root,
            "output_dir": self.output_dir,
            "collect_mode": self.collect_mode,
            "connection_mode": self.connection_mode,
            "concurrency_mode": self.concurrency_mode,
            "connection_count": self.connection_count,
            "source_server_count": self.source_server_count,
            "connections_per_server": self.connections_per_server,
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "batch_size": self.batch_size,
            "request_interval_ms": self.request_interval_ms,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "backoff_seconds": self.backoff_seconds,
            "timeout_ms": self.timeout_ms,
            "expected_layer": self.expected_layer,
            "schedule_hint": self.schedule_hint,
            "write_mode": self.write_mode,
            "partition_by": list(self.partition_by),
            "primary_key": list(self.primary_key),
            "date_field": self.date_field,
            "required_datasets": list(self.required_datasets),
            "required_plugin": self.required_plugin,
            "dependency": _jsonable_mapping(self.dependency),
            "dependency_status": self.dependency_status,
            "dependency_message": self.dependency_message,
            "dependency_errors": [_jsonable_mapping(error) for error in self.dependency_errors],
            "category": self.category,
            "tags": list(self.tags),
            "next_run_at": self.next_run_at,
            "backoff_until": self.backoff_until,
            "last_run_id": self.last_run_id,
            "last_status": self.last_status,
            "last_success_at": self.last_success_at,
            "last_failure_at": self.last_failure_at,
            "last_error": self.last_error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        if include_guidance:
            payload.update(collector_task_guidance(self))
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CollectorTask":
        return cls(
            task_id=_string(data.get("task_id"), "task_id"),
            collector_name=_string(data.get("collector_name"), "collector_name"),
            name=_string(data.get("name", ""), "name"),
            template_id=_optional_string(data.get("template_id"), "template_id"),
            created_by=_string(data.get("created_by", "user"), "created_by"),
            enabled=bool(data.get("enabled", True)),
            trigger_type=_string(data.get("trigger_type", TriggerType.MANUAL.value), "trigger_type"),
            interval_seconds=_optional_int(data.get("interval_seconds"), "interval_seconds"),
            daily_time=_optional_string(data.get("daily_time"), "daily_time"),
            interface_name=_optional_string(data.get("interface_name"), "interface_name"),
            provider_id=_optional_string(data.get("provider_id"), "provider_id"),
            downloader_profile=_optional_string(data.get("downloader_profile"), "downloader_profile"),
            params=dict(_mapping(data.get("params", {}), "params")),
            fields=_optional_string_list(data.get("fields"), "fields"),
            formats=_optional_string_list(data.get("formats"), "formats"),
            resource_group=_string(data.get("resource_group", "default"), "resource_group"),
            output_root=_optional_string(data.get("output_root"), "output_root"),
            output_dir=_optional_string(data.get("output_dir"), "output_dir"),
            collect_mode=_optional_string(data.get("collect_mode"), "collect_mode"),
            connection_mode=_optional_string(data.get("connection_mode"), "connection_mode"),
            concurrency_mode=_optional_string(data.get("concurrency_mode"), "concurrency_mode"),
            connection_count=_optional_int(data.get("connection_count"), "connection_count"),
            source_server_count=_optional_int(data.get("source_server_count"), "source_server_count"),
            connections_per_server=_optional_int(data.get("connections_per_server"), "connections_per_server"),
            max_concurrent_tasks=_optional_int(data.get("max_concurrent_tasks"), "max_concurrent_tasks"),
            batch_size=_optional_int(data.get("batch_size"), "batch_size"),
            request_interval_ms=_optional_int(data.get("request_interval_ms"), "request_interval_ms"),
            retry_count=_optional_int(data.get("retry_count"), "retry_count"),
            max_retries=_optional_non_negative_option(data.get("max_retries"), "max_retries"),
            backoff_seconds=_optional_non_negative_option(data.get("backoff_seconds"), "backoff_seconds"),
            timeout_ms=_optional_int(data.get("timeout_ms"), "timeout_ms"),
            expected_layer=_optional_string(data.get("expected_layer"), "expected_layer"),
            schedule_hint=_optional_string(data.get("schedule_hint"), "schedule_hint"),
            write_mode=_optional_string(data.get("write_mode"), "write_mode"),
            partition_by=_optional_string_list(data.get("partition_by"), "partition_by") or [],
            primary_key=_optional_string_list(data.get("primary_key"), "primary_key") or [],
            date_field=_optional_string(data.get("date_field"), "date_field"),
            required_datasets=_optional_string_list(data.get("required_datasets"), "required_datasets") or [],
            required_plugin=_optional_string(data.get("required_plugin"), "required_plugin"),
            dependency=dict(_mapping(data.get("dependency", {}), "dependency")),
            dependency_status=_optional_string(data.get("dependency_status"), "dependency_status"),
            dependency_message=_optional_string(data.get("dependency_message"), "dependency_message"),
            dependency_errors=_dependency_error_list(data.get("dependency_errors"), "dependency_errors"),
            category=_optional_string(data.get("category"), "category"),
            tags=_optional_string_list(data.get("tags"), "tags") or [],
            next_run_at=_optional_string(data.get("next_run_at"), "next_run_at"),
            backoff_until=_optional_string(data.get("backoff_until"), "backoff_until"),
            last_run_id=_optional_string(data.get("last_run_id"), "last_run_id"),
            last_status=_optional_string(data.get("last_status"), "last_status"),
            last_success_at=_optional_string(data.get("last_success_at"), "last_success_at"),
            last_failure_at=_optional_string(data.get("last_failure_at"), "last_failure_at"),
            last_error=_optional_string(data.get("last_error"), "last_error"),
            created_at=_string(data.get("created_at", ""), "created_at"),
            updated_at=_string(data.get("updated_at", ""), "updated_at"),
        )


@dataclass(frozen=True)
class CollectorRun:
    """Persisted record for one task run."""

    run_id: str
    task_id: str
    collector_name: str
    trigger_type: str
    status: str = TaskStatus.PENDING.value
    provider_id: str | None = None
    downloader_profile: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    fields: list[str] | None = None
    formats: list[str] | None = None
    resource_group: str = "default"
    output_root: str | None = None
    output_dir: str | None = None
    output_paths: dict[str, str] = field(default_factory=dict)
    run_signature: str = ""
    started_at: str | None = None
    finished_at: str | None = None
    duration_ms: int | None = None
    error: str | None = None
    skip_reason: str | None = None
    retry_count: int = 0
    next_run_at: str | None = None
    backoff_until: str | None = None
    params_override: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)
    quality: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    stage_timings: dict[str, int | None] = field(default_factory=dict)
    error_category: str | None = None
    error_summary: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        _validate_identifier(self.run_id, "run_id", allow_dot=True)
        _validate_identifier(self.task_id, "task_id", allow_dot=True)
        _normalize_status(self.status)
        object.__setattr__(self, "trigger_type", _normalize_trigger(self.trigger_type))
        if not self.resource_group:
            object.__setattr__(self, "resource_group", "default")

    def to_dict(self, *, include_guidance: bool = True) -> dict[str, Any]:
        summary = _run_output_summary(self)
        payload = {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "collector_name": self.collector_name,
            "trigger_type": self.trigger_type,
            "status": self.status,
            "provider_id": self.provider_id,
            "downloader_profile": self.downloader_profile,
            "params": _jsonable_mapping(self.params),
            "fields": list(self.fields) if self.fields is not None else None,
            "formats": list(self.formats) if self.formats is not None else None,
            "resource_group": self.resource_group,
            "output_root": self.output_root,
            "output_dir": self.output_dir,
            "output_paths": dict(sorted(self.output_paths.items())),
            "run_signature": self.run_signature,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "skip_reason": self.skip_reason,
            "retry_count": self.retry_count,
            "next_run_at": self.next_run_at,
            "backoff_until": self.backoff_until,
            "params_override": _jsonable_mapping(self.params_override),
            "result": _jsonable_mapping(self.result),
            "quality": _jsonable_mapping(self.quality),
            "events": [_jsonable_mapping(event) for event in self.events],
            "stage_timings": _jsonable_mapping(self.stage_timings),
            "error_category": self.error_category,
            "error_summary": self.error_summary,
            "metadata": _jsonable_mapping(self.metadata),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            **summary,
        }
        if include_guidance:
            payload.update(collector_run_guidance(self))
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CollectorRun":
        error = _optional_string(data.get("error"), "error")
        error_category = _optional_error_category(data.get("error_category"), "error_category")
        if error_category in {None, "unknown"} and _is_scheduler_interrupted_error(error):
            error_category = "scheduler_interrupted"
        return cls(
            run_id=_string(data.get("run_id"), "run_id"),
            task_id=_string(data.get("task_id"), "task_id"),
            collector_name=_string(data.get("collector_name"), "collector_name"),
            trigger_type=_string(data.get("trigger_type"), "trigger_type"),
            status=_string(data.get("status", TaskStatus.PENDING.value), "status"),
            provider_id=_optional_string(data.get("provider_id"), "provider_id"),
            downloader_profile=_optional_string(data.get("downloader_profile"), "downloader_profile"),
            params=dict(_mapping(data.get("params", {}), "params")),
            fields=_optional_string_list(data.get("fields"), "fields"),
            formats=_optional_string_list(data.get("formats"), "formats"),
            resource_group=_string(data.get("resource_group", "default"), "resource_group"),
            output_root=_optional_string(data.get("output_root"), "output_root"),
            output_dir=_optional_string(data.get("output_dir"), "output_dir"),
            output_paths=_string_mapping(data.get("output_paths", {}), "output_paths"),
            run_signature=_string(data.get("run_signature", ""), "run_signature"),
            started_at=_optional_string(data.get("started_at"), "started_at"),
            finished_at=_optional_string(data.get("finished_at"), "finished_at"),
            duration_ms=_optional_int(data.get("duration_ms"), "duration_ms"),
            error=error,
            skip_reason=_optional_string(data.get("skip_reason"), "skip_reason"),
            retry_count=int(data.get("retry_count", 0) or 0),
            next_run_at=_optional_string(data.get("next_run_at"), "next_run_at"),
            backoff_until=_optional_string(data.get("backoff_until"), "backoff_until"),
            params_override=dict(_mapping(data.get("params_override", {}), "params_override")),
            result=dict(_mapping(data.get("result", {}), "result")),
            quality=dict(_mapping(data.get("quality", {}), "quality")),
            events=_events_list(data.get("events", data.get("event_log", [])), "events"),
            stage_timings=_stage_timings_mapping(data.get("stage_timings", {}), "stage_timings"),
            error_category=error_category,
            error_summary=_optional_string(data.get("error_summary"), "error_summary"),
            metadata=dict(_mapping(data.get("metadata", {}), "metadata")),
            created_at=_string(data.get("created_at", ""), "created_at"),
            updated_at=_string(data.get("updated_at", ""), "updated_at"),
        )


class CollectorSchedulerStore:
    """JSON-backed Collector task/run state store."""

    def __init__(self, *, data_root: str | Path | None = None, path: str | Path | None = None) -> None:
        self.data_root = Path(data_root or os.getenv("AXDATA_DATA_DIR", "data")).expanduser().resolve()
        self.path = collector_scheduler_store_path(data_root=data_root, path=path)
        self._lock = RLock()

    def list_tasks(self) -> tuple[CollectorTask, ...]:
        with self._lock:
            state = self._load_state()
            return tuple(sorted(state.tasks.values(), key=lambda task: task.task_id))

    def get_task(self, task_id: str) -> CollectorTask | None:
        with self._lock:
            return self._load_state().tasks.get(task_id)

    def create_task(self, task: CollectorTask) -> CollectorTask:
        with self._lock:
            state = self._load_state()
            if task.task_id in state.tasks:
                raise CollectorSchedulerError(f"Collector task {task.task_id!r} already exists.")
            now = utc_now_iso()
            next_run_at = task.next_run_at or compute_next_run_at(task, now=parse_datetime(now), data_root=self.data_root)
            created = replace(task, created_at=task.created_at or now, updated_at=now, next_run_at=next_run_at)
            state.tasks[created.task_id] = created
            self._save_state(state)
            return created

    def save_task(self, task: CollectorTask) -> CollectorTask:
        with self._lock:
            state = self._load_state()
            if task.task_id not in state.tasks:
                raise CollectorSchedulerError(f"Collector task {task.task_id!r} does not exist.")
            updated = replace(task, updated_at=utc_now_iso())
            state.tasks[updated.task_id] = updated
            self._save_state(state)
            return updated

    def update_task(self, task_id: str, **updates: Any) -> CollectorTask:
        with self._lock:
            state = self._load_state()
            task = state.tasks.get(task_id)
            if task is None:
                raise CollectorSchedulerError(f"Collector task {task_id!r} does not exist.")
            patch = {key: value for key, value in updates.items() if value is not _MISSING}
            updated = replace(task, **patch, updated_at=utc_now_iso())
            if "next_run_at" not in patch and _schedule_patch_needs_next_run_refresh(patch):
                updated = replace(
                    updated,
                    next_run_at=compute_next_run_at(updated, data_root=self.data_root) if updated.enabled else None,
                )
            state.tasks[task_id] = updated
            self._save_state(state)
            return updated

    def delete_task(self, task_id: str) -> CollectorTask:
        with self._lock:
            state = self._load_state()
            task = state.tasks.pop(task_id, None)
            if task is None:
                raise CollectorSchedulerError(f"Collector task {task_id!r} does not exist.")
            self._save_state(state)
            return task

    def set_task_enabled(self, task_id: str, enabled: bool) -> CollectorTask:
        task = self.get_task(task_id)
        if task is None:
            raise CollectorSchedulerError(f"Collector task {task_id!r} does not exist.")
        enabled_task = replace(task, enabled=bool(enabled))
        next_run_at = compute_next_run_at(enabled_task, data_root=self.data_root) if enabled else None
        return self.update_task(task_id, enabled=bool(enabled), next_run_at=next_run_at)

    def list_runs(
        self,
        *,
        task_id: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> tuple[CollectorRun, ...]:
        if status is not None:
            status = _normalize_status(status)
        with self._lock:
            runs = list(self._load_state().runs.values())
        if task_id is not None:
            runs = [run for run in runs if run.task_id == task_id]
        if status is not None:
            runs = [run for run in runs if run.status == status]
        runs.sort(key=_run_sort_key, reverse=True)
        if limit is not None:
            runs = runs[: max(int(limit), 0)]
        return tuple(runs)

    def get_run(self, run_id: str) -> CollectorRun | None:
        with self._lock:
            return self._load_state().runs.get(run_id)

    def latest_run_by_task(self, task_id: str) -> CollectorRun | None:
        runs = self.list_runs(task_id=task_id, limit=1)
        return runs[0] if runs else None

    def active_runs(self) -> tuple[CollectorRun, ...]:
        with self._lock:
            runs = [run for run in self._load_state().runs.values() if run.status in ACTIVE_RUN_STATUSES]
        return tuple(sorted(runs, key=_run_sort_key))

    def run_summary(self, *, recent_limit: int = 100) -> dict[str, Any]:
        """Return bounded run status data after loading the JSON store once."""

        limit = max(int(recent_limit), 0)
        with self._lock:
            runs = list(self._load_state().runs.values())
        status_counts: dict[str, int] = {}
        latest_by_task: dict[str, CollectorRun] = {}
        active_runs: list[CollectorRun] = []
        for run in runs:
            status_counts[run.status] = status_counts.get(run.status, 0) + 1
            if run.status in ACTIVE_RUN_STATUSES:
                active_runs.append(run)
            existing = latest_by_task.get(run.task_id)
            if existing is None or _run_sort_key(run) >= _run_sort_key(existing):
                latest_by_task[run.task_id] = run

        recent_runs = sorted(runs, key=_run_sort_key, reverse=True)[:limit]
        return {
            "total_run_count": len(runs),
            "recent_run_count": len(recent_runs),
            "recent_run_limit": limit,
            "recent_runs": tuple(recent_runs),
            "active_runs": tuple(sorted(active_runs, key=_run_sort_key)),
            "latest_by_task": latest_by_task,
            "status_counts": dict(sorted(status_counts.items())),
        }

    def find_active_run(self, *, task_id: str | None = None, run_signature: str | None = None) -> CollectorRun | None:
        for run in self.active_runs():
            if task_id is not None and run.task_id == task_id:
                return run
            if run_signature is not None and run.run_signature == run_signature:
                return run
        return None

    def create_run(self, run: CollectorRun) -> CollectorRun:
        with self._lock:
            state = self._load_state()
            if run.run_id in state.runs:
                raise CollectorSchedulerError(f"Collector run {run.run_id!r} already exists.")
            now = utc_now_iso()
            created = replace(run, created_at=run.created_at or now, updated_at=now)
            state.runs[created.run_id] = created
            self._save_state(state)
            return created

    def update_run(self, run_id: str, **updates: Any) -> CollectorRun:
        with self._lock:
            state = self._load_state()
            run = state.runs.get(run_id)
            if run is None:
                raise CollectorSchedulerError(f"Collector run {run_id!r} does not exist.")
            patch = {key: value for key, value in updates.items() if value is not _MISSING}
            updated = replace(run, **patch, updated_at=utc_now_iso())
            state.runs[run_id] = updated
            task = state.tasks.get(updated.task_id)
            if task is not None and updated.status in FINAL_RUN_STATUSES:
                state.tasks[task.task_id] = _task_with_run_summary(task, updated, data_root=self.data_root)
            self._save_state(state)
            return updated

    def delete_runs(self, run_ids: Iterable[str]) -> tuple[CollectorRun, ...]:
        removed: list[CollectorRun] = []
        normalized_ids = [str(run_id) for run_id in run_ids if str(run_id)]
        if not normalized_ids:
            return ()
        with self._lock:
            state = self._load_state()
            for run_id in normalized_ids:
                run = state.runs.pop(run_id, None)
                if run is not None:
                    removed.append(run)
            if removed:
                self._save_state(state)
        return tuple(removed)

    def recover_interrupted_runs(self) -> tuple[CollectorRun, ...]:
        """Mark stale queued/running runs as failed after process restart."""

        recovered: list[CollectorRun] = []
        with self._lock:
            state = self._load_state()
            now = utc_now_iso()
            for run_id, run in list(state.runs.items()):
                if run.status not in ACTIVE_RUN_STATUSES:
                    continue
                updated = replace(
                    run,
                    status=TaskStatus.FAILED.value,
                    finished_at=now,
                    duration_ms=_duration_ms(run.started_at, now),
                    error="Collector scheduler process stopped before this run finished.",
                    error_category="scheduler_interrupted",
                    error_summary="Collector scheduler process stopped before this run finished.",
                    events=_append_run_event(
                        run,
                        stage="failed",
                        level="error",
                        message="Collector scheduler process stopped before this run finished.",
                        category="scheduler_interrupted",
                        timestamp=now,
                    ),
                    stage_timings=_merge_stage_timings(
                        run.stage_timings,
                        {
                            "total_ms": _duration_ms(run.started_at or run.created_at, now),
                        },
                    ),
                    updated_at=now,
                )
                state.runs[run_id] = updated
                task = state.tasks.get(updated.task_id)
                if task is not None:
                    state.tasks[task.task_id] = _task_with_run_summary(task, updated, data_root=self.data_root)
                recovered.append(updated)
            if recovered:
                self._save_state(state)
        return tuple(recovered)

    def _load_state(self) -> "_SchedulerState":
        if not self.path.exists():
            return _SchedulerState()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise CollectorSchedulerError(f"Invalid collector scheduler JSON at {self.path}: {exc}") from exc
        if not isinstance(payload, Mapping):
            raise CollectorSchedulerError(f"Collector scheduler store at {self.path} must contain a JSON object.")
        version = payload.get("version", SCHEDULER_STORE_VERSION)
        if version != SCHEDULER_STORE_VERSION:
            raise CollectorSchedulerError(
                f"Unsupported collector scheduler store version {version!r}; "
                f"expected {SCHEDULER_STORE_VERSION!r}."
            )
        tasks_payload = payload.get("tasks", {})
        runs_payload = payload.get("runs", {})
        if not isinstance(tasks_payload, Mapping) or not isinstance(runs_payload, Mapping):
            raise CollectorSchedulerError("Collector scheduler store tasks and runs must be JSON objects.")
        tasks = {
            str(task_id): CollectorTask.from_dict(_mapping(task_payload, f"tasks.{task_id}"))
            for task_id, task_payload in tasks_payload.items()
        }
        runs = {
            str(run_id): CollectorRun.from_dict(_mapping(run_payload, f"runs.{run_id}"))
            for run_id, run_payload in runs_payload.items()
        }
        return _SchedulerState(tasks=tasks, runs=runs)

    def _save_state(self, state: "_SchedulerState") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": SCHEDULER_STORE_VERSION,
            "tasks": {
                task_id: task.to_dict(include_guidance=False)
                for task_id, task in sorted(state.tasks.items())
            },
            "runs": {
                run_id: run.to_dict(include_guidance=False)
                for run_id, run in sorted(state.runs.items())
            },
        }
        temp_path = self.path.with_name(f".{self.path.name}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temp_path.replace(self.path)


@dataclass
class _SchedulerState:
    tasks: dict[str, CollectorTask] = field(default_factory=dict)
    runs: dict[str, CollectorRun] = field(default_factory=dict)


class _Missing:
    pass


_MISSING = _Missing()


def _schedule_patch_needs_next_run_refresh(patch: Mapping[str, Any]) -> bool:
    return any(key in patch for key in ("enabled", "trigger_type", "interval_seconds", "daily_time"))


@dataclass(frozen=True)
class _ResourceLease:
    manager: "_ResourceGroupLimiter"
    resource_group: str
    run_id: str

    def release(self) -> None:
        self.manager.release(self)


class _ResourceGroupLimiter:
    """Single-process resource-group limiter for Collector runs."""

    def __init__(self, limits: Mapping[str, int] | None = None) -> None:
        self._condition = Condition(Lock())
        self._limits = {str(key): max(int(value), 1) for key, value in dict(limits or {}).items()}
        self._active: dict[str, int] = {}
        self._waiters: dict[str, tuple[str, int]] = {}
        self._sequence = 0

    def acquire(
        self,
        *,
        run_id: str,
        resource_group: str,
        on_wait: Any | None = None,
    ) -> _ResourceLease:
        group = resource_group or "default"
        with self._condition:
            self._sequence += 1
            sequence = self._sequence
            while self._active.get(group, 0) >= self._limit_for(group):
                self._waiters[run_id] = (group, sequence)
                wait_snapshot = self._snapshot_locked(group, run_id=run_id)
                if on_wait is not None:
                    self._condition.release()
                    try:
                        on_wait(wait_snapshot)
                    finally:
                        self._condition.acquire()
                self._condition.wait(timeout=0.2)
            self._waiters.pop(run_id, None)
            self._active[group] = self._active.get(group, 0) + 1
            return _ResourceLease(manager=self, resource_group=group, run_id=run_id)

    def release(self, lease: _ResourceLease) -> None:
        with self._condition:
            current = self._active.get(lease.resource_group, 0)
            remaining = max(current - 1, 0)
            if remaining:
                self._active[lease.resource_group] = remaining
            else:
                self._active.pop(lease.resource_group, None)
            self._condition.notify_all()

    def reset(self) -> None:
        with self._condition:
            self._active.clear()
            self._waiters.clear()
            self._sequence = 0
            self._condition.notify_all()

    def _limit_for(self, resource_group: str) -> int:
        return max(int(self._limits.get(resource_group, self._limits.get("*", 1))), 1)

    def _snapshot_locked(self, resource_group: str, *, run_id: str | None = None) -> dict[str, int]:
        queue_position = 0
        if run_id is not None and run_id in self._waiters:
            group, sequence = self._waiters[run_id]
            queue_position = 1 + sum(
                1
                for waiter_group, waiter_sequence in self._waiters.values()
                if waiter_group == group and waiter_sequence < sequence
            )
        return {
            "resource_active": self._active.get(resource_group, 0),
            "resource_limit": self._limit_for(resource_group),
            "waiting_count": sum(1 for group, _ in self._waiters.values() if group == resource_group),
            "queue_position": queue_position,
        }


class CollectorSchedulerService:
    """Single-process Collector scheduler and queue.

    The service persists every run through :class:`CollectorSchedulerStore`.
    It is intentionally local and in-process: resource limits and duplicate
    suppression do not claim distributed consistency.
    """

    def __init__(
        self,
        *,
        data_root: str | Path | None = None,
        store: CollectorSchedulerStore | None = None,
        max_workers: int = 4,
        resource_group_limits: Mapping[str, int] | None = None,
        failure_backoff_seconds: int = 300,
        tick_seconds: float = 30.0,
    ) -> None:
        self.data_root = Path(data_root or os.getenv("AXDATA_DATA_DIR", "data")).expanduser().resolve()
        self.store = store or CollectorSchedulerStore(data_root=self.data_root)
        self.failure_backoff_seconds = max(int(failure_backoff_seconds), 0)
        self.tick_seconds = max(float(tick_seconds), 0.1)
        self._executor = ThreadPoolExecutor(max_workers=max(int(max_workers), 1), thread_name_prefix="axdata-collector")
        self._resources = _ResourceGroupLimiter(resource_group_limits)
        self._futures: dict[str, Future[Any]] = {}
        self._futures_lock = Lock()
        self._loop_started = False
        self._stop_loop = False
        self.seed_default_tasks()

    def seed_default_tasks(self) -> tuple[CollectorTask, ...]:
        return seed_default_collector_tasks(self.store, data_root=self.data_root)

    def list_tasks(self) -> tuple[CollectorTask, ...]:
        refreshed: list[CollectorTask] = []
        for task in self.store.list_tasks():
            refreshed.append(self._refresh_task_dependency(task))
        return tuple(sorted(refreshed, key=lambda task: task.task_id))

    def refresh_task(self, task: CollectorTask) -> CollectorTask:
        return self._refresh_task_dependency(task)

    def create_task(self, collector_name: str, **kwargs: Any) -> CollectorTask:
        task = create_collector_task(collector_name, data_root=self.data_root, **kwargs)
        return self.store.create_task(task)

    def delete_task(self, task_id: str) -> CollectorTask:
        return self.store.delete_task(task_id)

    def submit_task(
        self,
        task_id: str,
        *,
        trigger_type: str = TriggerType.MANUAL.value,
        skip_active_duplicates: bool = True,
        enforce_failure_backoff: bool = True,
        allow_disabled_manual_run: bool = False,
        params_override: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> CollectorRun:
        task = self.store.get_task(task_id)
        if task is None:
            raise CollectorSchedulerError(f"Collector task {task_id!r} does not exist.")
        task = self._refresh_task_dependency(task)
        return self.submit_task_object(
            task,
            trigger_type=trigger_type,
            skip_active_duplicates=skip_active_duplicates,
            enforce_failure_backoff=enforce_failure_backoff,
            allow_disabled_manual_run=allow_disabled_manual_run,
            params_override=params_override,
            metadata=metadata,
        )

    def submit_task_object(
        self,
        task: CollectorTask,
        *,
        trigger_type: str = TriggerType.MANUAL.value,
        skip_active_duplicates: bool = True,
        enforce_failure_backoff: bool = True,
        allow_disabled_manual_run: bool = False,
        params_override: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> CollectorRun:
        task = self._refresh_task_dependency(task)
        trigger = _normalize_trigger(trigger_type)
        run = create_run_from_task(
            task,
            trigger_type=trigger,
            params_override=params_override,
            metadata=metadata,
        )
        created = self.store.create_run(run)
        should_allow_disabled_manual_run = allow_disabled_manual_run and trigger == TriggerType.MANUAL.value
        if not task.enabled and not should_allow_disabled_manual_run:
            now = utc_now_iso()
            return self.store.update_run(
                created.run_id,
                status=TaskStatus.SKIPPED.value,
                finished_at=now,
                duration_ms=0,
                skip_reason="task_disabled",
                error_category="invalid_params",
                error_summary="Task is disabled.",
                events=_append_run_event(
                    created,
                    stage="skipped",
                    level="warning",
                    message="Task is disabled.",
                    category="invalid_params",
                    timestamp=now,
                    details={"skip_reason": "task_disabled"},
                ),
                stage_timings=_merge_stage_timings(created.stage_timings, {"queue_wait_ms": 0, "total_ms": 0}),
                metadata={"message": "Task is disabled."},
            )

        missing_dependency = _missing_task_dependency(task)
        run_dataset_errors = _run_dataset_dependency_errors(task, created.params, data_root=self.data_root)
        if str(task.dependency_status or "").strip().lower() == "blocked" and task.required_datasets:
            if run_dataset_errors:
                missing_dependency = str(run_dataset_errors[0].get("message") or missing_dependency)
            else:
                missing_dependency = None
        if missing_dependency is not None:
            now = utc_now_iso()
            dependency_category = _task_dependency_error_category(task)
            dependency_error_payload = {
                "required_plugin": task.required_plugin,
                "dependency_status": task.dependency_status,
                "required_datasets": list(task.required_datasets),
                "dependency_errors": run_dataset_errors or task.dependency_errors,
            }
            return self.store.update_run(
                created.run_id,
                status=TaskStatus.FAILED.value,
                finished_at=now,
                duration_ms=0,
                error=missing_dependency,
                error_category=dependency_category,
                error_summary=missing_dependency,
                events=_append_run_event(
                    created,
                    stage="failed",
                    level="error",
                    message=missing_dependency,
                    category=dependency_category,
                    timestamp=now,
                    details=dependency_error_payload,
                ),
                stage_timings=_merge_stage_timings(created.stage_timings, {"queue_wait_ms": 0, "total_ms": 0}),
                metadata={
                    "message": missing_dependency,
                    **dependency_error_payload,
                },
            )

        if run_dataset_errors:
            message = str(run_dataset_errors[0].get("message") or "基础数据依赖未满足。")
            now = utc_now_iso()
            return self.store.update_run(
                created.run_id,
                status=TaskStatus.FAILED.value,
                finished_at=now,
                duration_ms=0,
                error=message,
                error_category="dependency_missing",
                error_summary=message,
                events=_append_run_event(
                    created,
                    stage="failed",
                    level="error",
                    message=message,
                    category="dependency_missing",
                    timestamp=now,
                    details={
                        "required_datasets": list(task.required_datasets),
                        "dependency_errors": run_dataset_errors,
                    },
                ),
                stage_timings=_merge_stage_timings(created.stage_timings, {"queue_wait_ms": 0, "total_ms": 0}),
                metadata={
                    "message": message,
                    "required_datasets": list(task.required_datasets),
                    "dependency_errors": run_dataset_errors,
                },
            )

        now = datetime.now(timezone.utc)
        if enforce_failure_backoff:
            backoff_until = parse_datetime(task.backoff_until)
            if backoff_until is not None and backoff_until > now:
                finished_at = utc_now_iso()
                return self.store.update_run(
                    created.run_id,
                    status=TaskStatus.SKIPPED.value,
                    finished_at=finished_at,
                    duration_ms=0,
                    skip_reason="failure_backoff",
                    backoff_until=backoff_until.isoformat(),
                    error_category="backoff_blocked",
                    error_summary="Task is in failure backoff.",
                    events=_append_run_event(
                        created,
                        stage="skipped",
                        level="warning",
                        message="Task is in failure backoff.",
                        category="backoff_blocked",
                        timestamp=finished_at,
                        details={"backoff_until": backoff_until.isoformat()},
                    ),
                    stage_timings=_merge_stage_timings(created.stage_timings, {"queue_wait_ms": 0, "total_ms": 0}),
                    metadata={"message": "Task is in failure backoff."},
                )

        if skip_active_duplicates:
            duplicate = self._find_active_duplicate(created)
            if duplicate is not None and duplicate.run_id != created.run_id:
                finished_at = utc_now_iso()
                return self.store.update_run(
                    created.run_id,
                    status=TaskStatus.SKIPPED.value,
                    finished_at=finished_at,
                    duration_ms=0,
                    skip_reason="active_duplicate",
                    error_category="duplicate_skipped",
                    error_summary=f"Active duplicate run {duplicate.run_id} is already queued or running.",
                    events=_append_run_event(
                        created,
                        stage="skipped",
                        level="warning",
                        message="Active duplicate run is already queued or running.",
                        category="duplicate_skipped",
                        timestamp=finished_at,
                        details={"duplicate_run_id": duplicate.run_id},
                    ),
                    stage_timings=_merge_stage_timings(created.stage_timings, {"queue_wait_ms": 0, "total_ms": 0}),
                    metadata={"duplicate_run_id": duplicate.run_id},
                )

        future = self._executor.submit(self._execute_run, created.run_id)
        with self._futures_lock:
            self._futures[created.run_id] = future
        return created

    def backfill_task(
        self,
        task_id: str,
        *,
        start: str,
        end: str,
        params_override: Mapping[str, Any] | None = None,
        trigger_type: str = TriggerType.MANUAL.value,
        skip_active_duplicates: bool = True,
        enforce_failure_backoff: bool = True,
    ) -> CollectorRun:
        start_date, end_date = normalize_date_range(start, end)
        merged_override = {"start_date": start_date, "end_date": end_date}
        merged_override.update(dict(params_override or {}))
        return self.submit_task(
            task_id,
            trigger_type=trigger_type,
            skip_active_duplicates=skip_active_duplicates,
            enforce_failure_backoff=enforce_failure_backoff,
            # 与手动运行同一口径:手动补采允许禁用任务,enabled 只管自动调度
            allow_disabled_manual_run=trigger_type == TriggerType.MANUAL.value,
            params_override=merged_override,
            metadata={
                "run_mode": "backfill",
                "backfill_range": {"start": start_date, "end": end_date},
            },
        )

    def tick_due_tasks(self, *, now: datetime | None = None) -> tuple[CollectorRun, ...]:
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        submitted: list[CollectorRun] = []
        for task in self.store.list_tasks():
            task = self._refresh_task_dependency(task)
            if not task.enabled or task.trigger_type not in {TriggerType.INTERVAL.value, TriggerType.DAILY.value}:
                continue
            if _missing_task_dependency(task) is not None:
                continue
            due_at = parse_datetime(task.next_run_at)
            if due_at is None:
                self.store.update_task(
                    task.task_id,
                    next_run_at=compute_next_run_at(task, now=current, data_root=self.data_root),
                )
                continue
            if due_at > current:
                continue
            if task.trigger_type == TriggerType.DAILY.value and not _is_due_daily_trade_day(
                due_at,
                data_root=self.data_root,
            ):
                self.store.update_task(
                    task.task_id,
                    next_run_at=compute_next_run_at(task, now=current, data_root=self.data_root),
                )
                continue
            updated_task = self.store.update_task(
                task.task_id,
                next_run_at=compute_next_run_at(task, now=current, data_root=self.data_root),
            )
            submitted.append(
                self.submit_task_object(
                    updated_task,
                    trigger_type=updated_task.trigger_type,
                    skip_active_duplicates=True,
                    enforce_failure_backoff=True,
                )
            )
        return tuple(submitted)

    def start_loop(self) -> None:
        if self._loop_started:
            return
        self._loop_started = True
        self._stop_loop = False
        self.store.recover_interrupted_runs()
        Thread(target=self._run_loop, name="axdata-collector-scheduler", daemon=True).start()

    def stop_loop(self) -> None:
        self._stop_loop = True

    def shutdown(self, *, wait: bool = True) -> None:
        self.stop_loop()
        self._executor.shutdown(wait=wait)

    def wait_for_run(self, run_id: str, *, timeout: float | None = None) -> CollectorRun | None:
        with self._futures_lock:
            future = self._futures.get(run_id)
        if future is not None:
            future.result(timeout=timeout)
        return self.store.get_run(run_id)

    def cancel_run(self, run_id: str) -> CollectorRun:
        run = self.store.get_run(run_id)
        if run is None:
            raise CollectorSchedulerError(f"Collector run {run_id!r} does not exist.")
        with self._futures_lock:
            future = self._futures.get(run_id)
        if future is not None and future.cancel():
            pass
        return self.store.update_run(
            run_id,
            status=TaskStatus.CANCELLED.value,
            finished_at=utc_now_iso(),
            duration_ms=_duration_ms(run.started_at, utc_now_iso()),
            metadata={**run.metadata, "cancel_note": "MVP cancellation marks state only; running work may finish."},
        )

    def _refresh_task_dependency(self, task: CollectorTask) -> CollectorTask:
        refreshed = refresh_task_dependency_status(task, data_root=self.data_root)
        refreshed = refresh_task_schedule_status(refreshed, data_root=self.data_root)
        if refreshed != task and self.store.get_task(task.task_id) is not None:
            refreshed = self.store.save_task(refreshed)
        return refreshed

    def _find_active_duplicate(self, run: CollectorRun) -> CollectorRun | None:
        for active_run in self.store.active_runs():
            if active_run.run_id == run.run_id:
                continue
            if active_run.task_id == run.task_id or active_run.run_signature == run.run_signature:
                return active_run
        return None

    def _run_loop(self) -> None:
        while not self._stop_loop:
            try:
                self.tick_due_tasks()
            except Exception:
                pass
            sleep(self.tick_seconds)

    def _execute_run(self, run_id: str) -> None:
        run = self.store.get_run(run_id)
        if run is None:
            return
        task = self.store.get_task(run.task_id)
        if task is None:
            finished_at = utc_now_iso()
            category = "provider_missing"
            summary = f"Collector task {run.task_id!r} no longer exists."
            self.store.update_run(
                run_id,
                status=TaskStatus.FAILED.value,
                finished_at=finished_at,
                duration_ms=_duration_ms(run.created_at, finished_at),
                error=summary,
                error_category=category,
                error_summary=summary,
                events=_append_run_event(
                    run,
                    stage="failed",
                    level="error",
                    message=summary,
                    category=category,
                    timestamp=finished_at,
                ),
                stage_timings=_merge_stage_timings(
                    run.stage_timings,
                    {"queue_wait_ms": _duration_ms(run.created_at, finished_at), "total_ms": _duration_ms(run.created_at, finished_at)},
                ),
            )
            return

        result: Mapping[str, Any]
        started_at: str | None = None
        max_attempts = _collector_run_max_attempts(task)
        retry_delay = _collector_retry_delay_seconds(task)
        attempt = 0
        run_finished = False
        while True:
            attempt += 1
            lease: _ResourceLease | None = None
            try:
                lease = self._resources.acquire(
                    run_id=run_id,
                    resource_group=task.resource_group,
                    on_wait=lambda usage: self._mark_waiting(run_id, usage),
                )
                attempt_started_at = utc_now_iso()
                if started_at is None:
                    started_at = attempt_started_at
                current_run = self.store.get_run(run_id)
                metadata = dict(current_run.metadata) if current_run is not None else {}
                metadata["resource_group"] = lease.resource_group
                metadata["max_retries"] = max_attempts - 1
                metadata["backoff_seconds"] = retry_delay
                metadata["attempt"] = attempt
                current_for_update = current_run or run
                events = _append_run_event(
                    current_for_update,
                    stage="started",
                    level="info",
                    message="Collector run started.",
                    timestamp=attempt_started_at,
                    details={
                        "resource_group": lease.resource_group,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                    },
                )
                events = _append_event_list(
                    run_id,
                    events,
                    stage="params_resolved",
                    level="info",
                    message="Collector run params resolved.",
                    timestamp=attempt_started_at,
                    details={"params_override": bool(current_for_update.params_override), "attempt": attempt},
                )
                events = _append_event_list(
                    run_id,
                    events,
                    stage="request_planned",
                    level="info",
                    message="Collector run request planned.",
                    timestamp=attempt_started_at,
                    details={
                        "collector_name": task.collector_name,
                        "downloader_profile": task.downloader_profile,
                        "attempt": attempt,
                    },
                )
                self.store.update_run(
                    run_id,
                    status=TaskStatus.RUNNING.value,
                    started_at=started_at,
                    retry_count=attempt - 1,
                    events=events,
                    stage_timings=_merge_stage_timings(
                        current_for_update.stage_timings,
                        {"queue_wait_ms": _duration_ms(current_for_update.created_at, started_at)},
                    ),
                    metadata=metadata,
                )
                from .collector_runner import run_collector

                result = run_collector(
                    task.collector_name,
                    data_root=self.data_root,
                    progress_callback=lambda percent, message, **details: self._record_progress(
                        run_id,
                        percent,
                        message,
                        details,
                    ),
                    **task_run_kwargs(task, run=run),
                )
                run_finished = True
                break
            except Exception as exc:  # background jobs must remain queryable
                current_run = self.store.get_run(run_id)
                if current_run is not None and current_run.status == TaskStatus.CANCELLED.value:
                    run_finished = True
                    return
                if attempt < max_attempts and _should_apply_failure_backoff(exc):
                    retry_at = (
                        datetime.now(timezone.utc) + timedelta(seconds=retry_delay)
                    ).isoformat()
                    current_for_update = current_run or run
                    category = _classify_run_error(exc)
                    summary = _error_summary(exc)
                    metadata = dict(current_for_update.metadata)
                    metadata["error_type"] = type(exc).__name__
                    metadata["last_attempt_error"] = summary
                    metadata["attempt"] = attempt
                    metadata["next_retry_at"] = retry_at
                    self.store.update_run(
                        run_id,
                        status=TaskStatus.QUEUED.value,
                        retry_count=attempt,
                        error=str(exc),
                        error_category=category,
                        error_summary=summary,
                        events=_append_run_event(
                            current_for_update,
                            stage="queued",
                            level="warning",
                            message=f"Collector run attempt {attempt} failed; retrying.",
                            category=category,
                            details={
                                "attempt": attempt,
                                "max_attempts": max_attempts,
                                "retry_after_seconds": retry_delay,
                                "next_retry_at": retry_at,
                                "error_type": type(exc).__name__,
                            },
                        ),
                        metadata=metadata,
                    )
                    if lease is not None:
                        lease.release()
                        lease = None
                    if retry_delay > 0:
                        sleep(retry_delay)
                    continue

                finished_at = utc_now_iso()
                backoff_until = None
                if _should_apply_failure_backoff(exc) and self.failure_backoff_seconds > 0:
                    backoff_until = (
                        datetime.now(timezone.utc) + timedelta(seconds=self.failure_backoff_seconds)
                    ).isoformat()
                current_for_update = current_run or run
                category = _classify_run_error(exc)
                summary = _error_summary(exc)
                metadata = dict(current_for_update.metadata)
                metadata["error_type"] = type(exc).__name__
                metadata["attempt"] = attempt
                metadata["max_retries"] = max_attempts - 1
                duration_start = started_at or current_for_update.started_at or current_for_update.created_at
                run_finished = True
                self.store.update_run(
                    run_id,
                    status=TaskStatus.FAILED.value,
                    finished_at=finished_at,
                    duration_ms=_duration_ms(duration_start, finished_at),
                    error=str(exc),
                    retry_count=attempt - 1,
                    error_category=category,
                    error_summary=summary,
                    backoff_until=backoff_until,
                    events=_append_run_event(
                        current_for_update,
                        stage="failed",
                        level="error",
                        message=summary,
                        category=category,
                        timestamp=finished_at,
                        details={"error_type": type(exc).__name__, "attempt": attempt, "max_attempts": max_attempts},
                    ),
                    stage_timings=_merge_stage_timings(
                        current_for_update.stage_timings,
                        {"total_ms": _duration_ms(current_for_update.created_at, finished_at)},
                    ),
                    metadata=metadata,
                )
                return
            finally:
                if lease is not None:
                    lease.release()
                if run_finished:
                    with self._futures_lock:
                        self._futures.pop(run_id, None)

        finished_at = utc_now_iso()
        current_run = self.store.get_run(run_id)
        if current_run is not None and current_run.status == TaskStatus.CANCELLED.value:
            return
        download_result = dict(result.get("download_result") or {}) if isinstance(result, Mapping) else {}
        output_paths = _extract_output_paths(result)
        quality = dict(download_result.get("quality") or {})
        metadata = dict(current_run.metadata) if current_run is not None else {}
        metadata["message"] = "Collector run completed."
        current_for_update = current_run or run
        events = _success_events_from_result(current_for_update, result, output_paths, quality, timestamp=finished_at)
        quality_category, quality_summary = _quality_error_details(quality)
        self.store.update_run(
            run_id,
            status=TaskStatus.SUCCESS.value,
            finished_at=finished_at,
            duration_ms=_duration_ms(current_for_update.started_at or started_at, finished_at),
            error=None,
            skip_reason=None,
            output_paths=output_paths,
            result=dict(result),
            quality=quality,
            events=events,
            stage_timings=_stage_timings_from_result(current_for_update, result, finished_at=finished_at),
            error_category=quality_category,
            error_summary=quality_summary,
            backoff_until=None,
            metadata=metadata,
        )

    def _mark_waiting(self, run_id: str, usage: Mapping[str, int]) -> None:
        run = self.store.get_run(run_id)
        if run is None or run.status not in ACTIVE_RUN_STATUSES:
            return
        metadata = dict(run.metadata)
        metadata["resource_wait"] = dict(usage)
        self.store.update_run(
            run_id,
            status=TaskStatus.QUEUED.value,
            error_category="resource_waiting",
            error_summary=f"Waiting for resource_group={run.resource_group}.",
            events=_append_run_event(
                run,
                stage="queued",
                level="info",
                message=f"Waiting for resource_group={run.resource_group}.",
                category="resource_waiting",
                details=usage,
            ),
            metadata=metadata,
        )

    def _record_progress(
        self,
        run_id: str,
        percent: int,
        message: str,
        details: Mapping[str, Any],
    ) -> None:
        run = self.store.get_run(run_id)
        if run is None or run.status != TaskStatus.RUNNING.value:
            return
        metadata = dict(run.metadata)
        metadata["progress"] = {
            "percent": int(percent),
            "message": str(message),
            **dict(details),
        }
        stage = _progress_stage(percent, message)
        self.store.update_run(
            run_id,
            events=_append_run_event(
                run,
                stage=stage,
                level="info",
                message=str(message),
                details={"percent": int(percent), **dict(details)},
            ),
            metadata=metadata,
        )


def collector_scheduler_store_path(
    *,
    data_root: str | Path | None = None,
    path: str | Path | None = None,
) -> Path:
    """Return the local collector scheduler state path."""

    if path is not None:
        return Path(path).expanduser().resolve()
    env_path = os.getenv("AXDATA_COLLECTOR_SCHEDULER_PATH", "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()
    root = Path(data_root or os.getenv("AXDATA_DATA_DIR", "data")).expanduser().resolve()
    return root.parent / "metadata" / "collector" / COLLECTOR_SCHEDULER_FILE_NAME


def create_collector_task(
    collector_name: str,
    *,
    task_id: str | None = None,
    name: str | None = None,
    enabled: bool = True,
    trigger_type: str = TriggerType.MANUAL.value,
    interval_seconds: int | None = None,
    daily_time: str | None = None,
    params: Mapping[str, Any] | None = None,
    fields: list[str] | tuple[str, ...] | str | None = None,
    formats: list[str] | tuple[str, ...] | str | None = None,
    data_root: str | Path | None = None,
    output_root: str | Path | None = None,
    output_dir: str | Path | None = None,
    **options: Any,
) -> CollectorTask:
    """Resolve a collector capability and build a persisted task definition."""

    from .collector_runner import build_collector_run_plan

    plan = build_collector_run_plan(
        collector_name,
        params=dict(params or {}),
        fields=_optional_string_list(fields, "fields"),
        data_root=data_root,
        formats=_optional_string_list(formats, "formats"),
    )
    now = utc_now_iso()
    task = CollectorTask(
        task_id=task_id or _new_task_id(collector_name),
        collector_name=collector_name,
        name=name or plan.display_name_zh or collector_name,
        template_id=_optional_string(options.get("template_id"), "template_id"),
        created_by=_string(options.get("created_by", "user"), "created_by"),
        enabled=enabled,
        trigger_type=trigger_type,
        interval_seconds=interval_seconds,
        daily_time=_normalize_daily_time(daily_time) if daily_time is not None else None,
        interface_name=_optional_string(options.get("interface_name") or getattr(plan, "target_interface", None), "interface_name"),
        provider_id=plan.provider_id,
        downloader_profile=plan.downloader_profile,
        params=plan.params,
        fields=plan.fields,
        formats=plan.formats,
        resource_group=plan.resource_group,
        output_root=str(Path(output_root).expanduser().resolve()) if output_root is not None else None,
        output_dir=str(Path(output_dir).expanduser().resolve()) if output_dir is not None else None,
        collect_mode=_optional_string(options.get("collect_mode"), "collect_mode"),
        connection_mode=_optional_string(options.get("connection_mode"), "connection_mode"),
        concurrency_mode=_optional_string(options.get("concurrency_mode"), "concurrency_mode"),
        connection_count=_optional_int(options.get("connection_count"), "connection_count"),
        source_server_count=_optional_int(options.get("source_server_count"), "source_server_count"),
        connections_per_server=_optional_int(options.get("connections_per_server"), "connections_per_server"),
        max_concurrent_tasks=_optional_int(options.get("max_concurrent_tasks"), "max_concurrent_tasks"),
        batch_size=_optional_int(options.get("batch_size"), "batch_size"),
        request_interval_ms=_optional_int(options.get("request_interval_ms"), "request_interval_ms"),
        retry_count=_optional_int(options.get("retry_count"), "retry_count"),
        max_retries=_optional_non_negative_option(options.get("max_retries"), "max_retries"),
        backoff_seconds=_optional_non_negative_option(options.get("backoff_seconds"), "backoff_seconds"),
        timeout_ms=_optional_int(options.get("timeout_ms"), "timeout_ms"),
        expected_layer=_optional_string(
            options.get("expected_layer") or dict(getattr(plan, "output", {}) or {}).get("layer"),
            "expected_layer",
        ),
        schedule_hint=_optional_string(options.get("schedule_hint"), "schedule_hint"),
        write_mode=_optional_string(
            options.get("write_mode") or dict(getattr(plan, "output", {}) or {}).get("write_mode") or "snapshot",
            "write_mode",
        ),
        partition_by=_optional_string_list(
            options.get("partition_by") or dict(getattr(plan, "output", {}) or {}).get("partition_by"),
            "partition_by",
        )
        or [],
        primary_key=_optional_string_list(
            options.get("primary_key") or dict(getattr(plan, "output", {}) or {}).get("primary_key"),
            "primary_key",
        )
        or [],
        date_field=_optional_string(
            options.get("date_field") or dict(getattr(plan, "output", {}) or {}).get("date_field"),
            "date_field",
        ),
        required_datasets=_optional_string_list(
            options.get("required_datasets") or getattr(plan, "required_datasets", ()),
            "required_datasets",
        )
        or [],
        required_plugin=_optional_string(options.get("required_plugin"), "required_plugin"),
        dependency=dict(_mapping(options.get("dependency", {}), "dependency")),
        dependency_status=_optional_string(options.get("dependency_status"), "dependency_status"),
        dependency_message=_optional_string(options.get("dependency_message"), "dependency_message"),
        dependency_errors=_dependency_error_list(options.get("dependency_errors"), "dependency_errors"),
        category=_optional_string(options.get("category"), "category"),
        tags=_optional_string_list(options.get("tags"), "tags") or [],
        created_at=now,
        updated_at=now,
    )
    task = replace(task, next_run_at=compute_next_run_at(task, now=parse_datetime(now), data_root=data_root))
    return refresh_task_dependency_status(task, data_root=data_root)


def seed_default_collector_tasks(
    store: CollectorSchedulerStore,
    *,
    data_root: str | Path | None = None,
) -> tuple[CollectorTask, ...]:
    """Seed visible product-default Collector tasks without overwriting users."""

    from .collector_templates import default_task_templates, default_task_to_create_kwargs

    seeded: list[CollectorTask] = []
    for template in default_task_templates(data_root=data_root):
        kwargs = default_task_to_create_kwargs(template)
        task_id = str(kwargs["task_id"])
        existing = store.get_task(task_id)
        if existing is None:
            task = _create_collector_task_from_seed_kwargs(kwargs, data_root=data_root)
            task = refresh_task_dependency_status(task, data_root=data_root)
            seeded.append(store.create_task(task))
            continue
        if existing.created_by != "system" or existing.template_id != template.template_id:
            continue
        refreshed = _merge_default_template_metadata(existing, kwargs, data_root=data_root)
        if refreshed != existing:
            seeded.append(store.save_task(refreshed))
        else:
            seeded.append(existing)
    return tuple(seeded)


def refresh_task_dependency_status(
    task: CollectorTask,
    *,
    data_root: str | Path | None = None,
) -> CollectorTask:
    """Refresh lightweight dependency visibility for a persisted task."""

    if not task.required_plugin and not task.dependency and not task.required_datasets:
        return replace(
            task,
            dependency_status=task.dependency_status or "ok",
            dependency_message=None,
            dependency_errors=[],
        )

    status, message = _resolve_task_dependency_status(task, data_root=data_root)
    dataset_errors: list[dict[str, Any]] = []
    if status in {"ok", "available"}:
        dataset_errors = _task_dataset_dependency_errors(task, data_root=data_root)
        if dataset_errors:
            status = "blocked"
            message = str(dataset_errors[0].get("message") or "基础数据依赖未满足。")
    return replace(
        task,
        dependency_status=status,
        dependency_message=message,
        dependency_errors=dataset_errors,
    )


def refresh_task_schedule_status(
    task: CollectorTask,
    *,
    data_root: str | Path | None = None,
) -> CollectorTask:
    if not task.enabled or task.trigger_type != TriggerType.DAILY.value:
        return task
    due_at = parse_datetime(task.next_run_at)
    if due_at is not None and _is_due_daily_trade_day(due_at, data_root=data_root):
        return task
    next_run_at = compute_next_run_at(task, data_root=data_root)
    if next_run_at == task.next_run_at:
        return task
    return replace(task, next_run_at=next_run_at)


def create_run_from_task(
    task: CollectorTask,
    *,
    trigger_type: str = TriggerType.MANUAL.value,
    status: str = TaskStatus.QUEUED.value,
    run_id: str | None = None,
    run_signature: str | None = None,
    params_override: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> CollectorRun:
    """Build a run record from a persisted task definition."""

    now = utc_now_iso()
    override = dict(params_override or {})
    params = dict(task.params)
    params.update(override)
    base_run = CollectorRun(
        run_id=run_id or _new_run_id(),
        task_id=task.task_id,
        collector_name=task.collector_name,
        trigger_type=trigger_type,
        status=status,
        provider_id=task.provider_id,
        downloader_profile=task.downloader_profile,
        params=params,
        fields=list(task.fields) if task.fields is not None else None,
        formats=list(task.formats) if task.formats is not None else None,
        resource_group=task.resource_group,
        output_root=task.output_root,
        output_dir=task.output_dir,
        run_signature=run_signature or collector_run_signature(task, params=params),
        params_override=override,
        stage_timings=_empty_stage_timings(),
        metadata=dict(metadata or {}),
        created_at=now,
        updated_at=now,
    )
    return replace(
        base_run,
        events=_append_run_event(
            base_run,
            stage="queued",
            level="info",
            message="Collector run queued.",
            timestamp=now,
            details={"trigger_type": trigger_type, "resource_group": task.resource_group},
        ),
    )


def _create_collector_task_from_seed_kwargs(
    kwargs: Mapping[str, Any],
    *,
    data_root: str | Path | None = None,
) -> CollectorTask:
    now = utc_now_iso()
    task = CollectorTask(
        task_id=_string(kwargs.get("task_id"), "task_id"),
        collector_name=_string(kwargs.get("collector_name"), "collector_name"),
        name=_string(kwargs.get("name", ""), "name"),
        template_id=_optional_string(kwargs.get("template_id"), "template_id"),
        created_by=_string(kwargs.get("created_by", "system"), "created_by"),
        enabled=bool(kwargs.get("enabled", False)),
        trigger_type=_string(kwargs.get("trigger_type", TriggerType.MANUAL.value), "trigger_type"),
        interval_seconds=_optional_int(kwargs.get("interval_seconds"), "interval_seconds"),
        daily_time=_optional_string(kwargs.get("daily_time"), "daily_time"),
        interface_name=_optional_string(kwargs.get("interface_name"), "interface_name"),
        provider_id=_optional_string(kwargs.get("provider_id"), "provider_id"),
        downloader_profile=_optional_string(kwargs.get("downloader_profile"), "downloader_profile"),
        params=dict(_mapping(kwargs.get("params", {}), "params")),
        fields=_optional_string_list(kwargs.get("fields"), "fields"),
        formats=_optional_string_list(kwargs.get("formats"), "formats"),
        resource_group=_string(kwargs.get("resource_group", "default"), "resource_group"),
        output_root=_optional_string(kwargs.get("output_root"), "output_root"),
        output_dir=_optional_string(kwargs.get("output_dir"), "output_dir"),
        max_retries=_optional_non_negative_option(kwargs.get("max_retries"), "max_retries"),
        backoff_seconds=_optional_non_negative_option(kwargs.get("backoff_seconds"), "backoff_seconds"),
        expected_layer=_optional_string(kwargs.get("expected_layer"), "expected_layer"),
        schedule_hint=_optional_string(kwargs.get("schedule_hint"), "schedule_hint"),
        write_mode=_optional_string(kwargs.get("write_mode"), "write_mode"),
        partition_by=_optional_string_list(kwargs.get("partition_by"), "partition_by") or [],
        primary_key=_optional_string_list(kwargs.get("primary_key"), "primary_key") or [],
        date_field=_optional_string(kwargs.get("date_field"), "date_field"),
        required_datasets=_optional_string_list(kwargs.get("required_datasets"), "required_datasets") or [],
        required_plugin=_optional_string(kwargs.get("required_plugin"), "required_plugin"),
        dependency=dict(_mapping(kwargs.get("dependency", {}), "dependency")),
        dependency_status=_optional_string(kwargs.get("dependency_status"), "dependency_status"),
        dependency_message=_optional_string(kwargs.get("dependency_message"), "dependency_message"),
        dependency_errors=_dependency_error_list(kwargs.get("dependency_errors"), "dependency_errors"),
        category=_optional_string(kwargs.get("category"), "category"),
        tags=_optional_string_list(kwargs.get("tags"), "tags") or [],
        created_at=now,
        updated_at=now,
    )
    return replace(task, next_run_at=compute_next_run_at(task, now=parse_datetime(now), data_root=data_root))


def _merge_default_template_metadata(
    task: CollectorTask,
    kwargs: Mapping[str, Any],
    *,
    data_root: str | Path | None = None,
) -> CollectorTask:
    refreshed = replace(
        task,
        interface_name=_optional_string(kwargs.get("interface_name"), "interface_name"),
        provider_id=task.provider_id or _optional_string(kwargs.get("provider_id"), "provider_id"),
        downloader_profile=task.downloader_profile or _optional_string(kwargs.get("downloader_profile"), "downloader_profile"),
        expected_layer=_optional_string(kwargs.get("expected_layer"), "expected_layer"),
        schedule_hint=_optional_string(kwargs.get("schedule_hint"), "schedule_hint"),
        write_mode=_optional_string(kwargs.get("write_mode"), "write_mode"),
        partition_by=_optional_string_list(kwargs.get("partition_by"), "partition_by") or [],
        primary_key=_optional_string_list(kwargs.get("primary_key"), "primary_key") or [],
        date_field=_optional_string(kwargs.get("date_field"), "date_field"),
        required_datasets=_optional_string_list(kwargs.get("required_datasets"), "required_datasets") or [],
        required_plugin=_optional_string(kwargs.get("required_plugin"), "required_plugin"),
        dependency=dict(_mapping(kwargs.get("dependency", {}), "dependency")),
        category=_optional_string(kwargs.get("category"), "category"),
        tags=_optional_string_list(kwargs.get("tags"), "tags") or [],
    )
    return refresh_task_dependency_status(refreshed, data_root=data_root)


def collector_run_signature(task: CollectorTask, *, params: Mapping[str, Any] | None = None) -> str:
    payload = {
        "task_id": task.task_id,
        "collector_name": task.collector_name,
        "provider_id": task.provider_id,
        "downloader_profile": task.downloader_profile,
        "params": _normalize_signature_value(params if params is not None else task.params),
        "fields": _normalize_signature_value(task.fields),
        "formats": _normalize_signature_value(task.formats),
        "output_root": task.output_root,
        "output_dir": task.output_dir,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_next_run_at(
    task: CollectorTask,
    *,
    now: datetime | None = None,
    data_root: str | Path | None = None,
) -> str | None:
    if not task.enabled:
        return None
    current = (now or datetime.now(timezone.utc)).astimezone(LOCAL_TIMEZONE)
    if task.trigger_type == TriggerType.INTERVAL.value:
        if task.interval_seconds is None or int(task.interval_seconds) <= 0:
            return None
        return (current + timedelta(seconds=int(task.interval_seconds))).astimezone(timezone.utc).isoformat()
    if task.trigger_type == TriggerType.DAILY.value:
        daily_time = _normalize_daily_time(task.daily_time)
        hour, minute = [int(part) for part in daily_time.split(":", 1)]
        candidate = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= current:
            candidate = candidate + timedelta(days=1)
        trade_day_candidate = _next_trade_day_schedule_candidate(candidate, data_root=data_root)
        if trade_day_candidate is None:
            return None
        return trade_day_candidate.astimezone(timezone.utc).isoformat()
    return None


def _next_trade_day_schedule_candidate(
    candidate: datetime,
    *,
    data_root: str | Path | None = None,
) -> datetime | None:
    current = candidate.astimezone(LOCAL_TIMEZONE)
    for _ in range(370):
        trade_day = _is_trade_day_for_schedule(current.date(), data_root=data_root)
        if trade_day is True:
            return current
        if trade_day is None:
            return None
        current = current + timedelta(days=1)
    return None


def _is_due_daily_trade_day(due_at: datetime, *, data_root: str | Path | None = None) -> bool:
    trade_day = _is_trade_day_for_schedule(due_at.astimezone(LOCAL_TIMEZONE).date(), data_root=data_root)
    return trade_day is True


def _is_trade_day_for_schedule(day: date, *, data_root: str | Path | None = None) -> bool | None:
    root = Path(data_root or os.getenv("AXDATA_DATA_DIR", "data")).expanduser().resolve()
    try:
        from .trade_calendar_cache import get_trade_calendar_cache_status

        status = get_trade_calendar_cache_status(root, today=day)
    except Exception:
        return None
    if not status.get("exists") or int(status.get("row_count") or 0) <= 0:
        return None
    if not status.get("covers_today"):
        return None
    value = status.get("today_is_open")
    return bool(value) if value is not None else None


def task_run_kwargs(task: CollectorTask, *, run: CollectorRun | None = None) -> dict[str, Any]:
    """Return kwargs accepted by collector_runner.run_collector."""

    params = dict(run.params) if run is not None else dict(task.params)
    return {
        "params": params,
        "fields": list(task.fields) if task.fields is not None else None,
        "output_root": task.output_root,
        "output_dir": task.output_dir,
        "formats": list(task.formats) if task.formats is not None else None,
        "collect_mode": task.collect_mode,
        "connection_mode": task.connection_mode,
        "concurrency_mode": task.concurrency_mode,
        "connection_count": task.connection_count,
        "source_server_count": task.source_server_count,
        "connections_per_server": task.connections_per_server,
        "max_concurrent_tasks": task.max_concurrent_tasks,
        "batch_size": task.batch_size,
        "request_interval_ms": task.request_interval_ms,
        "retry_count": task.retry_count,
        "timeout_ms": task.timeout_ms,
    }


def collector_task_guidance(task: CollectorTask) -> dict[str, Any]:
    """Return user-facing guidance for a Collector task."""

    base = _collector_task_state(task)
    missing_dependency = _missing_task_dependency(task)
    if missing_dependency is not None:
        first_dependency_error = task.dependency_errors[0] if task.dependency_errors else {}
        return {
            **base,
            "status_message": f"Collector task 依赖不可用：{missing_dependency}。",
            "next_action": first_dependency_error.get("next_action") or missing_dependency,
            "action_command": (
                str(first_dependency_error.get("action_command"))
                if first_dependency_error.get("action_command")
                else f"axdata plugin enable {task.required_plugin}" if task.required_plugin else None
            ),
        }
    if not task.enabled:
        if task.trigger_type == TriggerType.MANUAL.value:
            return {
                **base,
                "status_message": "手动模式：不会自动调度，需要时点击“手动采集”。",
                "next_action": None,
                "action_command": f"axdata collector task run {task.task_id} --wait --json",
            }
        if task.created_by == "system":
            return {
                **base,
                "status_message": "系统默认 Collector task 当前为禁用；不会自动采集，但可以手动运行一次。",
                "next_action": "确认参数后可手动运行，或显式启用该 task。",
                "action_command": f"axdata collector task run {task.task_id} --wait --json",
            }
        return {
            **base,
            "status_message": "自动调度已关闭；开启定时后才会自动采集。",
            "next_action": "需要恢复自动采集时，显式启用该 task。",
            "action_command": f"axdata collector task enable {task.task_id}",
        }
    backoff_until = parse_datetime(task.backoff_until)
    if backoff_until is not None and backoff_until > datetime.now(timezone.utc):
        return {
            **base,
            "status_message": f"Collector task 处于失败退避中，直到 {task.backoff_until}。",
            "next_action": "先查看最近失败 run，确认错误原因后再手动运行或等待退避结束。",
            "action_command": f"axdata collector run list --task-id {task.task_id} --status failed --json",
        }
    if task.last_status == TaskStatus.FAILED.value and task.last_error:
        return {
            **base,
            "status_message": f"最近一次 Collector run 失败：{task.last_error}",
            "next_action": "查看最近失败 run 的 error、quality 和 output_paths。",
            "action_command": f"axdata collector run list --task-id {task.task_id} --status failed --json",
        }
    if task.last_status == TaskStatus.SUCCESS.value:
        return {
            **base,
            "status_message": "最近一次 Collector run 已成功完成。",
            "next_action": None,
            "action_command": None,
        }
    if task.trigger_type == TriggerType.MANUAL.value:
        return {
            **base,
            "status_message": "Collector task 已就绪，当前为 manual 触发。",
            "next_action": "需要采集时手动提交一次 run。",
            "action_command": f"axdata collector task run {task.task_id} --wait --json",
        }
    if task.trigger_type == TriggerType.DAILY.value and not task.next_run_at:
        return {
            **base,
            "status_message": "交易日定时需要本地交易日历缓存；当前还不能确定下一次交易日运行时间。",
            "next_action": "请到 Web 配置里的“基础数据 / 交易日历”更新交易日历缓存。",
            "action_command": "axdata collector task run trade_cal_refresh --wait --json",
        }
    if task.next_run_at:
        return {
            **base,
            "status_message": f"Collector task 已启用，下一次计划运行时间为 {task.next_run_at}。",
            "next_action": None,
            "action_command": None,
        }
    return {
        **base,
        "status_message": "Collector task 已启用。",
        "next_action": None,
        "action_command": None,
    }


def collector_run_guidance(run: CollectorRun) -> dict[str, Any]:
    """Return user-facing guidance for a Collector run."""

    base = _collector_run_state(run)
    if run.status == TaskStatus.FAILED.value:
        message = f"Collector run 失败：{run.error}" if run.error else "Collector run 失败。"
        dependency_errors = run.metadata.get("dependency_errors") if isinstance(run.metadata, Mapping) else None
        if run.error_category == "dependency_missing" and isinstance(dependency_errors, list) and dependency_errors:
            first = dependency_errors[0] if isinstance(dependency_errors[0], Mapping) else {}
            return {
                **base,
                "status_message": message,
                "next_action": str(first.get("next_action") or "请先补齐基础数据依赖后再运行。"),
                "action_command": str(first.get("action_command") or "") or None,
            }
        next_action, action_command = _run_error_action(run)
        return {
            **base,
            "status_message": message,
            "next_action": next_action,
            "action_command": action_command,
        }
    if run.status == TaskStatus.SKIPPED.value:
        if run.skip_reason == "task_disabled":
            return {
                **base,
                "status_message": "Collector run 已跳过，因为 task 已禁用。",
                "next_action": "需要采集时先启用该 task。",
                "action_command": f"axdata collector task enable {run.task_id}",
            }
        if run.skip_reason == "failure_backoff":
            return {
                **base,
                "status_message": f"Collector run 已跳过，因为 task 正在失败退避到 {run.backoff_until}。",
                "next_action": "查看最近失败 run，确认错误后等待退避结束或调整任务配置。",
                "action_command": f"axdata collector run list --task-id {run.task_id} --status failed --json",
            }
        if run.skip_reason == "active_duplicate":
            duplicate_run_id = str(run.metadata.get("duplicate_run_id") or "").strip()
            return {
                **base,
                "status_message": (
                    f"Collector run 已跳过，因为已有活跃重复 run {duplicate_run_id}。"
                    if duplicate_run_id
                    else "Collector run 已跳过，因为已有活跃重复 run。"
                ),
                "next_action": "等待活跃 run 完成，或查看当前 active runs。",
                "action_command": "axdata collector status --json",
            }
        return {
            **base,
            "status_message": f"Collector run 已跳过：{run.skip_reason or '未提供原因'}。",
            "next_action": "查看 run metadata 了解跳过原因。",
            "action_command": f"axdata collector run info {run.run_id} --json",
        }
    if run.status == TaskStatus.QUEUED.value:
        resource_wait = run.metadata.get("resource_wait") if isinstance(run.metadata, Mapping) else None
        if isinstance(resource_wait, Mapping):
            active = resource_wait.get("resource_active")
            limit = resource_wait.get("resource_limit")
            waiting = resource_wait.get("waiting_count")
            return {
                **base,
                "status_message": (
                    f"Collector run 正在等待 resource_group={run.resource_group}；"
                    f"当前占用 {active}/{limit}，等待数 {waiting}。"
                ),
                "next_action": "等待资源释放，或降低同一 resource_group 的并发任务数量。",
                "action_command": "axdata collector status --json",
            }
        return {
            **base,
            "status_message": f"Collector run 已排队，等待 resource_group={run.resource_group}。",
            "next_action": "查看 collector status 了解 active runs。",
            "action_command": "axdata collector status --json",
        }
    if run.status == TaskStatus.RUNNING.value:
        return {
            **base,
            "status_message": f"Collector run 正在运行，resource_group={run.resource_group}。",
            "next_action": "等待完成后查看 quality 和 output_paths。",
            "action_command": f"axdata collector run info {run.run_id} --json",
        }
    if run.status == TaskStatus.SUCCESS.value:
        quality_status = str(run.quality.get("quality_status") or "").strip().lower()
        if run.error_category == "quality_failed" or quality_status == "error":
            return {
                **base,
                "status_message": "Collector run 已完成，但质量检查发现 blocking 问题。",
                "next_action": "查看 quality_errors、输出文件和本次参数；修复后重新运行 task。",
                "action_command": f"axdata collector run info {run.run_id} --json",
            }
        output_count = len(run.output_paths)
        return {
            **base,
            "status_message": (
                f"Collector run 已成功完成，输出 {output_count} 个文件。"
                if output_count
                else "Collector run 已成功完成。"
            ),
            "next_action": None,
            "action_command": None,
        }
    if run.status == TaskStatus.CANCELLED.value:
        return {
            **base,
            "status_message": "Collector run 已取消；当前 MVP 取消是状态标记。",
            "next_action": "如底层工作已开始，确认没有残留写入或临时文件。",
            "action_command": f"axdata collector run info {run.run_id} --json",
        }
    return {
        **base,
        "status_message": f"Collector run 状态为 {run.status}。",
        "next_action": "查看 collector status 了解队列和 active runs。",
        "action_command": "axdata collector status --json",
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def normalize_date_range(start: str, end: str) -> tuple[str, str]:
    start_date = _normalize_date_text(start, "start")
    end_date = _normalize_date_text(end, "end")
    if start_date > end_date:
        raise CollectorSchedulerError("start must be before or equal to end.")
    return start_date, end_date


def _collector_task_state(task: CollectorTask) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    backoff_until = parse_datetime(task.backoff_until)
    missing_dependency = _missing_task_dependency(task)
    if missing_dependency is not None:
        return {
            "queue_status": "dependency_missing",
            "can_run_now": True,
            "blocked_reason": "dependency_missing",
            "last_error_summary": _short_error(task.last_error),
        }
    if not task.enabled:
        return {
            "queue_status": "disabled",
            "can_run_now": task.created_by == "system",
            "blocked_reason": "task_disabled",
            "last_error_summary": _short_error(task.last_error),
        }
    if backoff_until is not None and backoff_until > now:
        return {
            "queue_status": "backoff",
            "can_run_now": False,
            "blocked_reason": "failure_backoff",
            "last_error_summary": _short_error(task.last_error),
        }
    return {
        "queue_status": "ready",
        "can_run_now": True,
        "blocked_reason": None,
        "last_error_summary": _short_error(task.last_error),
    }


def _collector_run_state(run: CollectorRun) -> dict[str, Any]:
    blocked_reason = None
    if run.status == TaskStatus.SKIPPED.value:
        blocked_reason = run.skip_reason
    elif run.status == TaskStatus.QUEUED.value and isinstance(run.metadata, Mapping):
        blocked_reason = "waiting_resource" if isinstance(run.metadata.get("resource_wait"), Mapping) else None
    elif run.status == TaskStatus.FAILED.value:
        blocked_reason = "run_failed"
    return {
        "queue_status": run.status,
        "can_run_now": run.status not in ACTIVE_RUN_STATUSES,
        "blocked_reason": blocked_reason,
        "last_error_summary": _short_error(run.error),
    }


def _task_with_run_summary(
    task: CollectorTask,
    run: CollectorRun,
    *,
    data_root: str | Path | None = None,
) -> CollectorTask:
    next_run_at = compute_next_run_at(task, data_root=data_root)
    if run.status == TaskStatus.FAILED.value:
        last_error = run.error
    elif run.status == TaskStatus.SUCCESS.value:
        last_error = None
    else:
        last_error = task.last_error
    last_success_at = run.finished_at if run.status == TaskStatus.SUCCESS.value else task.last_success_at
    last_failure_at = run.finished_at if run.status == TaskStatus.FAILED.value else task.last_failure_at
    return replace(
        task,
        last_run_id=run.run_id,
        last_status=run.status,
        last_success_at=last_success_at,
        last_failure_at=last_failure_at,
        last_error=last_error,
        backoff_until=run.backoff_until,
        next_run_at=next_run_at,
        updated_at=utc_now_iso(),
    )


def _resolve_task_dependency_status(
    task: CollectorTask,
    *,
    data_root: str | Path | None = None,
) -> tuple[str, str | None]:
    provider_id = task.required_plugin or _optional_string(task.dependency.get("provider_id"), "dependency.provider_id")
    if not provider_id:
        return "ok", None
    try:
        from .collector_registry import build_collector_registry
        from .provider_catalog import build_builtin_provider_registry
        from .plugin_config import load_plugin_config

        config = load_plugin_config(data_root=data_root)
        provider_registry = build_builtin_provider_registry(data_root=data_root)
        snapshot = provider_registry.snapshot()
        collector_snapshot = build_collector_registry(
            provider_registry=provider_registry,
            plugin_config=config,
            data_root=data_root,
        ).snapshot()
    except Exception as exc:
        message = _tdx_dependency_message(provider_id) if _is_tdx_dependency(provider_id) else str(exc)
        return "unknown", message

    if provider_id in set(getattr(config, "removed_provider_ids", ())):
        return "uninstalled", _provider_dependency_message(provider_id, status="uninstalled")
    collector_plugin = collector_snapshot.plugins.get(provider_id)
    if collector_plugin is not None:
        if not collector_plugin.enabled:
            return "disabled", _provider_dependency_message(provider_id, status="disabled")
        if collector_plugin.status != "enabled":
            return collector_plugin.status or "missing", _provider_dependency_message(
                provider_id,
                status=collector_plugin.status or "missing",
            )
        if task.collector_name and task.collector_name not in collector_snapshot.collectors:
            return "missing", _provider_dependency_message(provider_id, status="missing")
        return "ok", None
    provider = snapshot.providers.get(provider_id)
    if provider is None:
        return "missing", _provider_dependency_message(provider_id, status="missing")
    if not provider.enabled:
        return "disabled", _provider_dependency_message(provider_id, status="disabled")
    status = str(getattr(provider, "status", "") or "")
    if status != "enabled":
        return status or "missing", _provider_dependency_message(provider_id, status=status or "missing")
    if task.collector_name and task.collector_name not in collector_snapshot.collectors:
        return "missing", _provider_dependency_message(provider_id, status="missing")
    return "ok", None


def _task_dataset_dependency_errors(
    task: CollectorTask,
    *,
    data_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    return _run_dataset_dependency_errors(task, task.params, data_root=data_root)


def _run_dataset_dependency_errors(
    task: CollectorTask,
    params: Mapping[str, Any],
    *,
    data_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    datasets = _unique_strings(task.required_datasets)
    if not datasets:
        return []
    errors: list[dict[str, Any]] = []
    for dataset in datasets:
        if dataset == "trade_cal":
            error = _trade_cal_dependency_error(params, data_root=data_root)
        else:
            error = {
                "dataset": dataset,
                "status": "unsupported",
                "message": f"基础数据依赖 {dataset!r} 暂不支持自动检查。",
                "next_action": "检查该基础数据是否已同步，或移除错误的 required_datasets 声明。",
                "action_command": None,
            }
        if error is not None:
            errors.append(error)
    return errors


def _trade_cal_dependency_error(
    params: Mapping[str, Any],
    *,
    data_root: str | Path | None,
) -> dict[str, Any] | None:
    start_date, end_date, range_source = _dependency_date_range(params)
    root = Path(data_root or os.getenv("AXDATA_DATA_DIR", "data")).expanduser().resolve()
    if range_source == "cache_available":
        try:
            from .trade_calendar_cache import get_trade_calendar_cache_status

            status = get_trade_calendar_cache_status(root)
        except Exception as exc:
            return {
                "dataset": "trade_cal",
                "status": "check_failed",
                "start_date": start_date,
                "end_date": end_date,
                "range_source": range_source,
                "message": f"交易日历检查失败：{exc}",
                "next_action": "请到 Web 配置里的“基础数据 / 交易日历”更新交易日历缓存。",
                "action_command": "axdata collector task run trade_cal_refresh --wait --json",
            }
        if bool(status.get("exists")) and int(status.get("row_count") or 0) > 0:
            return None
    try:
        from .trade_calendar_cache import check_trade_calendar_cache

        status = check_trade_calendar_cache(root, start_date=start_date, end_date=end_date)
    except Exception as exc:
        return {
            "dataset": "trade_cal",
            "status": "check_failed",
            "start_date": start_date,
            "end_date": end_date,
            "range_source": range_source,
            "message": f"交易日历检查失败：{exc}",
            "next_action": "请到 Web 配置里的“基础数据 / 交易日历”更新交易日历缓存。",
            "action_command": "axdata collector task run trade_cal_refresh --wait --json",
        }
    if bool(status.get("is_available")):
        return None
    exists = bool(status.get("exists")) and int(status.get("row_count") or 0) > 0
    if exists:
        message = f"交易日历未覆盖 {start_date}-{end_date}，请先补全指定范围。"
        action_command = f"axdata collector task backfill trade_cal_refresh --start {start_date} --end {end_date} --wait --json"
        status_name = "coverage_insufficient"
    else:
        message = "交易日历未同步，请先同步基础数据：交易日历。"
        action_command = "axdata collector task run trade_cal_refresh --wait --json"
        status_name = "missing"
    return {
        "dataset": "trade_cal",
        "status": status_name,
        "start_date": start_date,
        "end_date": end_date,
        "range_source": range_source,
        "message": message,
        "next_action": "打开 Web 配置里的“基础数据 / 交易日历”同步入口，或运行命令补全本地交易日历。",
        "action_command": action_command,
        "missing_count": status.get("missing_count"),
        "missing_dates": list(status.get("missing_dates") or []),
        "cache_start_date": status.get("cache_start_date"),
        "cache_end_date": status.get("cache_end_date"),
    }


def _dependency_date_range(params: Mapping[str, Any]) -> tuple[str, str, str]:
    for year_key in ("year", "cal_year"):
        year = _normalize_year_text(params.get(year_key))
        if year is not None:
            return f"{year}0101", f"{year}1231", year_key

    start_value = _first_date_param(params, ("start_date", "start", "begin_date", "from_date"))
    end_value = _first_date_param(params, ("end_date", "end", "to_date"))
    if start_value and end_value:
        return start_value, end_value, "date_range"
    if start_value:
        return start_value, start_value, "start_date"
    if end_value:
        return end_value, end_value, "end_date"

    point_value = _first_date_param(
        params,
        (
            "trade_date",
            "cal_date",
            "date",
            "target_date",
            "target_trade_date",
            "snapshot_date",
            "data_date",
        ),
    )
    if point_value:
        return point_value, point_value, "date"

    today = datetime.now(LOCAL_TIMEZONE).strftime("%Y%m%d")
    return today, today, "cache_available"


def _first_date_param(params: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = _normalize_dependency_date(params.get(key))
        if value is not None:
            return value
    return None


def _normalize_dependency_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        for item in value:
            normalized = _normalize_dependency_date(item)
            if normalized is not None:
                return normalized
        return None
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(digits) < 8:
        return None
    candidate = digits[:8]
    try:
        datetime.strptime(candidate, "%Y%m%d")
    except ValueError:
        return None
    return candidate


def _normalize_year_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) == 4 and text.isdigit():
        return text
    return None


def _unique_strings(values: list[str] | tuple[str, ...]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _missing_task_dependency(task: CollectorTask) -> str | None:
    status = str(task.dependency_status or "").strip().lower()
    if status in {"", "available", "ok"}:
        return None
    if _is_tdx_dependency(task.required_plugin):
        return _tdx_dependency_message(task.required_plugin)
    if task.dependency_message:
        return task.dependency_message
    if task.required_plugin:
        return f"Provider {task.required_plugin!r} is not installed or enabled."
    return "Collector task dependency is not available."


def _task_dependency_error_category(task: CollectorTask) -> str:
    status = str(task.dependency_status or "").strip().lower()
    if status == "disabled":
        return "plugin_disabled"
    if status == "uninstalled":
        return "provider_missing"
    if status == "missing":
        return "provider_missing"
    if status == "blocked":
        return "dependency_missing"
    return "dependency_missing"


def _provider_dependency_message(provider_id: str | None, *, status: str) -> str:
    if _is_tdx_dependency(provider_id):
        return _tdx_dependency_message(provider_id)
    if not provider_id:
        return "Collector task dependency is not available."
    if status == "disabled":
        return f"Provider {provider_id!r} is disabled."
    if status == "uninstalled":
        return f"Provider {provider_id!r} has been uninstalled from the current AxData plugin state."
    return f"Provider {provider_id!r} is not installed or enabled."


def _tdx_dependency_message(provider_id: str | None) -> str:
    if provider_id == "axdata.source.tdx_external":
        return "请安装/启用 TDX 插件 (axdata.source.tdx_external)"
    if provider_id == "axdata.collector.tdx":
        return "请安装/启用 TDX 采集器插件 (axdata.collector.tdx)"
    if provider_id:
        return f"Provider {provider_id!r} is not installed or enabled."
    return "Collector task dependency is not available."


def _is_tdx_dependency(provider_id: str | None) -> bool:
    return provider_id in {"axdata.source.tdx_external", "axdata.collector.tdx"}


def _duration_ms(started_at: str | None, finished_at: str | None) -> int | None:
    started = parse_datetime(started_at)
    finished = parse_datetime(finished_at)
    if started is None or finished is None:
        return None
    return max(int((finished - started).total_seconds() * 1000), 0)


def _empty_stage_timings() -> dict[str, int | None]:
    return {field_name: None for field_name in RUN_TIMING_FIELDS}


def _merge_stage_timings(
    current: Mapping[str, Any] | None,
    updates: Mapping[str, Any],
) -> dict[str, int | None]:
    merged = _empty_stage_timings()
    for key, value in dict(current or {}).items():
        if key in merged:
            merged[key] = _optional_non_negative_int(value)
    for key, value in updates.items():
        if key in merged:
            merged[key] = _optional_non_negative_int(value)
    return merged


def _append_run_event(
    run: CollectorRun,
    *,
    stage: str,
    level: str = "info",
    message: str,
    category: str | None = None,
    details: Mapping[str, Any] | None = None,
    timestamp: str | None = None,
) -> list[dict[str, Any]]:
    return _append_event_list(
        run.run_id,
        run.events,
        stage=stage,
        level=level,
        message=message,
        category=category,
        details=details,
        timestamp=timestamp,
    )


def _append_event_list(
    run_id: str,
    events: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    stage: str,
    level: str = "info",
    message: str,
    category: str | None = None,
    details: Mapping[str, Any] | None = None,
    timestamp: str | None = None,
) -> list[dict[str, Any]]:
    normalized = [_normalize_run_event(event, index + 1) for index, event in enumerate(events or [])]
    next_sequence = max((int(event.get("sequence") or 0) for event in normalized), default=0) + 1
    event: dict[str, Any] = {
        "sequence": next_sequence,
        "event_id": f"{run_id}:{next_sequence:04d}",
        "timestamp": timestamp or utc_now_iso(),
        "stage": _normalize_event_stage(stage),
        "level": _normalize_event_level(level),
        "message": _truncate_text(message, RUN_EVENT_STRING_LIMIT),
        "category": _normalize_event_category(category),
        "details": _sanitize_event_details(details or {}),
    }
    normalized.append(event)
    if len(normalized) > RUN_EVENT_HISTORY_LIMIT:
        normalized = [normalized[0], *normalized[-(RUN_EVENT_HISTORY_LIMIT - 1):]]
    return normalized


def _normalize_run_event(value: Mapping[str, Any], sequence: int) -> dict[str, Any]:
    event = dict(value)
    event["sequence"] = int(event.get("sequence") or sequence)
    event["event_id"] = str(event.get("event_id") or f"event:{event['sequence']:04d}")
    event["timestamp"] = str(event.get("timestamp") or "")
    event["stage"] = _normalize_event_stage(event.get("stage"))
    event["level"] = _normalize_event_level(event.get("level"))
    event["message"] = _truncate_text(event.get("message") or "", RUN_EVENT_STRING_LIMIT)
    event["category"] = _normalize_event_category(event.get("category"))
    event["details"] = _sanitize_event_details(event.get("details") if isinstance(event.get("details"), Mapping) else {})
    return event


def _stage_timings_from_result(
    run: CollectorRun,
    result: Mapping[str, Any],
    *,
    finished_at: str,
) -> dict[str, int | None]:
    download_result = result.get("download_result")
    download_breakdown = dict(download_result.get("duration_breakdown_ms") or {}) if isinstance(download_result, Mapping) else {}
    collector_breakdown = dict(result.get("collector_duration_breakdown_ms") or {}) if isinstance(result, Mapping) else {}
    source_request_ms = _optional_non_negative_int(download_breakdown.get("source_request"))
    runner_fetch_ms = _optional_non_negative_int(download_breakdown.get("runner_fetch"))
    connection_ms = _optional_non_negative_int(download_breakdown.get("connection")) or 0
    transform_ms = _optional_non_negative_int(download_breakdown.get("transform")) or 0
    download_ms = _optional_non_negative_int(collector_breakdown.get("download"))
    if download_ms is None and source_request_ms is not None:
        download_ms = source_request_ms + connection_ms + transform_ms
    if download_ms is None and runner_fetch_ms is not None:
        download_ms = runner_fetch_ms + transform_ms
    return _merge_stage_timings(
        run.stage_timings,
        {
            "params_resolve_ms": _optional_non_negative_int(collector_breakdown.get("params_resolve")),
            "provider_resolve_ms": _optional_non_negative_int(collector_breakdown.get("provider_resolve")),
            "download_ms": download_ms,
            "write_ms": _optional_non_negative_int(download_breakdown.get("write")),
            "quality_ms": _optional_non_negative_int(download_breakdown.get("quality")),
            "total_ms": _duration_ms(run.created_at, finished_at),
        },
    )


def _success_events_from_result(
    run: CollectorRun,
    result: Mapping[str, Any],
    output_paths: Mapping[str, str],
    quality: Mapping[str, Any],
    *,
    timestamp: str,
) -> list[dict[str, Any]]:
    events = list(run.events)
    is_runner_entry = bool(result.get("runner_entry")) or (
        isinstance(result.get("download_result"), Mapping)
        and result["download_result"].get("collect_mode") == "runner_entry"
    )
    events = _append_event_list(
        run.run_id,
        events,
        stage="provider_resolved",
        level="info",
        message=(
            "Collector runner entry resolved."
            if is_runner_entry
            else "Collector provider and downloader profile resolved."
        ),
        timestamp=timestamp,
        details={
            "provider_id": result.get("provider_id"),
            "collector_plugin_id": result.get("collector_plugin_id"),
            "target_interface": result.get("target_interface"),
            "downloader_profile": result.get("downloader_profile"),
            "runner_entry": result.get("runner_entry"),
        },
    )
    download_result = result.get("download_result") if isinstance(result, Mapping) else None
    row_count = download_result.get("row_count") if isinstance(download_result, Mapping) else None
    events = _append_event_list(
        run.run_id,
        events,
        stage="downloaded",
        level="info",
        message="Collector runner produced records." if is_runner_entry else "Source data downloaded.",
        timestamp=timestamp,
        details={"row_count": row_count},
    )
    events = _append_event_list(
        run.run_id,
        events,
        stage="written",
        level="info",
        message="Collector output files written.",
        timestamp=timestamp,
        details={"formats": sorted(output_paths)},
    )
    quality_status = str(quality.get("quality_status") or "").strip().lower()
    quality_level = "warning" if quality_status == "warn" else "error" if quality_status == "error" else "info"
    events = _append_event_list(
        run.run_id,
        events,
        stage="quality_checked",
        level=quality_level,
        message=f"Quality check completed with status {quality_status or 'unknown'}.",
        category="quality_failed" if quality_status == "error" else None,
        timestamp=timestamp,
        details={
            "quality_status": quality_status or None,
            "warning_count": len(quality.get("quality_warnings") or []),
            "error_count": len(quality.get("quality_errors") or []),
        },
    )
    events = _append_event_list(
        run.run_id,
        events,
        stage="metadata_recorded",
        level="info",
        message="Collector run metadata recorded.",
        timestamp=timestamp,
    )
    return _append_event_list(
        run.run_id,
        events,
        stage="finished",
        level="info",
        message="Collector run finished.",
        timestamp=timestamp,
    )


def _quality_error_details(quality: Mapping[str, Any]) -> tuple[str | None, str | None]:
    if str(quality.get("quality_status") or "").strip().lower() != "error":
        return None, None
    errors = [str(item) for item in quality.get("quality_errors") or [] if str(item).strip()]
    summary = "; ".join(errors) if errors else "Quality check failed."
    return "quality_failed", _truncate_text(summary, 300)


def _progress_stage(percent: int, message: str) -> str:
    text = str(message or "").lower()
    if "写入" in text:
        return "written"
    if "质量" in text or "quality" in text:
        return "quality_checked"
    if "源端" in text or "请求" in text or int(percent) < 70:
        return "downloaded"
    return "request_planned"


def _classify_run_error(exc: Exception) -> str:
    name = type(exc).__name__
    text = str(exc).lower()
    if isinstance(exc, (PermissionError,)):
        return "storage_permission"
    if isinstance(exc, FileNotFoundError):
        return "storage_missing"
    if isinstance(exc, (TimeoutError, SocketTimeout, ConnectionError)):
        return "network_error"
    if name in {"ModuleNotFoundError", "ImportError"}:
        return "dependency_missing"
    if name in {"CollectorSchedulerError", "SourceRequestValidationError", "ValueError", "TypeError"}:
        return "invalid_params"
    if "provider" in text and ("not enabled" in text or "disabled" in text):
        return "plugin_disabled"
    if "plugin" in text and ("failed" in text or "import" in text or "entry point" in text):
        return "plugin_failed"
    if "not available" in text or "no available" in text or "known collectors" in text or "known downloader profiles" in text:
        return "provider_missing"
    if "permission" in text or "access is denied" in text:
        return "storage_permission"
    if "no such file" in text or "not found" in text or "does not exist" in text:
        return "storage_missing"
    if "schema" in text or "required column" in text or "missing required" in text:
        return "schema_mismatch"
    if "quality" in text:
        return "quality_failed"
    if "write" in text or "parquet" in text or "csv" in text or "jsonl" in text:
        return "write_failed"
    if "empty" in text or "no rows" in text:
        return "upstream_empty"
    if "timeout" in text or "connection" in text or "network" in text or "dns" in text:
        return "network_error"
    if "upstream" in text or "source" in text or "remote" in text or "http" in text:
        return "upstream_error"
    return "unknown"


def _error_summary(exc: Exception) -> str:
    text = str(exc).strip() or type(exc).__name__
    return _truncate_text(text, 300)


def _normalize_date_text(value: str, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise CollectorSchedulerError(f"{field_name} is required.")
    compact = text.replace("-", "")
    if len(compact) != 8 or not compact.isdigit():
        raise CollectorSchedulerError(f"{field_name} must be YYYYMMDD or YYYY-MM-DD.")
    try:
        datetime.strptime(compact, "%Y%m%d")
    except ValueError as exc:
        raise CollectorSchedulerError(f"{field_name} must be a valid date.") from exc
    return compact


def _run_sort_key(run: CollectorRun) -> tuple[str, str]:
    timestamp = run.updated_at or run.finished_at or run.started_at or run.created_at
    return (timestamp or "", run.run_id)


def _short_error(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text if len(text) <= 180 else f"{text[:177]}..."


def _is_scheduler_interrupted_error(value: str | None) -> bool:
    text = str(value or "").strip().lower()
    return "collector scheduler process stopped before this run finished" in text


def _run_error_action(run: CollectorRun) -> tuple[str, str]:
    category = run.error_category or "unknown"
    if category == "scheduler_interrupted" or _is_scheduler_interrupted_error(run.error):
        return (
            "上次采集时 API/调度器进程被重启或停止；确认服务稳定后重新运行 task。",
            f"axdata collector run info {run.run_id} --json",
        )
    if category == "provider_missing":
        return (
            "确认 collector/downloader profile 和 Provider 是否已安装并启用。",
            f"axdata collector run info {run.run_id} --json",
        )
    if category == "plugin_disabled":
        return (
            "启用对应 Provider 后重新运行 task。",
            "axdata plugin list --json",
        )
    if category == "invalid_params":
        return (
            "检查本次 params/params_override 是否符合接口参数要求。",
            f"axdata collector run info {run.run_id} --json",
        )
    if category in {"network_error", "upstream_error", "upstream_empty"}:
        return (
            "检查上游连通性、限流和参数范围；默认测试不需要真实网络。",
            f"axdata collector run info {run.run_id} --json",
        )
    if category in {"schema_mismatch", "quality_failed"}:
        return (
            "查看 quality_errors 和 schema 字段映射，确认源端字段或 profile 质量契约。",
            f"axdata collector run info {run.run_id} --json",
        )
    if category in {"write_failed", "storage_missing", "storage_permission"}:
        return (
            "检查 output_root/output_dir 是否存在、可写，以及目标文件是否被占用。",
            f"axdata collector run info {run.run_id} --json",
        )
    return (
        "查看 error、event log、stage timings、quality 和 output_paths；修复后可重新运行 task。",
        f"axdata collector run info {run.run_id} --json",
    )


def _extract_output_paths(result: Mapping[str, Any]) -> dict[str, str]:
    download_result = result.get("download_result")
    if isinstance(download_result, Mapping):
        output_paths = download_result.get("output_paths")
        if isinstance(output_paths, Mapping):
            return {str(key): str(value) for key, value in output_paths.items()}
        output_path = download_result.get("output_path")
        if output_path:
            return {"default": str(output_path)}
    output_paths = result.get("output_paths")
    if isinstance(output_paths, Mapping):
        return {str(key): str(value) for key, value in output_paths.items()}
    output_path = result.get("output_path")
    if output_path:
        return {"default": str(output_path)}
    return {}


def _run_output_summary(run: CollectorRun) -> dict[str, Any]:
    download_result = run.result.get("download_result") if isinstance(run.result, Mapping) else None
    download = download_result if isinstance(download_result, Mapping) else {}
    quality = run.quality if isinstance(run.quality, Mapping) else {}
    return {
        "records_read": _first_int(
            download.get("row_count"),
            quality.get("row_count_value"),
            quality.get("rows_written"),
        ),
        "rows_written": _first_int(
            quality.get("rows_written"),
            download.get("rows_written"),
            download.get("row_count"),
            quality.get("row_count_value"),
        ),
        "write_mode": _first_text(quality.get("write_mode"), download.get("write_mode")),
        "partition_by": _first_list(quality.get("partition_by"), download.get("partition_by")),
        "primary_key": _first_list(quality.get("write_primary_key"), download.get("primary_key"), quality.get("primary_key")),
        "date_field": _first_text(quality.get("write_date_field"), quality.get("date_field"), download.get("date_field")),
        "replace_range_start": _first_text(quality.get("replace_range_start"), download.get("replace_range_start")),
        "replace_range_end": _first_text(quality.get("replace_range_end"), download.get("replace_range_end")),
        "rows_before": _first_int(quality.get("rows_before"), download.get("rows_before")),
        "rows_after": _first_int(quality.get("rows_after"), download.get("rows_after")),
        "duplicate_rows_dropped": _first_int(
            quality.get("duplicate_rows_dropped"),
            download.get("duplicate_rows_dropped"),
        ),
        "partitions_touched": _first_list(quality.get("partitions_touched"), download.get("partitions_touched")),
        "write_metadata": _jsonable_mapping(download.get("write_metadata")) if isinstance(download.get("write_metadata"), Mapping) else None,
    }


def _first_int(*values: Any) -> int | None:
    for value in values:
        parsed = _optional_non_negative_int(value)
        if parsed is not None:
            return parsed
    return None


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _first_list(*values: Any) -> list[str]:
    for value in values:
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",") if item.strip()]
        elif isinstance(value, (list, tuple)):
            items = [str(item).strip() for item in value if str(item).strip()]
        else:
            items = []
        if items:
            return items
    return []


def _should_apply_failure_backoff(exc: Exception) -> bool:
    non_backoff_errors = {
        "CollectorError",
        "DownloaderError",
        "SourceInterfaceNotFound",
        "SourceRequestValidationError",
        "ValueError",
        "CollectorSchedulerError",
    }
    return type(exc).__name__ not in non_backoff_errors


def _collector_run_max_attempts(task: CollectorTask) -> int:
    return max(int(task.max_retries or 0), 0) + 1


def _collector_retry_delay_seconds(task: CollectorTask) -> int:
    if task.backoff_seconds is None:
        return 0
    return max(int(task.backoff_seconds), 0)


def _new_task_id(collector_name: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", collector_name).strip("_.-") or "collector"
    return f"task_{clean}_{uuid4().hex[:8]}"


def _new_run_id() -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{now}_{uuid4().hex[:8]}"


def _normalize_status(value: str | None, *, allow_none: bool = False) -> str | None:
    if value is None and allow_none:
        return None
    text = str(value or "").strip().lower()
    allowed = {item.value for item in TaskStatus}
    if text not in allowed:
        raise CollectorSchedulerError(f"status must be one of: {', '.join(sorted(allowed))}.")
    return text


def _normalize_trigger(value: str) -> str:
    text = str(value or "").strip().lower()
    allowed = {item.value for item in TriggerType}
    if text not in allowed:
        raise CollectorSchedulerError(f"trigger_type must be one of: {', '.join(sorted(allowed))}.")
    return text


def _normalize_daily_time(value: str | None) -> str:
    text = str(value or "").strip()
    try:
        parsed = datetime.strptime(text, "%H:%M")
    except ValueError as exc:
        raise CollectorSchedulerError("daily_time must be HH:MM.") from exc
    return parsed.strftime("%H:%M")


def _validate_identifier(value: str, field_name: str, *, allow_dot: bool = False) -> None:
    pattern = r"^[A-Za-z0-9_.-]+$" if allow_dot else r"^[A-Za-z0-9_-]+$"
    if not isinstance(value, str) or not re.match(pattern, value):
        raise CollectorSchedulerError(f"{field_name} must contain only letters, numbers, '_', '-', or '.'.")


def _string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise CollectorSchedulerError(f"{field_name} must be a string.")
    return value


def _optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise CollectorSchedulerError(f"{field_name} must be a string.")
    return value


def _optional_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CollectorSchedulerError(f"{field_name} must be an integer.") from exc


def _optional_non_negative_option(value: Any, field_name: str) -> int | None:
    parsed = _optional_int(value, field_name)
    if parsed is not None and parsed < 0:
        raise CollectorSchedulerError(f"{field_name} must be non-negative.")
    return parsed


def _optional_string_list(value: Any, field_name: str) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        raw = [item.strip() for item in value.split(",")]
    elif isinstance(value, (list, tuple)):
        raw = [str(item).strip() for item in value]
    else:
        raise CollectorSchedulerError(f"{field_name} must be a string or an array of strings.")
    return [item for item in raw if item]


def _dependency_error_list(value: Any, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise CollectorSchedulerError(f"{field_name} must be an array.")
    errors: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise CollectorSchedulerError(f"{field_name} items must be JSON objects.")
        errors.append(_jsonable_mapping(item))
    return errors


def _events_list(value: Any, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise CollectorSchedulerError(f"{field_name} must be an array.")
    events: list[dict[str, Any]] = []
    for index, item in enumerate(value, start=1):
        if not isinstance(item, Mapping):
            raise CollectorSchedulerError(f"{field_name} items must be JSON objects.")
        events.append(_normalize_run_event(item, index))
    return events


def _stage_timings_mapping(value: Any, field_name: str) -> dict[str, int | None]:
    if value is None:
        return _empty_stage_timings()
    if not isinstance(value, Mapping):
        raise CollectorSchedulerError(f"{field_name} must be a JSON object.")
    return _merge_stage_timings(value, {})


def _optional_error_category(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    text = str(value or "").strip()
    if not text:
        return None
    if text not in RUN_ERROR_CATEGORIES:
        raise CollectorSchedulerError(
            f"{field_name} must be one of: {', '.join(sorted(RUN_ERROR_CATEGORIES))}."
        )
    return text


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CollectorSchedulerError(f"{field_name} must be a JSON object.")
    return value


def _string_mapping(value: Any, field_name: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise CollectorSchedulerError(f"{field_name} must be a JSON object.")
    return {str(key): str(item) for key, item in value.items()}


def _jsonable_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _normalize_signature_value(item) for key, item in sorted(value.items())}


def _optional_non_negative_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return max(parsed, 0)


def _normalize_event_stage(value: Any) -> str:
    text = str(value or "").strip()
    return text or "unknown"


def _normalize_event_level(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text in {"info", "warning", "error"} else "info"


def _normalize_event_category(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value or "").strip()
    if not text:
        return None
    return text if text in RUN_ERROR_CATEGORIES else "unknown"


def _sanitize_event_details(value: Mapping[str, Any]) -> dict[str, Any]:
    details: dict[str, Any] = {}
    for index, (key, item) in enumerate(sorted(dict(value).items())):
        if index >= RUN_EVENT_DETAIL_LIMIT:
            details["truncated"] = True
            break
        key_text = str(key)
        if _looks_sensitive(key_text):
            details[key_text] = "<redacted>"
        else:
            details[key_text] = _truncate_detail(item)
    return details


def _truncate_detail(value: Any) -> Any:
    normalized = _normalize_signature_value(value)
    if isinstance(normalized, str):
        return _truncate_text(normalized, RUN_EVENT_STRING_LIMIT)
    if isinstance(normalized, list):
        return [_truncate_detail(item) for item in normalized[:RUN_EVENT_DETAIL_LIMIT]]
    if isinstance(normalized, Mapping):
        return _sanitize_event_details(normalized)
    return normalized


def _truncate_text(value: Any, limit: int) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return f"{text[: max(limit - 3, 0)]}..."


def _looks_sensitive(key: str) -> bool:
    lowered = key.lower()
    return any(marker in lowered for marker in ("token", "secret", "password", "passwd", "authorization", "api_key"))


def _normalize_signature_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_signature_value(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_normalize_signature_value(item) for item in value]
    if isinstance(value, set):
        return [_normalize_signature_value(item) for item in sorted(value, key=str)]
    if isinstance(value, Path):
        return str(value.expanduser().resolve())
    if isinstance(value, datetime):
        return value.isoformat()
    return value
