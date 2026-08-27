"""Minimal CollectorSpec runner.

Collector plugins only declare capabilities. New independent collectors run
through their declared ``runner_entry`` and then reuse the neutral AxData
writer, quality checker, and metadata log path. Legacy Provider-manifest
collectors continue to resolve DownloaderProfile and delegate to the downloader
engine for compatibility.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from importlib import import_module
from inspect import Parameter, signature
from pathlib import Path
from time import perf_counter
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4

import pandas as pd

from .downloader_engine import (
    DownloadMetadataWriter,
    DownloadQualityChecker,
    DownloadWriter,
    WriteStrategyError,
    emit_progress,
)


class CollectorError(RuntimeError):
    """Raised when a collector cannot be resolved or run."""


@dataclass(frozen=True)
class CollectorRunRequest:
    """Serializable request shape for a manual collector run."""

    collector_name: str
    params: dict[str, Any] | None = None
    fields: list[str] | None = None
    data_root: str | Path | None = None
    output_root: str | Path | None = None
    output_dir: str | Path | None = None
    formats: list[str] | tuple[str, ...] | str | None = None


@dataclass(frozen=True)
class CollectorRunPlan:
    """Resolved collector run target before source execution starts."""

    collector_name: str
    collector_id: str
    display_name_zh: str
    collector_plugin_id: str
    dataset_id: str | None
    provider_id: str | None
    effective_trust_level: str
    built_in: bool
    resource_group: str
    runner_entry: str | None
    is_legacy: bool
    legacy_source: str | None
    interfaces: tuple[str, ...]
    required_interfaces: tuple[str, ...]
    required_datasets: tuple[str, ...]
    downloader_profile: str | None
    target_interface: str | None
    params: dict[str, Any]
    fields: list[str] | None
    formats: list[str] | None
    output: dict[str, Any]
    quality: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "collector_name": self.collector_name,
            "name": self.collector_name,
            "collector_id": self.collector_id,
            "display_name_zh": self.display_name_zh,
            "collector_plugin_id": self.collector_plugin_id,
            "dataset_id": self.dataset_id,
            "provider_id": self.provider_id,
            "effective_trust_level": self.effective_trust_level,
            "built_in": self.built_in,
            "resource_group": self.resource_group,
            "runner_entry": self.runner_entry,
            "is_legacy": self.is_legacy,
            "legacy_source": self.legacy_source,
            "interfaces": list(self.interfaces),
            "required_interfaces": list(self.required_interfaces),
            "required_datasets": list(self.required_datasets),
            "downloader_profile": self.downloader_profile,
            "target_interface": self.target_interface,
            "params": dict(self.params),
            "fields": list(self.fields) if self.fields is not None else None,
            "formats": list(self.formats) if self.formats is not None else None,
            "output": dict(self.output),
            "quality": dict(self.quality),
        }


@dataclass(frozen=True)
class _IndependentCollectorProfile:
    interface_name: str
    collector_id: str
    dataset_id: str
    display_name: str
    downloader_type: str
    default_params: dict[str, Any]
    default_fields: list[str] | None
    output_layer: str
    output_format: str
    supported_formats: list[str]
    primary_key: str | tuple[str, ...]
    resource_group: str
    default_output_path_parts: list[str]
    snapshot_date_meta_keys: list[str]
    file_stem_template: str
    write_mode: str
    partition_by: list[str]
    quality_rules: list[str]
    required_columns: list[str]
    expected_columns: list[str]
    date_field: str | None
    datetime_field: str | None
    numeric_positive_columns: list[str]
    field_mappings: dict[str, str]
    calendar_check: bool
    field_types: dict[str, str] = field(default_factory=dict)


LOCAL_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
_DOWNLOAD_WRITER = DownloadWriter()
_QUALITY_CHECKER = DownloadQualityChecker()
_METADATA_WRITER = DownloadMetadataWriter()


def build_collector_run_plan(
    collector_name: str,
    *,
    params: dict[str, Any] | None = None,
    fields: list[str] | None = None,
    data_root: str | Path | None = None,
    formats: list[str] | tuple[str, ...] | str | None = None,
) -> CollectorRunPlan:
    """Resolve an enabled CollectorSpec into a runnable collector target."""

    from .collector_registry import build_collector_registry
    from .provider_catalog import build_builtin_provider_registry

    provider_registry = build_builtin_provider_registry(data_root=data_root)
    provider_snapshot = provider_registry.snapshot()
    collector_registry = build_collector_registry(
        provider_registry=provider_registry,
        data_root=data_root,
    )
    snapshot = collector_registry.snapshot()
    route = snapshot.collectors.get(collector_name)
    if route is None:
        known = ", ".join(sorted(snapshot.collectors)) or "<empty>"
        raise CollectorError(
            f"Collector {collector_name!r} is not available in the current Collector registry. "
            f"Known collectors: {known}."
        )

    plugin = route.plugin
    if plugin is None or not plugin.enabled:
        raise CollectorError(
            f"Collector {collector_name!r} belongs to collector plugin "
            f"{route.collector_plugin_id!r}, but that plugin is not enabled."
        )

    collector = route.collector
    compatibility_provider_id = route.provider_id or route.collector_plugin_id
    merged_params = dict(collector.default_params)
    merged_params.update(dict(params or {}))
    resolved_formats = _resolve_formats(formats, collector.output)
    if collector.runner_entry:
        return CollectorRunPlan(
            collector_name=collector.name,
            collector_id=route.collector_id,
            display_name_zh=collector.display_name_zh,
            collector_plugin_id=route.collector_plugin_id,
            dataset_id=collector.dataset_id,
            provider_id=compatibility_provider_id,
            effective_trust_level=route.effective_trust_level,
            built_in=route.built_in,
            resource_group=collector.resource_group,
            runner_entry=collector.runner_entry,
            is_legacy=route.is_legacy,
            legacy_source=route.legacy_source,
            interfaces=tuple(collector.interfaces),
            required_interfaces=tuple(collector.required_interfaces),
            required_datasets=tuple(collector.required_datasets),
            downloader_profile=None,
            target_interface=None,
            params=merged_params,
            fields=list(fields) if fields is not None else None,
            formats=resolved_formats,
            output=dict(collector.output),
            quality=dict(collector.quality),
        )

    _validate_interfaces_available(
        collector_name,
        tuple(collector.required_interfaces),
        provider_snapshot.interfaces,
        kind="required",
    )
    _validate_interfaces_available(
        collector_name,
        tuple(collector.interfaces),
        provider_snapshot.interfaces,
        kind="declared",
    )
    target_interface, downloader_profile = _resolve_target_downloader(
        collector,
        data_root=data_root,
    )
    if target_interface not in provider_snapshot.interfaces:
        raise CollectorError(
            f"Collector {collector_name!r} resolved target interface {target_interface!r}, "
            "but that interface is not available in the current Provider registry."
        )

    return CollectorRunPlan(
        collector_name=collector.name,
        collector_id=route.collector_id,
        display_name_zh=collector.display_name_zh,
        collector_plugin_id=route.collector_plugin_id,
        dataset_id=collector.dataset_id,
        provider_id=compatibility_provider_id,
        effective_trust_level=route.effective_trust_level,
        built_in=route.built_in,
        resource_group=collector.resource_group,
        runner_entry=collector.runner_entry,
        is_legacy=route.is_legacy,
        legacy_source=route.legacy_source,
        interfaces=tuple(collector.interfaces),
        required_interfaces=tuple(collector.required_interfaces),
        required_datasets=tuple(collector.required_datasets),
        downloader_profile=downloader_profile,
        target_interface=target_interface,
        params=merged_params,
        fields=list(fields) if fields is not None else None,
        formats=resolved_formats,
        output=dict(collector.output),
        quality=dict(collector.quality),
    )


def run_collector(
    collector_name: str,
    *,
    params: dict[str, Any] | None = None,
    fields: list[str] | None = None,
    data_root: str | Path | None = None,
    output_root: str | Path | None = None,
    output_dir: str | Path | None = None,
    formats: list[str] | tuple[str, ...] | str | None = None,
    collect_mode: str | None = None,
    connection_mode: str | None = None,
    concurrency_mode: str | None = None,
    connection_count: int | None = None,
    source_server_count: int | None = None,
    connections_per_server: int | None = None,
    max_concurrent_tasks: int | None = None,
    batch_size: int | None = None,
    request_interval_ms: int | None = None,
    retry_count: int | None = None,
    timeout_ms: int | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Run one enabled collector synchronously through the downloader engine."""

    started_perf = perf_counter()
    plan_started_perf = perf_counter()
    plan = build_collector_run_plan(
        collector_name,
        params=params,
        fields=fields,
        data_root=data_root,
        formats=formats,
    )
    provider_resolve_ms = _elapsed_ms(plan_started_perf)
    if plan.runner_entry:
        runner_started_perf = perf_counter()
        download_result = _run_independent_collector(
            plan,
            data_root=data_root,
            output_root=output_root,
            output_dir=output_dir,
            connection_count=connection_count,
            source_server_count=source_server_count,
            connections_per_server=connections_per_server,
            max_concurrent_tasks=max_concurrent_tasks,
            batch_size=batch_size,
            request_interval_ms=request_interval_ms,
            retry_count=retry_count,
            timeout_ms=timeout_ms,
            progress_callback=progress_callback,
        )
        runner_ms = _elapsed_ms(runner_started_perf)
        return {
            **plan.to_dict(),
            "status": download_result.get("status", "success"),
            "job_id": download_result.get("job_id"),
            "download_result": download_result,
            "collector_duration_breakdown_ms": {
                "params_resolve": 0,
                "provider_resolve": provider_resolve_ms,
                "download": runner_ms,
                "total": _elapsed_ms(started_perf),
            },
        }

    from .downloaders import run_downloader

    download_started_perf = perf_counter()
    if plan.target_interface is None:
        raise CollectorError(f"Legacy collector {collector_name!r} did not resolve a target interface.")
    download_result = run_downloader(
        plan.target_interface,
        params=plan.params,
        fields=plan.fields,
        data_root=data_root,
        output_root=output_root,
        output_dir=output_dir,
        formats=plan.formats,
        collect_mode=collect_mode,
        connection_mode=connection_mode,
        concurrency_mode=concurrency_mode,
        connection_count=connection_count,
        source_server_count=source_server_count,
        connections_per_server=connections_per_server,
        max_concurrent_tasks=max_concurrent_tasks,
        batch_size=batch_size,
        request_interval_ms=request_interval_ms,
        retry_count=retry_count,
        timeout_ms=timeout_ms,
        progress_callback=progress_callback,
    )
    download_ms = _elapsed_ms(download_started_perf)
    return {
        **plan.to_dict(),
        "status": download_result.get("status", "success"),
        "job_id": download_result.get("job_id"),
        "download_result": download_result,
        "collector_duration_breakdown_ms": {
            "params_resolve": 0,
            "provider_resolve": provider_resolve_ms,
            "download": download_ms,
            "total": _elapsed_ms(started_perf),
        },
    }


def _run_independent_collector(
    plan: CollectorRunPlan,
    *,
    data_root: str | Path | None,
    output_root: str | Path | None,
    output_dir: str | Path | None,
    connection_count: int | None = None,
    source_server_count: int | None = None,
    connections_per_server: int | None = None,
    max_concurrent_tasks: int | None = None,
    batch_size: int | None = None,
    request_interval_ms: int | None = None,
    retry_count: int | None = None,
    timeout_ms: int | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    _emit_progress(progress_callback, 1, "准备独立采集器")
    profile = _profile_from_plan(plan)
    selected_formats = _normalize_formats(plan.formats, profile)
    started_at = _utc_now()
    started_perf = perf_counter()
    run_id = f"run_{started_at.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"

    _emit_progress(progress_callback, 20, "执行采集器入口")
    fetch_started_perf = perf_counter()
    execution_options = {
        "connection_count": connection_count,
        "source_server_count": source_server_count,
        "connections_per_server": connections_per_server,
        "max_concurrent_tasks": max_concurrent_tasks,
        "batch_size": batch_size,
        "request_interval_ms": request_interval_ms,
        "retry_count": retry_count,
        "timeout_ms": timeout_ms,
    }
    raw_result = _call_runner_entry(
        plan,
        data_root=data_root,
        output_root=output_root,
        output_dir=output_dir,
        formats=selected_formats,
        execution_options=execution_options,
        progress_callback=progress_callback,
    )
    fetch_ms = _elapsed_ms(fetch_started_perf)

    _emit_progress(progress_callback, 70, "整理采集器返回数据")
    transform_started_perf = perf_counter()
    frame, runner_meta = _frame_and_meta_from_runner_result(raw_result)
    # 空结果不校验字段:0 行时 DataFrame 无列,字段缺失属正常(如午夜采当日电报)
    if plan.fields is not None and not frame.empty:
        missing = [field for field in plan.fields if field not in frame.columns]
        if missing:
            raise CollectorError(
                "Independent collector returned records missing requested field(s): "
                f"{', '.join(missing)}."
            )
        frame = frame.loc[:, plan.fields]
    records_for_date = frame.to_dict(orient="records")
    snapshot_date, snapshot_date_source = _snapshot_date(
        profile,
        runner_meta,
        started_at,
        records=records_for_date,
    )
    collection_time = _collection_time(started_at)
    file_stem = _file_stem(profile, snapshot_date=snapshot_date, collection_time=collection_time)
    transform_ms = _elapsed_ms(transform_started_perf)

    output_directory = _resolve_output_directory(
        profile,
        data_root=data_root,
        output_root=output_root,
        output_dir=output_dir,
    )
    # 写前防丢守卫:主键粒度过粗(大批行折叠到极少数键)直接拒绝写入,
    # 避免像 [trade_date, trade_date] 这类退化主键把全天数据压成一行
    _guard_primary_key_granularity(profile, frame)
    write_started_perf = perf_counter()
    try:
        write_result = _write_outputs_with_metadata(
            profile,
            frame,
            output_dir=output_directory,
            file_stem=file_stem,
            formats=selected_formats,
            progress_callback=progress_callback,
        )
    except WriteStrategyError as exc:
        raise CollectorError(str(exc)) from exc
    output_paths = dict(write_result["output_paths"])
    write_metadata = dict(write_result["metadata"])
    write_ms = _elapsed_ms(write_started_perf)

    _emit_progress(progress_callback, 96, "检查写出质量")
    quality_started_perf = perf_counter()
    quality = _QUALITY_CHECKER.evaluate(
        frame,
        primary_key=profile.primary_key,
        rules=profile.quality_rules,
        required_columns=profile.required_columns,
        expected_columns=profile.expected_columns,
        date_field=profile.date_field,
        datetime_field=profile.datetime_field,
        numeric_positive_columns=profile.numeric_positive_columns,
        field_mappings=profile.field_mappings,
        calendar_check=profile.calendar_check,
        trade_calendar_dates=_load_local_trade_calendar_dates(
            profile,
            runner_meta,
            data_root=data_root,
            output_root=output_root,
        ),
    )
    quality.update(_quality_write_metadata(write_metadata))
    quality["row_reconciliation"] = _reconcile_row_counts(
        frame,
        key_fields=_normalized_key_fields(profile.primary_key),
        write_metadata=write_metadata,
        output_paths=output_paths,
    )
    quality_ms = _elapsed_ms(quality_started_perf)
    _emit_progress(progress_callback, 100, "采集完成")

    finished_at = _utc_now()
    total_ms = _elapsed_ms(started_perf)
    response = {
        "job_id": run_id,
        "interface_name": profile.interface_name,
        "collector_id": plan.collector_id,
        "collector_plugin_id": plan.collector_plugin_id,
        "provider_id": plan.provider_id,
        "dataset_id": plan.dataset_id,
        "runner_entry": plan.runner_entry,
        "status": "success",
        "downloader_type": profile.downloader_type,
        "collect_mode": "runner_entry",
        "request_plan": {
            "collector_id": plan.collector_id,
            "runner_entry": plan.runner_entry,
            "params": dict(plan.params),
            "fields": list(plan.fields) if plan.fields is not None else None,
            "options": {},
            "adapter_options": {},
        },
        "params": dict(plan.params),
        "fields": list(plan.fields) if plan.fields is not None else None,
        "row_count": int(len(frame)),
        "snapshot_date": snapshot_date,
        "snapshot_date_source": snapshot_date_source,
        "collection_time": collection_time,
        "file_stem": file_stem,
        "connection_mode": "runner_entry",
        "connection_count": int(connection_count or max_concurrent_tasks or 0),
        "concurrency": {key: value for key, value in execution_options.items() if value is not None},
        "output_formats": selected_formats,
        "output_paths": {key: str(value) for key, value in output_paths.items()},
        "output_path": str(output_paths[selected_formats[0]]),
        "output": dict(plan.output),
        **_top_level_write_metadata(write_metadata),
        "write_metadata": write_metadata,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": total_ms,
        "duration_breakdown_ms": {
            "connection": 0,
            "runner_fetch": fetch_ms,
            "transform": transform_ms,
            "write": write_ms,
            "quality": quality_ms,
            "total": total_ms,
        },
        "source_meta": {
            "collector_id": plan.collector_id,
            "collector_plugin_id": plan.collector_plugin_id,
            "dataset_id": plan.dataset_id,
            "runner_entry": plan.runner_entry,
            "runner_meta": dict(runner_meta),
        },
        "runner_result": {"meta": dict(runner_meta)},
        "quality": quality,
    }
    log_path = _METADATA_WRITER.write_run_log(output_directory, file_stem=file_stem, result=response)
    response["log_path"] = str(log_path)
    return response


def _call_runner_entry(
    plan: CollectorRunPlan,
    *,
    data_root: str | Path | None,
    output_root: str | Path | None,
    output_dir: str | Path | None,
    formats: list[str],
    execution_options: Mapping[str, Any] | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> Any:
    runner = _load_runner_entry(plan.runner_entry)
    kwargs = {
        "params": dict(plan.params),
        "fields": list(plan.fields) if plan.fields is not None else None,
        "collector": plan.to_dict(),
        "data_root": Path(data_root).expanduser().resolve() if data_root is not None else None,
        "output_root": Path(output_root).expanduser().resolve() if output_root is not None else None,
        "output_dir": Path(output_dir).expanduser().resolve() if output_dir is not None else None,
        "formats": list(formats),
        "progress_callback": progress_callback,
        **{key: value for key, value in dict(execution_options or {}).items() if value is not None},
    }
    return runner(**_runner_kwargs(runner, kwargs))


def _load_runner_entry(runner_entry: str | None) -> Callable[..., Any]:
    if not runner_entry:
        raise CollectorError("Independent collector must declare runner_entry.")
    module_name, separator, function_name = runner_entry.partition(":")
    if not separator or not module_name.strip() or not function_name.strip():
        raise CollectorError("runner_entry must use 'module:function' syntax.")
    module = import_module(module_name)
    target: Any = module
    for part in function_name.split("."):
        target = getattr(target, part)
    if not callable(target):
        raise CollectorError(f"runner_entry {runner_entry!r} is not callable.")
    return target


def _runner_kwargs(runner: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        parameters = signature(runner).parameters
    except (TypeError, ValueError):
        return kwargs
    if any(parameter.kind == Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return kwargs
    accepted = {
        name
        for name, parameter in parameters.items()
        if parameter.kind
        in {
            Parameter.POSITIONAL_OR_KEYWORD,
            Parameter.KEYWORD_ONLY,
        }
    }
    return {key: value for key, value in kwargs.items() if key in accepted}


def _frame_and_meta_from_runner_result(result: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
    meta: dict[str, Any] = {}
    records: Any = result
    if isinstance(result, pd.DataFrame):
        return result.copy(), meta
    if isinstance(result, Mapping):
        meta_value = result.get("meta")
        if isinstance(meta_value, Mapping):
            meta = dict(meta_value)
        for key in ("records", "data", "rows"):
            if key in result:
                records = result[key]
                break
        else:
            records = result
    if isinstance(records, pd.DataFrame):
        return records.copy(), meta
    if records is None:
        return pd.DataFrame(), meta
    if isinstance(records, Mapping):
        return pd.DataFrame.from_records([dict(records)]), meta
    try:
        return pd.DataFrame.from_records(records), meta
    except TypeError as exc:
        raise CollectorError("runner_entry must return records, data, rows, or a pandas DataFrame.") from exc


def _profile_from_plan(plan: CollectorRunPlan) -> _IndependentCollectorProfile:
    output = dict(plan.output or {})
    quality = dict(plan.quality or {})
    dataset_id = plan.dataset_id or plan.collector_id
    output_layer = str(output.get("output_layer") or output.get("layer") or "snapshot")
    supported_formats = [
        item.lower()
        for item in _string_list(output.get("supported_formats") or output.get("formats") or ["parquet"])
    ]
    if not supported_formats:
        supported_formats = ["parquet"]
    output_format = supported_formats[0]
    primary_key_values = _string_list(
        output.get("primary_key")
        or quality.get("primary_key")
        or quality.get("required_columns")
        or output.get("required_columns")
        or []
    )
    primary_key: str | tuple[str, ...]
    if len(primary_key_values) == 1:
        primary_key = primary_key_values[0]
    else:
        primary_key = tuple(primary_key_values)
    quality_rules = _string_list(quality.get("rules") or output.get("quality_rules") or [])
    if not quality_rules:
        quality_rules = ["schema", "row_count"]
        if primary_key_values:
            quality_rules.append("primary_key")
    default_dir_name = str(output.get("default_dir_name") or dataset_id).strip() or dataset_id
    default_output_path_parts = _string_list(output.get("default_output_path_parts") or [])
    return _IndependentCollectorProfile(
        interface_name=dataset_id,
        collector_id=plan.collector_id,
        dataset_id=dataset_id,
        display_name=plan.display_name_zh,
        downloader_type="collector_runner_entry",
        default_params=dict(plan.params),
        default_fields=plan.fields,
        output_layer=output_layer,
        output_format=output_format,
        supported_formats=supported_formats,
        primary_key=primary_key,
        resource_group=plan.resource_group,
        default_output_path_parts=default_output_path_parts or [output_layer, default_dir_name],
        snapshot_date_meta_keys=_string_list(
            output.get("snapshot_date_meta_keys")
            or ["snapshot_date", "data_date", "trade_date", "date"]
        ),
        file_stem_template=_file_stem_template(output.get("file_name_template")),
        write_mode=str(output.get("write_mode") or "snapshot"),
        partition_by=_string_list(output.get("partition_by") or []),
        quality_rules=quality_rules,
        required_columns=_string_list(
            quality.get("required_columns")
            or output.get("required_columns")
            or primary_key_values
        ),
        expected_columns=_string_list(quality.get("expected_columns") or output.get("expected_columns") or []),
        date_field=_optional_string(output.get("date_field") or quality.get("date_field")),
        datetime_field=_optional_string(output.get("datetime_field") or quality.get("datetime_field")),
        numeric_positive_columns=_string_list(
            quality.get("numeric_positive_columns")
            or output.get("numeric_positive_columns")
            or []
        ),
        field_mappings=_string_mapping(quality.get("field_mappings") or output.get("field_mappings") or {}),
        calendar_check=_truthy(quality.get("calendar_check") or output.get("calendar_check") or False),
        field_types=_declared_field_types(output),
    )


def _normalized_key_fields(primary_key: Any) -> list[str]:
    if primary_key is None:
        return []
    if isinstance(primary_key, str):
        primary_key = (primary_key,)
    return [str(item) for item in primary_key if str(item).strip()]


def _frame_distinct_keys(frame: "pd.DataFrame", key_fields: list[str]) -> int | None:
    usable = [field for field in key_fields if field in frame.columns]
    if not usable or frame.empty:
        return None
    return int(frame.drop_duplicates(subset=usable).shape[0])


def _guard_primary_key_granularity(profile: _IndependentCollectorProfile, frame: "pd.DataFrame") -> None:
    """写前防丢守卫:主键粒度过粗(过半行折叠到重复键)时拒绝写入。

    典型场景是推断出的退化主键(如 [trade_date, trade_date]),
    会让 upsert 把全天多行数据压成一行,数据静默丢失。
    """
    key_fields = _normalized_key_fields(profile.primary_key)
    if not key_fields or frame.empty:
        return
    total = int(len(frame))
    distinct = _frame_distinct_keys(frame, key_fields)
    if distinct is None or distinct * 2 > total:
        return
    raise CollectorError(
        f"Primary key granularity guard: {total} collected rows collapse to only "
        f"{distinct} distinct key(s) on ({', '.join(key_fields)}); refusing to write "
        "because upsert would silently drop rows. Check the collector primary_key."
    )


def _reconcile_row_counts(
    frame: "pd.DataFrame",
    *,
    key_fields: list[str],
    write_metadata: dict[str, Any],
    output_paths: dict[str, Any],
) -> dict[str, Any]:
    """行数对账:采集行数 / 写入行数 / 落盘行数三点必须一致。"""
    collected = int(len(frame))
    distinct = _frame_distinct_keys(frame, key_fields) if key_fields else None
    rows_written = _optional_int(write_metadata.get("rows_written"))
    rows_after = _optional_int(write_metadata.get("rows_after"))
    dropped = _optional_int(write_metadata.get("duplicate_rows_dropped")) or 0
    stored = _parquet_metadata_row_count(output_paths.get("parquet")) if output_paths.get("parquet") else None

    problems: list[str] = []
    if rows_written is not None and rows_written != collected:
        problems.append(f"rows_written={rows_written} != collected_rows={collected}")
    if stored is not None and rows_after is not None and stored != rows_after:
        problems.append(f"stored_rows={stored} != rows_after={rows_after}")
    if rows_after is not None and distinct is not None and rows_after < distinct:
        problems.append(f"rows_after={rows_after} < distinct_primary_keys={distinct}")
    reconciliation: dict[str, Any] = {
        "collected_rows": collected,
        "distinct_primary_keys": distinct,
        "rows_written": rows_written,
        "rows_after": rows_after,
        "stored_rows": stored,
        "duplicate_rows_dropped": dropped,
        "status": "error" if problems else "ok",
    }
    if problems:
        reconciliation["message"] = "; ".join(problems)
    return reconciliation


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parquet_metadata_row_count(path: Any) -> int | None:
    """仅读 parquet 文件元数据统计行数,不加载数据本体。"""
    try:
        import pyarrow.parquet as pq

        target = Path(path)
        files = [target] if target.is_file() else sorted(target.rglob("*.parquet"))
        if not files:
            return None
        return int(sum(pq.read_metadata(file).num_rows for file in files))
    except Exception:
        return None


def _declared_field_types(output: dict[str, Any]) -> dict[str, str]:
    """从 datasets 字段声明提取列名到类型的映射,写盘前统一列类型。"""

    declared: dict[str, str] = {}
    datasets = output.get("datasets") or []
    if isinstance(datasets, Mapping):
        datasets = [datasets]
    for dataset in datasets if isinstance(datasets, Sequence) else []:
        if not isinstance(dataset, Mapping):
            continue
        for field_decl in dataset.get("fields") or []:
            if not isinstance(field_decl, Mapping):
                continue
            name = str(field_decl.get("name") or "").strip()
            dtype = str(field_decl.get("type") or field_decl.get("dtype") or "").strip()
            if name and dtype:
                declared.setdefault(name, dtype)
    return declared


def _elapsed_ms(started_perf: float) -> int:
    return max(0, int((perf_counter() - started_perf) * 1000))


def _validate_interfaces_available(
    collector_name: str,
    interface_names: tuple[str, ...],
    available_interfaces: Any,
    *,
    kind: str,
) -> None:
    missing = [name for name in interface_names if name not in available_interfaces]
    if not missing:
        return
    label = "required" if kind == "required" else "declared"
    raise CollectorError(
        f"Collector {collector_name!r} references unavailable {label} interface(s): "
        f"{', '.join(missing)}."
    )


def _resolve_target_downloader(
    collector: Any,
    *,
    data_root: str | Path | None,
) -> tuple[str, str | None]:
    if collector.downloader_profile:
        profile = _find_downloader_profile_by_name(
            collector.downloader_profile,
            data_root=data_root,
        )
        target_interface = str(profile.get("interface_name") or "")
        if not target_interface:
            raise CollectorError(
                f"Collector {collector.name!r} downloader profile "
                f"{collector.downloader_profile!r} does not declare an interface."
            )
        if collector.interfaces and target_interface not in set(collector.interfaces):
            raise CollectorError(
                f"Collector {collector.name!r} downloader profile "
                f"{collector.downloader_profile!r} targets {target_interface!r}, "
                "which is not listed in collector.interfaces."
            )
        return target_interface, collector.downloader_profile

    interfaces = tuple(collector.interfaces)
    if len(interfaces) != 1:
        raise CollectorError(
            f"Collector {collector.name!r} must declare downloader_profile or exactly one interface "
            "for the minimal runner."
        )
    target_interface = interfaces[0]
    from .downloaders import DownloaderError, get_downloader_profile

    try:
        profile = get_downloader_profile(target_interface, data_root=data_root)
    except DownloaderError as exc:
        raise CollectorError(
            f"Collector {collector.name!r} cannot run because interface "
            f"{target_interface!r} has no available downloader profile."
        ) from exc
    return target_interface, profile.manifest_downloader_name


def _find_downloader_profile_by_name(
    profile_name: str,
    *,
    data_root: str | Path | None,
) -> dict[str, Any]:
    from .downloaders import list_downloader_profiles

    profiles = [dict(profile) for profile in list_downloader_profiles(data_root=data_root)]
    for profile in profiles:
        if profile.get("manifest_downloader_name") == profile_name:
            return profile
    for profile in profiles:
        if profile.get("interface_name") == profile_name:
            return profile
    known = ", ".join(
        sorted(
            str(profile.get("manifest_downloader_name") or profile.get("interface_name"))
            for profile in profiles
            if profile.get("manifest_downloader_name") or profile.get("interface_name")
        )
    ) or "<empty>"
    raise CollectorError(
        f"Collector downloader profile {profile_name!r} is not available in the current "
        f"Provider registry. Known downloader profiles: {known}."
    )


def _resolve_formats(
    formats: list[str] | tuple[str, ...] | str | None,
    output: Any,
) -> list[str] | None:
    if formats is not None:
        return _string_list(formats)
    if not isinstance(output, dict):
        return None
    output_formats = output.get("formats")
    if output_formats is None:
        return None
    resolved = _string_list(output_formats)
    return resolved or None


def _normalize_formats(
    formats: list[str] | tuple[str, ...] | str | None,
    profile: _IndependentCollectorProfile,
) -> list[str]:
    requested = _string_list(formats) if formats is not None else [profile.output_format]
    selected: list[str] = []
    for item in requested:
        normalized = item.lower()
        if normalized not in profile.supported_formats:
            supported = ", ".join(profile.supported_formats)
            raise CollectorError(f"Unsupported output format {item!r}; supported formats: {supported}.")
        if normalized not in selected:
            selected.append(normalized)
    if not selected:
        raise CollectorError("At least one output format is required.")
    return selected


def _string_list(value: list[str] | tuple[str, ...] | str | Any) -> list[str]:
    if isinstance(value, str):
        raw = value.split(",")
    else:
        try:
            raw = list(value)
        except TypeError:
            raw = [value]
    return [str(item).strip() for item in raw if str(item).strip()]


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _string_mapping(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _load_local_trade_calendar_dates(
    profile: _IndependentCollectorProfile,
    runner_meta: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    output_root: str | Path | None,
) -> dict[str, list[str]] | None:
    if not profile.calendar_check:
        return None
    interface_name = str(runner_meta.get("interface_name") or "").strip()
    if interface_name:
        from .downloaders import _load_local_trade_calendar_dates as load_dates
        from .downloaders import get_downloader_profile

        try:
            downloader_profile = get_downloader_profile(interface_name, data_root=data_root)
        except Exception:
            downloader_profile = None
        if downloader_profile is not None:
            return load_dates(downloader_profile, data_root=data_root, output_root=output_root)
    from .downloaders import _candidate_calendar_roots, _read_trade_calendar_dates

    for root in _candidate_calendar_roots(data_root=data_root, output_root=output_root):
        dates = _read_trade_calendar_dates(root)
        if dates:
            return dates
    return None


def _emit_progress(progress_callback: Callable[..., None] | None, percent: int, message: str, **details: Any) -> None:
    emit_progress(progress_callback, percent, message, **details)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _collection_time(value: datetime) -> str:
    return value.astimezone(LOCAL_TIMEZONE).strftime("%Y%m%d_%H%M")


def _file_stem(profile: _IndependentCollectorProfile, *, snapshot_date: str, collection_time: str) -> str:
    return profile.file_stem_template.format(
        interface_name=profile.interface_name,
        collector_id=profile.collector_id,
        dataset_id=profile.dataset_id,
        snapshot_date=snapshot_date,
        data_date=snapshot_date,
        run_time=collection_time,
        collection_time=collection_time,
    )


def _file_stem_template(value: Any) -> str:
    text = str(value or "").strip()
    return text or "{interface_name}_{snapshot_date}_{collection_time}"


def _snapshot_date(
    profile: _IndependentCollectorProfile,
    meta: dict[str, Any],
    fallback_time: datetime,
    *,
    records: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    for key in profile.snapshot_date_meta_keys:
        value = meta.get(key)
        normalized = _normalize_snapshot_date(value)
        if normalized is not None:
            return normalized, key
    record_trade_dates = {
        normalized
        for record in records or []
        if (normalized := _normalize_snapshot_date(record.get("trade_date"))) is not None
    }
    if len(record_trade_dates) == 1:
        return next(iter(record_trade_dates)), "record_trade_date"
    return fallback_time.astimezone(LOCAL_TIMEZONE).strftime("%Y%m%d"), "collected_at"


def _normalize_snapshot_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


def _resolve_output_directory(
    profile: _IndependentCollectorProfile,
    *,
    data_root: str | Path | None,
    output_root: str | Path | None,
    output_dir: str | Path | None,
) -> Path:
    if output_dir is not None:
        return Path(output_dir).expanduser().resolve()
    if output_root is not None:
        root_path = Path(output_root).expanduser().resolve()
    elif data_root is not None:
        root_path = Path(data_root).expanduser().resolve()
    else:
        root_path = (Path.cwd() / "data").resolve()
    return root_path.joinpath(*profile.default_output_path_parts)


def _write_outputs_with_metadata(
    profile: _IndependentCollectorProfile,
    frame: pd.DataFrame,
    *,
    output_dir: Path,
    file_stem: str,
    formats: list[str],
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    result = _DOWNLOAD_WRITER.write_outputs_with_metadata(
        profile,
        frame,
        output_dir=output_dir,
        file_stem=file_stem,
        formats=formats,
        progress_callback=progress_callback,
    )
    output_paths = {
        str(format_name): Path(path)
        for format_name, path in dict(result.output_paths).items()
    }
    return {
        "output_paths": output_paths,
        "metadata": dict(result.metadata.to_dict()),
    }


def _top_level_write_metadata(write_metadata: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "write_mode",
        "partition_by",
        "primary_key",
        "date_field",
        "replace_range_start",
        "replace_range_end",
        "rows_before",
        "rows_written",
        "rows_after",
        "duplicate_rows_dropped",
        "partitions_touched",
    )
    return {key: write_metadata.get(key) for key in keys}


def _quality_write_metadata(write_metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "write_mode": write_metadata.get("write_mode"),
        "partition_by": list(write_metadata.get("partition_by") or []),
        "write_primary_key": list(write_metadata.get("primary_key") or []),
        "write_date_field": write_metadata.get("date_field"),
        "replace_range_start": write_metadata.get("replace_range_start"),
        "replace_range_end": write_metadata.get("replace_range_end"),
        "rows_before": write_metadata.get("rows_before"),
        "rows_written": write_metadata.get("rows_written"),
        "rows_after": write_metadata.get("rows_after"),
        "duplicate_rows_dropped": write_metadata.get("duplicate_rows_dropped"),
        "partitions_touched": list(write_metadata.get("partitions_touched") or []),
    }
