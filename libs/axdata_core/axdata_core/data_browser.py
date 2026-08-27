"""Local dataset discovery and small preview queries."""

from __future__ import annotations

import json
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .collector_registry import build_collector_registry
from .collector_scheduler import CollectorSchedulerStore, collector_scheduler_store_path
from .schema import Field, TableSchema, get_schema, list_tables
from .storage import core_table_path, core_table_partition_path

DEFAULT_PREVIEW_LIMIT = 3
MAX_PREVIEW_LIMIT = 100
MAX_DISCOVERY_RUNS = 500
MAX_DOWNLOADER_LOGS_PER_DIR = 200
MAX_MISSING_PATHS = 20
MAX_PARQUET_STATS_FILES = 200
MAX_PARQUET_STATS_DIRS = 2000
KNOWN_DATA_LAYERS = frozenset({"raw", "staging", "core", "factor", "snapshot", "snapshots"})
DATASET_FORMAT_DIRS = frozenset({"parquet", "csv", "duckdb", "jsonl", "logs"})
_NO_MATCHING_PARQUET_PARTITIONS = object()


class DataBrowserError(ValueError):
    """Raised when local dataset discovery or preview cannot continue."""


@dataclass
class DatasetSummary:
    """A locally discovered dataset and its user-facing metadata."""

    dataset: str
    interface_name: str
    display_name_zh: str | None = None
    description: str = ""
    provider: str | None = None
    source: str | None = None
    layer: str | None = None
    output_paths: dict[str, str] = field(default_factory=dict)
    row_count: int | None = None
    date_min: str | None = None
    date_max: str | None = None
    datetime_min: str | None = None
    datetime_max: str | None = None
    columns: list[str] = field(default_factory=list)
    quality_status: str | None = None
    quality_warnings: list[str] = field(default_factory=list)
    quality_errors: list[str] = field(default_factory=list)
    quality: dict[str, Any] = field(default_factory=dict)
    latest_run_id: str | None = None
    latest_run_status: str | None = None
    updated_at: str | None = None
    missing_paths: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    write_mode: str | None = None
    partition_by: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    date_field: str | None = None
    replace_range_start: str | None = None
    replace_range_end: str | None = None
    rows_before: int | None = None
    rows_written: int | None = None
    rows_after: int | None = None
    duplicate_rows_dropped: int | None = None
    partitions_touched: list[str] = field(default_factory=list)
    field_schema: list[dict[str, Any]] = field(default_factory=list)
    logical_table: str | None = None
    storage_layout: str | None = None
    default_query_fields: list[str] = field(default_factory=list)
    default_filter_fields: list[str] = field(default_factory=list)
    available_formats: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "interface_name": self.interface_name,
            "display_name_zh": self.display_name_zh,
            "description": self.description,
            "provider": self.provider,
            "source": self.source,
            "layer": self.layer,
            "output_paths": dict(sorted(self.output_paths.items())),
            "row_count": self.row_count,
            "date_min": self.date_min,
            "date_max": self.date_max,
            "datetime_min": self.datetime_min,
            "datetime_max": self.datetime_max,
            "columns": list(self.columns),
            "quality_status": self.quality_status,
            "quality_warnings": list(self.quality_warnings),
            "quality_errors": list(self.quality_errors),
            "quality": _jsonable(self.quality),
            "latest_run_id": self.latest_run_id,
            "latest_run_status": self.latest_run_status,
            "updated_at": self.updated_at,
            "missing_paths": list(self.missing_paths),
            "metadata": _jsonable(self.metadata),
            "write_mode": self.write_mode,
            "partition_by": list(self.partition_by),
            "primary_key": list(self.primary_key),
            "date_field": self.date_field,
            "replace_range_start": self.replace_range_start,
            "replace_range_end": self.replace_range_end,
            "rows_before": self.rows_before,
            "rows_written": self.rows_written,
            "rows_after": self.rows_after,
            "duplicate_rows_dropped": self.duplicate_rows_dropped,
            "partitions_touched": list(self.partitions_touched),
            "field_schema": _jsonable(self.field_schema),
            "logical_table": self.logical_table,
            "storage_layout": self.storage_layout,
            "default_query_fields": list(self.default_query_fields),
            "default_filter_fields": list(self.default_filter_fields),
            "available_formats": list(self.available_formats),
        }


@dataclass(frozen=True)
class DataPreview:
    """Small filtered preview result for one dataset."""

    dataset: DatasetSummary
    rows: list[dict[str, Any]]
    limit: int
    filters: dict[str, Any]
    columns: list[str]
    preview_format: str = "parquet"
    preview_paths: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset.to_dict(),
            "rows": _jsonable(self.rows),
            "limit": self.limit,
            "filters": _jsonable(self.filters),
            "columns": list(self.columns),
            "preview_format": self.preview_format,
            "preview_paths": list(self.preview_paths),
            "count": len(self.rows),
        }


def list_datasets(
    *,
    data_root: str | Path | None = None,
    include_core: bool = True,
) -> list[DatasetSummary]:
    """Discover local datasets from run metadata and known core parquet paths."""

    root = _resolve_data_root(data_root)
    entries: dict[str, DatasetSummary] = {}

    for summary in _declared_dataset_summaries(root):
        _merge_summary(entries, summary)

    for run in _load_collector_runs(root):
        summary = _summary_from_run(root, run)
        if summary is None:
            continue
        _merge_summary(entries, summary)

    for log_payload in _iter_downloader_logs(root, entries):
        summary = _summary_from_log(root, log_payload)
        if summary is None:
            continue
        _merge_summary(entries, summary)

    if include_core:
        for table in list_tables():
            summary = _summary_from_core_table(root, table)
            if summary is not None:
                _merge_summary(entries, summary)

    local_entries = [
        summary
        for summary in entries.values()
        if _has_local_output_reference(summary)
    ]
    return sorted(
        local_entries,
        key=lambda item: (
            item.updated_at is not None,
            item.updated_at or "",
            item.dataset,
        ),
        reverse=True,
    )


def get_dataset(
    dataset: str,
    *,
    data_root: str | Path | None = None,
) -> DatasetSummary:
    """Return one discovered dataset by dataset name or interface name."""

    normalized = _normalize_dataset_name(dataset)
    candidates = list_datasets(data_root=data_root)
    for summary in candidates:
        if _normalize_dataset_name(summary.dataset) == normalized:
            return summary
    for summary in candidates:
        if _normalize_dataset_name(summary.interface_name) == normalized:
            return summary
    known = ", ".join(item.dataset for item in candidates) or "<empty>"
    raise DataBrowserError(f"Dataset {dataset!r} was not found. Known datasets: {known}.")


def preview_dataset(
    dataset: str,
    *,
    data_root: str | Path | None = None,
    fields: Sequence[str] | str | None = None,
    filters: Mapping[str, Any] | None = None,
    symbol: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int | None = DEFAULT_PREVIEW_LIMIT,
) -> DataPreview:
    """Preview at most 100 rows from one local dataset."""

    summary = get_dataset(dataset, data_root=data_root)
    if summary.missing_paths:
        missing = ", ".join(summary.missing_paths)
        raise FileNotFoundError(
            f"Dataset {summary.dataset!r} has stale output path metadata. Missing path(s): {missing}."
        )
    parquet_paths = _existing_parquet_paths(summary.output_paths)
    if not parquet_paths:
        raise FileNotFoundError(
            f"Dataset {summary.dataset!r} has no existing parquet output. "
            "Run a Collector/Downloader first, or choose a dataset with parquet output."
        )

    normalized_limit = clamp_limit(limit)
    query_filters = _normalize_preview_filters(summary, filters, symbol)
    start_date = _normalize_date_text(start)
    end_date = _normalize_date_text(end)
    selected_fields = _normalize_fields(fields)
    rows, columns = _query_parquet(
        parquet_paths,
        fields=selected_fields,
        filters=query_filters,
        date_field=_date_field(summary),
        start=start_date,
        end=end_date,
        limit=normalized_limit,
        known_columns=summary.columns,
    )
    return DataPreview(
        dataset=summary,
        rows=rows,
        limit=normalized_limit,
        filters={**query_filters, **_date_filter_payload(start_date, end_date)},
        columns=columns,
        preview_format="parquet",
        preview_paths=[str(path) for path in parquet_paths],
    )


def delete_dataset(
    dataset: str,
    *,
    data_root: str | Path | None = None,
) -> dict[str, Any]:
    """Delete one local dataset directory and stale run/log references."""

    root = _resolve_data_root(data_root)
    summary = get_dataset(dataset, data_root=root)
    safe_paths = _safe_dataset_delete_paths(summary, root)
    deleted_paths: list[str] = []
    missing_paths: list[str] = []
    for path in safe_paths:
        if not path.exists():
            missing_paths.append(str(path))
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        deleted_paths.append(str(path))

    deleted_logs = _delete_dataset_log_files(summary, root)
    deleted_runs = _delete_dataset_runs(summary, root, safe_paths)

    if not deleted_paths and not deleted_logs and not deleted_runs:
        raise DataBrowserError(f"Dataset {summary.dataset!r} has no AxData-owned local output to delete.")

    return {
        "dataset": summary.to_dict(),
        "deleted_paths": deleted_paths,
        "missing_paths": missing_paths,
        "deleted_logs": deleted_logs,
        "deleted_runs": deleted_runs,
    }


def _safe_dataset_delete_paths(summary: DatasetSummary, root: Path) -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()
    for path_text in summary.output_paths.values():
        path = _safe_axdata_data_path(path_text, root)
        path = _dataset_delete_root_for_path(path, summary, root)
        if path not in seen:
            seen.add(path)
            paths.append(path)
    if not paths:
        raise DataBrowserError(f"Dataset {summary.dataset!r} has no local output path.")
    return paths


def _dataset_delete_root_for_path(path: Path, summary: DatasetSummary, root: Path) -> Path:
    root_resolved = root.resolve()
    names = _dataset_path_names(summary)
    for layer_root in _allowed_dataset_roots(root_resolved):
        if not _is_relative_to(path, layer_root):
            continue
        relative = path.relative_to(layer_root)
        parts = relative.parts
        if not parts:
            break
        for index, part in enumerate(parts):
            if _dataset_component_matches(part, names):
                return _safe_axdata_data_path(str(layer_root.joinpath(*parts[: index + 1])), root)
        if layer_root.name == "core" and parts[0].startswith("table="):
            return _safe_axdata_data_path(str(layer_root / parts[0]), root)
        if len(parts) == 1:
            return _safe_axdata_data_path(str(path), root)
        if len(parts) > 1 and parts[1] in DATASET_FORMAT_DIRS:
            return _safe_axdata_data_path(str(layer_root / parts[0]), root)
        if len(parts) > 2 and parts[2] in DATASET_FORMAT_DIRS:
            return _safe_axdata_data_path(str(layer_root / parts[0] / parts[1]), root)
        return _safe_axdata_data_path(str(layer_root / parts[0]), root)
    return path


def _dataset_path_names(summary: DatasetSummary) -> set[str]:
    names = {
        _normalize_path_component(summary.dataset),
        _normalize_path_component(summary.interface_name),
    }
    names.discard("")
    expanded = set(names)
    for name in names:
        expanded.add(f"table={name}")
        expanded.add(f"interface={name}")
        if "." in name:
            expanded.add(name.rsplit(".", 1)[-1])
    return expanded


def _normalize_path_component(value: str | None) -> str:
    return str(value or "").strip().lower().replace("\\", "/")


def _dataset_component_matches(component: str, names: set[str]) -> bool:
    text = _normalize_path_component(component)
    if text in names:
        return True
    if "=" in text:
        key, value = text.split("=", 1)
        return value in names or f"{key}={value}" in names
    return False


def _safe_axdata_data_path(path_text: str, root: Path) -> Path:
    if not str(path_text).strip():
        raise DataBrowserError("Dataset output path is empty.")
    root_resolved = root.resolve()
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = root_resolved / path
    resolved = path.resolve()
    if resolved == root_resolved or not _is_relative_to(resolved, root_resolved):
        raise DataBrowserError(f"Refusing to delete path outside AxData data directory: {resolved}")
    allowed_roots = _allowed_dataset_roots(root_resolved)
    if not any(resolved != allowed and _is_relative_to(resolved, allowed) for allowed in allowed_roots):
        raise DataBrowserError(f"Refusing to delete non-dataset path: {resolved}")
    return resolved


def _allowed_dataset_roots(root_resolved: Path) -> list[Path]:
    return [
        root_resolved / "raw",
        root_resolved / "staging",
        root_resolved / "core",
        root_resolved / "factor",
        root_resolved / "snapshot",
        root_resolved / "snapshots",
    ]


def _delete_dataset_log_files(summary: DatasetSummary, root: Path) -> list[str]:
    deleted: list[str] = []
    log_path = summary.metadata.get("log_path") if isinstance(summary.metadata, Mapping) else None
    if not isinstance(log_path, str) or not log_path.strip():
        return deleted
    try:
        path = _safe_axdata_data_path(log_path, root)
    except DataBrowserError:
        return deleted
    if path.is_file() and path.suffix.lower() == ".json":
        path.unlink()
        deleted.append(str(path))
    return deleted


def _delete_dataset_runs(summary: DatasetSummary, root: Path, deleted_targets: Sequence[Path]) -> list[str]:
    store_path = collector_scheduler_store_path(data_root=root)
    if not store_path.exists():
        return []
    targets = tuple(path.resolve() for path in deleted_targets)
    store = CollectorSchedulerStore(data_root=root)
    run_ids: list[str] = []
    for run in store.list_runs():
        run_paths = _safe_run_output_paths(run, root)
        if any(_paths_overlap(run_path, target) for run_path in run_paths for target in targets):
            run_ids.append(str(run.run_id))
    return [run.run_id for run in store.delete_runs(run_ids)]


def _safe_run_output_paths(run: Any, root: Path) -> set[Path]:
    output_paths: dict[str, Any] = {}
    raw_output_paths = getattr(run, "output_paths", None)
    if isinstance(raw_output_paths, Mapping):
        output_paths.update(raw_output_paths)
    result = getattr(run, "result", None)
    if isinstance(result, Mapping):
        download_result = result.get("download_result")
        if isinstance(download_result, Mapping) and isinstance(download_result.get("output_paths"), Mapping):
            output_paths.update(download_result["output_paths"])
    paths: set[Path] = set()
    for path_text in output_paths.values():
        if not isinstance(path_text, str):
            continue
        try:
            paths.add(_safe_axdata_data_path(path_text, root))
        except DataBrowserError:
            continue
    return paths


def _paths_overlap(left: Path, right: Path) -> bool:
    if left == right:
        return True
    return _is_relative_to(left, right) or _is_relative_to(right, left)


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def clamp_limit(limit: int | None, *, default: int = DEFAULT_PREVIEW_LIMIT, maximum: int = MAX_PREVIEW_LIMIT) -> int:
    if limit is None:
        return default
    try:
        value = int(limit)
    except (TypeError, ValueError) as exc:
        raise DataBrowserError("limit must be an integer.") from exc
    if value <= 0:
        raise DataBrowserError("limit must be positive.")
    return min(value, maximum)


def _resolve_data_root(data_root: str | Path | None) -> Path:
    return Path(data_root or os.getenv("AXDATA_DATA_DIR", "data")).expanduser().resolve()


def _load_collector_runs(root: Path) -> tuple[Any, ...]:
    store_path = collector_scheduler_store_path(data_root=root)
    if not store_path.exists():
        return ()
    try:
        return CollectorSchedulerStore(data_root=root).list_runs(limit=MAX_DISCOVERY_RUNS)
    except Exception:
        return ()


def _declared_dataset_summaries(root: Path) -> Iterable[DatasetSummary]:
    try:
        registry = build_collector_registry(data_root=root)
    except Exception:
        return ()
    summaries: list[DatasetSummary] = []
    for registration in registry.list_collectors():
        collector = registration.collector
        output = dict(collector.output or {})
        for declaration in _collector_output_declarations(collector, output):
            dataset = str(declaration.get("dataset_id") or collector.dataset_id or collector.collector_id)
            layer = _string_or_none(declaration.get("layer") or output.get("layer") or output.get("output_layer"))
            declared_paths = _declared_output_paths(root, declaration, output)
            actual_paths = {
                key: value
                for key, value in declared_paths.items()
                if Path(value).exists()
            }
            summary = DatasetSummary(
                dataset=dataset,
                interface_name=str(declaration.get("table") or declaration.get("logical_table") or dataset),
                display_name_zh=_string_or_none(declaration.get("display_name_zh")) or collector.display_name_zh,
                description=str(declaration.get("description") or collector.description or ""),
                provider=registration.collector_plugin_id,
                source=_string_or_none(declaration.get("source") or registration.collector_plugin_id),
                layer=layer,
                output_paths=actual_paths,
                columns=_string_list(declaration.get("columns") or declaration.get("default_query_fields")),
                metadata={
                    "collector_name": collector.collector_id,
                    "collector_plugin_id": registration.collector_plugin_id,
                    "declared_only": True,
                    "expected_output_paths": declared_paths,
                },
                write_mode=_string_or_none(declaration.get("write_mode") or output.get("write_mode")),
                partition_by=_string_list(declaration.get("partition_by") or output.get("partition_by")),
                primary_key=_string_list(declaration.get("primary_key") or output.get("primary_key")),
                date_field=_string_or_none(declaration.get("date_field") or output.get("date_field")),
            )
            _apply_dataset_declaration(summary, declaration, root=root)
            summaries.append(_enrich_summary_from_paths(summary, root=root))
    return summaries


def _has_local_output_reference(summary: DatasetSummary) -> bool:
    """Return whether a dataset represents local files or stale local metadata."""

    return bool(summary.output_paths)


def _collector_output_declarations(collector: Any, output: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_datasets = output.get("datasets") or output.get("outputs")
    declarations: list[dict[str, Any]] = []
    if isinstance(raw_datasets, Sequence) and not isinstance(raw_datasets, (str, bytes, bytearray)):
        for item in raw_datasets:
            if isinstance(item, Mapping):
                declarations.append(dict(item))
    if declarations:
        return declarations

    dataset_id = getattr(collector, "dataset_id", None)
    if not dataset_id:
        return []
    return [
        {
            "dataset_id": dataset_id,
            "display_name_zh": getattr(collector, "display_name_zh", None),
            "description": getattr(collector, "description", ""),
            "layer": output.get("layer") or output.get("output_layer"),
            "table": output.get("table") or output.get("logical_table") or dataset_id,
            "fields": output.get("fields"),
            "primary_key": output.get("primary_key"),
            "date_field": output.get("date_field"),
            "partition_by": output.get("partition_by"),
            "write_mode": output.get("write_mode"),
            "storage": output.get("storage"),
            "formats": output.get("supported_formats") or output.get("formats"),
        }
    ]


def _declared_output_paths(root: Path, declaration: Mapping[str, Any], output: Mapping[str, Any]) -> dict[str, str]:
    path_parts = _string_list(
        declaration.get("default_output_path_parts")
        or declaration.get("path_parts")
        or output.get("default_output_path_parts")
    )
    if not path_parts:
        layer = str(declaration.get("layer") or output.get("layer") or output.get("output_layer") or "snapshot")
        table = str(declaration.get("table") or declaration.get("logical_table") or declaration.get("dataset_id") or "")
        default_dir_name = str(
            declaration.get("default_dir_name")
            or output.get("default_dir_name")
            or declaration.get("dataset_id")
            or table
        )
        path_parts = [layer, f"table={table}" if layer == "core" and table and "." not in table else default_dir_name]
    base = root.joinpath(*path_parts)
    formats = _declared_formats(declaration, output)
    return {format_name: str(base / format_name) for format_name in formats}


def _output_dataset_declaration(
    payload: Mapping[str, Any],
    *,
    interface_name: str,
    layer: str | None,
) -> dict[str, Any]:
    output = payload.get("output")
    if not isinstance(output, Mapping):
        output = {}
    raw_datasets = output.get("datasets") or output.get("outputs")
    if isinstance(raw_datasets, Sequence) and not isinstance(raw_datasets, (str, bytes, bytearray)):
        target_names = {
            _normalize_dataset_name(interface_name),
            _normalize_dataset_name(str(payload.get("dataset_id") or "")),
            _normalize_dataset_name(str(payload.get("table") or "")),
        }
        target_names.discard("")
        for item in raw_datasets:
            if not isinstance(item, Mapping):
                continue
            declaration = dict(item)
            names = {
                _normalize_dataset_name(str(declaration.get("dataset_id") or "")),
                _normalize_dataset_name(str(declaration.get("table") or "")),
                _normalize_dataset_name(str(declaration.get("logical_table") or "")),
            }
            names.discard("")
            if target_names & names:
                return declaration
        for item in raw_datasets:
            if isinstance(item, Mapping):
                return dict(item)
    dataset_id = payload.get("dataset_id")
    if dataset_id:
        return {
            "dataset_id": dataset_id,
            "table": payload.get("table") or output.get("table") or dataset_id,
            "layer": layer or output.get("layer") or output.get("output_layer"),
            "fields": output.get("fields"),
            "primary_key": output.get("primary_key"),
            "date_field": output.get("date_field"),
            "partition_by": output.get("partition_by"),
            "write_mode": output.get("write_mode"),
            "storage": output.get("storage"),
            "formats": output.get("supported_formats") or output.get("formats"),
        }
    return {}


def _summary_from_run(root: Path, run: Any) -> DatasetSummary | None:
    output_paths = {str(key): str(value) for key, value in dict(getattr(run, "output_paths", {}) or {}).items()}
    result = dict(getattr(run, "result", {}) or {})
    download_result = dict(result.get("download_result") or {})
    if not output_paths:
        raw_output_paths = download_result.get("output_paths")
        if isinstance(raw_output_paths, Mapping):
            output_paths = {str(key): str(value) for key, value in raw_output_paths.items()}
    if not output_paths:
        return None

    interface_name = str(
        result.get("target_interface")
        or download_result.get("interface_name")
        or getattr(run, "downloader_profile", None)
        or getattr(run, "collector_name", "")
    )
    quality = dict(getattr(run, "quality", {}) or download_result.get("quality") or {})
    source_meta = dict(download_result.get("source_meta") or {})
    write_metadata = _write_metadata_from_payload(download_result, quality)
    layer = _layer_from_payload(download_result, result, output_paths)
    declaration_payload = dict(download_result)
    if "output" not in declaration_payload and isinstance(result.get("output"), Mapping):
        declaration_payload["output"] = result["output"]
    declared = _output_dataset_declaration(declaration_payload, interface_name=interface_name, layer=layer)
    summary = DatasetSummary(
        dataset=str(declared.get("dataset_id") or _dataset_id(interface_name, layer)),
        interface_name=interface_name,
        display_name_zh=_string_or_none(declared.get("display_name_zh")),
        description=str(declared.get("description") or ""),
        provider=getattr(run, "provider_id", None),
        source=_source_from_payload(getattr(run, "provider_id", None), source_meta),
        layer=_string_or_none(declared.get("layer")) or layer,
        output_paths=output_paths,
        row_count=_int_or_none(download_result.get("row_count") or quality.get("row_count_value")),
        quality=quality,
        quality_status=_string_or_none(quality.get("quality_status")),
        quality_warnings=_string_list(quality.get("quality_warnings")),
        quality_errors=_string_list(quality.get("quality_errors")),
        latest_run_id=getattr(run, "run_id", None),
        latest_run_status=getattr(run, "status", None),
        updated_at=getattr(run, "finished_at", None) or getattr(run, "updated_at", None),
        metadata={
            "collector_name": getattr(run, "collector_name", None),
            "task_id": getattr(run, "task_id", None),
            "downloader_profile": getattr(run, "downloader_profile", None),
            "params": dict(getattr(run, "params", {}) or {}),
            "snapshot_date": download_result.get("snapshot_date"),
            "log_path": download_result.get("log_path"),
            "collector_output_dataset": dict(declared),
        },
        **write_metadata,
    )
    _apply_dataset_declaration(summary, declared, root=root)
    return _enrich_summary_from_paths(summary, root=root)


def _iter_downloader_logs(root: Path, entries: Mapping[str, DatasetSummary]) -> Iterable[dict[str, Any]]:
    seen: set[Path] = set()
    for log_dir in _known_log_dirs(root):
        yield from _iter_log_dir(log_dir, seen)
    for summary in entries.values():
        for path_text in summary.output_paths.values():
            path = Path(path_text).expanduser()
            candidates = []
            if path.is_file():
                candidates.append(path.parent.parent / "logs")
            elif path.is_dir():
                candidates.append(path / "logs")
                candidates.append(path.parent / "logs")
            for log_dir in candidates:
                yield from _iter_log_dir(log_dir, seen)


def _known_log_dirs(root: Path) -> Iterable[Path]:
    """Yield metadata log directories under known AxData data roots only."""

    candidates = [
        root / "raw",
        root / "staging",
        root / "core",
        root / "factor",
        root / "snapshot",
        root / "snapshots",
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        yield from candidate.glob("*/logs")
        yield from candidate.glob("*/*/logs")
        yield from candidate.glob("*/*/*/logs")


def _iter_log_dir(log_dir: Path, seen: set[Path]) -> Iterable[dict[str, Any]]:
    try:
        resolved = log_dir.resolve()
    except OSError:
        return
    if resolved in seen or not resolved.exists():
        return
    seen.add(resolved)
    log_paths = sorted(
        resolved.glob("*.json"),
        key=lambda path: _path_sort_mtime(path),
        reverse=True,
    )[:MAX_DOWNLOADER_LOGS_PER_DIR]
    for log_path in log_paths:
        try:
            payload = json.loads(log_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            payload.setdefault("log_path", str(log_path))
            yield payload


def _summary_from_log(root: Path, payload: Mapping[str, Any]) -> DatasetSummary | None:
    output_paths = payload.get("output_paths")
    if not isinstance(output_paths, Mapping):
        return None
    interface_name = str(payload.get("interface_name") or payload.get("target_interface") or "")
    if not interface_name:
        return None
    quality = dict(payload.get("quality") or {})
    layer = _layer_from_payload(payload, {}, output_paths)
    source_meta = dict(payload.get("source_meta") or {})
    write_metadata = _write_metadata_from_payload(payload, quality)
    declared = _output_dataset_declaration(payload, interface_name=interface_name, layer=layer)
    summary = DatasetSummary(
        dataset=str(declared.get("dataset_id") or _dataset_id(interface_name, layer)),
        interface_name=interface_name,
        display_name_zh=_string_or_none(declared.get("display_name_zh")),
        description=str(declared.get("description") or ""),
        provider=_string_or_none(payload.get("provider_id") or payload.get("collector_plugin_id")),
        source=_source_from_payload(payload.get("provider_id") or payload.get("collector_plugin_id"), source_meta),
        layer=_string_or_none(declared.get("layer")) or layer,
        output_paths={str(key): str(value) for key, value in output_paths.items()},
        row_count=_int_or_none(payload.get("row_count") or quality.get("row_count_value")),
        quality=quality,
        quality_status=_string_or_none(quality.get("quality_status")),
        quality_warnings=_string_list(quality.get("quality_warnings")),
        quality_errors=_string_list(quality.get("quality_errors")),
        latest_run_id=_string_or_none(payload.get("job_id")),
        latest_run_status=_string_or_none(payload.get("status")),
        updated_at=_string_or_none(payload.get("finished_at")),
        metadata={
            "snapshot_date": payload.get("snapshot_date"),
            "log_path": payload.get("log_path"),
            "collector_name": payload.get("collector_name") or payload.get("collector_id"),
            "collector_plugin_id": payload.get("collector_plugin_id"),
            "params": dict(payload.get("params") or {}) if isinstance(payload.get("params"), Mapping) else {},
            "collector_output_dataset": dict(declared),
        },
        **write_metadata,
    )
    _apply_dataset_declaration(summary, declared, root=root)
    return _enrich_summary_from_paths(summary, root=root)


def _summary_from_core_table(root: Path, table: str) -> DatasetSummary | None:
    output_paths: dict[str, str] = {}
    file_path = core_table_path(table, root)
    partition_path = core_table_partition_path(table, root)
    if file_path.exists():
        output_paths["parquet"] = str(file_path)
    elif partition_path.exists() and _parquet_dir_may_have_files(partition_path):
        output_paths["parquet"] = str(partition_path)
    else:
        return None

    schema = get_schema(table)
    summary = DatasetSummary(
        dataset=_dataset_id(table, "core"),
        interface_name=table,
        display_name_zh=schema.display_name_zh,
        description=schema.description,
        layer="core",
        output_paths=output_paths,
        columns=list(schema.field_names),
        field_schema=_field_schema_from_table_schema(schema),
        logical_table=schema.name,
        storage_layout="core_table",
        default_query_fields=list(schema.field_names),
        default_filter_fields=_default_filter_fields(schema),
        available_formats=["parquet", "csv", "duckdb"],
        metadata={
            "schema": schema.name,
            "primary_key": list(schema.primary_key),
            "date_field": schema.date_field,
        },
        primary_key=list(schema.primary_key),
        date_field=schema.date_field,
    )
    return _enrich_summary_from_paths(summary, root=root)


def _merge_summary(entries: dict[str, DatasetSummary], summary: DatasetSummary) -> None:
    existing = entries.get(summary.dataset)
    if existing is None:
        entries[summary.dataset] = summary
        return
    if _sort_time(summary.updated_at) >= _sort_time(existing.updated_at):
        merged = summary
        for key, value in existing.output_paths.items():
            merged.output_paths.setdefault(key, value)
    else:
        merged = existing
        for key, value in summary.output_paths.items():
            merged.output_paths.setdefault(key, value)
    if not merged.columns:
        merged.columns = existing.columns or summary.columns
    if not merged.display_name_zh:
        merged.display_name_zh = existing.display_name_zh or summary.display_name_zh
    if not merged.description:
        merged.description = existing.description or summary.description
    if not merged.field_schema:
        merged.field_schema = existing.field_schema or summary.field_schema
    if not merged.logical_table:
        merged.logical_table = existing.logical_table or summary.logical_table
    if not merged.storage_layout:
        merged.storage_layout = existing.storage_layout or summary.storage_layout
    if not merged.default_query_fields:
        merged.default_query_fields = existing.default_query_fields or summary.default_query_fields
    if not merged.default_filter_fields:
        merged.default_filter_fields = existing.default_filter_fields or summary.default_filter_fields
    if not merged.available_formats:
        merged.available_formats = existing.available_formats or summary.available_formats
    if not merged.write_mode:
        merged.write_mode = existing.write_mode or summary.write_mode
    if not merged.partition_by:
        merged.partition_by = existing.partition_by or summary.partition_by
    if not merged.primary_key:
        merged.primary_key = existing.primary_key or summary.primary_key
    if not merged.date_field:
        merged.date_field = existing.date_field or summary.date_field
    entries[summary.dataset] = merged


def _apply_dataset_declaration(
    summary: DatasetSummary,
    declaration: Mapping[str, Any],
    *,
    root: Path,
) -> None:
    if not declaration:
        return
    summary.display_name_zh = summary.display_name_zh or _string_or_none(declaration.get("display_name_zh"))
    summary.description = summary.description or str(declaration.get("description") or "")
    summary.logical_table = summary.logical_table or _string_or_none(
        declaration.get("table") or declaration.get("logical_table")
    )
    storage = declaration.get("storage")
    storage_layout = storage.get("layout") if isinstance(storage, Mapping) else None
    summary.storage_layout = summary.storage_layout or _string_or_none(
        declaration.get("storage_layout") or storage_layout
    )
    summary.default_query_fields = summary.default_query_fields or _string_list(
        declaration.get("default_query_fields")
    )
    summary.default_filter_fields = summary.default_filter_fields or _string_list(
        declaration.get("default_filter_fields")
    )
    summary.available_formats = summary.available_formats or _declared_formats(declaration, {})
    fields = _field_schema_from_declaration(declaration)
    if fields:
        summary.field_schema = fields
        if not summary.columns:
            summary.columns = [field["name"] for field in fields if field.get("name")]
    if not summary.primary_key:
        summary.primary_key = _string_list(declaration.get("primary_key"))
    if not summary.partition_by:
        summary.partition_by = _string_list(declaration.get("partition_by"))
    if not summary.date_field:
        summary.date_field = _string_or_none(declaration.get("date_field"))
    expected_paths = _declared_output_paths(root, declaration, declaration)
    if expected_paths:
        summary.metadata = {
            **summary.metadata,
            "expected_output_paths": {
                **dict(summary.metadata.get("expected_output_paths") or {}),
                **expected_paths,
            },
        }
    _apply_known_schema(summary)


def _apply_known_schema(summary: DatasetSummary) -> None:
    schema = _schema_for_summary(summary)
    if schema is None:
        return
    has_existing_parquet = bool(_existing_parquet_paths(summary.output_paths))
    summary.display_name_zh = summary.display_name_zh or schema.display_name_zh
    summary.description = summary.description or schema.description
    summary.logical_table = summary.logical_table or schema.name
    if not summary.field_schema or _field_schema_needs_descriptions(summary.field_schema):
        summary.field_schema = _field_schema_from_table_schema(schema)
    if not summary.columns and not has_existing_parquet:
        summary.columns = list(schema.field_names)
    summary.primary_key = summary.primary_key or list(schema.primary_key)
    summary.date_field = summary.date_field or schema.date_field
    summary.default_query_fields = summary.default_query_fields or list(schema.field_names)
    summary.default_filter_fields = summary.default_filter_fields or _default_filter_fields(schema)
    summary.available_formats = summary.available_formats or ["parquet", "csv", "duckdb"]


def _field_schema_needs_descriptions(fields: Sequence[Mapping[str, Any]]) -> bool:
    return not any(
        field.get("description_zh") or field.get("display_name_zh") != field.get("name")
        for field in fields
    )


def _schema_for_summary(summary: DatasetSummary) -> TableSchema | None:
    for candidate in (summary.logical_table, summary.dataset, summary.interface_name):
        if not candidate:
            continue
        try:
            return get_schema(candidate)
        except KeyError:
            continue
    return None


def _field_schema_from_table_schema(schema: TableSchema) -> list[dict[str, Any]]:
    return [_field_schema_from_field(field) for field in schema.fields]


def _field_schema_from_field(field: Field) -> dict[str, Any]:
    return {
        "name": field.name,
        "type": field.dtype,
        "display_name_zh": field.description_zh or field.description or field.name,
        "description": field.description,
        "description_zh": field.description_zh,
        "required": not field.nullable,
        "unit": field.unit,
        "aliases": list(field.aliases),
    }


def _field_schema_from_declaration(declaration: Mapping[str, Any]) -> list[dict[str, Any]]:
    fields = declaration.get("fields")
    if isinstance(fields, Mapping):
        return [
            _field_schema_from_mapping(name, value)
            for name, value in fields.items()
        ]
    if isinstance(fields, Sequence) and not isinstance(fields, (str, bytes, bytearray)):
        parsed: list[dict[str, Any]] = []
        for item in fields:
            if isinstance(item, Mapping):
                name = _string_or_none(item.get("name"))
                if name:
                    parsed.append(_field_schema_from_mapping(name, item))
            elif isinstance(item, str) and item.strip():
                parsed.append({"name": item.strip(), "type": "", "display_name_zh": item.strip()})
        return parsed
    columns = _string_list(declaration.get("columns") or declaration.get("expected_columns"))
    return [{"name": column, "type": "", "display_name_zh": column} for column in columns]


def _field_schema_from_mapping(name: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {
            "name": name,
            "type": "",
            "display_name_zh": str(value) if value is not None else name,
        }
    return {
        "name": name,
        "type": _string_or_none(value.get("type") or value.get("dtype")) or "",
        "display_name_zh": _string_or_none(
            value.get("display_name_zh") or value.get("name_zh") or value.get("description_zh")
        )
        or name,
        "description": _string_or_none(value.get("description")) or "",
        "description_zh": _string_or_none(value.get("description_zh")) or "",
        "required": bool(value.get("required", False)),
        "unit": _string_or_none(value.get("unit")),
    }


def _default_filter_fields(schema: TableSchema) -> list[str]:
    fields: list[str] = []
    for candidate in ("ts_code", "instrument_id", "symbol", "exchange"):
        if schema.has_field(candidate):
            fields.append(candidate)
    if schema.date_field:
        fields.extend(["start_date", "end_date"])
    return fields


def _declared_formats(
    declaration: Mapping[str, Any],
    output: Mapping[str, Any],
) -> list[str]:
    values = _string_list(
        declaration.get("formats")
        or declaration.get("supported_formats")
        or output.get("supported_formats")
        or output.get("formats")
        or ["parquet"]
    )
    ordered = []
    for value in values:
        clean = value.strip().lower()
        if clean and clean not in ordered:
            ordered.append(clean)
    return ordered or ["parquet"]


def _enrich_summary_from_paths(summary: DatasetSummary, *, root: Path) -> DatasetSummary:
    missing: list[str] = []
    for path_text in summary.output_paths.values():
        path = Path(path_text).expanduser()
        if not path.is_absolute():
            path = (root / path).resolve()
        if not path.exists():
            if len(missing) < MAX_MISSING_PATHS:
                missing.append(str(path))
    summary.missing_paths = missing

    if not summary.columns:
        summary.columns = _quality_columns(summary.quality)
    _apply_known_schema(summary)

    quality_date_range = summary.quality.get("date_range") if isinstance(summary.quality, Mapping) else None
    if isinstance(quality_date_range, Mapping):
        date_min = _string_or_none(quality_date_range.get("min"))
        date_max = _string_or_none(quality_date_range.get("max"))
    else:
        date_min = date_max = None
    if _date_field(summary):
        summary.date_min = summary.date_min or date_min
        summary.date_max = summary.date_max or date_max
    else:
        summary.datetime_min = summary.datetime_min or date_min
        summary.datetime_max = summary.datetime_max or date_max

    needs_row_count = summary.row_count is None
    needs_columns = not summary.columns or (
        bool(_existing_parquet_paths(summary.output_paths))
        and bool(summary.field_schema)
        and summary.columns == [field.get("name") for field in summary.field_schema if field.get("name")]
    )
    needs_date_stats = bool(_date_field(summary) and not (date_min and date_max))
    if not (needs_row_count or needs_columns or needs_date_stats):
        return summary

    parquet_paths = _existing_parquet_paths(summary.output_paths)
    if not parquet_paths:
        return summary

    try:
        stats = _parquet_stats(
            parquet_paths,
            date_field=_date_field(summary),
            include_date_range=needs_date_stats,
        )
    except Exception as exc:
        summary.metadata = {**summary.metadata, "inspect_error": str(exc)}
        return summary

    summary.row_count = summary.row_count if summary.row_count is not None else stats.get("row_count")
    if needs_columns:
        summary.columns = list(stats.get("columns") or [])
    if stats.get("stats_limited"):
        summary.metadata = {
            **summary.metadata,
            "parquet_stats_limited": True,
            "parquet_stats_file_count": stats.get("stats_file_count"),
            "parquet_stats_file_limit": MAX_PARQUET_STATS_FILES,
            "parquet_stats_dir_limit": MAX_PARQUET_STATS_DIRS,
        }

    stats_date_min = _string_or_none(stats.get("date_min"))
    stats_date_max = _string_or_none(stats.get("date_max"))

    if _date_field(summary):
        summary.date_min = summary.date_min or date_min or stats_date_min
        summary.date_max = summary.date_max or date_max or stats_date_max
    else:
        summary.datetime_min = summary.datetime_min or date_min or stats_date_min
        summary.datetime_max = summary.datetime_max or date_max or stats_date_max
    return summary


def _existing_parquet_paths(output_paths: Mapping[str, str]) -> list[Path]:
    path_text = output_paths.get("parquet")
    if not path_text:
        return []
    path = Path(path_text).expanduser()
    if path.is_file() and path.suffix.lower() == ".parquet":
        return [path.resolve()]
    if path.is_dir():
        if not _parquet_dir_may_have_files(path):
            return []
        return [path.resolve()]
    return []


def _parquet_stats(
    paths: Sequence[Path],
    *,
    date_field: str | None,
    include_date_range: bool = True,
) -> dict[str, Any]:
    import pyarrow.parquet as pq

    columns: list[str] = []
    row_count = 0
    date_values: list[str] = []
    parquet_files, stats_limited, partition_dates = _parquet_files_for_stats(paths, date_field=date_field)
    date_values.extend(partition_dates)
    for path in parquet_files:
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata
        row_count += int(metadata.num_rows if metadata is not None else 0)
        physical_columns = [str(column) for column in parquet_file.schema_arrow.names]
        partition_values = _hive_partition_values(path)
        for column in (*physical_columns, *partition_values):
            if column not in columns:
                columns.append(column)
        if include_date_range and date_field:
            partition_date = partition_values.get(date_field)
            if partition_date is not None:
                date_values.append(_normalize_date_text(partition_date) or str(partition_date))
            elif date_field in physical_columns and metadata is not None:
                date_values.extend(_parquet_column_stat_values(metadata, physical_columns.index(date_field)))

    result: dict[str, Any] = {
        "columns": columns,
        "stats_file_count": len(parquet_files),
        "stats_limited": stats_limited,
    }
    if not stats_limited:
        result["row_count"] = int(row_count)
    normalized_dates = sorted(
        value
        for value in (_normalize_date_text(item) for item in date_values)
        if value
    )
    if normalized_dates:
        result["date_min"] = normalized_dates[0]
        result["date_max"] = normalized_dates[-1]
    return result


def _parquet_files_for_stats(paths: Sequence[Path], *, date_field: str | None) -> tuple[list[Path], bool, list[str]]:
    files: list[Path] = []
    partition_dates: list[str] = []
    limited = False
    for path in paths:
        if path.is_file():
            limited = _append_stats_file(files, path) or limited
        elif path.is_dir():
            if date_field:
                partition_dates.extend(_date_partition_values(path, date_field=date_field))
            limited = _append_stats_files_from_dir(files, path) or limited
    return files, limited, partition_dates


def _append_stats_files_from_dir(files: list[Path], directory: Path) -> bool:
    remaining = max(MAX_PARQUET_STATS_FILES - len(files), 0)
    if remaining <= 0:
        return True
    found, limited = _bounded_parquet_files(directory, file_limit=remaining)
    files.extend(found)
    return limited


def _append_stats_file(files: list[Path], path: Path) -> bool:
    if len(files) >= MAX_PARQUET_STATS_FILES:
        return True
    files.append(path.resolve())
    return False


def _parquet_dir_may_have_files(directory: Path) -> bool:
    files, limited = _bounded_parquet_files(directory, file_limit=1)
    return bool(files) or limited


def _bounded_parquet_files(directory: Path, *, file_limit: int) -> tuple[list[Path], bool]:
    if file_limit <= 0:
        return [], True
    files: list[Path] = []
    dirs_seen = 0
    for root, dirnames, filenames in os.walk(directory):
        dirs_seen += 1
        if dirs_seen > MAX_PARQUET_STATS_DIRS:
            return files, True
        dirnames.sort()
        for filename in sorted(filenames):
            if not filename.lower().endswith(".parquet"):
                continue
            files.append((Path(root) / filename).resolve())
            if len(files) >= file_limit:
                return files, True
    return files, False


def _date_partition_values(directory: Path, *, date_field: str) -> list[str]:
    values: list[str] = []
    prefix = f"{date_field}="
    try:
        candidates = directory.glob(f"{date_field}=*")
    except OSError:
        return values
    for path in candidates:
        if path.is_dir() and path.name.startswith(prefix):
            values.append(path.name.split("=", 1)[1])
    return values


def _parquet_column_stat_values(metadata: Any, column_index: int) -> list[str]:
    values: list[str] = []
    for row_group_index in range(metadata.num_row_groups):
        column = metadata.row_group(row_group_index).column(column_index)
        stats = column.statistics
        if stats is None:
            continue
        for value in (stats.min, stats.max):
            text = _stat_value_text(value)
            if text:
                values.append(text)
    return values


def _hive_partition_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for part in path.parts[:-1]:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key and key != "table":
            values[key] = value
    return values


def _stat_value_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            pass
    return str(value)


def _query_parquet(
    paths: Sequence[Path],
    *,
    fields: Sequence[str] | None,
    filters: Mapping[str, Any],
    date_field: str | None,
    start: str | None,
    end: str | None,
    limit: int,
    known_columns: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    import duckdb

    read_path = _parquet_read_path(paths, date_field=date_field, start=start, end=end)
    if read_path is _NO_MATCHING_PARQUET_PARTITIONS:
        available = list(known_columns or [])
        selected = list(fields or available)
        missing = [field for field in selected if available and field not in available]
        if missing:
            raise DataBrowserError("Unknown field(s): " + ", ".join(missing))
        for field in filters:
            if available and field not in available:
                raise DataBrowserError(f"Unknown filter field: {field}")
        return [], selected
    where_sql, params = _where_clause(filters, date_field=date_field, start=start, end=end)
    with duckdb.connect(database=":memory:") as conn:
        available = [
            str(row[0])
            for row in conn.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = true, union_by_name = true)",
                [read_path],
            ).fetchall()
        ]
        visible_available = _visible_preview_columns(available, known_columns)
        selected = list(fields or visible_available)
        missing = [field for field in selected if field not in available]
        if missing:
            raise DataBrowserError("Unknown field(s): " + ", ".join(missing))
        for field in filters:
            if field not in available:
                raise DataBrowserError(f"Unknown filter field: {field}")
        if date_field and (start or end) and date_field not in available:
            raise DataBrowserError(f"Date filter field {date_field!r} is not present in dataset.")
        select_sql = ", ".join(_quote_identifier(field) for field in selected) or "*"
        sql = (
            f"SELECT {select_sql} "
            "FROM read_parquet(?, hive_partitioning = true, union_by_name = true)"
            f"{where_sql} LIMIT ?"
        )
        frame = conn.execute(sql, [read_path, *params, limit]).fetchdf()
        return frame.to_dict(orient="records"), list(frame.columns)


def _visible_preview_columns(available: Sequence[str], known_columns: Sequence[str] | None) -> list[str]:
    known = {str(column) for column in known_columns or []}
    visible: list[str] = []
    for column in available:
        if column == "table" and column not in known:
            continue
        visible.append(column)
    return visible


def _parquet_read_path(
    paths: Sequence[Path],
    *,
    date_field: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> str | list[str] | object:
    if len(paths) == 1:
        only = paths[0]
        if only.is_file():
            return str(only)
        if only.is_dir():
            date_globs = _date_partition_globs(only, date_field=date_field, start=start, end=end)
            if date_globs is not None:
                return date_globs or _NO_MATCHING_PARQUET_PARTITIONS
            return str(only / "**" / "*.parquet")
    common = Path(os.path.commonpath([str(path.parent) for path in paths]))
    return str(common / "**" / "*.parquet")


def _date_partition_globs(
    directory: Path,
    *,
    date_field: str | None,
    start: str | None,
    end: str | None,
) -> list[str] | None:
    if not date_field or not (start or end):
        return None
    partition_dirs = [
        path
        for path in directory.glob(f"{date_field}=*")
        if path.is_dir()
    ]
    date_files = [path for path in directory.glob("*.parquet") if _date_file_value(path) is not None]
    if not partition_dirs and not date_files:
        return None

    start_compact = start.replace("-", "") if start else None
    end_compact = end.replace("-", "") if end else None
    if start_compact is not None and (len(start_compact) != 8 or not start_compact.isdigit()):
        return None
    if end_compact is not None and (len(end_compact) != 8 or not end_compact.isdigit()):
        return None

    globs: list[str] = []
    for path in sorted(partition_dirs):
        value = path.name.split("=", 1)[1].replace("-", "")
        if start_compact is not None and value < start_compact:
            continue
        if end_compact is not None and value > end_compact:
            continue
        globs.append(str(path / "**" / "*.parquet"))
    for path in sorted(date_files):
        value = _date_file_value(path)
        if value is None:
            continue
        if start_compact is not None and value < start_compact:
            continue
        if end_compact is not None and value > end_compact:
            continue
        globs.append(str(path))
    return globs


def _date_file_value(path: Path) -> str | None:
    value = path.stem.replace("-", "")
    return value if len(value) == 8 and value.isdigit() else None


def _where_clause(
    filters: Mapping[str, Any],
    *,
    date_field: str | None,
    start: str | None,
    end: str | None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    for field, value in filters.items():
        quoted = _quote_identifier(field)
        if isinstance(value, (list, tuple, set, frozenset)):
            values = [item for item in value if item is not None]
            if not values:
                clauses.append("1 = 0")
                continue
            placeholders = ", ".join("?" for _ in values)
            clauses.append(f"{quoted} IN ({placeholders})")
            params.extend(values)
        elif value is None:
            clauses.append(f"{quoted} IS NULL")
        else:
            clauses.append(f"{quoted} = ?")
            params.append(value)

    if date_field and (start or end):
        quoted_date = _quote_identifier(date_field)
        if start:
            clauses.append(f"REPLACE(CAST({quoted_date} AS VARCHAR), '-', '') >= ?")
            params.append(start)
        if end:
            clauses.append(f"REPLACE(CAST({quoted_date} AS VARCHAR), '-', '') <= ?")
            params.append(end)

    if not clauses:
        return "", params
    return " WHERE " + " AND ".join(clauses), params


def _normalize_preview_filters(
    summary: DatasetSummary,
    filters: Mapping[str, Any] | None,
    symbol: str | None,
) -> dict[str, Any]:
    normalized = {
        str(key): _normalize_filter_value(value)
        for key, value in dict(filters or {}).items()
        if value is not None and str(key).strip()
    }
    if symbol:
        symbol_field = _symbol_field(summary)
        if symbol_field:
            normalized.setdefault(symbol_field, symbol)
    return normalized


def _symbol_field(summary: DatasetSummary) -> str | None:
    columns = set(summary.columns)
    for candidate in ("ts_code", "instrument_id", "symbol", "code"):
        if candidate in columns:
            return candidate
    return None


def _quality_columns(quality: Mapping[str, Any]) -> list[str]:
    columns = quality.get("schema_columns") if isinstance(quality, Mapping) else None
    if isinstance(columns, str):
        return [field.strip() for field in columns.split(",") if field.strip()]
    if isinstance(columns, Iterable):
        return [str(field) for field in columns if str(field).strip()]
    return []


def _date_field(summary: DatasetSummary) -> str | None:
    if summary.date_field:
        return summary.date_field
    quality_field = summary.quality.get("date_field") if isinstance(summary.quality, Mapping) else None
    if isinstance(quality_field, str) and quality_field:
        return quality_field
    metadata_field = summary.metadata.get("date_field") if isinstance(summary.metadata, Mapping) else None
    if isinstance(metadata_field, str) and metadata_field:
        return metadata_field
    columns = set(summary.columns)
    for candidate in ("trade_date", "cal_date", "publish_date", "report_date", "date", "trade_time", "quote_time", "datetime"):
        if candidate in columns:
            return candidate
    try:
        schema = get_schema(summary.interface_name)
    except KeyError:
        return None
    return schema.date_field or schema.datetime_field


def _normalize_fields(fields: Sequence[str] | str | None) -> list[str] | None:
    if fields is None or fields == "":
        return None
    if isinstance(fields, str):
        return [field.strip() for field in fields.split(",") if field.strip()]
    return [str(field).strip() for field in fields if str(field).strip()]


def _normalize_filter_value(value: Any) -> Any:
    if isinstance(value, str):
        return _normalize_date_text(value)
    if isinstance(value, tuple):
        return [_normalize_filter_value(item) for item in value]
    if isinstance(value, list):
        return [_normalize_filter_value(item) for item in value]
    return value


def _normalize_date_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text.replace("-", "")
    return text


def _date_filter_payload(start: str | None, end: str | None) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if start:
        payload["start"] = start
    if end:
        payload["end"] = end
    return payload


def _layer_from_payload(
    primary: Mapping[str, Any],
    secondary: Mapping[str, Any],
    output_paths: Mapping[str, Any],
) -> str | None:
    for payload in (primary, secondary):
        output = payload.get("output")
        if isinstance(output, Mapping):
            layer = _string_or_none(output.get("layer") or output.get("output_layer"))
            if layer:
                return layer
        for key in ("layer", "output_layer"):
            layer = _string_or_none(payload.get(key))
            if layer:
                return layer
    joined = " ".join(str(path).replace("\\", "/") for path in output_paths.values())
    for layer in KNOWN_DATA_LAYERS:
        if f"/{layer}/" in joined or f"\\{layer}\\" in joined or f"{layer}/table=" in joined:
            return layer
    return "snapshot"


def _write_metadata_from_payload(primary: Mapping[str, Any], quality: Mapping[str, Any]) -> dict[str, Any]:
    nested = primary.get("write_metadata")
    source = dict(nested) if isinstance(nested, Mapping) else primary
    primary_key = _string_list(
        source.get("primary_key")
        if "primary_key" in source
        else quality.get("write_primary_key")
    )
    partition_by = _string_list(source.get("partition_by") or quality.get("partition_by"))
    date_field = _string_or_none(
        source.get("date_field")
        or quality.get("write_date_field")
        or quality.get("date_field")
    )
    return {
        "write_mode": _string_or_none(source.get("write_mode") or quality.get("write_mode")),
        "partition_by": partition_by,
        "primary_key": primary_key,
        "date_field": date_field,
        "replace_range_start": _string_or_none(
            source.get("replace_range_start") or quality.get("replace_range_start")
        ),
        "replace_range_end": _string_or_none(
            source.get("replace_range_end") or quality.get("replace_range_end")
        ),
        "rows_before": _int_or_none(source.get("rows_before") if "rows_before" in source else quality.get("rows_before")),
        "rows_written": _int_or_none(
            source.get("rows_written") if "rows_written" in source else quality.get("rows_written")
        ),
        "rows_after": _int_or_none(source.get("rows_after") if "rows_after" in source else quality.get("rows_after")),
        "duplicate_rows_dropped": _int_or_none(
            source.get("duplicate_rows_dropped")
            if "duplicate_rows_dropped" in source
            else quality.get("duplicate_rows_dropped")
        ),
        "partitions_touched": _string_list(source.get("partitions_touched") or quality.get("partitions_touched")),
    }


def _dataset_id(interface_name: str, layer: str | None) -> str:
    clean = interface_name.strip() or "dataset"
    if layer and layer not in {"snapshot", "core"}:
        return f"{layer}.{clean}"
    return clean


def _source_from_payload(provider: Any, source_meta: Mapping[str, Any]) -> str | None:
    source = _string_or_none(source_meta.get("source") or source_meta.get("source_code"))
    if source:
        return source
    provider_text = _string_or_none(provider)
    if provider_text and "." in provider_text:
        return provider_text.rsplit(".", 1)[-1]
    return provider_text


def _normalize_dataset_name(value: str) -> str:
    return re.sub(r"\s+", "", value.strip().lower())


def _sort_time(value: str | None) -> str:
    return value or ""


def _path_sort_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Iterable):
        return [str(item) for item in value]
    return [str(value)]


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if value == value and value not in {float("inf"), float("-inf")} else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _jsonable(value.item())
        except (TypeError, ValueError):
            pass
    return value
