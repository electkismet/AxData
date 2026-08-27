"""Neutral downloader engine seams.

This module contains the source-neutral pieces of the downloader pipeline:
request planning, output writing, quality summaries, and run metadata logs.
Source-specific logic such as TDX server selection stays in provider-owned
adapter factories and downloader profiles.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Mapping, Sequence
from typing import Any, Callable

import pandas as pd

ProgressCallback = Callable[..., None]


@dataclass(frozen=True)
class DownloadRequestPlan:
    """Source request plan produced from one downloader profile."""

    interface_name: str
    params: dict[str, Any]
    fields: list[str] | None
    options: dict[str, Any]
    adapter_options: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "interface_name": self.interface_name,
            "params": dict(self.params),
            "fields": list(self.fields) if self.fields is not None else None,
            "options": dict(self.options),
            "adapter_options": dict(self.adapter_options),
        }


@dataclass(frozen=True)
class WriteStrategyMetadata:
    """Local write semantics and row-count deltas for one output."""

    write_mode: str = "snapshot"
    partition_by: tuple[str, ...] = ()
    primary_key: tuple[str, ...] = ()
    date_field: str | None = None
    replace_range_start: str | None = None
    replace_range_end: str | None = None
    rows_before: int | None = None
    rows_written: int = 0
    rows_after: int | None = None
    duplicate_rows_dropped: int = 0
    partitions_touched: tuple[str, ...] = ()
    formats: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
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
            "formats": {
                str(format_name): dict(metadata)
                for format_name, metadata in self.formats.items()
            },
        }


@dataclass(frozen=True)
class WriteOutputsResult:
    """Paths plus write metadata returned by the local writer."""

    output_paths: Mapping[str, Path]
    metadata: WriteStrategyMetadata


@dataclass(frozen=True)
class WriteParquetResult:
    """Actual parquet path and strategy metadata for one parquet write."""

    output_path: Path
    metadata: WriteStrategyMetadata


class WriteStrategyError(ValueError):
    """Raised when a requested local write strategy is invalid."""


@dataclass(frozen=True)
class DefaultRequestPlanner:
    """Build one adapter request from a downloader profile."""

    def build(
        self,
        profile: Any,
        *,
        params: dict[str, Any] | None = None,
        fields: list[str] | None = None,
        concurrency: Any | None = None,
    ) -> DownloadRequestPlan:
        request_params = self._request_params_for_download(profile, params or {})
        request_fields = fields if fields is not None else profile.default_fields
        options: dict[str, Any] = {}
        adapter_options: dict[str, Any] = {}
        if concurrency is not None:
            concurrency_options = concurrency.to_dict()
            options["concurrency"] = concurrency_options
            adapter_options.update(
                {
                    "source_server_count": concurrency_options["source_server_count"],
                    "connections_per_server": concurrency_options["connections_per_server"],
                }
            )
        return DownloadRequestPlan(
            interface_name=profile.interface_name,
            params=request_params,
            fields=request_fields,
            options=options,
            adapter_options=adapter_options,
        )

    def _request_params_for_download(
        self,
        profile: Any,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        allowed_names = self._downloader_param_names(profile)
        if allowed_names is None:
            request_params = dict(profile.default_params)
            request_params.update(params)
            return request_params

        request_params = {
            key: value
            for key, value in profile.default_params.items()
            if key in allowed_names
        }
        request_params.update(
            {
                key: value
                for key, value in params.items()
                if key in allowed_names
            }
        )
        return request_params

    def _downloader_param_names(self, profile: Any) -> set[str] | None:
        if profile.params is None:
            return None
        return {str(row[0]) for row in profile.params if row}


@dataclass(frozen=True)
class DownloadWriter:
    """Write downloader records to local files with atomic replacement."""

    supported_write_modes: tuple[str, ...] = (
        "append",
        "snapshot",
        "overwrite_partition",
        "replace_range",
        "upsert_by_key",
    )

    def write_outputs_with_metadata(
        self,
        profile: Any,
        frame: pd.DataFrame,
        *,
        output_dir: Path,
        file_stem: str,
        formats: list[str],
        progress_callback: ProgressCallback | None = None,
        write_mode: str | None = None,
        replace_range_start: str | None = None,
        replace_range_end: str | None = None,
        field_types: Mapping[str, str] | None = None,
    ) -> WriteOutputsResult:
        output_paths: dict[str, Path] = {}
        format_metadata: dict[str, dict[str, Any]] = {}
        selected_write_mode = self.normalize_write_mode(write_mode or getattr(profile, "write_mode", None))
        primary_key = _primary_key_tuple(getattr(profile, "primary_key", ()))
        partition_by = tuple(str(item) for item in getattr(profile, "partition_by", ()) or ())
        date_field = _date_field(profile)
        field_types = dict(field_types or getattr(profile, "field_types", {}) or {})
        total = len(formats)
        representative: WriteStrategyMetadata | None = None

        for index, output_format in enumerate(formats):
            emit_progress(
                progress_callback,
                75 + int(index * 20 / max(total, 1)),
                f"写入 {output_format} 文件",
                progress_current=index,
                progress_total=total,
                progress_unit="个格式",
                eta_ms=None,
            )
            output_path = output_dir / output_format / f"{file_stem}.{output_format}"
            format_write_mode = selected_write_mode if output_format == "parquet" else "snapshot"
            # 与 write_parquet_with_result 的降级规则一致:分区列缺失时按普通文件路由
            effective_partition_by = (
                tuple(field_name for field_name in partition_by if field_name in frame.columns)
                if output_format == "parquet" and format_write_mode == "upsert_by_key"
                else partition_by
            )
            if output_format == "parquet" and (
                format_write_mode in {"overwrite_partition", "replace_range"}
                or (format_write_mode == "upsert_by_key" and effective_partition_by)
            ):
                output_path = output_dir / output_format
            output_path.parent.mkdir(parents=True, exist_ok=True)
            if output_format == "parquet":
                parquet_result = self.write_parquet_with_result(
                    frame,
                    output_path,
                    write_mode=format_write_mode,
                    primary_key=primary_key,
                    partition_by=partition_by,
                    date_field=date_field,
                    replace_range_start=replace_range_start,
                    replace_range_end=replace_range_end,
                    field_types=field_types,
                )
                output_path = parquet_result.output_path
                metadata = parquet_result.metadata
            else:
                self.write_frame_atomic(frame, output_path, output_format)
                metadata = WriteStrategyMetadata(
                    write_mode=format_write_mode,
                    partition_by=(),
                    primary_key=primary_key,
                    date_field=date_field,
                    rows_written=int(len(frame)),
                    rows_after=int(len(frame)),
                )
            output_paths[output_format] = output_path
            format_metadata[output_format] = metadata.to_dict()
            if representative is None or output_format == "parquet":
                representative = metadata
            emit_progress(
                progress_callback,
                75 + int((index + 1) * 20 / max(total, 1)),
                f"写入 {output_format} 文件完成",
                progress_current=index + 1,
                progress_total=total,
                progress_unit="个格式",
                eta_ms=None,
            )

        if representative is None:
            representative = WriteStrategyMetadata(
                write_mode=selected_write_mode,
                partition_by=partition_by,
                primary_key=primary_key,
                date_field=date_field,
                rows_written=int(len(frame)),
            )
        return WriteOutputsResult(
            output_paths=output_paths,
            metadata=WriteStrategyMetadata(
                write_mode=representative.write_mode,
                partition_by=representative.partition_by,
                primary_key=representative.primary_key,
                date_field=representative.date_field,
                replace_range_start=representative.replace_range_start,
                replace_range_end=representative.replace_range_end,
                rows_before=representative.rows_before,
                rows_written=representative.rows_written,
                rows_after=representative.rows_after,
                duplicate_rows_dropped=representative.duplicate_rows_dropped,
                partitions_touched=representative.partitions_touched,
                formats=format_metadata,
            ),
        )

    def write_outputs(
        self,
        profile: Any,
        frame: pd.DataFrame,
        *,
        output_dir: Path,
        file_stem: str,
        formats: list[str],
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Path]:
        return dict(
            self.write_outputs_with_metadata(
                profile,
                frame,
                output_dir=output_dir,
                file_stem=file_stem,
                formats=formats,
                progress_callback=progress_callback,
            ).output_paths
        )

    def normalize_write_mode(self, value: str | None) -> str:
        text = str(value or "snapshot").strip().lower()
        if text not in self.supported_write_modes:
            allowed = ", ".join(self.supported_write_modes)
            raise WriteStrategyError(f"write_mode must be one of: {allowed}.")
        return text

    def write_parquet_with_mode(
        self,
        frame: pd.DataFrame,
        output_path: Path,
        *,
        write_mode: str,
        primary_key: Sequence[str] | str | None = None,
        partition_by: Sequence[str] | None = None,
        date_field: str | None = None,
        replace_range_start: str | None = None,
        replace_range_end: str | None = None,
    ) -> WriteStrategyMetadata:
        return self.write_parquet_with_result(
            frame,
            output_path,
            write_mode=write_mode,
            primary_key=primary_key,
            partition_by=partition_by,
            date_field=date_field,
            replace_range_start=replace_range_start,
            replace_range_end=replace_range_end,
        ).metadata

    def write_parquet_with_result(
        self,
        frame: pd.DataFrame,
        output_path: Path,
        *,
        write_mode: str,
        primary_key: Sequence[str] | str | None = None,
        partition_by: Sequence[str] | None = None,
        date_field: str | None = None,
        replace_range_start: str | None = None,
        replace_range_end: str | None = None,
        field_types: Mapping[str, str] | None = None,
    ) -> WriteParquetResult:
        mode = self.normalize_write_mode(write_mode)
        key_fields = _primary_key_tuple(primary_key)
        partition_fields = tuple(str(item) for item in partition_by or () if str(item).strip())
        # 数据帧缺失的分区字段自动降级为普通单文件写出:
        # 避免统一 upsert_by_key 后,不含分区列的数据(如只有 trade_time 的 K 线)崩溃
        if mode == "upsert_by_key":
            partition_fields = tuple(field_name for field_name in partition_fields if field_name in frame.columns)
        field = str(date_field).strip() if date_field else None
        rows_written = int(len(frame))
        # 按声明字段类型统一列类型:避免全空列写成 arrow null 类型,
        # 与其他日期有值的分区形成混合模式后,目录级读取/合并抛 cast 错误
        frame = _coerce_frame_dtypes(frame, field_types)

        if mode == "upsert_by_key" and not key_fields:
            raise WriteStrategyError("upsert_by_key requires primary_key.")
        if mode == "replace_range" and not field:
            raise WriteStrategyError("replace_range requires date_field.")
        if mode == "overwrite_partition" and not partition_fields:
            raise WriteStrategyError("overwrite_partition requires partition_by.")

        if mode in {"append", "snapshot"}:
            rows_before = _parquet_row_count(output_path) if output_path.exists() else None
            if mode == "append":
                output_path = _next_available_path(output_path)
            self.write_frame_atomic(frame, output_path, "parquet")
            return WriteParquetResult(
                output_path=output_path,
                metadata=WriteStrategyMetadata(
                    write_mode=mode,
                    partition_by=partition_fields,
                    primary_key=key_fields,
                    date_field=field,
                    rows_before=rows_before,
                    rows_written=rows_written,
                    rows_after=rows_written,
                ),
            )

        if mode == "upsert_by_key":
            if frame.empty:
                # 空结果不写盘也不校验主键:0 行 DataFrame 无列属正常
                # (如盘中采集当日尚未发布的复盘数据),与采集器空结果语义一致
                return WriteParquetResult(
                    output_path=output_path,
                    metadata=WriteStrategyMetadata(
                        write_mode=mode,
                        partition_by=partition_fields,
                        primary_key=key_fields,
                        date_field=field,
                        rows_written=0,
                    ),
                )
            self._ensure_columns(frame, key_fields, "primary_key")
            existing = self._read_existing_parquet(output_path)
            rows_before = int(len(existing)) if existing is not None else 0
            existing_partitions = _partition_labels(existing, partition_fields) if (
                partition_fields and existing is not None and not existing.empty
            ) else ()
            if existing is None or existing.empty:
                combined = frame.copy()
            else:
                self._ensure_columns(existing, key_fields, "existing primary_key")
                combined = pd.concat([existing, frame.copy()], ignore_index=True, sort=False)
            # 键归一化只用于去重比较,不回写数据:
            # 时间戳类键字段(如 trade_time)不能被压缩成日期,否则丢时间分量并造成键碰撞
            comparison_keys = self._normalize_key_columns(
                combined.loc[:, list(key_fields)].copy(), key_fields
            )
            keep_mask = ~comparison_keys.duplicated(keep="last")
            duplicate_rows_dropped = int((~keep_mask).sum())
            merged = combined.loc[keep_mask].reset_index(drop=True)
            merged = _coerce_frame_dtypes(merged, field_types)
            self._remove_stale_partitions(
                output_path,
                partition_fields,
                existing_partitions,
                _partition_labels(merged, partition_fields),
                date_field=field,
            )
            self._write_parquet_target(merged, output_path, partition_by=partition_fields, date_field=field)
            return WriteParquetResult(
                output_path=output_path,
                metadata=WriteStrategyMetadata(
                    write_mode=mode,
                    partition_by=partition_fields,
                    primary_key=key_fields,
                    date_field=field,
                    rows_before=rows_before,
                    rows_written=rows_written,
                    rows_after=int(len(merged)),
                    duplicate_rows_dropped=duplicate_rows_dropped,
                    partitions_touched=_partition_labels(frame, partition_fields),
                ),
            )

        if mode == "replace_range":
            self._ensure_columns(frame, [field], "date_field")
            start, end = _replace_range_bounds(
                frame,
                field,
                replace_range_start=replace_range_start,
                replace_range_end=replace_range_end,
            )
            existing = self._read_existing_parquet(output_path)
            rows_before = int(len(existing)) if existing is not None else 0
            existing_partitions = _partition_labels(existing, partition_fields) if (
                partition_fields and existing is not None and not existing.empty
            ) else ()
            if existing is None or existing.empty:
                kept = pd.DataFrame(columns=frame.columns)
            else:
                self._ensure_columns(existing, [field], "existing date_field")
                dates = existing[field].map(_normalize_date_text)
                kept = existing.loc[~dates.map(lambda item: _date_in_range(item, start, end))].copy()
            merged = pd.concat([kept, frame], ignore_index=True, sort=False)
            if partition_fields and field in partition_fields:
                target_dir = output_path if output_path.suffix.lower() != ".parquet" else output_path.parent
                self._remove_date_range_partitions(target_dir, field, start, end)
            self._remove_stale_partitions(
                output_path,
                partition_fields,
                existing_partitions,
                _partition_labels(merged, partition_fields),
                date_field=field,
            )
            self._write_parquet_target(merged, output_path, partition_by=partition_fields, date_field=field)
            return WriteParquetResult(
                output_path=output_path,
                metadata=WriteStrategyMetadata(
                    write_mode=mode,
                    partition_by=partition_fields,
                    primary_key=key_fields,
                    date_field=field,
                    replace_range_start=start,
                    replace_range_end=end,
                    rows_before=rows_before,
                    rows_written=rows_written,
                    rows_after=int(len(merged)),
                    partitions_touched=_partition_labels(frame, partition_fields),
                ),
            )

        self._ensure_columns(frame, partition_fields, "partition_by")
        target_dir = output_path if output_path.suffix.lower() != ".parquet" else output_path.parent
        touched = _partition_labels(frame, partition_fields)
        rows_before = self._partition_rows_before(target_dir, touched)
        self._write_partitioned_parquet(
            frame,
            target_dir,
            partition_by=partition_fields,
            replace_partitions=True,
            date_field=field,
        )
        rows_after = _parquet_row_count(target_dir)
        return WriteParquetResult(
            output_path=target_dir,
            metadata=WriteStrategyMetadata(
                write_mode=mode,
                partition_by=partition_fields,
                primary_key=key_fields,
                date_field=field,
                rows_before=rows_before,
                rows_written=rows_written,
                rows_after=rows_after,
                partitions_touched=touched,
            ),
        )

    def _read_existing_parquet(self, output_path: Path) -> pd.DataFrame | None:
        if not output_path.exists():
            return None
        try:
            if output_path.is_dir():
                # 按文件读取后在 pandas 层合并:历史分区可能存在 null 类型与
                # 有值类型的混合模式,数据集级读取会触发 arrow cast 错误。
                # Hive 分区目录(key=value)隐含的列需手动恢复
                files = sorted(output_path.rglob("*.parquet"))
                if not files:
                    return None
                frames = []
                for path in files:
                    frame = pd.read_parquet(path, engine="pyarrow")
                    for key, value in _partition_values_from_path(path, output_path).items():
                        if key not in frame.columns:
                            frame[key] = value
                    frames.append(frame)
                return pd.concat(frames, ignore_index=True, sort=False)
            return pd.read_parquet(output_path, engine="pyarrow")
        except (FileNotFoundError, OSError):
            return None

    def _write_parquet_target(
        self,
        frame: pd.DataFrame,
        output_path: Path,
        *,
        partition_by: Sequence[str],
        date_field: str | None,
    ) -> None:
        if partition_by:
            target_dir = output_path if output_path.suffix.lower() != ".parquet" else output_path.parent
            self._write_partitioned_parquet(
                frame,
                target_dir,
                partition_by=partition_by,
                replace_partitions=True,
                date_field=date_field,
            )
            return
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.write_frame_atomic(frame, output_path, "parquet")

    def _write_partitioned_parquet(
        self,
        frame: pd.DataFrame,
        target_dir: Path,
        *,
        partition_by: Sequence[str],
        replace_partitions: bool,
        date_field: str | None,
    ) -> None:
        self._ensure_columns(frame, partition_by, "partition_by")
        target_dir.mkdir(parents=True, exist_ok=True)
        if frame.empty:
            return
        partition_fields = list(partition_by)
        flat_date_field = partition_fields[0] if _is_flat_date_partition(partition_fields, date_field) else None
        grouped = frame.groupby(partition_fields, dropna=False, sort=True)
        for keys, group in grouped:
            key_tuple = keys if isinstance(keys, tuple) else (keys,)
            if flat_date_field:
                partition_value = key_tuple[0]
                output_file = target_dir / f"{_normalize_date_text(_partition_value_text(partition_value))}.parquet"
                if replace_partitions:
                    self._remove_flat_partition_file(output_file)
                    for partition_dir in _legacy_partition_dirs(target_dir, flat_date_field, partition_value):
                        self._remove_partition_parquet_files(partition_dir)
                physical = group.reset_index(drop=True)
                self.write_frame_atomic(physical, output_file, "parquet")
                continue
            partition_dir = target_dir.joinpath(
                *[
                    f"{field}={_partition_value_text(value)}"
                    for field, value in zip(partition_fields, key_tuple, strict=False)
                ]
            )
            if replace_partitions:
                self._remove_partition_parquet_files(partition_dir)
            partition_dir.mkdir(parents=True, exist_ok=True)
            physical = group.drop(columns=partition_fields, errors="ignore").reset_index(drop=True)
            self.write_frame_atomic(physical, partition_dir / "part-0.parquet", "parquet")

    def _remove_partition_parquet_files(self, partition_dir: Path) -> None:
        if not partition_dir.exists():
            return
        for path in partition_dir.glob("*.parquet"):
            if path.is_file():
                path.unlink()

    def _remove_date_range_partitions(self, target_dir: Path, date_field: str, start: str, end: str) -> None:
        if not target_dir.exists():
            return
        prefix = f"{date_field}="
        for partition_dir in target_dir.glob(f"{date_field}=*"):
            if not partition_dir.is_dir() or not partition_dir.name.startswith(prefix):
                continue
            value = _normalize_date_text(partition_dir.name.split("=", 1)[1])
            if _date_in_range(value, start, end):
                self._remove_partition_parquet_files(partition_dir)
        for partition_file in target_dir.glob("*.parquet"):
            if not partition_file.is_file():
                continue
            value = _normalize_date_text(partition_file.stem)
            if len(value) == 8 and value.isdigit() and _date_in_range(value, start, end):
                self._remove_flat_partition_file(partition_file)

    def _remove_stale_partitions(
        self,
        output_path: Path,
        partition_by: Sequence[str],
        previous_labels: Sequence[str],
        current_labels: Sequence[str],
        *,
        date_field: str | None,
    ) -> None:
        if not partition_by:
            return
        target_dir = output_path if output_path.suffix.lower() != ".parquet" else output_path.parent
        stale_labels = sorted(set(previous_labels) - set(current_labels))
        for label in stale_labels:
            self._remove_partition_parquet_files(target_dir.joinpath(*label.split("/")))
            flat_file = _flat_date_partition_file_from_label(target_dir, partition_by, label, date_field)
            if flat_file is not None:
                self._remove_flat_partition_file(flat_file)

    def _partition_rows_before(self, target_dir: Path, partition_labels: Sequence[str]) -> int:
        total = 0
        for label in partition_labels:
            partition_dir = target_dir.joinpath(*label.split("/"))
            total += _parquet_row_count(partition_dir) or 0
            field, value = _single_partition_label(label)
            if field and value:
                total += _parquet_row_count(target_dir / f"{_normalize_date_text(value)}.parquet") or 0
        return total

    def _remove_flat_partition_file(self, path: Path) -> None:
        if path.is_file():
            path.unlink()

    def _ensure_columns(self, frame: pd.DataFrame, columns: Sequence[str], label: str) -> None:
        missing = [column for column in columns if column not in frame.columns]
        if missing:
            raise WriteStrategyError(f"{label} column(s) missing: {', '.join(missing)}.")

    def _normalize_key_columns(self, frame: pd.DataFrame, key_fields: Sequence[str]) -> pd.DataFrame:
        for field in key_fields:
            if field in frame.columns:
                frame[field] = frame[field].map(_key_value_text)
        return frame

    def write_frame_atomic(
        self,
        frame: pd.DataFrame,
        output_path: Path,
        output_format: str,
    ) -> None:
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{output_path.stem}.",
            suffix=f".{output_format}.tmp",
            dir=str(output_path.parent),
        )
        tmp_path = Path(tmp_name)
        os.close(fd)
        try:
            if output_format == "parquet":
                frame.to_parquet(tmp_path, engine="pyarrow", index=False)
            elif output_format == "csv":
                frame.to_csv(tmp_path, index=False, encoding="utf-8-sig")
            elif output_format == "jsonl":
                frame.to_json(tmp_path, orient="records", lines=True, force_ascii=False)
            elif output_format == "duckdb":
                import duckdb

                tmp_path.unlink(missing_ok=True)
                with duckdb.connect(str(tmp_path)) as conn:
                    conn.register("records", frame)
                    conn.execute("CREATE OR REPLACE TABLE data AS SELECT * FROM records")
                    conn.unregister("records")
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
            os.replace(tmp_path, output_path)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


@dataclass(frozen=True)
class DownloadQualityChecker:
    """Evaluate built-in downloader quality summaries."""

    sample_limit: int = 5

    def evaluate(
        self,
        frame: pd.DataFrame,
        *,
        primary_key: str | tuple[str, ...],
        rules: list[str] | tuple[str, ...] | None = None,
        required_columns: list[str] | tuple[str, ...] | None = None,
        expected_columns: list[str] | tuple[str, ...] | None = None,
        date_field: str | None = None,
        datetime_field: str | None = None,
        numeric_positive_columns: list[str] | tuple[str, ...] | None = None,
        field_mappings: dict[str, str] | None = None,
        calendar_check: bool = False,
        trade_calendar_dates: Mapping[str, Sequence[str]] | Sequence[str] | None = None,
        symbol_field: str | None = None,
        suspension_dates: Mapping[str, Sequence[str]] | None = None,
    ) -> dict[str, Any]:
        enabled_rules = set(rules or ("schema", "primary_key", "row_count"))
        quality: dict[str, Any] = {}
        warnings: list[str] = []
        errors: list[str] = []

        actual_columns = [str(column) for column in frame.columns]
        expected_column_list = [str(column) for column in expected_columns or ()]
        required_column_list = [str(column) for column in required_columns or ()]
        missing_required = [column for column in required_column_list if column not in actual_columns]
        unexpected_columns = [
            column
            for column in actual_columns
            if expected_column_list and column not in expected_column_list
        ]

        if "schema" in enabled_rules:
            quality["schema"] = "missing_required" if missing_required else "pass"
            if missing_required:
                errors.append(
                    "Missing required column(s): " + ", ".join(missing_required)
                )
            if unexpected_columns:
                warnings.append(
                    "Unexpected column(s): " + ", ".join(unexpected_columns)
                )
        if "primary_key" in enabled_rules:
            primary_key_result = self.primary_key_quality(frame, primary_key)
            quality["primary_key"] = primary_key_result["status"]
            if primary_key_result["status"] != "pass":
                errors.append(primary_key_result["message"])
            quality["duplicate_key_count"] = primary_key_result["duplicate_key_count"]
            quality["duplicate_key_samples"] = primary_key_result.get("duplicate_key_samples", [])
        if "row_count" in enabled_rules:
            quality["row_count"] = "pass" if len(frame) > 0 else "warning"
            if len(frame) == 0:
                warnings.append("No rows returned.")

        null_counts = self._null_counts(frame, required_column_list)
        for column, count in null_counts.items():
            if count > 0:
                errors.append(f"Required column {column!r} has {count} null row(s).")

        date_range_field = date_field or datetime_field
        date_range = self._date_range(frame, date_range_field)
        date_profile = self._date_profile(frame, date_range_field)
        numeric_checks = self._numeric_positive_checks(frame, numeric_positive_columns or ())
        for column, result in numeric_checks.items():
            if result["status"] == "missing":
                warnings.append(f"Numeric check skipped because column {column!r} is missing.")
            elif result["status"] == "negative":
                errors.append(
                    f"Numeric column {column!r} has {result['negative_count']} negative row(s)."
                )

        calendar_quality = self._calendar_quality(
            frame,
            date_range_field,
            trade_calendar_dates=trade_calendar_dates,
            calendar_check=calendar_check,
            symbol_field=symbol_field,
            primary_key=primary_key,
            suspension_dates=suspension_dates,
        )
        warnings.extend(calendar_quality.pop("_warnings", []))
        errors.extend(calendar_quality.pop("_errors", []))

        market_quality = self._market_rule_quality(frame, date_range_field, primary_key)
        warnings.extend(market_quality.pop("_warnings", []))
        errors.extend(market_quality.pop("_errors", []))

        quality.update(
            {
                "quality_status": "error" if errors else "warn" if warnings else "ok",
                "quality_warnings": warnings,
                "quality_errors": errors,
                "row_count_value": int(len(frame)),
                "required_columns_present": not missing_required,
                "missing_required_columns": missing_required,
                "null_counts": null_counts,
                "schema_columns": actual_columns,
                "expected_columns": expected_column_list,
                "unexpected_columns": unexpected_columns,
                "date_field": date_range_field,
                "date_range": date_range,
                "min_date": date_profile["min_date"],
                "max_date": date_profile["max_date"],
                "actual_date_count": date_profile["actual_date_count"],
                "numeric_positive_checks": numeric_checks,
                "field_mappings": dict(field_mappings or {}),
            }
        )
        quality.update(calendar_quality)
        quality.update(market_quality)
        return quality

    def primary_key_quality(
        self,
        frame: pd.DataFrame,
        primary_key: str | tuple[str, ...],
    ) -> dict[str, Any]:
        primary_key_fields = (primary_key,) if isinstance(primary_key, str) else primary_key
        missing_fields = [field for field in primary_key_fields if field not in frame.columns]
        if missing_fields:
            return {
                "status": "missing",
                "duplicate_key_count": 0,
                "duplicate_key_samples": [],
                "message": "Missing primary key field(s): " + ", ".join(missing_fields),
            }
        if frame[list(primary_key_fields)].isna().any().any():
            return {
                "status": "missing_value",
                "duplicate_key_count": 0,
                "duplicate_key_samples": [],
                "message": "Primary key contains null value(s).",
            }
        duplicate_key_count = int(frame.duplicated(subset=list(primary_key_fields), keep=False).sum())
        if duplicate_key_count:
            duplicate_rows = frame.loc[
                frame.duplicated(subset=list(primary_key_fields), keep=False),
                list(primary_key_fields),
            ]
            return {
                "status": "duplicate",
                "duplicate_key_count": duplicate_key_count,
                "duplicate_key_samples": self._sample_records(duplicate_rows.drop_duplicates()),
                "message": f"Found {duplicate_key_count} duplicate primary key row(s).",
            }
        return {
            "status": "pass",
            "duplicate_key_count": 0,
            "duplicate_key_samples": [],
            "message": "",
        }

    def primary_key_status(
        self,
        frame: pd.DataFrame,
        primary_key: str | tuple[str, ...],
    ) -> str:
        return self.primary_key_quality(frame, primary_key)["status"]

    def _null_counts(
        self,
        frame: pd.DataFrame,
        required_columns: list[str],
    ) -> dict[str, int]:
        return {
            column: int(frame[column].isna().sum())
            for column in required_columns
            if column in frame.columns
        }

    def _date_range(
        self,
        frame: pd.DataFrame,
        field: str | None,
    ) -> dict[str, str | None]:
        if not field:
            return {"min": None, "max": None}
        if field not in frame.columns or frame.empty:
            return {"min": None, "max": None}
        values = frame[field].dropna()
        if values.empty:
            return {"min": None, "max": None}
        text_values = values.astype(str)
        return {"min": str(text_values.min()), "max": str(text_values.max())}

    def _date_profile(self, frame: pd.DataFrame, field: str | None) -> dict[str, Any]:
        dates = self._normalized_dates(frame, field)
        unique_dates = sorted(set(dates.dropna().astype(str).tolist()))
        return {
            "min_date": unique_dates[0] if unique_dates else None,
            "max_date": unique_dates[-1] if unique_dates else None,
            "actual_date_count": len(unique_dates),
        }

    def _numeric_positive_checks(
        self,
        frame: pd.DataFrame,
        columns: list[str] | tuple[str, ...],
    ) -> dict[str, dict[str, Any]]:
        checks: dict[str, dict[str, Any]] = {}
        for column in columns:
            column_name = str(column)
            if column_name not in frame.columns:
                checks[column_name] = {
                    "status": "missing",
                    "negative_count": 0,
                    "null_count": 0,
                }
                continue
            numeric = pd.to_numeric(frame[column_name], errors="coerce")
            negative_count = int((numeric < 0).sum())
            null_count = int(numeric.isna().sum())
            checks[column_name] = {
                "status": "negative" if negative_count else "pass",
                "negative_count": negative_count,
                "null_count": null_count,
            }
        return checks

    def _calendar_quality(
        self,
        frame: pd.DataFrame,
        date_field: str | None,
        *,
        trade_calendar_dates: Mapping[str, Sequence[str]] | Sequence[str] | None,
        calendar_check: bool,
        symbol_field: str | None,
        primary_key: str | tuple[str, ...],
        suspension_dates: Mapping[str, Sequence[str]] | None,
    ) -> dict[str, Any]:
        base: dict[str, Any] = {
            "calendar_check_applied": bool(calendar_check),
            "calendar_coverage_status": None,
            "expected_trading_day_count": None,
            "actual_trading_day_count": None,
            "missing_trading_dates": [],
            "extra_non_trading_dates": [],
            "date_gap_count": 0,
            "missing_date_samples": [],
            "per_symbol_date_coverage": [],
            "unexplained_missing_dates": [],
            "suspension_explained_missing_dates": [],
        }
        warnings: list[str] = []
        errors: list[str] = []
        if not calendar_check:
            return base
        if not date_field or date_field not in frame.columns:
            base["calendar_coverage_status"] = "warn"
            warnings.append("Calendar alignment skipped because no date field is present.")
            base["calendar_next_action"] = "确认 profile 是否声明 date_field 或 datetime_field。"
            base["_warnings"] = warnings
            return base

        actual_dates = sorted(set(self._normalized_dates(frame, date_field).dropna().astype(str).tolist()))
        base["actual_trading_day_count"] = len(actual_dates)
        if not actual_dates:
            base["calendar_coverage_status"] = "warn"
            warnings.append("Calendar alignment skipped because no valid trade dates were parsed.")
            base["calendar_next_action"] = "检查日期字段是否为 YYYYMMDD 或可解析时间。"
            base["_warnings"] = warnings
            return base

        calendar_by_exchange = self._normalize_calendar_dates(trade_calendar_dates)
        all_calendar_dates = sorted({date for dates in calendar_by_exchange.values() for date in dates})
        if not all_calendar_dates:
            base["calendar_coverage_status"] = "warn"
            warnings.append("Trading calendar is not available; date gaps are not classified as errors.")
            base["calendar_next_action"] = "先采集 trade_cal，再重新运行行情或复权因子采集。"
            base["date_gap_explanation"] = (
                "No local trade calendar was available. Missing dates may be trading gaps, "
                "suspensions, listing windows, source omissions, or uncollected ranges."
            )
            base["_warnings"] = warnings
            return base

        start = actual_dates[0]
        end = actual_dates[-1]
        expected_dates = [date for date in all_calendar_dates if start <= date <= end]
        missing_dates = sorted(set(expected_dates) - set(actual_dates))
        extra_dates = sorted(set(actual_dates) - set(all_calendar_dates))

        base.update(
            {
                "calendar_date_range": {"min": all_calendar_dates[0], "max": all_calendar_dates[-1]},
                "expected_trading_day_count": len(expected_dates),
                "missing_trading_dates": self._sample_values(missing_dates),
                "extra_non_trading_dates": self._sample_values(extra_dates),
                "date_gap_count": len(missing_dates),
                "missing_date_samples": self._sample_values(missing_dates),
            }
        )
        if start < all_calendar_dates[0] or end > all_calendar_dates[-1]:
            warnings.append(
                "Trading calendar does not fully cover the dataset date range; gap counts may be incomplete."
            )

        symbol_name = symbol_field or self._infer_symbol_field(frame, primary_key)
        per_symbol, unexplained, explained = self._per_symbol_calendar_coverage(
            frame,
            date_field,
            calendar_by_exchange=calendar_by_exchange,
            all_calendar_dates=all_calendar_dates,
            symbol_field=symbol_name,
            suspension_dates=suspension_dates or {},
        )
        base["per_symbol_date_coverage"] = per_symbol
        base["unexplained_missing_dates"] = self._sample_gap_records(unexplained)
        base["suspension_explained_missing_dates"] = self._sample_gap_records(explained)

        if extra_dates:
            base["calendar_coverage_status"] = "error"
            errors.append("Data contains non-trading date(s): " + ", ".join(self._sample_values(extra_dates)))
        elif missing_dates or unexplained:
            base["calendar_coverage_status"] = "warn"
            warnings.append(
                "Trading date gap(s) found; gaps may be suspensions, not-yet-listed/delisted windows, "
                "source omissions, or uncollected ranges."
            )
        elif warnings:
            base["calendar_coverage_status"] = "warn"
        else:
            base["calendar_coverage_status"] = "ok"
        base["date_gap_explanation"] = (
            "Missing trading dates are not always data errors. They can be caused by suspensions, "
            "listing/delisting windows, upstream omissions, or partial collection."
        )
        if not explained:
            base["suspension_coverage_status"] = "not_available"
        else:
            base["suspension_coverage_status"] = "partial"
        base["_warnings"] = warnings
        base["_errors"] = errors
        return base

    def _market_rule_quality(
        self,
        frame: pd.DataFrame,
        date_field: str | None,
        primary_key: str | tuple[str, ...],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "price_ohlc_anomaly_count": 0,
            "price_ohlc_anomaly_samples": [],
            "negative_volume_count": 0,
            "negative_volume_samples": [],
            "negative_amount_count": 0,
            "negative_amount_samples": [],
            "invalid_adj_factor_count": 0,
            "invalid_adj_factor_samples": [],
        }
        errors: list[str] = []

        ohlc_columns = ("open", "high", "low", "close")
        if all(column in frame.columns for column in ohlc_columns):
            ohlc = frame.loc[:, list(ohlc_columns)].apply(pd.to_numeric, errors="coerce")
            mask = (
                (ohlc["high"] < ohlc["low"])
                | (ohlc["high"] < ohlc["open"])
                | (ohlc["high"] < ohlc["close"])
                | (ohlc["low"] > ohlc["open"])
                | (ohlc["low"] > ohlc["close"])
            )
            indexes = list(frame.index[mask.fillna(False)])
            result["price_ohlc_anomaly_count"] = len(indexes)
            result["price_ohlc_anomaly_samples"] = self._sample_rows(
                frame,
                indexes,
                self._sample_fields(frame, primary_key, date_field, *ohlc_columns),
            )
            if indexes:
                errors.append(f"OHLC field rule failed for {len(indexes)} row(s).")

        volume_columns = [column for column in ("vol", "volume") if column in frame.columns]
        volume_indexes = self._negative_indexes(frame, volume_columns)
        result["negative_volume_count"] = len(volume_indexes)
        result["negative_volume_samples"] = self._sample_rows(
            frame,
            volume_indexes,
            self._sample_fields(frame, primary_key, date_field, *volume_columns),
        )
        if volume_indexes:
            errors.append(f"Volume field has {len(volume_indexes)} negative row(s).")

        amount_indexes = self._negative_indexes(frame, ["amount"] if "amount" in frame.columns else [])
        result["negative_amount_count"] = len(amount_indexes)
        result["negative_amount_samples"] = self._sample_rows(
            frame,
            amount_indexes,
            self._sample_fields(frame, primary_key, date_field, "amount"),
        )
        if amount_indexes:
            errors.append(f"Amount field has {len(amount_indexes)} negative row(s).")

        if "adj_factor" in frame.columns:
            factors = pd.to_numeric(frame["adj_factor"], errors="coerce")
            invalid_mask = factors.notna() & (factors <= 0)
            invalid_indexes = list(frame.index[invalid_mask])
            result["invalid_adj_factor_count"] = len(invalid_indexes)
            result["invalid_adj_factor_samples"] = self._sample_rows(
                frame,
                invalid_indexes,
                self._sample_fields(frame, primary_key, date_field, "adj_factor"),
            )
            if invalid_indexes:
                errors.append(f"adj_factor must be > 0; found {len(invalid_indexes)} invalid row(s).")

        result["_errors"] = errors
        return result

    def _normalized_dates(self, frame: pd.DataFrame, field: str | None) -> pd.Series:
        if not field or field not in frame.columns:
            return pd.Series([], dtype="object")
        return frame[field].map(self._normalize_date_value)

    def _normalize_date_value(self, value: Any) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        text = str(value).strip()
        if not text:
            return None
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) >= 8:
            return digits[:8]
        return None

    def _normalize_calendar_dates(
        self,
        trade_calendar_dates: Mapping[str, Sequence[str]] | Sequence[str] | None,
    ) -> dict[str, set[str]]:
        if trade_calendar_dates is None:
            return {}
        if isinstance(trade_calendar_dates, Mapping):
            normalized: dict[str, set[str]] = {}
            for exchange, values in trade_calendar_dates.items():
                dates = {
                    normalized_date
                    for value in values or ()
                    if (normalized_date := self._normalize_date_value(value)) is not None
                }
                if dates:
                    normalized[str(exchange).upper()] = dates
            return normalized
        return {
            "ALL": {
                normalized_date
                for value in trade_calendar_dates
                if (normalized_date := self._normalize_date_value(value)) is not None
            }
        }

    def _per_symbol_calendar_coverage(
        self,
        frame: pd.DataFrame,
        date_field: str,
        *,
        calendar_by_exchange: Mapping[str, set[str]],
        all_calendar_dates: Sequence[str],
        symbol_field: str | None,
        suspension_dates: Mapping[str, Sequence[str]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[dict[str, str]]]:
        if not symbol_field or symbol_field not in frame.columns:
            return [], [], []
        date_series = self._normalized_dates(frame, date_field)
        rows = frame.assign(__axdata_quality_date=date_series)
        coverage: list[dict[str, Any]] = []
        unexplained: list[dict[str, str]] = []
        explained: list[dict[str, str]] = []
        for symbol_value, group in rows.dropna(subset=["__axdata_quality_date"]).groupby(symbol_field, dropna=True):
            symbol = str(symbol_value)
            actual = sorted(set(group["__axdata_quality_date"].dropna().astype(str).tolist()))
            if not actual:
                continue
            expected_source = self._calendar_dates_for_symbol(symbol, calendar_by_exchange, all_calendar_dates)
            expected = [date for date in expected_source if actual[0] <= date <= actual[-1]]
            missing = sorted(set(expected) - set(actual))
            suspensions = {
                normalized_date
                for value in suspension_dates.get(symbol, ())
                if (normalized_date := self._normalize_date_value(value)) is not None
            }
            explained_dates = [date for date in missing if date in suspensions]
            unexplained_dates = [date for date in missing if date not in suspensions]
            coverage.append(
                {
                    "symbol": symbol,
                    "expected_trading_day_count": len(expected),
                    "actual_trading_day_count": len(actual),
                    "missing_date_count": len(missing),
                    "missing_date_samples": self._sample_values(missing),
                    "extra_non_trading_date_count": len(set(actual) - set(expected_source)),
                }
            )
            unexplained.extend({"symbol": symbol, "date": date} for date in unexplained_dates)
            explained.extend({"symbol": symbol, "date": date} for date in explained_dates)
        return coverage[: self.sample_limit], unexplained, explained

    def _calendar_dates_for_symbol(
        self,
        symbol: str,
        calendar_by_exchange: Mapping[str, set[str]],
        all_calendar_dates: Sequence[str],
    ) -> list[str]:
        exchange = self._exchange_for_symbol(symbol)
        if exchange and exchange in calendar_by_exchange:
            return sorted(calendar_by_exchange[exchange])
        if "ALL" in calendar_by_exchange:
            return sorted(calendar_by_exchange["ALL"])
        return list(all_calendar_dates)

    def _exchange_for_symbol(self, symbol: str) -> str | None:
        text = symbol.upper()
        if text.endswith(".SH"):
            return "SSE"
        if text.endswith(".SZ"):
            return "SZSE"
        if text.endswith(".BJ"):
            return "BSE"
        return None

    def _infer_symbol_field(
        self,
        frame: pd.DataFrame,
        primary_key: str | tuple[str, ...],
    ) -> str | None:
        primary_fields = (primary_key,) if isinstance(primary_key, str) else primary_key
        for candidate in ("ts_code", "instrument_id", "symbol", "code"):
            if candidate in frame.columns and candidate in primary_fields:
                return candidate
        for candidate in ("ts_code", "instrument_id", "symbol", "code"):
            if candidate in frame.columns:
                return candidate
        return None

    def _negative_indexes(self, frame: pd.DataFrame, columns: Sequence[str]) -> list[Any]:
        indexes: set[Any] = set()
        for column in columns:
            numeric = pd.to_numeric(frame[column], errors="coerce")
            indexes.update(frame.index[numeric < 0].tolist())
        return list(indexes)

    def _sample_fields(self, frame: pd.DataFrame, primary_key: str | tuple[str, ...], date_field: str | None, *fields: str) -> list[str]:
        primary_fields = (primary_key,) if isinstance(primary_key, str) else primary_key
        selected: list[str] = []
        for field in (*primary_fields, date_field, *fields):
            if field and field in frame.columns and field not in selected:
                selected.append(str(field))
        return selected

    def _sample_rows(self, frame: pd.DataFrame, indexes: Sequence[Any], fields: Sequence[str]) -> list[dict[str, Any]]:
        if not indexes or not fields:
            return []
        return self._sample_records(frame.loc[list(indexes), list(fields)].head(self.sample_limit))

    def _sample_records(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        rows = frame.head(self.sample_limit).to_dict(orient="records")
        return [
            {str(key): self._jsonable_scalar(value) for key, value in row.items()}
            for row in rows
        ]

    def _sample_values(self, values: Sequence[str]) -> list[str]:
        return [str(value) for value in list(values)[: self.sample_limit]]

    def _sample_gap_records(self, rows: Sequence[Mapping[str, str]]) -> list[dict[str, str]]:
        return [dict(row) for row in list(rows)[: self.sample_limit]]

    def _jsonable_scalar(self, value: Any) -> Any:
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if hasattr(value, "item"):
            try:
                return value.item()
            except (TypeError, ValueError):
                pass
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except (TypeError, ValueError):
                pass
        return value


@dataclass(frozen=True)
class DownloadMetadataWriter:
    """Write downloader run metadata sidecar files."""

    def write_run_log(
        self,
        output_dir: Path,
        *,
        file_stem: str,
        result: dict[str, Any],
    ) -> Path:
        log_path = output_dir / "logs" / f"{file_stem}.json"
        payload = {
            key: value
            for key, value in result.items()
            if key not in {"source_meta"}
        }
        payload["source_meta"] = result.get("source_meta", {})
        self.write_text_atomic(log_path, json.dumps(payload, ensure_ascii=False, indent=2))
        return log_path

    def write_text_atomic(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
            os.replace(tmp_path, path)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass


def emit_progress(
    progress_callback: ProgressCallback | None,
    percent: int,
    message: str,
    **details: Any,
) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(max(0, min(100, int(percent))), message, **details)
    except TypeError:
        progress_callback(max(0, min(100, int(percent))), message)


def _primary_key_tuple(value: Sequence[str] | str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        text = value.strip()
        return (text,) if text else ()
    return tuple(str(item).strip() for item in value if str(item).strip())


def _date_field(profile: Any) -> str | None:
    for name in ("date_field", "datetime_field"):
        value = getattr(profile, name, None)
        text = str(value).strip() if value is not None else ""
        if text:
            return text
    return None


def _normalize_date_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


def _replace_range_bounds(
    frame: pd.DataFrame,
    date_field: str,
    *,
    replace_range_start: str | None,
    replace_range_end: str | None,
) -> tuple[str, str]:
    normalized_dates = [
        date
        for date in (_normalize_date_text(value) for value in frame[date_field])
        if date is not None
    ]
    start = _normalize_date_text(replace_range_start) if replace_range_start else None
    end = _normalize_date_text(replace_range_end) if replace_range_end else None
    if start is None:
        start = min(normalized_dates) if normalized_dates else None
    if end is None:
        end = max(normalized_dates) if normalized_dates else None
    if start is None or end is None:
        raise WriteStrategyError("replace_range requires at least one valid date value.")
    if start > end:
        raise WriteStrategyError("replace_range_start must be before or equal to replace_range_end.")
    return start, end


def _date_in_range(value: str | None, start: str, end: str) -> bool:
    return value is not None and start <= value <= end


def _partition_value_text(value: Any) -> str:
    normalized = _normalize_date_text(value)
    if normalized is not None:
        return normalized
    if value is None:
        return "__null__"
    try:
        if pd.isna(value):
            return "__null__"
    except (TypeError, ValueError):
        pass
    return str(value).replace("/", "_").replace("\\", "_")


def _is_flat_date_partition(partition_by: Sequence[str], date_field: str | None) -> bool:
    return bool(date_field and len(partition_by) == 1 and partition_by[0] == date_field)


def _legacy_partition_dirs(target_dir: Path, field: str, value: Any) -> tuple[Path, ...]:
    text = _partition_value_text(value)
    dirs = [target_dir / f"{field}={text}"]
    normalized = _normalize_date_text(value)
    if normalized and len(normalized) == 8:
        dashed = f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"
        dirs.append(target_dir / f"{field}={dashed}")
    return tuple(dict.fromkeys(dirs))


def _single_partition_label(label: str) -> tuple[str | None, str | None]:
    if "/" in label or "=" not in label:
        return None, None
    field, value = label.split("=", 1)
    return (field or None), (value or None)


def _flat_date_partition_file_from_label(
    target_dir: Path,
    partition_by: Sequence[str],
    label: str,
    date_field: str | None,
) -> Path | None:
    if not _is_flat_date_partition(partition_by, date_field):
        return None
    field, value = _single_partition_label(label)
    if field != date_field or value is None:
        return None
    normalized = _normalize_date_text(value)
    if normalized is None:
        return None
    return target_dir / f"{normalized}.parquet"


def _partition_values_from_path(file_path: Path, partition_root: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        relative_parts = file_path.relative_to(partition_root).parts[:-1]
    except ValueError:
        return values
    for part in relative_parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key:
            values[key] = value
    return values


def _coerce_frame_dtypes(frame: pd.DataFrame, field_types: Mapping[str, str] | None) -> pd.DataFrame:
    """按声明字段类型统一列类型,保证跨日期分区模式一致。

    全空列若按 arrow null 类型落盘,与其他日期有值的分区会形成混合模式,
    数据集级读取(浏览/合并)会抛 "Unsupported cast" 错误。
    """
    if not field_types:
        return frame
    for column, type_name in field_types.items():
        if column not in frame.columns:
            continue
        target = str(type_name).strip().lower()
        try:
            if "int" in target:
                frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
            elif "float" in target or "number" in target:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            elif "str" in target:
                frame[column] = frame[column].astype("string")
        except (ValueError, TypeError):
            continue
    return frame


def _key_value_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    # 恰好 8 位数字视为纯日期表示,归一化为 YYYYMMDD 便于跨格式比较;
    # 带时间分量的值保留原文,避免不同时刻被压缩成同一天的日期造成键碰撞
    if len(digits) == 8:
        return digits
    return text


def _partition_labels(frame: pd.DataFrame, partition_by: Sequence[str]) -> tuple[str, ...]:
    if not partition_by:
        return ()
    labels: set[str] = set()
    for row in frame.loc[:, list(partition_by)].drop_duplicates().to_dict(orient="records"):
        labels.add(
            "/".join(
                f"{field}={_partition_value_text(row.get(field))}"
                for field in partition_by
            )
        )
    return tuple(sorted(labels))


def _parquet_row_count(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        import pyarrow.parquet as pq

        if path.is_file():
            metadata = pq.ParquetFile(path).metadata
            return int(metadata.num_rows if metadata is not None else 0)
        total = 0
        for parquet_path in path.rglob("*.parquet"):
            metadata = pq.ParquetFile(parquet_path).metadata
            total += int(metadata.num_rows if metadata is not None else 0)
        return total
    except Exception:
        return None


def _next_available_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 10000):
        candidate = path.with_name(f"{path.stem}_{index:04d}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise WriteStrategyError(f"Could not find an available append path for {path}.")
