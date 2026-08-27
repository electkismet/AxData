"""Collector capability declarations for the TDX provider package."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from axdata_core.plugins import CollectorSpec, PluginInfo, ProviderManifest


TDX_COLLECTOR_INTERFACES: tuple[str, ...] = (
    "stock_codes_tdx",
    "stock_suspensions_tdx",
    "stock_st_list_tdx",
    "stock_daily_share_tdx",
    "stock_daily_price_limit_tdx",
    "stock_kline_daily_tdx",
    "stock_limit_ladder_tdx",
    "stock_theme_strength_rank_tdx",
)

TDX_INDEPENDENT_COLLECTOR_INTERFACES: tuple[str, ...] = TDX_COLLECTOR_INTERFACES

TDX_BASE_COLLECTOR_INTERFACES: tuple[str, ...] = (
    "stock_codes_tdx",
    "stock_suspensions_tdx",
    "stock_st_list_tdx",
    "stock_daily_share_tdx",
    "stock_daily_price_limit_tdx",
)

TDX_LEGACY_COLLECTOR_INTERFACES: tuple[str, ...] = ()

TDX_COLLECTOR_PLUGIN_ID = "axdata.collector.tdx"
TDX_COLLECTOR_RUNNER_ENTRY = "axdata_source_tdx.collectors:run_tdx_collector"

_TDX_DATASET_IDS: dict[str, str] = {
    "stock_codes_tdx": "tdx.stock_codes",
    "stock_suspensions_tdx": "tdx.stock_suspensions",
    "stock_st_list_tdx": "tdx.stock_st_list",
    "stock_daily_share_tdx": "tdx.stock_daily_share",
    "stock_daily_price_limit_tdx": "tdx.stock_daily_price_limit",
    "stock_kline_daily_tdx": "tdx.stock_daily",
    "stock_limit_ladder_tdx": "tdx.stock_limit_ladder",
    "stock_theme_strength_rank_tdx": "tdx.stock_theme_strength_rank",
}

_TDX_CATEGORIES: dict[str, str] = {
    "stock_codes_tdx": "stock_reference",
    "stock_suspensions_tdx": "stock_status",
    "stock_st_list_tdx": "stock_status",
    "stock_daily_share_tdx": "share_capital",
    "stock_daily_price_limit_tdx": "price_limit",
    "stock_kline_daily_tdx": "daily",
    "stock_limit_ladder_tdx": "shortline",
    "stock_theme_strength_rank_tdx": "theme_strength",
}

_TDX_DESCRIPTIONS: dict[str, str] = {
    "stock_codes_tdx": "独立通达信采集器：采集最新股票列表并写入本地数据层。",
    "stock_suspensions_tdx": "独立通达信采集器：采集最新停牌股票列表并写入本地快照。",
    "stock_st_list_tdx": "独立通达信采集器：采集最新 ST / *ST 股票列表并写入本地快照。",
    "stock_daily_share_tdx": "独立通达信采集器：采集每日股本盘前快照并写入本地数据层。",
    "stock_daily_price_limit_tdx": "独立通达信采集器：采集最新涨跌停价格快照并写入本地数据层。",
    "stock_kline_daily_tdx": "独立通达信采集器：采集显式代码列表的日 K 线小样本并写入本地数据层；生产级全市场 raw/staging -> core 转换仍需后续任务补齐。",
    "stock_limit_ladder_tdx": "独立通达信采集器：采集当前连板天梯快照并写入本地数据层。",
    "stock_theme_strength_rank_tdx": "独立通达信采集器：采集当前题材强度排行快照并写入本地数据层。",
}

_TDX_EXECUTION_OPTIONS: dict[str, dict[str, Any]] = {
    "stock_codes_tdx": {"source_server_count": 1, "connections_per_server": 3},
    "stock_suspensions_tdx": {"source_server_count": 4, "connections_per_server": 2},
    "stock_st_list_tdx": {"source_server_count": 1, "connections_per_server": 3},
    "stock_daily_share_tdx": {"source_server_count": 4, "connections_per_server": 2},
    "stock_daily_price_limit_tdx": {"source_server_count": 4, "connections_per_server": 2},
    "stock_kline_daily_tdx": {"source_server_count": 1, "connections_per_server": 1},
    "stock_limit_ladder_tdx": {"source_server_count": 4, "connections_per_server": 2},
    "stock_theme_strength_rank_tdx": {"source_server_count": 4, "connections_per_server": 2},
}

_TDX_CALENDAR_CHECK_INTERFACES: set[str] = {
    "stock_kline_daily_tdx",
}

_TDX_REQUIRED_DATASETS: dict[str, tuple[str, ...]] = {
    "stock_kline_daily_tdx": ("trade_cal",),
}

_TDX_OUTPUT_PATH_PARTS: dict[str, list[str]] = {
    "stock_kline_daily_tdx": ["core", "table=daily"],
}

_TDX_LOGICAL_TABLES: dict[str, str] = {
    "stock_kline_daily_tdx": "daily",
}


_COLLECTOR_OUTPUT_LAYER: dict[str, str] = {
    "stock_kline_daily_tdx": "core",
}


def tdx_collectors(
    collector_spec_cls: type[Any],
    *,
    interfaces: tuple[Any, ...],
    downloaders: tuple[Any, ...],
) -> tuple[Any, ...]:
    """Return CollectorSpec declarations backed by TDX downloader profiles."""

    interface_by_name = {interface.name: interface for interface in interfaces}
    downloader_by_interface = {downloader.interface_name: downloader for downloader in downloaders}
    collectors: list[Any] = []
    for interface_name in TDX_LEGACY_COLLECTOR_INTERFACES:
        interface = interface_by_name[interface_name]
        downloader = downloader_by_interface[interface_name]
        output = dict(getattr(downloader, "output", {}) or {})
        default_options = dict(getattr(downloader, "default_options", {}) or {})
        output_layer = str(
            _COLLECTOR_OUTPUT_LAYER.get(interface_name)
            or output.get("output_layer")
            or "snapshot"
        )
        formats = _formats(default_options, output)
        collectors.append(
            collector_spec_cls(
                name=f"tdx.{interface_name}.snapshot",
                display_name_zh=f"{interface.display_name_zh}采集",
                description=(
                    f"通过通达信插件接口 {interface_name} 采集并写出本地快照；"
                    "默认参数保持小样本或接口声明口径，批量运行需显式配置。"
                ),
                interfaces=(interface_name,),
                downloader_profile=downloader.name,
                resource_group=downloader.resource_group,
                default_params=dict(default_options.get("params") or {}),
                required_interfaces=(interface_name,),
                output={
                    "layer": output_layer,
                    "formats": formats,
                    "supported_formats": list(output.get("supported_formats") or formats),
                },
            )
        )
    return tuple(collectors)


def tdx_collector_manifest() -> ProviderManifest:
    """Return the TDX collector-only manifest for TDX collection tasks."""

    return ProviderManifest(
        plugin=PluginInfo(
            plugin_id=TDX_COLLECTOR_PLUGIN_ID,
            name_zh="通达信采集器",
            version="0.1.0",
            description="通达信核心本地资产独立采集器插件。",
        ),
        provider=None,
        interfaces=(),
        downloaders=(),
        collectors=tdx_collector_specs(),
    )


def tdx_collector_specs() -> tuple[CollectorSpec, ...]:
    """Return independent CollectorSpec declarations for TDX collectors."""

    from .catalog import tdx_external_downloader_profiles, tdx_external_interfaces

    interfaces = {interface.name: interface for interface in tdx_external_interfaces()}
    downloaders = {downloader.interface_name: downloader for downloader in tdx_external_downloader_profiles()}
    specs: list[CollectorSpec] = []
    for interface_name in TDX_INDEPENDENT_COLLECTOR_INTERFACES:
        specs.append(
            _independent_tdx_collector_spec(
                interface=interfaces[interface_name],
                downloader=downloaders[interface_name],
            )
        )
    return tuple(specs)


def run_tdx_collector(
    *,
    params: Mapping[str, Any] | None = None,
    fields: Sequence[str] | None = None,
    collector: Mapping[str, Any] | None = None,
    data_root: str | Path | None = None,
    progress_callback: Any | None = None,
    connection_count: int | None = None,
    source_server_count: int | None = None,
    connections_per_server: int | None = None,
    max_concurrent_tasks: int | None = None,
    batch_size: int | None = None,
    request_interval_ms: int | None = None,
    retry_count: int | None = None,
    timeout_ms: int | None = None,
    **_kwargs: Any,
) -> dict[str, Any]:
    """Run an independent TDX collector and return records for AxData Writer."""

    collector_info = dict(collector or {})
    collector_id = str(collector_info.get("collector_id") or collector_info.get("name") or "")
    interface_name = _interface_name_from_collector_id(collector_id)
    request_params = dict(params or {})
    request_fields = list(fields) if fields is not None else None
    records, adapter_meta = _request_tdx_records(
        interface_name,
        request_params,
        fields=request_fields,
        data_root=data_root,
        progress_callback=progress_callback,
        execution_options=_tdx_request_execution_options(
            source_server_count=source_server_count,
            connections_per_server=connections_per_server,
        ),
    )
    meta = {
        "source": "tdx",
        "interface_name": interface_name,
        "collector_id": collector_id,
        "collector_plugin_id": TDX_COLLECTOR_PLUGIN_ID,
        "dataset_id": _TDX_DATASET_IDS[interface_name],
        "data_date": _infer_data_date(interface_name, request_params, records, adapter_meta),
    }
    meta.update(adapter_meta)
    return {"records": records, "meta": meta}


def _formats(default_options: dict[str, Any], output: dict[str, Any]) -> list[str]:
    raw = default_options.get("formats") or output.get("formats") or ["parquet"]
    if isinstance(raw, str):
        return [raw]
    values = [str(item) for item in raw if str(item)]
    return values or ["parquet"]


def _supported_formats(default_options: dict[str, Any], output: dict[str, Any]) -> list[str]:
    raw = output.get("supported_formats") or default_options.get("formats") or ["parquet", "csv"]
    if isinstance(raw, str):
        return [raw]
    values = [str(item) for item in raw if str(item)]
    return values or ["parquet"]


def _independent_tdx_collector_spec(*, interface: Any, downloader: Any) -> CollectorSpec:
    interface_name = str(interface.name)
    output = dict(getattr(downloader, "output", {}) or {})
    default_options = dict(getattr(downloader, "default_options", {}) or {})
    output_layer = str(output.get("output_layer") or "snapshot")
    formats = _formats(default_options, output)
    supported_formats = _supported_formats(default_options, output)
    primary_key = _string_list(output.get("primary_key"))
    required_columns = _string_list(output.get("required_columns")) or primary_key
    expected_columns = _string_list(output.get("expected_columns"))
    numeric_positive_columns = _string_list(output.get("numeric_positive_columns"))
    field_mappings = dict(output.get("field_mappings") or {})
    quality: dict[str, Any] = {
        "required_columns": required_columns,
        "expected_columns": expected_columns,
        "numeric_positive_columns": numeric_positive_columns,
        "field_mappings": field_mappings,
        "calendar_check": interface_name in _TDX_CALENDAR_CHECK_INTERFACES,
    }
    if output.get("date_field"):
        quality["date_field"] = output.get("date_field")
    if output.get("datetime_field"):
        quality["datetime_field"] = output.get("datetime_field")

    collector_output = {
        "layer": output_layer,
        "formats": formats,
        "supported_formats": _collector_supported_formats(supported_formats),
        "default_output_path_parts": list(
            _TDX_OUTPUT_PATH_PARTS.get(interface_name)
            or output.get("default_output_path_parts")
            or output.get("path_parts")
            or []
        ),
        "default_dir_name": _TDX_DATASET_IDS[interface_name],
        "file_name_template": "{dataset_id}_{snapshot_date}",
        # 写入模式全库统一为 upsert_by_key:同主键新值替换、补采幂等、重跑即修复
        "write_mode": "upsert_by_key",
        "primary_key": primary_key,
        "required_columns": required_columns,
        "expected_columns": expected_columns,
        "numeric_positive_columns": numeric_positive_columns,
        "field_mappings": field_mappings,
        "snapshot_date_meta_keys": list(
            output.get("snapshot_date_meta_keys")
            or ["tdx_stats_date", "snapshot_date", "data_date", "trade_date", "date"]
        ),
    }
    collector_output["datasets"] = [
        _collector_output_dataset_declaration(
            interface=interface,
            interface_name=interface_name,
            output=collector_output,
            output_layer=output_layer,
            primary_key=primary_key,
            expected_columns=expected_columns,
            numeric_positive_columns=numeric_positive_columns,
        )
    ]
    # write_mode 不再从 downloader 透传,采集器统一 upsert_by_key
    for key in ("date_field", "datetime_field", "partition_by"):
        if output.get(key) is not None:
            collector_output[key] = output.get(key)

    return CollectorSpec(
        name=f"tdx.{interface_name}.snapshot",
        display_name_zh=f"{interface.display_name_zh}采集",
        description=_TDX_DESCRIPTIONS[interface_name],
        collector_plugin_id=TDX_COLLECTOR_PLUGIN_ID,
        dataset_id=_TDX_DATASET_IDS[interface_name],
        asset_class="stock",
        category=_TDX_CATEGORIES[interface_name],
        resource_group=str(downloader.resource_group),
        runner_entry=TDX_COLLECTOR_RUNNER_ENTRY,
        default_schedule={"kind": "manual"},
        default_params=dict(default_options.get("params") or {}),
        output=collector_output,
        config_schema={"execution": _execution_config(interface_name)},
        quality=quality,
        required_datasets=_TDX_REQUIRED_DATASETS.get(interface_name, ()),
        lifecycle_status="stable",
    )


def _collector_supported_formats(values: Sequence[str]) -> list[str]:
    allowed = ["parquet", "csv", "duckdb", "jsonl"]
    selected = [str(item).strip().lower() for item in values if str(item).strip()]
    result = [item for item in allowed if item in selected]
    return result or ["parquet"]


def _collector_output_dataset_declaration(
    *,
    interface: Any,
    interface_name: str,
    output: Mapping[str, Any],
    output_layer: str,
    primary_key: Sequence[str],
    expected_columns: Sequence[str],
    numeric_positive_columns: Sequence[str],
) -> dict[str, Any]:
    logical_table = _TDX_LOGICAL_TABLES.get(interface_name, _TDX_DATASET_IDS[interface_name])
    date_field = output.get("date_field")
    partition_by = _string_list(output.get("partition_by"))
    return {
        "dataset_id": logical_table if logical_table == "daily" else _TDX_DATASET_IDS[interface_name],
        "table": logical_table,
        "display_name_zh": str(getattr(interface, "display_name_zh", "") or logical_table),
        "description": _TDX_DESCRIPTIONS[interface_name],
        "layer": output_layer,
        "primary_key": list(primary_key),
        "date_field": date_field,
        "partition_by": partition_by,
        "write_mode": output.get("write_mode") or "snapshot",
        "fields": _output_fields(expected_columns, output),
        "default_query_fields": _output_field_names(expected_columns, output),
        "default_filter_fields": _default_filter_fields(_output_field_names(expected_columns, output), date_field),
        "quality_rules": {
            "required_columns": list(primary_key),
            "numeric_positive_columns": list(numeric_positive_columns),
        },
        "storage": {
            "layout": "daily_file" if partition_by == ["trade_date"] else "snapshot",
            "path_parts": list(output.get("default_output_path_parts") or []),
        },
        "default_output_path_parts": list(output.get("default_output_path_parts") or []),
        "formats": ["parquet", "csv", "duckdb"],
    }


def _output_field_names(expected_columns: Sequence[str], output: Mapping[str, Any]) -> list[str]:
    fields = [str(item) for item in expected_columns if str(item)]
    if fields:
        return fields
    fields = _string_list(output.get("required_columns"))
    return fields


def _output_fields(expected_columns: Sequence[str], output: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [{"name": name} for name in _output_field_names(expected_columns, output)]


def _default_filter_fields(fields: Sequence[str], date_field: Any) -> list[str]:
    available = set(str(field) for field in fields)
    filters = [field for field in ("ts_code", "instrument_id", "symbol", "exchange") if field in available]
    if date_field:
        filters.extend(["start_date", "end_date"])
    return filters


def _execution_config(interface_name: str) -> dict[str, Any]:
    defaults = dict(_TDX_EXECUTION_OPTIONS.get(interface_name, {}))
    source_server_count = int(defaults.get("source_server_count") or 1)
    connections_per_server = int(defaults.get("connections_per_server") or 1)
    max_concurrent_tasks = int(defaults.get("max_concurrent_tasks") or source_server_count * connections_per_server)
    return {
        "default_mode": "recommended",
        "defaults": {
            "source_server_count": source_server_count,
            "connections_per_server": connections_per_server,
            "max_concurrent_tasks": max_concurrent_tasks,
        },
        "limits": {
            "source_server_count": {"min": 1, "max": max(source_server_count, 8)},
            "connections_per_server": {"min": 1, "max": max(connections_per_server, 4)},
            "max_concurrent_tasks": {"min": 1, "max": max(max_concurrent_tasks, 16)},
        },
    }


def _request_tdx_records(
    interface_name: str,
    params: Mapping[str, Any],
    *,
    fields: Sequence[str] | None,
    data_root: str | Path | None,
    progress_callback: Any | None,
    execution_options: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from .downloader import tdx_adapter_options
    from .server_cache import tdx_server_cache_root, tdx_stats_cache_root
    from .request_adapter import TdxRequestAdapter

    server_cache_root = tdx_server_cache_root(data_root)
    stats_cache_root = tdx_stats_cache_root(data_root)
    options = tdx_adapter_options(
        interface_name=interface_name,
        pool_size=1,
        server_cache_root=server_cache_root,
        stats_cache_root=stats_cache_root,
    ) or {}
    options.update(_TDX_EXECUTION_OPTIONS.get(interface_name, {}))
    options.update({key: value for key, value in dict(execution_options or {}).items() if value is not None})
    adapter = TdxRequestAdapter(
        progress_callback=progress_callback,
        use_parallel_suspension_quotes=interface_name == "stock_suspensions_tdx",
        options=options,
    )
    records = [dict(row) for row in adapter.request(interface_name, dict(params))]
    if fields is not None:
        allowed = set(fields)
        records = [{key: value for key, value in row.items() if key in allowed} for row in records]
    meta = dict(getattr(adapter, "last_meta", {}) or {})
    return records, meta


def _tdx_request_execution_options(
    *,
    source_server_count: int | None = None,
    connections_per_server: int | None = None,
) -> dict[str, Any]:
    """Return only execution options accepted by the TDX request adapter.

    Collector tasks track generic scheduler controls such as connection_count
    and max_concurrent_tasks, but the TDX source request layer only accepts
    source_server_count / connections_per_server for quote connections.
    """

    options = {
        "source_server_count": source_server_count,
        "connections_per_server": connections_per_server,
    }
    return {key: value for key, value in options.items() if value is not None}


def _interface_name_from_collector_id(collector_id: str) -> str:
    prefix = "tdx."
    suffix = ".snapshot"
    if not collector_id.startswith(prefix) or not collector_id.endswith(suffix):
        known = ", ".join(f"tdx.{name}.snapshot" for name in TDX_INDEPENDENT_COLLECTOR_INTERFACES)
        raise KeyError(f"Unknown TDX collector {collector_id!r}. Known collectors: {known}.")
    interface_name = collector_id[len(prefix) : -len(suffix)]
    if interface_name not in TDX_INDEPENDENT_COLLECTOR_INTERFACES:
        known = ", ".join(f"tdx.{name}.snapshot" for name in TDX_INDEPENDENT_COLLECTOR_INTERFACES)
        raise KeyError(f"Unknown TDX collector {collector_id!r}. Known collectors: {known}.")
    return interface_name


def _infer_data_date(
    interface_name: str,
    params: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    meta: Mapping[str, Any],
) -> str | None:
    for source in (meta, params):
        for key in ("data_date", "trade_date", "tdx_stats_date", "snapshot_date", "date", "event_date"):
            value = _normalize_date(source.get(key))
            if value is not None:
                return value
    for record in records:
        for key in ("trade_date", "event_date", "finance_updated_date"):
            value = _normalize_date(record.get(key))
            if value is not None:
                return value
    if interface_name in {"stock_codes_tdx", "stock_suspensions_tdx", "stock_st_list_tdx"}:
        return None
    return None


def _normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            normalized = _normalize_date(item)
            if normalized is not None:
                return normalized
        return None
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    try:
        return [str(item) for item in value if str(item)]
    except TypeError:
        return [str(value)]
