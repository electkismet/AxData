from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi.testclient import TestClient

from axdata_core import SourceRequestResult
from axdata_core import downloaders as downloaders_module
from axdata_core.cli import main as cli_main
from axdata_core.collector_scheduler import CollectorSchedulerService
from axdata_core.data_browser import get_dataset, list_datasets, preview_dataset
from axdata_core.plugin_config import disable_provider, enable_provider
from axdata_core.query import query_table
from axdata_core.storage import core_table_partition_path
from tests.tdx_plugin_helpers import (
    TDX_COLLECTOR_PLUGIN_ID,
    TDX_EXT_PROVIDER_ID,
    TDX_PROVIDER_ID,
    build_registry_with_local_tdx_plugins,
    ensure_local_tdx_plugin_paths,
)


DEFAULT_TASK_IDS = {
    "stock_kline_daily_tdx_sample",
}

RUN_TARGET_INTERFACE_BY_PRODUCT = {
    "stock_kline_daily_tdx": "tdx.stock_daily",
}


def _patch_registry_with_local_tdx_plugins(monkeypatch: Any) -> None:
    ensure_local_tdx_plugin_paths()
    import axdata_core
    import axdata_core.provider_catalog as provider_catalog

    base_builder = provider_catalog.build_builtin_provider_registry

    def build_registry(**kwargs: Any):
        return build_registry_with_local_tdx_plugins(base_builder=base_builder, **kwargs)

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", build_registry)
    if "build_builtin_provider_registry" in axdata_core.__dict__:
        monkeypatch.setattr(axdata_core, "build_builtin_provider_registry", build_registry)


def _write_trade_calendar(data_root: Path, dates: list[str], *, exchange: str = "SZSE") -> None:
    frame = pd.DataFrame(
        [
            {
                "exchange": exchange,
                "cal_date": date,
                "is_open": True,
                "pretrade_date": None,
            }
            for date in dates
        ]
    )
    partition = core_table_partition_path("trade_cal", data_root) / f"exchange={exchange}"
    partition.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(partition / "part-0.parquet", engine="pyarrow", index=False)
    cache_path = data_root / "cache" / "exchange" / "trade_calendar" / "trade_calendar.parquet"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    frame.assign(next_trade_date=None).to_parquet(cache_path, engine="pyarrow", index=False)


def _fake_default_task_request(
    interface_name: str,
    *,
    params: dict[str, Any],
    fields: list[str] | None,
    persist: bool,
    adapter: Any = None,
    options: dict[str, Any] | None = None,
    data_root: str | Path | None = None,
) -> SourceRequestResult:
    assert persist is False
    assert data_root is not None
    if interface_name == "stock_basic_info_exchange":
        return SourceRequestResult(
            records=[
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "exchange": "SZSE",
                    "asset_type": "stock",
                    "name": "Ping An Bank",
                    "list_date": "19910403",
                },
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "asset_type": "stock",
                    "name": "Shanghai Pudong Development Bank",
                    "list_date": "19991110",
                },
            ],
            meta={"source": "exchange", "snapshot_date": "20260618"},
        )
    if interface_name == "stock_trade_calendar_exchange":
        assert params == {"year": "2026"}
        return SourceRequestResult(
            records=[
                {"cal_date": "20260617", "is_open": True, "pretrade_date": "20260616"},
                {"cal_date": "20260618", "is_open": True, "pretrade_date": "20260617"},
            ],
            meta={"source": "exchange", "snapshot_date": "20260618"},
        )
    if interface_name == "stock_kline_daily_tdx":
        assert params == {"code": "000001.SZ", "count": 2, "adjust": "none"}
        return SourceRequestResult(
            records=[
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "tdx_code": "sz000001",
                    "exchange": "SZSE",
                    "trade_time": "2026-06-17T15:00:00+08:00",
                    "period": "day",
                    "open": 10.0,
                    "high": 10.3,
                    "low": 9.9,
                    "close": 10.2,
                    "volume": 1200.0,
                    "amount": 12240.0,
                },
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "tdx_code": "sz000001",
                    "exchange": "SZSE",
                    "trade_time": "2026-06-18T15:00:00+08:00",
                    "period": "day",
                    "open": 10.2,
                    "high": 10.4,
                    "low": 10.1,
                    "close": 10.3,
                    "volume": 1300.0,
                    "amount": 13390.0,
                },
            ],
            meta={"source": "tdx", "trade_date": "20260618"},
        )
    raise AssertionError(interface_name)


def _fake_tdx_records(
    interface_name: str,
    params: dict[str, Any],
    *,
    fields: list[str] | None,
    data_root: str | Path | None,
    progress_callback: Any,
    execution_options: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    assert execution_options == {}
    result = _fake_default_task_request(
        interface_name,
        params=params,
        fields=fields,
        persist=False,
        data_root=data_root,
    )
    return result.records, result.meta


def _run_default_task(service: CollectorSchedulerService, task_id: str, *, overrides: dict[str, Any] | None = None):
    submitted = service.submit_task(task_id, allow_disabled_manual_run=True, params_override=overrides)
    if submitted.status not in {"success", "failed", "skipped", "cancelled"}:
        finished = service.wait_for_run(submitted.run_id, timeout=10)
        assert finished is not None
        return finished
    return submitted


def test_default_collector_tasks_mock_sample_e2e_reaches_parquet_browser_api_cli_and_quality(
    monkeypatch: Any,
    tmp_path: Path,
    capsys: Any,
) -> None:
    _patch_registry_with_local_tdx_plugins(monkeypatch)
    data_root = tmp_path / "data"
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    enable_provider(TDX_PROVIDER_ID, data_root=data_root)
    disable_provider(TDX_EXT_PROVIDER_ID, data_root=data_root)
    _write_trade_calendar(data_root, ["20260617", "20260618"])
    monkeypatch.setattr(downloaders_module, "request_interface", _fake_default_task_request)
    import axdata_source_tdx.collectors as tdx_collectors

    monkeypatch.setattr(tdx_collectors, "_request_tdx_records", _fake_tdx_records)

    service = CollectorSchedulerService(data_root=data_root, max_workers=1)
    try:
        seeded = {task.task_id: task for task in service.list_tasks()}
        assert DEFAULT_TASK_IDS <= set(seeded)
        assert all(seeded[task_id].enabled is False for task_id in DEFAULT_TASK_IDS)

        runs = {
            "stock_kline_daily_tdx": _run_default_task(
                service,
                "stock_kline_daily_tdx_sample",
                overrides={"count": 2},
            ),
        }
    finally:
        service.shutdown()

    for dataset, run in runs.items():
        run_payload = run.to_dict()
        assert run.status == "success"
        assert run.duration_ms is not None
        assert run.output_paths.get("parquet")
        assert Path(run.output_paths["parquet"]).exists()
        assert run_payload["records_read"] == 2
        assert run_payload["rows_written"] == 2
        assert run_payload["write_metadata"] is not None
        assert run_payload["write_metadata"]["write_mode"] == run_payload["write_mode"]
        assert run.quality["quality_status"] == "ok"
        assert run.quality["row_count_value"] == 2
        assert run.stage_timings["download_ms"] is not None
        assert run.stage_timings["write_ms"] is not None
        assert run.stage_timings["quality_ms"] is not None
        assert run.events[-1]["stage"] == "finished"
        assert run.result["target_interface"] is None
        assert run.result["download_result"]["interface_name"] == RUN_TARGET_INTERFACE_BY_PRODUCT[dataset]

    run_payloads = {name: run.to_dict() for name, run in runs.items()}
    assert runs["stock_kline_daily_tdx"].quality["calendar_coverage_status"] == "ok"
    daily_payload = run_payloads["stock_kline_daily_tdx"]
    assert daily_payload["write_mode"] == "upsert_by_key"
    assert daily_payload["primary_key"] == ["instrument_id", "trade_time", "period"]
    # K 线数据帧只有 trade_time 没有 trade_date 列,声明分区自动降级为普通单文件
    assert daily_payload["partition_by"] == []
    assert daily_payload["partitions_touched"] == []

    datasets = {item.dataset: item for item in list_datasets(data_root=data_root)}
    assert {"daily"} <= set(datasets)
    assert "tdx.stock_daily" not in datasets
    assert "tdx.stock_adj_factor" not in datasets
    assert "adj_factor" not in datasets
    assert "exchange.stock_basic_info" not in datasets
    assert "exchange.trade_calendar" not in datasets
    assert datasets["daily"].provider == "axdata.collector.tdx"
    assert datasets["daily"].metadata["collector_name"] == "tdx.stock_kline_daily_tdx.snapshot"
    assert datasets["daily"].write_mode == "upsert_by_key"
    assert datasets["daily"].rows_written == 2
    assert datasets["daily"].quality_status == "ok"

    preview = preview_dataset(
        "daily",
        data_root=data_root,
        fields=["instrument_id", "trade_time", "close"],
        symbol="000001.SZ",
        limit=20,
    )
    assert preview.rows == [
        {"instrument_id": "000001.SZ", "trade_time": "2026-06-17T15:00:00+08:00", "close": 10.2},
        {"instrument_id": "000001.SZ", "trade_time": "2026-06-18T15:00:00+08:00", "close": 10.3}
    ]

    inspected = get_dataset("daily", data_root=data_root)
    assert inspected.quality["field_mappings"] == {
        "instrument_id": "ts_code",
        "trade_time": "trade_date",
        "volume": "vol",
    }

    queried = query_table(
        "daily",
        root=data_root,
        fields=["close"],
        limit=10,
    )
    assert queried["close"].tolist() == [10.2, 10.3]

    import apps.api.collector_routes as collector_routes
    import apps.api.main as api_main

    collector_routes._collector_service_cache.clear()
    client = TestClient(api_main.app)
    tasks_payload = client.get("/v1/collector/tasks").json()["data"]
    assert {row["task_id"] for row in tasks_payload} >= DEFAULT_TASK_IDS
    status_payload = client.get("/v1/collector/status").json()["data"]
    assert status_payload["status_counts"]["success"] == 1
    run_payload = client.get(f"/v1/collector/runs/{runs['stock_kline_daily_tdx'].run_id}").json()["data"]
    assert run_payload["rows_written"] == 2
    assert run_payload["write_mode"] == "upsert_by_key"
    api_dataset = client.get("/v1/data/datasets/daily").json()["data"]
    assert api_dataset["rows_written"] == 2
    api_preview = client.get(
        "/v1/data/datasets/daily/preview",
        params={"symbol": "000001.SZ", "limit": 20},
    ).json()
    assert api_preview["data"] == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_time": "2026-06-17T15:00:00+08:00",
            "period": "day",
            "open": 10.0,
            "high": 10.3,
            "low": 9.9,
            "close": 10.2,
            "volume": 1200.0,
            "amount": 12240.0,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_time": "2026-06-18T15:00:00+08:00",
            "period": "day",
            "open": 10.2,
            "high": 10.4,
            "low": 10.1,
            "close": 10.3,
            "volume": 1300.0,
            "amount": 13390.0,
        }
    ]
    query_payload = client.post(
        "/v1/query",
        json={
            "table": "daily",
            "params": {},
            "fields": ["close"],
            "limit": 10,
        },
    ).json()
    assert query_payload["data"] == [{"close": 10.2}, {"close": 10.3}]

    assert cli_main(["--data-root", str(data_root), "collector", "run", "list", "--json"]) == 0
    cli_runs = json.loads(capsys.readouterr().out)
    assert {row["task_id"] for row in cli_runs} >= DEFAULT_TASK_IDS
    assert any(row["rows_written"] == 2 and row["write_mode"] == "upsert_by_key" for row in cli_runs)

    assert cli_main(["--data-root", str(data_root), "collector", "status", "--json"]) == 0
    cli_status = json.loads(capsys.readouterr().out)
    assert cli_status["status_counts"]["success"] == 1
    assert cli_status["latest_runs"]["stock_kline_daily_tdx_sample"]["rows_written"] == 2

    assert cli_main(["--data-root", str(data_root), "data", "inspect", "daily", "--json"]) == 0
    cli_dataset = json.loads(capsys.readouterr().out)
    assert cli_dataset["write_mode"] == "upsert_by_key"
    assert cli_dataset["quality_status"] == "ok"

    assert cli_main(
        [
            "--data-root",
            str(data_root),
            "data",
            "preview",
            "daily",
            "--symbol",
            "000001.SZ",
            "--fields",
            "instrument_id,trade_time,close",
            "--json",
        ]
    ) == 0
    cli_preview = json.loads(capsys.readouterr().out)
    assert cli_preview["rows"] == [
        {"instrument_id": "000001.SZ", "trade_time": "2026-06-17T15:00:00+08:00", "close": 10.2},
        {"instrument_id": "000001.SZ", "trade_time": "2026-06-18T15:00:00+08:00", "close": 10.3}
    ]

    assert cli_main(
        [
            "--data-root",
            str(data_root),
            "query",
            "daily",
            "--fields",
            "close",
            "--json",
        ]
    ) == 0
    cli_query = json.loads(capsys.readouterr().out)
    assert cli_query["rows"] == [{"close": 10.2}, {"close": 10.3}]


def test_default_tdx_tasks_fail_clearly_when_plugin_disabled(
    monkeypatch: Any,
    tmp_path: Path,
    capsys: Any,
) -> None:
    _patch_registry_with_local_tdx_plugins(monkeypatch)
    data_root = tmp_path / "data"
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    disable_provider(TDX_COLLECTOR_PLUGIN_ID, data_root=data_root)
    disable_provider(TDX_EXT_PROVIDER_ID, data_root=data_root)

    service = CollectorSchedulerService(data_root=data_root, max_workers=1)
    try:
        tasks = {task.task_id: task for task in service.list_tasks()}
        assert tasks["stock_kline_daily_tdx_sample"].dependency_status == "disabled"
        assert "请安装/启用 TDX 采集器插件" in (tasks["stock_kline_daily_tdx_sample"].dependency_message or "")

        failed_runs = [
            service.submit_task("stock_kline_daily_tdx_sample", allow_disabled_manual_run=True),
        ]
    finally:
        service.shutdown()

    for run in failed_runs:
        assert run.status == "failed"
        assert run.error_category == "plugin_disabled"
        assert "请安装/启用 TDX 采集器插件" in (run.error_summary or "")
        assert run.duration_ms == 0
        run_payload = run.to_dict()
        assert run_payload["rows_written"] is None
        assert run_payload["write_metadata"] is None
        assert run.output_paths == {}
        assert run.events[-1]["stage"] == "failed"
        assert run.metadata["dependency_status"] == "disabled"

    import apps.api.collector_routes as collector_routes
    import apps.api.main as api_main

    collector_routes._collector_service_cache.clear()
    client = TestClient(api_main.app)
    api_run = client.post("/v1/collector/tasks/stock_kline_daily_tdx_sample/run", json={}).json()["data"]
    assert api_run["status"] == "failed"
    assert api_run["error_category"] == "plugin_disabled"
    assert "请安装/启用 TDX 采集器插件" in api_run["error_summary"]
    api_missing = client.get("/v1/collector/tasks/stock_adj_factor_tdx_sample")
    assert api_missing.status_code == 404

    assert cli_main(
        [
            "--data-root",
            str(data_root),
            "collector",
            "task",
            "run",
            "stock_kline_daily_tdx_sample",
            "--wait",
            "--json",
        ]
    ) == 0
    cli_run = json.loads(capsys.readouterr().out)
    assert cli_run["status"] == "failed"
    assert cli_run["error_category"] == "plugin_disabled"
    assert "请安装/启用 TDX 采集器插件" in cli_run["error_summary"]

    assert cli_main(["--data-root", str(data_root), "collector", "run", "list", "--status", "failed", "--json"]) == 0
    failed_history = json.loads(capsys.readouterr().out)
    assert len(failed_history) >= 2
    assert all(row["status"] == "failed" for row in failed_history)
    assert any(row["task_id"] == "stock_kline_daily_tdx_sample" for row in failed_history)
