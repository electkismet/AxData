from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import axdata_core.sample_collectors as sample_collectors
from axdata_core.collector_runner import CollectorError, build_collector_run_plan, run_collector
from axdata_core.plugins import (
    CollectorSpec,
    DownloaderProfile as PluginDownloaderProfile,
    InterfaceCollectionSpec,
    InterfaceSpec,
    PluginInfo,
    ProviderInfo,
    ProviderManifest,
)
from axdata_core.provider_registry import ProviderRegistry


def _stock_codes_interface(source_code: str = "runner_demo") -> InterfaceSpec:
    return InterfaceSpec(
        name="stock_codes_tdx",
        display_name_zh="股票列表",
        source_code=source_code,
        source_name_zh="Runner 示例",
        asset_class="stock",
        collection=InterfaceCollectionSpec(
            supported=True,
            default_profile="runner_demo.stock_codes.snapshot",
        ),
    )


def _runner_registry(*, collector: CollectorSpec | None = None) -> ProviderRegistry:
    provider_manifest = ProviderManifest(
        provider=ProviderInfo(
            provider_id="axdata.source.runner_demo",
            source_code="runner_demo",
            source_name_zh="Runner 示例",
            version="0.1.0",
        ),
        interfaces=(_stock_codes_interface(),),
        downloaders=(
            PluginDownloaderProfile(
                name="runner_demo.stock_codes.snapshot",
                interface_name="stock_codes_tdx",
                display_name_zh="股票列表采集",
                resource_group="runner.demo",
                mode="snapshot",
                default_options={"formats": ["jsonl"], "params": {"scope": "profile"}},
                default_limits={"max_connections_total": 2},
                output={"formats": ["jsonl"], "primary_key": "instrument_id"},
            ),
        ),
    )
    collector_manifest = ProviderManifest(
        plugin=PluginInfo(
            plugin_id="axdata.plugin.close_refresh",
            name_zh="收盘刷新",
            version="0.1.0",
        ),
        provider=None,
        interfaces=(),
        collectors=(
            collector
            or CollectorSpec(
                name="close_refresh_daily",
                display_name_zh="收盘刷新",
                interfaces=("stock_codes_tdx",),
                required_interfaces=("stock_codes_tdx",),
                downloader_profile="runner_demo.stock_codes.snapshot",
                resource_group="runner.demo",
                default_params={"scope": "all", "adjust": "none"},
                output={"layer": "raw", "formats": ["csv"]},
            ),
        ),
    )
    registry = ProviderRegistry(
        enabled_provider_ids={
            "axdata.source.runner_demo",
            "axdata.plugin.close_refresh",
        },
    )
    registry.register_manifest(provider_manifest)
    registry.register_manifest(collector_manifest)
    return registry


def _independent_runner_registry() -> ProviderRegistry:
    manifest = ProviderManifest(
        plugin=PluginInfo(
            plugin_id="axdata.collector.sample",
            name_zh="独立样例采集器",
            version="0.1.0",
        ),
        provider=None,
        interfaces=(),
        collectors=(
            CollectorSpec(
                name="sample.stock_snapshot.snapshot",
                display_name_zh="样例股票快照采集",
                description="本地 fake 样例采集器。",
                collector_plugin_id="axdata.collector.sample",
                dataset_id="sample.stock_snapshot",
                asset_class="stock",
                category="snapshot",
                resource_group="sample.local",
                runner_entry="axdata_core.sample_collectors:sample_stock_snapshot",
                default_params={"instrument_id": "000001.SZ", "trade_date": "20260701"},
                output={
                    "layer": "snapshot",
                    "formats": ["parquet", "jsonl"],
                    "primary_key": ["instrument_id"],
                    "required_columns": ["instrument_id", "trade_date"],
                    "expected_columns": ["instrument_id", "trade_date", "name", "last_price"],
                    "date_field": "trade_date",
                    "file_name_template": "{dataset_id}_{data_date}_{run_time}",
                },
                quality={
                    "required_columns": ["instrument_id", "trade_date"],
                    "expected_columns": ["instrument_id", "trade_date", "name", "last_price"],
                    "numeric_positive_columns": ["last_price"],
                },
            ),
        ),
    )
    registry = ProviderRegistry(enabled_provider_ids={"axdata.collector.sample"})
    registry.register_manifest(manifest)
    return registry


def test_independent_collector_plan_does_not_require_interfaces_or_downloader(monkeypatch, tmp_path) -> None:
    registry = _independent_runner_registry()

    import axdata_core.provider_catalog as provider_catalog

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", lambda **_kwargs: registry)

    plan = build_collector_run_plan(
        "sample.stock_snapshot.snapshot",
        params={"instrument_id": "600000.SH"},
        fields=["instrument_id", "trade_date"],
        data_root=tmp_path / "data",
    )

    assert plan.collector_id == "sample.stock_snapshot.snapshot"
    assert plan.collector_plugin_id == "axdata.collector.sample"
    assert plan.dataset_id == "sample.stock_snapshot"
    assert plan.provider_id == "axdata.collector.sample"
    assert plan.runner_entry == "axdata_core.sample_collectors:sample_stock_snapshot"
    assert plan.is_legacy is False
    assert plan.legacy_source is None
    assert plan.interfaces == ()
    assert plan.required_interfaces == ()
    assert plan.target_interface is None
    assert plan.downloader_profile is None
    assert plan.params == {"instrument_id": "600000.SH", "trade_date": "20260701"}
    assert plan.fields == ["instrument_id", "trade_date"]
    assert plan.formats == ["parquet", "jsonl"]


def test_independent_collector_runs_runner_entry_and_writes_outputs(monkeypatch, tmp_path) -> None:
    registry = _independent_runner_registry()

    import axdata_core.provider_catalog as provider_catalog
    import axdata_core.downloaders as downloaders

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", lambda **_kwargs: registry)

    def unexpected_run_downloader(*_args, **_kwargs):  # pragma: no cover
        raise AssertionError("independent runner_entry path must not call run_downloader")

    monkeypatch.setattr(downloaders, "run_downloader", unexpected_run_downloader)

    result = run_collector(
        "sample.stock_snapshot.snapshot",
        params={"instrument_id": "600000.SH"},
        data_root=tmp_path / "data",
        output_root=tmp_path / "export",
        formats=["parquet", "jsonl"],
    )

    download_result = result["download_result"]
    assert result["status"] == "success"
    assert result["runner_entry"] == "axdata_core.sample_collectors:sample_stock_snapshot"
    assert result["target_interface"] is None
    assert result["downloader_profile"] is None
    assert download_result["collector_id"] == "sample.stock_snapshot.snapshot"
    assert download_result["collector_plugin_id"] == "axdata.collector.sample"
    assert download_result["dataset_id"] == "sample.stock_snapshot"
    assert download_result["interface_name"] == "sample.stock_snapshot"
    assert download_result["collect_mode"] == "runner_entry"
    assert download_result["row_count"] == 1
    assert set(download_result["output_paths"]) == {"parquet", "jsonl"}
    assert Path(download_result["output_paths"]["parquet"]).is_file()
    assert Path(download_result["output_paths"]["jsonl"]).is_file()
    assert Path(download_result["log_path"]).is_file()
    assert download_result["quality"]["quality_status"] == "ok", download_result["quality"]
    assert download_result["quality"]["primary_key"] == "pass"
    assert download_result["quality"]["row_count_value"] == 1
    assert download_result["quality"]["write_primary_key"] == ["instrument_id"]
    assert download_result["write_mode"] == "snapshot"
    assert "runner_fetch" in download_result["duration_breakdown_ms"]
    assert "source_request" not in download_result["duration_breakdown_ms"]


def test_independent_collector_runner_entry_allows_minimal_signature(monkeypatch, tmp_path) -> None:
    registry = _independent_runner_registry()

    import axdata_core.provider_catalog as provider_catalog

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", lambda **_kwargs: registry)

    def minimal_sample(params):
        return {
            "records": [
                {
                    "instrument_id": params["instrument_id"],
                    "trade_date": params["trade_date"],
                    "name": "minimal",
                    "last_price": 1.0,
                }
            ],
            "meta": {"data_date": params["trade_date"]},
        }

    monkeypatch.setattr(sample_collectors, "sample_stock_snapshot", minimal_sample)

    result = run_collector(
        "sample.stock_snapshot.snapshot",
        params={"instrument_id": "600000.SH"},
        data_root=tmp_path / "data",
        output_root=tmp_path / "export",
        formats=["parquet"],
    )

    assert result["status"] == "success"
    assert result["download_result"]["row_count"] == 1
    assert result["download_result"]["quality"]["quality_status"] == "ok"


def test_build_collector_run_plan_merges_defaults_and_resolves_profile(monkeypatch, tmp_path) -> None:
    registry = _runner_registry()

    import axdata_core.provider_catalog as provider_catalog

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", lambda **_kwargs: registry)

    plan = build_collector_run_plan(
        "close_refresh_daily",
        params={"scope": "stock"},
        fields=["instrument_id"],
        data_root=tmp_path / "data",
    )

    assert plan.provider_id == "axdata.plugin.close_refresh"
    assert plan.collector_plugin_id == "axdata.plugin.close_refresh"
    assert plan.is_legacy is True
    assert plan.resource_group == "runner.demo"
    assert plan.target_interface == "stock_codes_tdx"
    assert plan.downloader_profile == "runner_demo.stock_codes.snapshot"
    assert plan.params == {"scope": "stock", "adjust": "none"}
    assert plan.fields == ["instrument_id"]
    assert plan.formats == ["csv"]


def test_run_collector_delegates_to_downloader_with_resolved_plan(monkeypatch, tmp_path) -> None:
    registry = _runner_registry()

    import axdata_core.provider_catalog as provider_catalog
    import axdata_core.downloaders as downloaders

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", lambda **_kwargs: registry)
    calls = []

    def fake_run_downloader(interface_name, **kwargs):
        calls.append({"interface_name": interface_name, **kwargs})
        return {
            "job_id": "run_collector_test",
            "interface_name": interface_name,
            "status": "success",
            "row_count": 1,
            "output_path": str(Path(kwargs["output_root"]) / "out.csv"),
        }

    monkeypatch.setattr(downloaders, "run_downloader", fake_run_downloader)

    result = run_collector(
        "close_refresh_daily",
        params={"scope": "stock"},
        fields=["instrument_id"],
        data_root=tmp_path / "data",
        output_root=tmp_path / "export",
        formats=["jsonl"],
    )

    assert result["collector_name"] == "close_refresh_daily"
    assert result["target_interface"] == "stock_codes_tdx"
    assert result["status"] == "success"
    assert result["download_result"]["row_count"] == 1
    assert calls[0]["interface_name"] == "stock_codes_tdx"
    assert calls[0]["params"] == {"scope": "stock", "adjust": "none"}
    assert calls[0]["fields"] == ["instrument_id"]
    assert calls[0]["data_root"] == tmp_path / "data"
    assert calls[0]["output_root"] == tmp_path / "export"
    assert calls[0]["formats"] == ["jsonl"]


def test_builtin_exchange_collector_is_not_registered(tmp_path) -> None:
    with pytest.raises(CollectorError, match="not available in the current Collector registry"):
        build_collector_run_plan(
            "exchange.stock_historical_list_exchange.snapshot",
            params={"trade_date": "20260103"},
            fields=["trade_date", "instrument_id", "name"],
            data_root=tmp_path / "data",
        )


def test_tdx_external_collectors_resolve_core_profiles_and_use_runner_entry(monkeypatch, tmp_path) -> None:
    from tests.tdx_plugin_helpers import (
        TDX_COLLECTOR_PLUGIN_ID,
        TDX_COLLECTOR_RUNNER_ENTRY,
        build_registry_with_local_tdx_plugins,
    )

    import axdata_core.downloaders as downloaders
    import axdata_core.provider_catalog as provider_catalog
    import axdata_source_tdx.collectors as tdx_collectors

    base_builder = provider_catalog.build_builtin_provider_registry

    def build_registry(**kwargs):
        return build_registry_with_local_tdx_plugins(
            base_builder=base_builder,
            **kwargs,
        )

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", build_registry)

    calls = []

    def unexpected_run_downloader(*_args, **_kwargs):  # pragma: no cover
        raise AssertionError("independent TDX collector must not call run_downloader")

    def fake_request_tdx_records(interface_name, params, *, fields, data_root, progress_callback, execution_options=None):
        calls.append(
            {
                "interface_name": interface_name,
                "params": dict(params),
                "fields": list(fields) if fields is not None else None,
                "data_root": data_root,
                "progress_callback": progress_callback,
                "execution_options": dict(execution_options or {}),
            }
        )
        if interface_name == "stock_kline_daily_tdx":
            return (
                [
                    {
                        "instrument_id": "000001.SZ",
                        "symbol": "000001",
                        "tdx_code": "sz000001",
                        "exchange": "SZSE",
                        "trade_time": "2026-06-18T15:00:00+08:00",
                        "period": "day",
                        "open": 10.0,
                        "high": 10.2,
                        "low": 9.9,
                        "close": 10.1,
                        "volume": 1000.0,
                        "amount": 10100.0,
                    }
                ],
                {"trade_date": "20260618"},
            )
        raise AssertionError(f"unexpected TDX interface {interface_name}")

    monkeypatch.setattr(downloaders, "run_downloader", unexpected_run_downloader)
    monkeypatch.setattr(tdx_collectors, "_request_tdx_records", fake_request_tdx_records)

    calendar_dir = tmp_path / "data" / "core" / "table=trade_cal" / "exchange=SZSE"
    calendar_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "exchange": "SZSE",
                "cal_date": "20260618",
                "is_open": True,
                "pretrade_date": "20260617",
            }
        ]
    ).to_parquet(calendar_dir / "part-0.parquet", engine="pyarrow", index=False)
    cache_dir = tmp_path / "data" / "cache" / "exchange" / "trade_calendar"
    cache_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "exchange": "SZSE",
                "cal_date": "20260618",
                "is_open": True,
                "pretrade_date": "20260617",
                "next_trade_date": None,
            }
        ]
    ).to_parquet(cache_dir / "trade_calendar.parquet", engine="pyarrow", index=False)

    plan = build_collector_run_plan(
        "tdx.stock_kline_daily_tdx.snapshot",
        params={"count": 2},
        data_root=tmp_path / "data",
    )
    result = run_collector(
        "tdx.stock_kline_daily_tdx.snapshot",
        params={"count": 2},
        data_root=tmp_path / "data",
        output_root=tmp_path / "export",
        formats=["parquet", "jsonl"],
    )

    assert plan.provider_id == TDX_COLLECTOR_PLUGIN_ID
    assert plan.collector_plugin_id == TDX_COLLECTOR_PLUGIN_ID
    assert plan.dataset_id == "tdx.stock_daily"
    assert plan.runner_entry == TDX_COLLECTOR_RUNNER_ENTRY
    assert plan.built_in is True
    assert plan.resource_group == "tdx.quote"
    assert plan.target_interface is None
    assert plan.downloader_profile is None
    assert plan.interfaces == ()
    assert plan.required_interfaces == ()
    assert plan.is_legacy is False
    assert plan.params == {"code": "000001.SZ", "count": 2, "adjust": "none"}
    assert plan.output["layer"] == "core"
    assert plan.output["formats"] == ["parquet"]
    assert plan.output["supported_formats"] == ["parquet", "csv", "jsonl"]

    assert result["provider_id"] == TDX_COLLECTOR_PLUGIN_ID
    assert result["collector_name"] == "tdx.stock_kline_daily_tdx.snapshot"
    assert result["runner_entry"] == TDX_COLLECTOR_RUNNER_ENTRY
    assert result["target_interface"] is None
    assert result["downloader_profile"] is None
    assert result["status"] == "success"
    download_result = result["download_result"]
    assert download_result["collector_plugin_id"] == TDX_COLLECTOR_PLUGIN_ID
    assert download_result["collector_id"] == "tdx.stock_kline_daily_tdx.snapshot"
    assert download_result["dataset_id"] == "tdx.stock_daily"
    assert download_result["interface_name"] == "tdx.stock_daily"
    assert download_result["row_count"] == 1
    assert set(download_result["output_paths"]) == {"parquet", "jsonl"}
    assert Path(download_result["output_paths"]["parquet"]).is_file()
    assert Path(download_result["output_paths"]["jsonl"]).is_file()
    assert download_result["quality"]["quality_status"] == "ok"
    assert calls == [
        {
            "interface_name": "stock_kline_daily_tdx",
            "params": {"code": "000001.SZ", "count": 2, "adjust": "none"},
            "fields": None,
            "data_root": (tmp_path / "data").resolve(),
            "progress_callback": None,
            "execution_options": {},
        }
    ]


def test_tdx_base_collector_uses_independent_runner_entry(monkeypatch, tmp_path) -> None:
    from tests.tdx_plugin_helpers import (
        TDX_BASE_COLLECTOR_DATASET_IDS,
        TDX_COLLECTOR_PLUGIN_ID,
        TDX_COLLECTOR_RUNNER_ENTRY,
        build_registry_with_local_tdx_plugins,
    )

    import axdata_core.downloaders as downloaders
    import axdata_core.provider_catalog as provider_catalog
    import axdata_source_tdx.collectors as tdx_collectors

    base_builder = provider_catalog.build_builtin_provider_registry

    def build_registry(**kwargs):
        return build_registry_with_local_tdx_plugins(
            base_builder=base_builder,
            **kwargs,
        )

    def unexpected_run_downloader(*_args, **_kwargs):  # pragma: no cover
        raise AssertionError("independent TDX collector must not call run_downloader")

    def fake_request_tdx_records(interface_name, params, *, fields, data_root, progress_callback, execution_options=None):
        assert interface_name == "stock_daily_share_tdx"
        assert params == {"scope": "all", "trade_date": "20260701"}
        assert fields is None
        assert data_root == (tmp_path / "data").resolve()
        assert progress_callback is None
        assert execution_options == {}
        return (
            [
                    {
                        "trade_date": "20260701",
                        "instrument_id": "000001.SZ",
                        "symbol": "000001",
                        "tdx_code": "000001",
                        "exchange": "SZSE",
                        "total_share": 123.4,
                        "float_share": 100.1,
                        "free_float_share_z": 80.2,
                        "finance_updated_date": "20260630",
                        "share_source": "fake",
                    }
                ],
            {"tdx_stats_date": "20260701"},
        )

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", build_registry)
    monkeypatch.setattr(downloaders, "run_downloader", unexpected_run_downloader)
    monkeypatch.setattr(tdx_collectors, "_request_tdx_records", fake_request_tdx_records)

    collector_name = "tdx.stock_daily_share_tdx.snapshot"
    plan = build_collector_run_plan(
        collector_name,
        params={"trade_date": "20260701"},
        data_root=tmp_path / "data",
    )
    result = run_collector(
        collector_name,
        params={"trade_date": "20260701"},
        data_root=tmp_path / "data",
        output_root=tmp_path / "export",
        formats=["parquet"],
    )

    assert plan.collector_id == collector_name
    assert plan.collector_plugin_id == TDX_COLLECTOR_PLUGIN_ID
    assert plan.provider_id == TDX_COLLECTOR_PLUGIN_ID
    assert plan.dataset_id == TDX_BASE_COLLECTOR_DATASET_IDS[collector_name]
    assert plan.runner_entry == TDX_COLLECTOR_RUNNER_ENTRY
    assert plan.target_interface is None
    assert plan.downloader_profile is None
    assert plan.interfaces == ()
    assert plan.required_interfaces == ()
    assert plan.is_legacy is False
    assert plan.legacy_source is None
    assert plan.params == {"scope": "all", "trade_date": "20260701"}

    download_result = result["download_result"]
    assert result["status"] == "success"
    assert result["runner_entry"] == TDX_COLLECTOR_RUNNER_ENTRY
    assert result["target_interface"] is None
    assert result["downloader_profile"] is None
    assert download_result["collector_id"] == collector_name
    assert download_result["collector_plugin_id"] == TDX_COLLECTOR_PLUGIN_ID
    assert download_result["dataset_id"] == "tdx.stock_daily_share"
    assert download_result["interface_name"] == "tdx.stock_daily_share"
    assert download_result["row_count"] == 1
    assert Path(download_result["output_paths"]["parquet"]).is_file()
    assert download_result["quality"]["quality_status"] == "ok", download_result["quality"]


def test_tdx_independent_collector_filters_scheduler_only_execution_options(monkeypatch, tmp_path) -> None:
    from tests.tdx_plugin_helpers import build_registry_with_local_tdx_plugins

    import axdata_core.downloaders as downloaders
    import axdata_core.provider_catalog as provider_catalog
    import axdata_source_tdx.collectors as tdx_collectors

    base_builder = provider_catalog.build_builtin_provider_registry

    def build_registry(**kwargs):
        return build_registry_with_local_tdx_plugins(
            base_builder=base_builder,
            **kwargs,
        )

    def unexpected_run_downloader(*_args, **_kwargs):  # pragma: no cover
        raise AssertionError("independent TDX collector must not call run_downloader")

    calls = []

    def fake_request_tdx_records(interface_name, params, *, fields, data_root, progress_callback, execution_options=None):
        calls.append(dict(execution_options or {}))
        return (
            [
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "tdx_code": "000001",
                    "exchange": "SZSE",
                    "name": "平安银行",
                    "market": "stock",
                }
            ],
            {"snapshot_date": "20260703"},
        )

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", build_registry)
    monkeypatch.setattr(downloaders, "run_downloader", unexpected_run_downloader)
    monkeypatch.setattr(tdx_collectors, "_request_tdx_records", fake_request_tdx_records)

    result = run_collector(
        "tdx.stock_codes_tdx.snapshot",
        data_root=tmp_path / "data",
        output_root=tmp_path / "export",
        formats=["parquet"],
        connection_count=6,
        source_server_count=2,
        connections_per_server=3,
        max_concurrent_tasks=6,
        batch_size=80,
        request_interval_ms=50,
        retry_count=2,
        timeout_ms=5000,
    )

    assert result["status"] == "success"
    assert calls == [{"source_server_count": 2, "connections_per_server": 3}]


def test_collector_runner_rejects_missing_required_interface(monkeypatch, tmp_path) -> None:
    collector = CollectorSpec(
        name="close_refresh_daily",
        display_name_zh="收盘刷新",
        interfaces=("stock_codes_tdx",),
        required_interfaces=("missing_interface",),
        downloader_profile="runner_demo.stock_codes.snapshot",
        resource_group="runner.demo",
    )
    registry = _runner_registry(collector=collector)

    import axdata_core.provider_catalog as provider_catalog

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", lambda **_kwargs: registry)

    try:
        build_collector_run_plan("close_refresh_daily", data_root=tmp_path / "data")
    except CollectorError as exc:
        assert "missing_interface" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("CollectorError was not raised")


def test_collector_runner_rejects_unavailable_downloader_profile(monkeypatch, tmp_path) -> None:
    collector = CollectorSpec(
        name="close_refresh_daily",
        display_name_zh="收盘刷新",
        interfaces=("stock_codes_tdx",),
        downloader_profile="missing_profile",
        resource_group="runner.demo",
    )
    registry = _runner_registry(collector=collector)

    import axdata_core.provider_catalog as provider_catalog

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", lambda **_kwargs: registry)

    try:
        build_collector_run_plan("close_refresh_daily", data_root=tmp_path / "data")
    except CollectorError as exc:
        assert "missing_profile" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("CollectorError was not raised")


def test_primary_key_granularity_guard_rejects_degenerate_key():
    import pandas as pd

    from axdata_core.collector_runner import _guard_primary_key_granularity, _profile_from_plan

    plan = _granularity_guard_plan(["trade_date", "trade_date"])
    profile = _profile_from_plan(plan)
    frame = pd.DataFrame(
        [{"trade_date": "20260826", "v": i} for i in range(10)]
    )
    try:
        _guard_primary_key_granularity(profile, frame)
    except CollectorError as exc:
        assert "collapse" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("CollectorError was not raised")


def test_primary_key_granularity_guard_allows_unique_keys():
    import pandas as pd

    from axdata_core.collector_runner import _guard_primary_key_granularity, _profile_from_plan

    plan = _granularity_guard_plan(["trade_date", "plate_id"])
    profile = _profile_from_plan(plan)
    frame = pd.DataFrame(
        [{"trade_date": "20260826", "plate_id": f"P{i}", "v": i} for i in range(10)]
    )
    _guard_primary_key_granularity(profile, frame)


def test_row_reconciliation_detects_count_mismatch(tmp_path):
    import pandas as pd

    from axdata_core.collector_runner import _reconcile_row_counts

    frame = pd.DataFrame([{"trade_date": "20260826", "instrument_id": f"00000{i}.SZ"} for i in range(3)])
    frame.to_parquet(tmp_path / "out.parquet", engine="pyarrow", index=False)

    ok = _reconcile_row_counts(
        frame,
        key_fields=["trade_date", "instrument_id"],
        write_metadata={"rows_written": 3, "rows_after": 3, "primary_key": ["trade_date", "instrument_id"]},
        output_paths={"parquet": tmp_path / "out.parquet"},
    )
    assert ok["status"] == "ok"
    assert ok["stored_rows"] == 3

    bad = _reconcile_row_counts(
        frame,
        key_fields=["trade_date", "instrument_id"],
        write_metadata={"rows_written": 2, "rows_after": 3, "primary_key": ["trade_date", "instrument_id"]},
        output_paths={"parquet": tmp_path / "out.parquet"},
    )
    assert bad["status"] == "error"
    assert "rows_written=2 != collected_rows=3" in bad["message"]


from types import SimpleNamespace


def _granularity_guard_plan(primary_key):
    output = {
        "layer": "snapshot",
        "formats": ["parquet"],
        "primary_key": primary_key,
        "file_name_template": "{dataset_id}_{snapshot_date}",
        "write_mode": "upsert_by_key",
        "datasets": [
            {
                "dataset_id": "guard.demo",
                "table": "guard_demo",
                "fields": [
                    {"name": "trade_date", "type": "date"},
                    {"name": "plate_id", "type": "string"},
                    {"name": "v", "type": "float64"},
                ],
            }
        ],
    }
    return SimpleNamespace(
        collector_name="guard.demo.snapshot",
        collector_id="guard.demo.snapshot",
        display_name_zh="守卫测试",
        collector_plugin_id="axdata.collector.guard",
        dataset_id="guard.demo",
        provider_id="axdata.collector.guard",
        effective_trust_level="community",
        built_in=False,
        resource_group="guard.http",
        runner_entry="axdata_guard.demo:run",
        is_legacy=False,
        legacy_source=None,
        interfaces=("guard_demo",),
        required_interfaces=(),
        required_datasets=(),
        downloader_profile=None,
        target_interface=None,
        params={},
        fields=None,
        formats=["parquet"],
        output=output,
        quality={},
    )
