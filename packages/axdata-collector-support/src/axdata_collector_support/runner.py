"""Generic AxData collector runner for builtin web sources.

每个数据源插件包通过 ``make_runner(source_code)`` 生成自己的 runner:
- 通过 ``axdata_core.adapters.{source}.provider_bridge`` 直接实例化源适配器
- 单次运行返回 records + meta,写盘 / 质量 / 进度由 AxData Collector Runner 管理
- 声明了 ``quality.date_field == "trade_date"`` 的日期型采集器支持
  start_date / end_date 区间补采(逐日请求、跳过空日期)
"""

from __future__ import annotations

import importlib
from datetime import date, timedelta
from typing import Any, Mapping, Sequence

_ADAPTER_FACTORIES: dict[str, tuple[str, str]] = {
    "cls": ("axdata_core.adapters.cls.provider_bridge", "create_cls_request_adapter"),
    "cninfo": ("axdata_core.adapters.cninfo.provider_bridge", "create_cninfo_request_adapter"),
    "eastmoney": ("axdata_core.adapters.eastmoney.provider_bridge", "create_eastmoney_request_adapter"),
    "exchange": ("axdata_core.adapters.exchange.provider_bridge", "create_exchange_request_adapter"),
    "kph": ("axdata_core.adapters.kph.provider_bridge", "create_kph_request_adapter"),
    "sina": ("axdata_core.adapters.sina.provider_bridge", "create_sina_request_adapter"),
    "tencent": ("axdata_core.adapters.tencent.provider_bridge", "create_tencent_request_adapter"),
}

_DATE_KEYS = ("trade_date", "publish_date", "announce_date", "report_date", "settlement_date")


def _create_adapter(source_code: str) -> Any:
    module_name, factory_name = _ADAPTER_FACTORIES[source_code]
    module = importlib.import_module(module_name)
    return getattr(module, factory_name)(None)


def _interface_from_collector_id(source_code: str, collector_id: str) -> str:
    parts = collector_id.split(".")
    middle = ".".join(parts[1:-1]) if len(parts) >= 3 else ".".join(parts[1:])
    return f"{source_code}_{middle}"


def _date_range(start: str, end: str) -> list[str]:
    try:
        d0 = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
        d1 = date(int(end[:4]), int(end[4:6]), int(end[6:8]))
    except ValueError:
        return [start]
    if d1 < d0:
        d0, d1 = d1, d0
    return [(d0 + timedelta(days=offset)).strftime("%Y%m%d") for offset in range((d1 - d0).days + 1)]


def make_runner(source_code: str):
    """Return a runner function bound to one builtin web source."""

    if source_code not in _ADAPTER_FACTORIES:
        raise ValueError(f"Unsupported collector source: {source_code}")

    def run_collector(
        *,
        params: Mapping[str, Any] | None = None,
        fields: Sequence[str] | None = None,
        collector: Mapping[str, Any] | None = None,
        data_root: str | None = None,
        progress_callback: Any | None = None,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        collector_info = dict(collector or {})
        collector_id = str(collector_info.get("collector_id") or collector_info.get("name") or "")
        interfaces = [str(item) for item in (collector_info.get("interfaces") or []) if str(item)]
        interface_name = interfaces[0] if interfaces else _interface_from_collector_id(source_code, collector_id)

        request_params = {key: value for key, value in dict(params or {}).items() if str(value).strip() != ""}
        dated = (collector_info.get("quality") or {}).get("date_field") == "trade_date"
        start_date = str(request_params.pop("start_date", "") or "").strip()
        end_date = str(request_params.pop("end_date", "") or "").strip()

        adapter = _create_adapter(source_code)

        if dated and (start_date or end_date) and not request_params.get("trade_date"):
            records: list[dict[str, Any]] = []
            for day in _date_range(start_date or end_date, end_date or start_date):
                records.extend(dict(row) for row in adapter.request(interface_name, {"trade_date": day}))
                if progress_callback is not None:
                    try:
                        progress_callback(len(records), f"已取 {day}")
                    except TypeError:
                        progress_callback(len(records))
        else:
            records = [dict(row) for row in adapter.request(interface_name, request_params)]

        if fields is not None:
            wanted = list(fields)
            records = [{key: row.get(key) for key in wanted} for row in records]

        meta: dict[str, Any] = {
            "source": source_code,
            "interface_name": interface_name,
            "collector_id": collector_id,
            "collector_plugin_id": f"axdata.collector.{source_code}",
            "dataset_id": collector_info.get("dataset_id") or "",
            "row_count": len(records),
        }
        data_date = next(
            (str(records[0][key]) for key in _DATE_KEYS if records and records[0].get(key)),
            None,
        )
        meta["data_date"] = data_date or end_date or start_date or date.today().strftime("%Y%m%d")
        meta.update(dict(getattr(adapter, "last_meta", {}) or {}))
        return {"records": records, "meta": meta}

    run_collector.__name__ = f"run_{source_code}_collector"
    run_collector.__qualname__ = run_collector.__name__
    return run_collector
