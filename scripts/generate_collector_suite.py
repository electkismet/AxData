"""Generate AxData collector plugin packages for builtin web sources.

从运行中的 AxData API 读取合并接口目录,为 7 个内置 Web 数据源
(cls / cninfo / eastmoney / exchange / kph / sina / tencent)生成
collector 插件包:manifest(axdata-plugin.json)、runner 模块和包脚手架。

用法:
    python scripts/generate_collector_suite.py             # 从 127.0.0.1:8666 读取
    python scripts/generate_collector_suite.py --json cache/ifaces.json
"""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PACKAGES_DIR = ROOT / "packages"

SOURCES: dict[str, str] = {
    "cls": "财联社采集器",
    "cninfo": "巨潮采集器",
    "eastmoney": "东方财富采集器",
    "exchange": "交易所采集器",
    "kph": "开盘红采集器",
    "sina": "新浪财经采集器",
    "tencent": "腾讯财经采集器",
}

SCHEDULE_TIME = {
    "cls": "16:30",
    "cninfo": "17:00",
    "eastmoney": "15:30",
    "exchange": "18:00",
    "kph": "15:40",
    "sina": "16:00",
    "tencent": "15:50",
}

ID_FIELD_PRIORITY = (
    "announcement_id",
    "notice_id",
    "report_id",
    "instrument_id",
    "index_code",
    "sector_code",
    "fund_code",
    "bond_code",
    "currency_code",
    "symbol",
    "code",
)

PAGE_PARAMS = {"page", "limit", "pagesize"}

VALID_ASSET_CLASSES = {"stock", "index", "etf", "fund", "bond", "future"}


SINA_KEEP_INTERFACES = {
    "sina_stock_restricted_release_queue_sina",
    "sina_stock_zh_index_spot_sina",
    "sina_tool_trade_date_hist_sina",
    "sina_stock_lhb_detail_daily_sina",
    "sina_stock_lhb_ggtj_sina",
    "sina_stock_lhb_jgmx_sina",
    "sina_stock_lhb_jgzz_sina",
    "sina_stock_lhb_yytj_sina",
}


def load_interfaces(json_path: str | None) -> list[dict[str, Any]]:
    if json_path:
        payload = json.loads(Path(json_path).read_text(encoding="utf-8"))
    else:
        with urllib.request.urlopen("http://127.0.0.1:8666/v1/request/interfaces", timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    items = payload.get("data", payload)
    if isinstance(items, dict):
        items = list(items.values())
    items = [item for item in items if item.get("source_code") in SOURCES]
    # 新浪只保留用户选定的核心接口,其余不生成采集器
    if SINA_KEEP_INTERFACES is not None:
        items = [
            item
            for item in items
            if item.get("source_code") != "sina" or str(item.get("name")) in SINA_KEEP_INTERFACES
        ]
    return items


def infer_primary_key(fields: list[str]) -> list[str]:
    field_set = set(fields)
    id_field = next((name for name in ID_FIELD_PRIORITY if name in field_set), None)
    if "trade_date" in field_set:
        if id_field and id_field != "trade_date":
            return ["trade_date", id_field]
        # 无证券代码类字段时用板块/事件类字段做天内区分,避免退化成重复的日期键
        discriminator = next(
            (
                name
                for name in ("plate_id", "plate_code", "sector_code", "index_code", "event_time", "tag_id", "sector_name", "plate_name", "name")
                if name in field_set
            ),
            None,
        )
        return ["trade_date", discriminator] if discriminator else ["trade_date"]
    return [id_field or (fields[0] if fields else "row")]


def build_default_params(interface: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    example_request = ((interface.get("example") or {}).get("request") or {})
    for param in interface.get("parameters") or []:
        name = str(param.get("name") or "")
        if not name:
            continue
        if name in PAGE_PARAMS:
            continue
        # 日期族参数一律空默认:不输日期由适配器按统一规则取
        # 最新交易日/最新报告期/最近区间,绝不再钉死接口示例日期
        if name in {"trade_date", "date", "start_date", "end_date"}:
            params[name] = ""
            continue
        value = example_request.get(name)
        if value is not None:
            params[name] = value
        elif param.get("required"):
            params[name] = ""
    return params


def build_collector(source: str, interface: dict[str, Any]) -> dict[str, Any]:
    name = str(interface["name"])
    short = name[len(source) + 1:] if name.startswith(f"{source}_") else name
    collector_name = f"{source}.{short}.snapshot"
    dataset_id = f"{source}.{short}"
    display = str(interface.get("display_name_zh") or name)

    fields = [str(field.get("name") or "") for field in interface.get("fields") or []]
    field_specs = [
        {
            "name": str(field.get("name") or ""),
            "type": str(field.get("dtype") or "string"),
            "description": str(field.get("description_zh") or field.get("description") or ""),
        }
        for field in interface.get("fields") or []
    ]
    primary_key = infer_primary_key(fields)
    dated = "trade_date" in fields
    layer = "core" if dated else "snapshot"
    # 写入模式全库统一为 upsert_by_key:同主键新值替换、补采幂等、重跑即修复
    write_mode = "upsert_by_key"
    required_columns = [name for name in primary_key if name in fields] or fields[:1]
    query_fields = [name for name in fields if name in set(primary_key) | {
        "trade_date", "instrument_id", "symbol", "name", "close_price", "last_price",
        "change_pct", "volume", "amount", "title", "sector_name", "index_name",
        "sector_code", "index_code", "rating", "org_name", "publish_date",
        "continuous_count", "main_inflow", "turnover_rate",
    }][:12] or fields[:10]

    output: dict[str, Any] = {
        "layer": layer,
        "formats": ["parquet", "csv"],
        "supported_formats": ["parquet", "csv", "duckdb"],
        "default_output_path_parts": (
            ["core", f"table={dataset_id.replace('.', '_')}"] if dated else ["snapshot", f"dataset={dataset_id}"]
        ),
        "default_dir_name": dataset_id,
        # 快照层按数据日期命名:同一天多次采集覆盖为最新,避免累积重复;core 层本就按交易日分文件
        "file_name_template": "{dataset_id}_{snapshot_date}" if not dated else "{dataset_id}_{snapshot_date}",
        "write_mode": write_mode,
        "primary_key": primary_key,
        "required_columns": required_columns,
        "snapshot_date_meta_keys": ["data_date", "snapshot_date", "trade_date", "publish_date", "date"],
    }
    dataset_decl: dict[str, Any] = {
        "dataset_id": dataset_id,
        "table": dataset_id.replace(".", "_"),
        "display_name_zh": display,
        "description": str(interface.get("description_zh") or interface.get("description") or ""),
        "layer": layer,
        "primary_key": primary_key,
        "write_mode": write_mode,
        "fields": field_specs,
        "default_query_fields": query_fields,
        "default_filter_fields": [name for name in primary_key][:2],
        "quality_rules": {"required_columns": required_columns},
        "storage": {
            "layout": "daily_file" if dated else "snapshot",
            "path_parts": output["default_output_path_parts"],
        },
        "default_output_path_parts": output["default_output_path_parts"],
        "formats": ["parquet", "csv", "duckdb"],
    }
    if dated:
        output["date_field"] = "trade_date"
        output["partition_by"] = ["trade_date"]
        dataset_decl["date_field"] = "trade_date"
        dataset_decl["partition_by"] = ["trade_date"]
    output["datasets"] = [dataset_decl]

    quality: dict[str, Any] = {
        "required_columns": required_columns,
        "primary_key": primary_key,
    }
    if dated:
        quality["date_field"] = "trade_date"

    default_schedule = (
        {"kind": "trading_day", "time": SCHEDULE_TIME[source], "timezone": "Asia/Shanghai"}
        if dated
        else {"kind": "manual"}
    )

    collector = {
        "collector_id": collector_name,
        "name": collector_name,
        "display_name_zh": f"{display}采集",
        "description": f"独立{SOURCES[source]}插件:采集{display}并写入本地{'core 层按交易日 Parquet' if dated else '快照层'}。",
        "collector_plugin_id": f"axdata.collector.{source}",
        "dataset_id": dataset_id,
        "category": interface.get("category") or "web",
        "resource_group": f"{source}.http",
        "runner_entry": f"axdata_collector_{source}.collectors:run_collector",
        "interfaces": [name],
        "required_interfaces": [],
        "required_datasets": [],
        "default_schedule": default_schedule,
        "default_params": build_default_params(interface),
        "output": output,
        "quality": quality,
    }
    asset_class = str(interface.get("asset_class") or "").strip().lower()
    if asset_class in VALID_ASSET_CLASSES:
        collector["asset_class"] = asset_class
    return collector


PYPROJECT = """[build-system]
requires = ["setuptools>=77", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "axdata-collector-{source}"
version = "0.1.0"
description = "{name_zh} collector plugin for AxData"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
  "axdata-core>=0.1.0",
  "axdata-collector-support>=0.1.0",
]
authors = [
  {{ name = "AxData" }},
]
license = "Apache-2.0"

[project.entry-points."axdata.plugins"]
{source} = "axdata_collector_{source}.plugin:plugin"

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
axdata_collector_{source} = ["axdata-plugin.json"]
"""

README = """# axdata-collector-{source}

{name_zh}(AxData collector-only plugin),由 `scripts/generate_collector_suite.py`
从接口目录自动生成,共 {count} 个采集器。

## 安装

```powershell
.\\.venv\\Scripts\\python -m pip install -e packages\\axdata-collector-{source}
```

安装后在 Web 控制台「插件管理」启用 `axdata.collector.{source}`,
再到「采集」页为任意采集器创建任务。
"""

INIT_PY = '''"""axdata_collector_{source} package."""

from .collectors import COLLECTOR_PLUGIN_ID, collector_manifest

__all__ = ["COLLECTOR_PLUGIN_ID", "collector_manifest"]
'''

COLLECTORS_PY = '''"""{name_zh} collector runner(通用支撑包生成)。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from axdata_collector_support.runner import make_runner
from axdata_core.plugins import ProviderManifest

COLLECTOR_PLUGIN_ID = "axdata.collector.{source}"
RUNNER_ENTRY = "axdata_collector_{source}.collectors:run_collector"

_MANIFEST_PATH = Path(__file__).with_name("axdata-plugin.json")


def collector_manifest() -> ProviderManifest:
    """Return the collector-only manifest loaded from axdata-plugin.json."""

    payload = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    return ProviderManifest.from_dict(payload)


run_collector = make_runner("{source}")
'''

PLUGIN_PY = '''"""{name_zh} collector plugin entry point."""

from __future__ import annotations

from typing import Any

from .collectors import COLLECTOR_PLUGIN_ID, collector_manifest


class _CollectorPlugin:
    @property
    def plugin_id(self) -> str:
        return COLLECTOR_PLUGIN_ID

    @property
    def manifest(self) -> Any:
        return collector_manifest()


plugin = _CollectorPlugin()
'''


def emit_package(source: str, collectors: list[dict[str, Any]], *, scaffold: bool) -> None:
    pkg_dir = PACKAGES_DIR / f"axdata-collector-{source}"
    src_dir = pkg_dir / "src" / f"axdata_collector_{source}"
    src_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "manifest_version": "1.0",
        "plugin_api_version": "1.0",
        "plugin": {
            "plugin_id": f"axdata.collector.{source}",
            "name_zh": SOURCES[source],
            "version": "0.1.0",
            "description": f"{SOURCES[source]}插件:按接口目录自动生成独立采集器。",
        },
        "provider": None,
        "interfaces": [],
        "downloaders": [],
        "collectors": collectors,
        "dependencies": [],
        "config_schema": {},
        "required_config": [],
        "resources": {},
        "build": {},
    }
    (src_dir / "axdata-plugin.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if scaffold:
        (pkg_dir / "pyproject.toml").write_text(
            PYPROJECT.format(source=source, name_zh=SOURCES[source]), encoding="utf-8"
        )
        (pkg_dir / "README.md").write_text(
            README.format(source=source, name_zh=SOURCES[source], count=len(collectors)), encoding="utf-8"
        )
        (src_dir / "__init__.py").write_text(INIT_PY.format(source=source), encoding="utf-8")
        (src_dir / "plugin.py").write_text(PLUGIN_PY.format(source=source, name_zh=SOURCES[source]), encoding="utf-8")
    # collectors.py 始终重写(eastmoney 旧的手写 runner 由通用版替代)
    (src_dir / "collectors.py").write_text(
        COLLECTORS_PY.format(source=source, name_zh=SOURCES[source]), encoding="utf-8"
    )
    # 清理旧支撑文件(eastmoney 手写版遗留)
    for stale in ("plugin_old.py",):
        stale_path = src_dir / stale
        if stale_path.exists():
            stale_path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description="生成内置 Web 源采集器插件包")
    parser.add_argument("--json", default=None, help="接口目录 JSON 文件(默认从 API 拉取)")
    parser.add_argument("--source", action="append", help="只生成指定源(可多次)")
    args = parser.parse_args()

    interfaces = load_interfaces(args.json)
    targets = set(args.source) if args.source else set(SOURCES)
    by_source: dict[str, list[dict[str, Any]]] = {source: [] for source in SOURCES}
    for interface in sorted(interfaces, key=lambda item: str(item.get("name"))):
        source = str(interface["source_code"])
        if source in by_source:
            by_source[source].append(build_collector(source, interface))

    total = 0
    for source, collectors in by_source.items():
        if source not in targets:
            continue
        pkg_dir = PACKAGES_DIR / f"axdata-collector-{source}"
        emit_package(source, collectors, scaffold=not pkg_dir.exists())
        total += len(collectors)
        dated = sum(1 for c in collectors if c["output"]["layer"] == "core")
        print(f"{SOURCES[source]:8s} axdata.collector.{source:10s} -> {len(collectors):3d} 个采集器(日期型 {dated})")
    print(f"共生成 {total} 个采集器")


if __name__ == "__main__":
    main()
