"""新浪财经采集器 collector runner(通用支撑包生成)。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from axdata_collector_support.runner import make_runner
from axdata_core.plugins import ProviderManifest

COLLECTOR_PLUGIN_ID = "axdata.collector.sina"
RUNNER_ENTRY = "axdata_collector_sina.collectors:run_collector"

_MANIFEST_PATH = Path(__file__).with_name("axdata-plugin.json")


def collector_manifest() -> ProviderManifest:
    """Return the collector-only manifest loaded from axdata-plugin.json."""

    payload = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    return ProviderManifest.from_dict(payload)


run_collector = make_runner("sina")
