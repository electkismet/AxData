"""交易所采集器 collector plugin entry point."""

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
