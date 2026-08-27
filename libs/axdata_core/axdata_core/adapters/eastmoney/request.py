"""Eastmoney source request adapter."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from axdata_core.source_errors import (
    SourceAdapterNotFound,
    SourceRequestValidationError,
    SourceUnavailableError,
)


SUPPORTED_INTERFACES = {
    "eastmoney_market_index_realtime",
    "eastmoney_stock_realtime_snapshot",
    "eastmoney_sector_realtime",
    "eastmoney_sector_constituents",
    "eastmoney_stock_sector_belong",
    "eastmoney_limit_up_pool",
    "eastmoney_limit_down_pool",
    "eastmoney_yesterday_limit_up_pool",
    "eastmoney_stock_changes",
    "eastmoney_stock_change_detail",
    "eastmoney_dragon_tiger_daily",
    "eastmoney_margin_trading",
    "eastmoney_research_reports",
}

EASTMONEY_DATA_CENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_REPORT_LIST_URL = "https://reportapi.eastmoney.com/report/list"
EASTMONEY_CLIST_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
EASTMONEY_ULIST_URL = "https://push2delay.eastmoney.com/api/qt/ulist.np/get"
EASTMONEY_ZT_POOL_URL = "https://push2ex.eastmoney.com/getTopicZTPool"
EASTMONEY_DT_POOL_URL = "https://push2ex.eastmoney.com/getTopicDTPool"
EASTMONEY_YESTERDAY_ZT_URL = "https://push2ex.eastmoney.com/getYesterdayZTPool"
EASTMONEY_STOCK_CHANGES_URL = "https://push2ex.eastmoney.com/getAllStockChanges"
EASTMONEY_STOCK_CHANGE_DETAIL_URL = "https://push2ex.eastmoney.com/getStockChanges"

_DEFAULT_INDEX_NAMES = ("上证指数", "深证成指", "创业板指", "科创50", "沪深300", "中证500")
_SECTOR_TYPE_FS = {
    "industry": "m:90+t:2+f:!50",
    "concept": "m:90+t:3+f:!50",
    "industry_l1": "m:90+s:2+f:!50",
    "industry_l2": "m:90+s:4+f:!50",
    "industry_l3": "m:90+s:8+f:!50",
}
_CHANGE_TYPE_NAMES = {
    "8201": "火箭发射",
    "8202": "快速反弹",
    "8203": "加速下跌",
    "8204": "高台跳水",
    "8193": "大笔买入",
    "8194": "大笔卖出",
    "8205": "封涨停板",
    "8206": "封跌停板",
    "8207": "打开跌停板",
    "8208": "打开涨停板",
    "64": "有大买盘",
    "128": "有大卖盘",
    "8209": "竞价上涨",
    "8210": "竞价下跌",
    "8211": "高开5日线",
    "8212": "低开5日线",
    "8213": "向上缺口",
    "8214": "向下缺口",
    "8215": "60日新高",
    "8216": "60日新低",
    "8217": "60日大幅上涨",
    "8218": "60日大幅下跌",
}


class EastmoneyRequestAdapter:
    """Request Eastmoney low-frequency metadata and return AxData fields."""

    source = "eastmoney"

    def __init__(self, opener: Any | None = None, *, timeout: float = 15.0) -> None:
        self._opener = opener
        self._timeout = timeout
        self.last_meta: dict[str, Any] = {}

    def supports(self, interface_name: str) -> bool:
        return interface_name in SUPPORTED_INTERFACES

    def request(self, interface_name: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if interface_name == "eastmoney_market_index_realtime":
            return self._request_market_index_realtime(params)
        if interface_name == "eastmoney_stock_realtime_snapshot":
            return self._request_stock_realtime_snapshot(params)
        if interface_name == "eastmoney_sector_realtime":
            return self._request_sector_realtime(params)
        if interface_name == "eastmoney_sector_constituents":
            return self._request_sector_constituents(params)
        if interface_name == "eastmoney_stock_sector_belong":
            return self._request_stock_sector_belong(params)
        if interface_name == "eastmoney_limit_up_pool":
            return self._request_limit_pool(params, pool="up")
        if interface_name == "eastmoney_limit_down_pool":
            return self._request_limit_pool(params, pool="down")
        if interface_name == "eastmoney_yesterday_limit_up_pool":
            return self._request_yesterday_limit_up_pool(params)
        if interface_name == "eastmoney_stock_changes":
            return self._request_stock_changes(params)
        if interface_name == "eastmoney_stock_change_detail":
            return self._request_stock_change_detail(params)
        if interface_name == "eastmoney_dragon_tiger_daily":
            return self._request_dragon_tiger_daily(params)
        if interface_name == "eastmoney_margin_trading":
            return self._request_margin_trading(params)
        if interface_name == "eastmoney_research_reports":
            return self._request_research_reports(params)
        raise SourceAdapterNotFound(
            f"Eastmoney source adapter does not support interface {interface_name!r}."
        )

    def _request_market_index_realtime(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        scope = str(params.get("scope") or "default").strip().lower()
        if scope not in {"default", "all"}:
            raise SourceRequestValidationError("scope must be default or all")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 500)
        rows = self._fetch_clist(
            {
                "np": "1",
                "fltt": "1",
                "invt": "2",
                "fs": "b:MK0010",
                "fields": "f12,f14,f2,f3,f4,f5,f6,f15,f16,f17,f18",
                "pn": "1",
                "pz": str(limit),
                "po": "1",
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "dect": "1",
            },
            context="Eastmoney market index realtime",
        )
        normalized = [_normalize_index_row(row) for row in rows]
        if scope == "default":
            by_name = {row.get("index_name"): row for row in normalized}
            normalized = [by_name[name] for name in _DEFAULT_INDEX_NAMES if name in by_name]
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_CLIST_URL,
            "scope": scope,
            "limit": limit,
        }
        return normalized

    def _request_stock_realtime_snapshot(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        codes = _parse_codes(params.get("code"), required=False)
        filter_st = _parse_bool(params.get("filter_st"), default=True)
        if codes:
            secids = [_secid_from_code(code) for code in codes[:100]]
            payload = self._fetch_json_or_jsonp(
                EASTMONEY_ULIST_URL,
                params={
                    "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17,f18,f20,f21,f23",
                    "fltt": "2",
                    "invt": "2",
                    "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                    "secids": ",".join(secids),
                },
                context="Eastmoney stock realtime snapshot",
                referer="https://quote.eastmoney.com/",
            )
            rows = _payload_diff_rows(payload, "Eastmoney stock realtime snapshot")
            page = None
            limit = len(codes)
        else:
            page = _positive_int(params.get("page"), default=1, name="page")
            limit = min(_positive_int(params.get("limit"), default=50, name="limit"), 200)
            rows = self._fetch_clist(
                {
                    "pn": str(page),
                    "pz": str(limit),
                    "po": "1",
                    "np": "1",
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": "2",
                    "invt": "2",
                    "fid": "f3",
                    "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
                    "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17,f18,f20,f21,f23",
                },
                context="Eastmoney stock realtime snapshot",
            )
        normalized = [_normalize_quote_row(row) for row in rows]
        if filter_st:
            normalized = [
                row for row in normalized
                if not str(row.get("name") or "").startswith(("ST", "*ST"))
                and not (row.get("last_price") is not None and row.get("last_price") <= 1.0)
            ]
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_ULIST_URL if codes else EASTMONEY_CLIST_URL,
            "requested_codes": list(codes),
            "page": page,
            "limit": limit,
            "filter_st": filter_st,
        }
        return normalized

    def _request_sector_realtime(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        sector_type = str(params.get("sector_type") or "industry").strip().lower()
        if sector_type not in _SECTOR_TYPE_FS:
            known = ", ".join(_SECTOR_TYPE_FS)
            raise SourceRequestValidationError(f"sector_type must be one of {known}")
        page = _positive_int(params.get("page"), default=1, name="page")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 200)
        rows = self._fetch_clist(
            {
                "pn": str(page),
                "pz": str(limit),
                "po": "1",
                "np": "1",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": _SECTOR_TYPE_FS[sector_type],
                "fields": "f12,f14,f2,f3,f4,f5,f6,f7,f8,f20,f62,f128,f136,f140,f104,f105",
            },
            context="Eastmoney sector realtime",
        )
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_CLIST_URL,
            "sector_type": sector_type,
            "page": page,
            "limit": limit,
        }
        return [_normalize_sector_row(row, sector_type=sector_type) for row in rows]

    def _request_sector_constituents(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        sector_code = _required_text(params.get("sector_code"), "sector_code").upper()
        page = _positive_int(params.get("page"), default=1, name="page")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 200)
        rows = self._fetch_clist(
            {
                "pn": str(page),
                "pz": str(limit),
                "po": "1",
                "np": "1",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": f"b:{sector_code}+f:!50",
                "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17,f18,f20,f21,f23",
            },
            context="Eastmoney sector constituents",
        )
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_CLIST_URL,
            "sector_code": sector_code,
            "page": page,
            "limit": limit,
        }
        return [_normalize_quote_row(row) for row in rows]

    def _request_stock_sector_belong(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        codes = _parse_codes(params.get("code"), required=True)
        secids = [_secid_from_code(code) for code in codes[:100]]
        payload = self._fetch_json_or_jsonp(
            EASTMONEY_ULIST_URL,
            params={
                "fields": "f12,f14,f100",
                "invt": "2",
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "secids": ",".join(secids),
            },
            context="Eastmoney stock sector belong",
            referer="https://quote.eastmoney.com/",
        )
        rows = _payload_diff_rows(payload, "Eastmoney stock sector belong")
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_ULIST_URL,
            "requested_codes": list(codes),
        }
        return [_normalize_stock_sector_belong_row(row) for row in rows]

    def _request_limit_pool(self, params: Mapping[str, Any], *, pool: str) -> list[dict[str, Any]]:
        trade_date = _normalize_date(params.get("trade_date"), "trade_date", required=False) or _today_yyyymmdd()
        url = EASTMONEY_ZT_POOL_URL if pool == "up" else EASTMONEY_DT_POOL_URL
        payload = self._fetch_json(
            url,
            params={
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "dpt": "wz.ztzt",
                "Pageindex": "0",
                "pagesize": "3000",
                "sort": "fbt:asc" if pool == "up" else "fund:asc",
                "date": trade_date,
            },
            context=f"Eastmoney limit {pool} pool",
            referer="https://quote.eastmoney.com/",
        )
        rows = _pool_rows(payload, f"Eastmoney limit {pool} pool")
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": url,
            "trade_date": trade_date,
            "pool": pool,
        }
        return [_normalize_limit_pool_row(row, trade_date=trade_date, pool=pool) for row in rows]

    def _request_yesterday_limit_up_pool(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        trade_date = _normalize_date(params.get("trade_date"), "trade_date", required=False) or _today_yyyymmdd()
        payload = self._fetch_json(
            EASTMONEY_YESTERDAY_ZT_URL,
            params={
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "dpt": "wz.ztzt",
                "Pageindex": "0",
                "pagesize": "3000",
                "sort": "zs:desc",
                "date": trade_date,
            },
            context="Eastmoney yesterday limit-up pool",
            referer="https://quote.eastmoney.com/",
        )
        rows = _pool_rows(payload, "Eastmoney yesterday limit-up pool")
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_YESTERDAY_ZT_URL,
            "trade_date": trade_date,
        }
        return [_normalize_yesterday_limit_up_row(row, trade_date=trade_date) for row in rows]

    def _request_stock_changes(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        change_type = str(params.get("change_type") or "8201").strip()
        if change_type not in _CHANGE_TYPE_NAMES:
            raise SourceRequestValidationError("change_type is not supported")
        filter_st = _parse_bool(params.get("filter_st"), default=True)
        payload = self._fetch_json(
            EASTMONEY_STOCK_CHANGES_URL,
            params={
                "type": change_type,
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "pageindex": "0",
                "pagesize": "10000",
                "dpt": "wzchanges",
            },
            context="Eastmoney stock changes",
            referer="https://quote.eastmoney.com/",
        )
        if not isinstance(payload, Mapping) or payload.get("rc") != 0:
            raise SourceUnavailableError("Eastmoney stock changes returned unexpected payload.")
        body = payload.get("data")
        rows = body.get("allstock", []) if isinstance(body, Mapping) else []
        normalized = [
            _normalize_stock_change_row(row, change_type=change_type)
            for row in rows if isinstance(row, Mapping)
        ]
        if filter_st:
            normalized = [
                row for row in normalized
                if not str(row.get("symbol") or "").startswith("4")
                and not str(row.get("name") or "").startswith(("ST", "*ST"))
            ]
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_STOCK_CHANGES_URL,
            "change_type": change_type,
            "change_type_name": _CHANGE_TYPE_NAMES[change_type],
            "filter_st": filter_st,
        }
        return normalized

    def _request_stock_change_detail(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        code = _required_text(params.get("code"), "code")
        symbol = _symbol_from_code(code)
        if not symbol:
            raise SourceRequestValidationError("code must be a six-digit A-share code")
        trade_date = _normalize_date(params.get("trade_date"), "trade_date", required=False) or _today_yyyymmdd()
        market_code = str(params.get("market") or _eastmoney_market_from_symbol(symbol)).strip()
        payload = self._fetch_json(
            EASTMONEY_STOCK_CHANGE_DETAIL_URL,
            params={
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "date": trade_date,
                "dpt": "wzchanges",
                "code": symbol,
                "market": market_code,
            },
            context="Eastmoney stock change detail",
            referer="https://quote.eastmoney.com/",
        )
        if not isinstance(payload, Mapping) or payload.get("rc") != 0:
            raise SourceUnavailableError("Eastmoney stock change detail returned unexpected payload.")
        body = payload.get("data")
        rows = body.get("data", []) if isinstance(body, Mapping) else []
        instrument_id = f"{symbol}.{_exchange_suffix(_exchange_from_symbol(symbol))}"
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_STOCK_CHANGE_DETAIL_URL,
            "trade_date": trade_date,
            "requested_code": code,
            "market": market_code,
        }
        return [
            _normalize_stock_change_detail_row(
                row,
                trade_date=trade_date,
                instrument_id=instrument_id,
                market_code=market_code,
            )
            for row in rows if isinstance(row, Mapping)
        ]

    def _request_dragon_tiger_daily(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        requested = _normalize_date(params.get("trade_date"), "trade_date", required=False)
        if requested is not None and requested >= _today_yyyymmdd():
            raise SourceRequestValidationError("trade_date must be earlier than today for this Eastmoney historical interface")
        # 显式传 page 时按单页语义返回;不传页码则自动翻页取全天完整榜单
        explicit_page = params.get("page") not in (None, "")
        page = _positive_int(params.get("page"), default=1, name="page")
        limit = min(_positive_int(params.get("limit"), default=200, name="limit"), 200)
        trade_date = requested or _today_yyyymmdd()
        rows: list[dict[str, Any]] = []
        payload: Mapping[str, Any] = {}
        # 未指定日期时从今天起回溯到最新有数据的交易日(与开盘红历史接口同一规则)
        for _ in range(1 if requested is not None else 12):
            payload = self._dragon_tiger_page(trade_date, page=page, limit=limit)
            rows = [_normalize_dragon_tiger_row(row) for row in payload["rows"]]
            if rows or requested is not None:
                break
            trade_date = _shift_yyyymmdd(trade_date, days=-1)
        total_count = _parse_int(payload.get("total_count"))
        if not explicit_page and total_count and rows:
            # 自动翻页直到取齐 total_count,防止繁忙交易日榜单被单页截断
            while len(rows) < total_count:
                page += 1
                payload = self._dragon_tiger_page(trade_date, page=page, limit=limit)
                page_rows = [_normalize_dragon_tiger_row(row) for row in payload["rows"]]
                if not page_rows:
                    break
                rows.extend(page_rows)
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_DATA_CENTER_URL,
            "trade_date": trade_date,
            "page": page,
            "limit": limit,
            "total_count": payload.get("total_count"),
            "total_pages": payload.get("total_pages"),
        }
        return rows

    def _dragon_tiger_page(self, trade_date: str, *, page: int, limit: int) -> Mapping[str, Any]:
        return self._fetch_data_center(
            {
                "sortColumns": "TRADE_DATE,SECURITY_CODE",
                "sortTypes": "-1,1",
                "pageSize": str(limit),
                "pageNumber": str(page),
                "reportName": "RPT_DAILYBILLBOARD_DETAILS",
                "columns": "ALL",
                "source": "WEB",
                "client": "WEB",
                "filter": f"(TRADE_DATE='{_date_dash(trade_date)}')",
            },
            context="Eastmoney dragon tiger daily",
            referer="https://data.eastmoney.com/stock/lhb.html",
            empty_is_ok=True,
        )

    def _request_margin_trading(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        code = params.get("code")
        if code in (None, ""):
            raise SourceRequestValidationError("code is required")
        symbol = _symbol_from_code(str(code))
        if not symbol:
            raise SourceRequestValidationError("code must be a six-digit A-share code")
        start_date = _normalize_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        page = _positive_int(params.get("page"), default=1, name="page")
        limit = min(_positive_int(params.get("limit"), default=200, name="limit"), 200)

        filter_parts = [f'(SCODE="{symbol}")']
        if start_date:
            filter_parts.append(f"(DATE>='{_date_dash(start_date)}')")
        if end_date:
            filter_parts.append(f"(DATE<='{_date_dash(end_date)}')")
        payload = self._fetch_data_center(
            {
                "reportName": "RPTA_WEB_RZRQ_GGMX",
                "columns": "ALL",
                "source": "WEB",
                "sortColumns": "date",
                "sortTypes": "-1",
                "pageNumber": str(page),
                "pageSize": str(limit),
                "filter": "".join(filter_parts),
            },
            context="Eastmoney margin trading",
            referer=f"https://data.eastmoney.com/rzrq/stock/{symbol}.html",
            empty_is_ok=True,
        )
        rows = [_normalize_margin_row(row) for row in payload["rows"]]
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_DATA_CENTER_URL,
            "requested_code": code,
            "page": page,
            "limit": limit,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "total_count": payload.get("total_count"),
            "total_pages": payload.get("total_pages"),
        }
        return rows

    def _request_research_reports(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        code = params.get("code")
        if code in (None, ""):
            raise SourceRequestValidationError("code is required")
        symbol = _symbol_from_code(str(code))
        if not symbol:
            raise SourceRequestValidationError("code must be a six-digit A-share code")
        start_date = _normalize_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        # 不传日期时默认最近一年窗口:研报接口无时间范围会返回最旧的一页
        if not start_date and not end_date:
            end_date = _today_yyyymmdd()
            start_date = _shift_yyyymmdd(end_date, days=-364)
        page = _positive_int(params.get("page"), default=1, name="page")
        limit = min(_positive_int(params.get("limit"), default=20, name="limit"), 100)
        query = {
            "pageNo": str(page),
            "pageSize": str(limit),
            "code": symbol,
            "industryCode": "*",
            "industry": "*",
            "rating": "",
            "ratingChange": "",
            "beginTime": _date_dash(start_date) if start_date else "",
            "endTime": _date_dash(end_date) if end_date else "",
            "qType": "0",
            "orgCode": "",
            "rcode": "",
        }
        payload = self._fetch_json(
            EASTMONEY_REPORT_LIST_URL,
            params=query,
            context="Eastmoney research reports",
            referer="https://data.eastmoney.com/report/stock.jshtml",
        )
        data = payload.get("data") if isinstance(payload, Mapping) else None
        if data is None:
            data = []
        if not isinstance(data, list):
            raise SourceUnavailableError("Eastmoney research reports returned unexpected payload.")
        rows = [_normalize_research_report_row(row) for row in data if isinstance(row, Mapping)]
        self.last_meta = {
            "source_name": "东方财富",
            "source_url": EASTMONEY_REPORT_LIST_URL,
            "requested_code": code,
            "page": page,
            "limit": limit,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "total_count": payload.get("hits"),
            "total_pages": payload.get("TotalPage"),
        }
        return rows

    def _fetch_data_center(
        self,
        params: Mapping[str, str],
        *,
        context: str,
        referer: str,
        empty_is_ok: bool,
    ) -> dict[str, Any]:
        payload = self._fetch_json(
            EASTMONEY_DATA_CENTER_URL,
            params=params,
            context=context,
            referer=referer,
        )
        if not isinstance(payload, Mapping):
            raise SourceUnavailableError(f"{context} returned unexpected payload.")
        if payload.get("success") is False:
            message = str(payload.get("message") or "")
            if empty_is_ok and "空" in message:
                return {"rows": [], "total_count": 0, "total_pages": 0}
            raise SourceUnavailableError(f"{context} returned error: {message or payload.get('code')}")
        result = payload.get("result")
        if not isinstance(result, Mapping):
            return {"rows": [], "total_count": 0, "total_pages": 0}
        rows = result.get("data") or []
        if not isinstance(rows, list):
            raise SourceUnavailableError(f"{context} returned unexpected rows.")
        return {
            "rows": [row for row in rows if isinstance(row, Mapping)],
            "total_count": result.get("count"),
            "total_pages": result.get("pages"),
        }

    def _fetch_json(
        self,
        url: str,
        *,
        params: Mapping[str, str],
        context: str,
        referer: str,
    ) -> Any:
        request_url = f"{url}?{urlencode(params)}"
        request = Request(
            request_url,
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                "Accept": "application/json,text/plain,*/*",
                "Referer": referer,
            },
        )
        try:
            response = self._open(request)
            with response:
                text = response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"{context} request failed: {exc}") from exc
        try:
            return json.loads(text)
        except ValueError as exc:
            raise SourceUnavailableError(f"{context} returned invalid JSON.") from exc

    def _fetch_json_or_jsonp(
        self,
        url: str,
        *,
        params: Mapping[str, str],
        context: str,
        referer: str,
    ) -> Any:
        request_url = f"{url}?{urlencode(params)}"
        request = Request(
            request_url,
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                "Accept": "application/json,text/plain,*/*",
                "Referer": referer,
            },
        )
        try:
            response = self._open(request)
            with response:
                text = response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"{context} request failed: {exc}") from exc
        text = text.strip()
        if text.startswith("jQuery"):
            match = re.search(r"\((.*)\)\s*;?$", text, re.DOTALL)
            if match:
                text = match.group(1)
        try:
            return json.loads(text)
        except ValueError as exc:
            raise SourceUnavailableError(f"{context} returned invalid JSON.") from exc

    def _fetch_clist(self, params: Mapping[str, str], *, context: str) -> list[Mapping[str, Any]]:
        payload = self._fetch_json_or_jsonp(
            EASTMONEY_CLIST_URL,
            params=params,
            context=context,
            referer="https://quote.eastmoney.com/",
        )
        return _payload_diff_rows(payload, context)

    def _open(self, request: Request) -> Any:
        if self._opener is not None:
            return self._opener(request, timeout=self._timeout)
        return urlopen(request, timeout=self._timeout)


def _normalize_dragon_tiger_row(row: Mapping[str, Any]) -> dict[str, Any]:
    instrument_id = _instrument_id_from_secucode(row.get("SECUCODE"), row.get("SECURITY_CODE"))
    symbol, exchange = _split_instrument_id(instrument_id)
    return {
        "trade_date": _date_from_datetime(row.get("TRADE_DATE")),
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "name": _clean_text(row.get("SECURITY_NAME_ABBR")),
        "reason": _clean_text(row.get("EXPLANATION")),
        "close_price": _parse_float(row.get("CLOSE_PRICE")),
        "change_pct": _parse_float(row.get("CHANGE_RATE")),
        "turnover_rate": _parse_float(row.get("TURNOVERRATE")),
        "buy_amount": _parse_float(row.get("BILLBOARD_BUY_AMT")),
        "sell_amount": _parse_float(row.get("BILLBOARD_SELL_AMT")),
        "net_buy_amount": _parse_float(row.get("BILLBOARD_NET_AMT")),
        "total_amount": _parse_float(row.get("BILLBOARD_DEAL_AMT")),
        "market": _clean_text(row.get("TRADE_MARKET")),
    }


def _normalize_margin_row(row: Mapping[str, Any]) -> dict[str, Any]:
    instrument_id = _instrument_id_from_secucode(row.get("SECUCODE"), row.get("SCODE"))
    symbol, exchange = _split_instrument_id(instrument_id)
    return {
        "trade_date": _date_from_datetime(row.get("DATE")),
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "name": _clean_text(row.get("SECNAME")),
        "market": _clean_text(row.get("TRADE_MARKET")),
        "close_price": _parse_float(row.get("SPJ")),
        "change_pct": _parse_float(row.get("ZDF")),
        "margin_balance": _parse_float(row.get("RZYE")),
        "margin_buy_amount": _parse_float(row.get("RZMRE")),
        "margin_repay_amount": _parse_float(row.get("RZCHE")),
        "margin_net_buy_amount": _parse_float(row.get("RZJME")),
        "short_balance": _parse_float(row.get("RQYE")),
        "short_sell_volume": _parse_float(row.get("RQMCL")),
        "short_repay_volume": _parse_float(row.get("RQCHL")),
        "short_net_sell_volume": _parse_float(row.get("RQJMG")),
        "total_balance": _parse_float(row.get("RZRQYE")),
        "market_value": _parse_float(row.get("SZ")),
    }


def _normalize_research_report_row(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _symbol_from_code(str(row.get("stockCode") or ""))
    market = str(row.get("market") or "")
    exchange = _exchange_from_market_or_symbol(market, symbol or "")
    instrument_id = f"{symbol}.{_exchange_suffix(exchange)}" if symbol else None
    rating_change = row.get("ratingChange")
    return {
        "report_id": _clean_text(row.get("infoCode")),
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "name": _clean_text(row.get("stockName")),
        "title": _clean_text(row.get("title")),
        "publish_date": _date_from_datetime(row.get("publishDate")),
        "org_name": _clean_text(row.get("orgName")),
        "rating": _clean_text(row.get("emRatingName") or row.get("sRatingName")),
        "rating_change": str(rating_change) if rating_change not in (None, "") else None,
        "researcher": _clean_text(row.get("researcher")),
        "eps_forecast_this_year": _parse_float(row.get("predictThisYearEps")),
        "pe_forecast_this_year": _parse_float(row.get("predictThisYearPe")),
        "file_size_kb": _parse_float(row.get("attachSize")),
        "page_count": _parse_int(row.get("attachPages")),
    }


def _normalize_index_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "index_code": _clean_text(row.get("f12")),
        "index_name": _clean_text(row.get("f14")),
        "last_price": _eastmoney_scaled_price(row.get("f2"), scale=100),
        "change_pct": _eastmoney_scaled_price(row.get("f3"), scale=100),
        "change": _eastmoney_scaled_price(row.get("f4"), scale=100),
        "volume": _parse_float(row.get("f5")),
        "amount": _parse_float(row.get("f6")),
        "high": _eastmoney_scaled_price(row.get("f15"), scale=100),
        "low": _eastmoney_scaled_price(row.get("f16"), scale=100),
        "open": _eastmoney_scaled_price(row.get("f17"), scale=100),
        "pre_close": _eastmoney_scaled_price(row.get("f18"), scale=100),
    }


def _normalize_quote_row(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _symbol_from_code(str(row.get("f12") or ""))
    exchange = _exchange_from_symbol(symbol or "")
    instrument_id = f"{symbol}.{_exchange_suffix(exchange)}" if symbol else None
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange if symbol else None,
        "name": _clean_text(row.get("f14")),
        "last_price": _parse_float(row.get("f2")),
        "change_pct": _parse_float(row.get("f3")),
        "change": _parse_float(row.get("f4")),
        "volume": _parse_float(row.get("f5")),
        "amount": _parse_float(row.get("f6")),
        "amplitude": _parse_float(row.get("f7")),
        "turnover_rate": _parse_float(row.get("f8")),
        "pe_ttm": _parse_float(row.get("f9")),
        "volume_ratio": _parse_float(row.get("f10")),
        "high": _parse_float(row.get("f15")),
        "low": _parse_float(row.get("f16")),
        "open": _parse_float(row.get("f17")),
        "pre_close": _parse_float(row.get("f18")),
        "total_market_value": _parse_float(row.get("f20")),
        "float_market_value": _parse_float(row.get("f21")),
        "pb": _parse_float(row.get("f23")),
    }


def _normalize_sector_row(row: Mapping[str, Any], *, sector_type: str) -> dict[str, Any]:
    return {
        "sector_code": _clean_text(row.get("f12")),
        "sector_name": _clean_text(row.get("f14")),
        "sector_type": sector_type,
        "last_price": _parse_float(row.get("f2")),
        "change_pct": _parse_float(row.get("f3")),
        "change": _parse_float(row.get("f4")),
        "volume": _parse_float(row.get("f5")),
        "amount": _parse_float(row.get("f6")),
        "amplitude": _parse_float(row.get("f7")),
        "turnover_rate": _parse_float(row.get("f8")),
        "total_market_value": _parse_float(row.get("f20")),
        "main_inflow": _parse_float(row.get("f62")),
        "lead_stock_name": _clean_text(row.get("f128")),
        "lead_stock_symbol": _symbol_from_code(str(row.get("f140") or "")),
        "lead_stock_change_pct": _parse_float(row.get("f136")),
        "up_count": _parse_int(row.get("f104")),
        "down_count": _parse_int(row.get("f105")),
    }


def _normalize_stock_sector_belong_row(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _symbol_from_code(str(row.get("f12") or ""))
    exchange = _exchange_from_symbol(symbol or "")
    return {
        "instrument_id": f"{symbol}.{_exchange_suffix(exchange)}" if symbol else None,
        "symbol": symbol,
        "exchange": exchange if symbol else None,
        "name": _clean_text(row.get("f14")),
        "sector_name": _clean_text(row.get("f100")),
    }


def _normalize_limit_pool_row(
    row: Mapping[str, Any],
    *,
    trade_date: str,
    pool: str,
) -> dict[str, Any]:
    symbol = _symbol_from_code(str(row.get("c") or ""))
    exchange = _exchange_from_market_code(row.get("m"), symbol or "")
    zttj = row.get("zttj") if isinstance(row.get("zttj"), Mapping) else {}
    return {
        "trade_date": trade_date,
        "instrument_id": f"{symbol}.{_exchange_suffix(exchange)}" if symbol else None,
        "symbol": symbol,
        "exchange": exchange if symbol else None,
        "name": _clean_text(row.get("n")),
        "market_code": _clean_text(row.get("m")),
        "last_price": _eastmoney_scaled_price(row.get("p"), scale=1000),
        "limit_price": _eastmoney_scaled_price(row.get("ztp") or row.get("dtp"), scale=1000),
        "change_pct": _parse_float(row.get("zdp")),
        "amount": _parse_float(row.get("amount")),
        "float_market_value": _parse_float(row.get("ltsz")),
        "turnover_rate": _parse_float(row.get("hs")),
        "first_limit_time": _format_hhmmss(row.get("fbt")),
        "last_limit_time": _format_hhmmss(row.get("lbt")),
        "continuous_count": _parse_int(row.get("lbc") if pool == "up" else row.get("days")),
        "open_times": _parse_int(row.get("zbc")),
        "main_inflow": _parse_float(row.get("fund")),
        "sector": _clean_text(row.get("hybk")),
        "zt_days": _parse_int(zttj.get("days")),
        "zt_count": _parse_int(zttj.get("ct")),
    }


def _normalize_yesterday_limit_up_row(row: Mapping[str, Any], *, trade_date: str) -> dict[str, Any]:
    normalized = _normalize_limit_pool_row(row, trade_date=trade_date, pool="up")
    normalized.update(
        {
            "limit_price": _eastmoney_scaled_price(row.get("ztp"), scale=1000),
            "amplitude": _parse_float(row.get("zf")),
            "open_ratio": _parse_float(row.get("zs")),
            "yesterday_limit_time": _format_hhmmss(row.get("yfbt")),
            "yesterday_continuous_count": _parse_int(row.get("ylbc")),
        }
    )
    return normalized


def _normalize_stock_change_row(row: Mapping[str, Any], *, change_type: str) -> dict[str, Any]:
    symbol = _symbol_from_code(str(row.get("c") or ""))
    exchange = _exchange_from_market_code(row.get("m"), symbol or "")
    return {
        "instrument_id": f"{symbol}.{_exchange_suffix(exchange)}" if symbol else None,
        "symbol": symbol,
        "exchange": exchange if symbol else None,
        "name": _clean_text(row.get("n")),
        "market_code": _clean_text(row.get("m")),
        "change_time": _format_hhmmss(row.get("tm")),
        "change_pct": _parse_float(row.get("i")),
        "change_type": change_type,
        "change_type_name": _CHANGE_TYPE_NAMES.get(change_type, change_type),
    }


def _normalize_stock_change_detail_row(
    row: Mapping[str, Any],
    *,
    trade_date: str,
    instrument_id: str,
    market_code: str,
) -> dict[str, Any]:
    symbol, exchange = _split_instrument_id(instrument_id)
    change_type = str(row.get("t") or "")
    return {
        "trade_date": trade_date,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "name": None,
        "market_code": market_code,
        "change_time": _format_hhmmss(row.get("tm")),
        "change_type": change_type,
        "change_type_name": _CHANGE_TYPE_NAMES.get(change_type, change_type),
        "price": _eastmoney_scaled_price(row.get("p"), scale=100),
        "change_pct": _parse_float(row.get("u")),
        "volume": _parse_float(row.get("v")),
    }


def _normalize_date(value: Any, name: str, *, required: bool) -> str | None:
    if value in (None, ""):
        if required:
            raise SourceRequestValidationError(f"{name} is required")
        return None
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise SourceRequestValidationError(f"{name} must be YYYYMMDD or YYYY-MM-DD")
    try:
        datetime.strptime(text, "%Y%m%d")
    except ValueError as exc:
        raise SourceRequestValidationError(f"{name} must be a valid date") from exc
    return text


def _today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def _shift_yyyymmdd(value: str, *, days: int) -> str:
    parsed = datetime.strptime(value, "%Y%m%d")
    return parsed.strftime("%Y%m%d") if days == 0 else (parsed + timedelta(days=days)).strftime("%Y%m%d")


def _date_dash(value: str | None) -> str:
    if not value:
        return ""
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


def _parse_codes(value: Any, *, required: bool) -> tuple[str, ...]:
    if value in (None, ""):
        if required:
            raise SourceRequestValidationError("code is required")
        return ()
    if isinstance(value, str):
        parts = [part for part in re.split(r"[,，\s]+", value) if part]
    elif isinstance(value, (list, tuple, set)):
        parts = [str(part) for part in value if str(part).strip()]
    else:
        parts = [str(value)]
    codes: list[str] = []
    for part in parts:
        symbol = _symbol_from_code(str(part))
        if not symbol:
            raise SourceRequestValidationError("code must be a six-digit A-share code")
        normalized = f"{symbol}.{_exchange_suffix(_exchange_from_symbol(symbol))}"
        if normalized not in codes:
            codes.append(normalized)
    if required and not codes:
        raise SourceRequestValidationError("code is required")
    return tuple(codes)


def _parse_bool(value: Any, *, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    raise SourceRequestValidationError("boolean params must be true or false")


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SourceRequestValidationError(f"{name} is required")
    return text


def _date_from_datetime(value: Any) -> str | None:
    text = str(value or "").strip()
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 8:
        return digits[:8]
    return None


def _positive_int(value: Any, *, default: int, name: str) -> int:
    if value in (None, ""):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SourceRequestValidationError(f"{name} must be a positive integer") from exc
    if parsed <= 0:
        raise SourceRequestValidationError(f"{name} must be a positive integer")
    return parsed


def _symbol_from_code(value: str) -> str | None:
    text = value.strip().upper()
    if text.endswith((".SH", ".SZ", ".BJ")):
        text = text[:-3]
    if len(text) >= 8 and text[:2] in {"SH", "SZ", "BJ"}:
        text = text[2:]
    if len(text) == 6 and text.isdigit():
        return text
    return None


def _secid_from_code(value: str) -> str:
    symbol = _symbol_from_code(value)
    if not symbol:
        raise SourceRequestValidationError("code must be a six-digit A-share code")
    return f"{_eastmoney_market_from_symbol(symbol)}.{symbol}"


def _eastmoney_market_from_symbol(symbol: str) -> str:
    return "1" if _exchange_from_symbol(symbol) == "SSE" else "0"


def _instrument_id_from_secucode(secucode: Any, fallback_symbol: Any) -> str | None:
    text = str(secucode or "").strip().upper()
    if re.match(r"^\d{6}\.(SH|SZ|BJ)$", text):
        return text
    symbol = _symbol_from_code(str(fallback_symbol or ""))
    if not symbol:
        return None
    return f"{symbol}.{_exchange_suffix(_exchange_from_symbol(symbol))}"


def _split_instrument_id(instrument_id: str | None) -> tuple[str | None, str | None]:
    if not instrument_id:
        return None, None
    symbol, suffix = instrument_id.split(".", 1)
    exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}.get(suffix)
    return symbol, exchange


def _exchange_from_market_or_symbol(market: str, symbol: str) -> str:
    text = market.upper()
    if "SHANGHAI" in text:
        return "SSE"
    if "BEIJING" in text:
        return "BSE"
    if "SHENZHEN" in text:
        return "SZSE"
    return _exchange_from_symbol(symbol)


def _exchange_from_symbol(symbol: str) -> str:
    if symbol.startswith(("6", "5", "9")):
        return "SSE"
    if symbol.startswith(("4", "8", "92")):
        return "BSE"
    return "SZSE"


def _exchange_from_market_code(market_code: Any, symbol: str) -> str:
    code = str(market_code or "").strip()
    if code == "1":
        return "SSE"
    if symbol:
        return _exchange_from_symbol(symbol)
    return "SZSE"


def _exchange_suffix(exchange: str | None) -> str:
    return {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}.get(str(exchange), "SZ")


def _parse_float(value: Any) -> float | None:
    text = _clean_text(value)
    if text is None or text == "--":
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    parsed = _parse_float(value)
    return int(parsed) if parsed is not None else None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"<[^>]+>", "", str(value))
    text = re.sub(r"\s+", " ", text).strip()
    if not text or text in {"-", "--", "null", "None"}:
        return None
    return text


def _eastmoney_scaled_price(value: Any, *, scale: float) -> float | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    return round(parsed / scale, 4)


def _format_hhmmss(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    digits = re.sub(r"\D", "", text)
    if not digits:
        return None
    if len(digits) <= 6:
        return digits.zfill(6)
    return digits[-6:]


def _payload_diff_rows(payload: Any, context: str) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        raise SourceUnavailableError(f"{context} returned unexpected payload.")
    if payload.get("rc") not in (0, "0", None):
        raise SourceUnavailableError(f"{context} returned rc={payload.get('rc')}")
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return []
    rows = data.get("diff") or []
    if not isinstance(rows, list):
        raise SourceUnavailableError(f"{context} returned unexpected rows.")
    return [row for row in rows if isinstance(row, Mapping)]


def _pool_rows(payload: Any, context: str) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping) or payload.get("rc") != 0:
        raise SourceUnavailableError(f"{context} returned unexpected payload.")
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return []
    rows = data.get("pool") or []
    if not isinstance(rows, list):
        raise SourceUnavailableError(f"{context} returned unexpected rows.")
    return [row for row in rows if isinstance(row, Mapping)]
