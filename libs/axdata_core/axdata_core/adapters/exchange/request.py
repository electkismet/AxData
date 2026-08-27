"""Official exchange source request adapter."""

from __future__ import annotations

import json
from io import BytesIO
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
import re
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import zipfile
import xml.etree.ElementTree as ET

from axdata_core.source_errors import (
    SourceAdapterNotFound,
    SourceRequestValidationError,
    SourceUnavailableError,
)
from axdata_core.schema import STOCK_BASIC_FIELDS


SUPPORTED_INTERFACES = {
    "stock_trade_calendar_exchange",
    "stock_historical_list_exchange",
    "stock_basic_info_exchange",
}

SZSE_CALENDAR_URL = "https://www.szse.cn/api/report/exchange/onepersistenthour/monthList"
SSE_STOCK_LIST_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SZSE_STOCK_LIST_URL = "https://www.szse.cn/api/report/ShowReport/data"
SZSE_STOCK_LIST_XLSX_URL = "https://www.szse.cn/api/report/ShowReport"
SZSE_DELISTING_LIST_URL = "https://www.szse.cn/api/report/ShowReport/data"
BSE_STOCK_LIST_URL = "https://www.bse.cn/nqxxController/nqxxCnzq.do"

EXCHANGE_ALIASES = {
    "SH": "SSE",
    "SSE": "SSE",
    "上海": "SSE",
    "上交所": "SSE",
    "SZ": "SZSE",
    "SZSE": "SZSE",
    "深圳": "SZSE",
    "深交所": "SZSE",
    "BJ": "BSE",
    "BSE": "BSE",
    "北京": "BSE",
    "北交所": "BSE",
}

SSE_BOARD_NAMES = {
    "1": "主板",
    "2": "科创板",
    "8": "科创板",
}

SZSE_XLSX_HEADER_MAP = {
    "板块": "bk",
    "证券类别": "bk",
    "A股代码": "agdm",
    "A股证券代码": "agdm",
    "证券代码": "agdm",
    "A股简称": "agjc",
    "A股证券简称": "agjc",
    "证券简称": "agjc",
    "A股上市日期": "agssrq",
    "上市日期": "agssrq",
    "A股总股本": "agzgb",
    "总股本": "agzgb",
    "A股流通股本": "agltgb",
    "流通股本": "agltgb",
    "所属行业": "sshymc",
    "行业": "sshymc",
    "是否尚未盈利": "ylbz",
    "盈利标识": "ylbz",
    "是否具有表决权差异安排": "sfjybjqcy",
    "表决权差异安排": "sfjybjqcy",
    "公司控制架构类型": "gskzjglx",
    "协议控制架构": "gskzjglx",
}

XLSX_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
XLSX_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
XLSX_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

STOCK_BASIC_FIELD_NAMES = tuple(field.name for field in STOCK_BASIC_FIELDS)
LIFECYCLE_FIELD_NAMES = (
    "instrument_id",
    "symbol",
    "exchange",
    "name",
    "market",
    "list_date",
    "delist_date",
    "listing_status",
)


class ExchangeRequestAdapter:
    """Adapter that requests official exchange data and returns AxData fields."""

    source = "exchange"

    def __init__(self, opener: Any | None = None, *, timeout: float = 20.0) -> None:
        self._opener = opener
        self._timeout = timeout
        self.last_meta: dict[str, Any] = {}
        # 生命周期文件实例级缓存:区间补采逐日循环时只下载一次,避免反复请求被源端限流
        self._lifecycle_cache: dict[str, list[dict[str, Any]]] = {}

    def supports(self, interface_name: str) -> bool:
        return interface_name in SUPPORTED_INTERFACES

    def request(self, interface_name: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if not self.supports(interface_name):
            raise SourceAdapterNotFound(
                f"Exchange source adapter does not support interface {interface_name!r}."
            )
        if interface_name == "stock_trade_calendar_exchange":
            return self._request_stock_trade_calendar(params)
        if interface_name == "stock_historical_list_exchange":
            return self._request_stock_historical_list(params)
        if interface_name == "stock_basic_info_exchange":
            return self._request_stock_basic_info(params)
        raise SourceAdapterNotFound(
            f"Exchange source adapter does not support interface {interface_name!r}."
        )

    def _request_stock_trade_calendar(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        start_date, end_date, date_mode = _resolve_date_range(params)

        fetch_start = _add_months(_first_day_of_month(start_date), -1)
        fetch_end = _add_months(_first_day_of_month(end_date), 1)
        calendar_by_date: dict[str, bool] = {}
        fetched_months: list[str] = []

        current = fetch_start
        while current <= fetch_end:
            month = f"{current.year:04d}-{current.month:02d}"
            month_rows = self._fetch_szse_month(month)
            fetched_months.append(month)
            for row in month_rows:
                cal_date = _normalize_source_date(row.get("jyrq"))
                if cal_date is None:
                    continue
                calendar_by_date[cal_date] = str(row.get("jybz", "")).strip() == "1"
            current = _add_months(current, 1)

        rows: list[dict[str, Any]] = []
        requested_days = _date_iter(start_date, end_date)
        open_dates = sorted(day for day, is_open in calendar_by_date.items() if is_open)

        for day in requested_days:
            cal_date = _format_date(day)
            if cal_date not in calendar_by_date:
                continue
            rows.append(
                {
                    "cal_date": cal_date,
                    "is_open": calendar_by_date[cal_date],
                    "pretrade_date": _previous_open_date(open_dates, cal_date),
                    "next_trade_date": _next_open_date(open_dates, cal_date),
                }
            )

        self.last_meta = {
            "source_name": "交易所官方",
            "source_url": SZSE_CALENDAR_URL,
            "source_exchange": "SZSE",
            "date_mode": date_mode,
            "requested_start_date": _format_date(start_date),
            "requested_end_date": _format_date(end_date),
            "fetched_months": fetched_months,
        }
        return rows

    def _request_stock_historical_list(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        trade_dates, date_mode = _resolve_historical_stock_dates(params)
        exchanges = _parse_exchanges(params.get("exchange"))

        rows: list[dict[str, Any]] = []
        fetched_exchanges: list[str] = []
        for exchange in exchanges:
            if exchange == "SSE":
                rows.extend(self._fetch_sse_lifecycle_rows())
            elif exchange == "SZSE":
                rows.extend(self._fetch_szse_lifecycle_rows())
            elif exchange == "BSE":
                rows.extend(self._fetch_bse_lifecycle_rows())
            fetched_exchanges.append(exchange)

        lifecycle_rows = _dedupe_by_instrument_id(rows)
        filtered: list[dict[str, Any]] = []
        for trade_date_text in trade_dates:
            for row in lifecycle_rows:
                if not _is_active_on(row, trade_date_text):
                    continue
                item = _project_fields(row, LIFECYCLE_FIELD_NAMES)
                item["trade_date"] = trade_date_text
                filtered.append(item)
        filtered.sort(key=_historical_stock_sort_key)

        self.last_meta = {
            "source_name": "交易所官方",
            "source_exchange": ",".join(fetched_exchanges),
            "date_mode": date_mode,
            "trade_date": trade_dates[0] if len(trade_dates) == 1 else None,
            "trade_dates": trade_dates,
            "requested_start_date": trade_dates[0],
            "requested_end_date": trade_dates[-1],
            "lifecycle_rule": "list_date <= trade_date and (delist_date is empty or delist_date >= trade_date)",
            "fetched_count": len(rows),
            "filtered_count": len(filtered),
        }
        return filtered

    def _request_stock_basic_info(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        exchanges = _parse_exchanges(params.get("exchange"))
        code_filters = _parse_text_filter_values(params.get("code"))
        name_filters = _parse_text_filter_values(params.get("name"))

        rows: list[dict[str, Any]] = []
        fetched_exchanges: list[str] = []
        for exchange in exchanges:
            if exchange == "SSE":
                rows.extend(self._fetch_sse_lifecycle_rows())
            elif exchange == "SZSE":
                rows.extend(self._fetch_szse_listed_rows())
            elif exchange == "BSE":
                rows.extend(self._fetch_bse_lifecycle_rows())
            fetched_exchanges.append(exchange)

        current_rows = [
            row
            for row in _dedupe_by_instrument_id(rows)
            if row.get("listing_status") == "listed" and not row.get("delist_date")
        ]
        filtered = [
            _project_fields(row, STOCK_BASIC_FIELD_NAMES)
            for row in current_rows
            if _row_matches_code_filters(row, code_filters)
            and _row_matches_name_filters(row, name_filters)
        ]
        filtered.sort(key=_stock_basic_sort_key)

        self.last_meta = {
            "source_name": "交易所官方",
            "source_exchange": ",".join(fetched_exchanges),
            "fetched_count": len(rows),
            "filtered_count": len(filtered),
        }
        return filtered

    def _fetch_szse_month(self, month: str) -> list[dict[str, Any]]:
        query = urlencode({"month": month})
        url = f"{SZSE_CALENDAR_URL}?{query}"
        request = Request(
            url,
            headers={
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Referer": "https://www.szse.cn/aboutus/calendar/index.html",
                "User-Agent": "Mozilla/5.0 AxData/0.1",
            },
        )
        last_error: BaseException | None = None
        for attempt in range(3):
            try:
                if self._opener is not None:
                    response = self._opener(request, timeout=self._timeout)
                else:
                    response = urlopen(request, timeout=self._timeout)
                with response:
                    payload = response.read().decode("utf-8")
                break
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(0.5 * (attempt + 1))
        else:
            raise SourceUnavailableError(f"Exchange calendar request failed for {month}: {last_error}") from last_error

        try:
            data = json.loads(payload)
        except ValueError as exc:
            raise SourceUnavailableError(f"Exchange calendar returned invalid JSON for {month}") from exc

        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            for key in ("data", "result", "rows"):
                value = data.get(key)
                if isinstance(value, list):
                    return [row for row in value if isinstance(row, dict)]
        raise SourceUnavailableError(f"Exchange calendar returned unexpected payload for {month}")

    def _fetch_sse_lifecycle_rows(self) -> list[dict[str, Any]]:
        if "sse" in self._lifecycle_cache:
            return self._lifecycle_cache["sse"]
        rows: list[dict[str, Any]] = []
        for stock_type in ("1", "8"):
            page_no = 1
            page_count = 1
            while page_no <= page_count:
                payload = self._fetch_json_urlopen(
                    SSE_STOCK_LIST_URL,
                    params={
                        "isPagination": "true",
                        "pageHelp.cacheSize": "1",
                        "pageHelp.beginPage": str(page_no),
                        "pageHelp.pageSize": "500",
                        "pageHelp.pageNo": str(page_no),
                        "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
                        "STOCK_TYPE": stock_type,
                        "REG_PROVINCE": "",
                        "CSRC_CODE": "",
                        "STOCK_CODE": "",
                        "COMPANY_STATUS": "1,2,3,4,5,6,7,8",
                        "type": "inParams",
                    },
                    headers={
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                        "Referer": "https://www.sse.com.cn/assortment/stock/list/share/",
                        "User-Agent": "Mozilla/5.0 AxData/0.1",
                    },
                    context="SSE stock list",
                )
                page_help = payload.get("pageHelp", {}) if isinstance(payload, dict) else {}
                page_count = _parse_positive_int(page_help.get("pageCount"), default=1)
                page_rows = page_help.get("data") or []
                if not isinstance(page_rows, list) or not page_rows:
                    break
                for row in page_rows:
                    if isinstance(row, dict):
                        rows.append(_normalize_sse_stock_row(row))
                page_no += 1
        self._lifecycle_cache["sse"] = rows
        return rows

    def _fetch_szse_lifecycle_rows(self) -> list[dict[str, Any]]:
        if "szse" in self._lifecycle_cache:
            return self._lifecycle_cache["szse"]
        rows = self._fetch_szse_listed_rows()
        delisted_rows = self._fetch_szse_delisted_rows()
        by_id = {str(row.get("instrument_id")): row for row in rows if row.get("instrument_id")}
        for row in delisted_rows:
            instrument_id = str(row.get("instrument_id") or "")
            current = by_id.get(instrument_id)
            if current is None:
                by_id[instrument_id] = row
                continue
            if row.get("delist_date"):
                current["delist_date"] = row.get("delist_date")
                current["listing_status"] = "delisted"
        merged = list(by_id.values())
        self._lifecycle_cache["szse"] = merged
        return merged

    def _fetch_szse_listed_rows(self) -> list[dict[str, Any]]:
        if self._opener is None:
            try:
                return self._fetch_szse_listed_xlsx_rows()
            except SourceUnavailableError:
                pass

        rows: list[dict[str, Any]] = []
        page_no = 1
        page_count = 1
        while page_no <= page_count:
            payload = self._fetch_json_urlopen(
                SZSE_STOCK_LIST_URL,
                params={
                    "SHOWTYPE": "JSON",
                    "CATALOGID": "1110",
                    "TABKEY": "tab1",
                    "PAGENO": str(page_no),
                    "PAGESIZE": "500",
                    "random": f"{datetime.now().timestamp():.6f}",
                },
                headers={
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Referer": "https://www.szse.cn/market/product/stock/list/index.html",
                    "User-Agent": "Mozilla/5.0 AxData/0.1",
                },
                context="SZSE stock list",
            )
            table_payload = _first_report_table(payload)
            metadata = table_payload.get("metadata", {}) if isinstance(table_payload, Mapping) else {}
            page_count = _parse_positive_int(metadata.get("pagecount"), default=1)
            page_rows = table_payload.get("data") if isinstance(table_payload, Mapping) else []
            if not isinstance(page_rows, list) or not page_rows:
                break
            for row in page_rows:
                if isinstance(row, dict):
                    rows.append(_normalize_szse_stock_row(row))
            page_no += 1
        self._lifecycle_cache["szse"] = rows
        return rows

    def _fetch_szse_listed_xlsx_rows(self) -> list[dict[str, Any]]:
        content = self._fetch_bytes_urlopen(
            SZSE_STOCK_LIST_XLSX_URL,
            params={
                "SHOWTYPE": "xlsx",
                "CATALOGID": "1110",
                "TABKEY": "tab1",
                "random": f"{datetime.now().timestamp():.6f}",
            },
            headers={
                "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*;q=0.8",
                "Referer": "https://www.szse.cn/market/product/stock/list/index.html",
                "User-Agent": "Mozilla/5.0 AxData/0.1",
            },
            context="SZSE stock list xlsx",
        )
        return [_normalize_szse_stock_row(row) for row in _parse_szse_xlsx_rows(content)]

    def _fetch_szse_delisted_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page_no = 1
        page_count = 1
        while page_no <= page_count:
            payload = self._fetch_json_urlopen(
                SZSE_DELISTING_LIST_URL,
                params={
                    "SHOWTYPE": "JSON",
                    "CATALOGID": "1793_ssgs",
                    "TABKEY": "tab2",
                    "PAGENO": str(page_no),
                    "PAGESIZE": "100",
                    "random": f"{datetime.now().timestamp():.6f}",
                },
                headers={
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Referer": "https://www.szse.cn/market/stock/suspend/index.html",
                    "User-Agent": "Mozilla/5.0 AxData/0.1",
                },
                context="SZSE delisting list",
            )
            table_payload = _report_table_by_name(payload, "终止上市")
            metadata = table_payload.get("metadata", {}) if isinstance(table_payload, Mapping) else {}
            page_count = _parse_positive_int(metadata.get("pagecount"), default=1)
            page_rows = table_payload.get("data") if isinstance(table_payload, Mapping) else []
            if not isinstance(page_rows, list) or not page_rows:
                break
            for row in page_rows:
                if isinstance(row, dict):
                    rows.append(_normalize_szse_delisted_row(row))
            page_no += 1
        return rows

    def _fetch_bse_lifecycle_rows(self) -> list[dict[str, Any]]:
        if "bse" in self._lifecycle_cache:
            return self._lifecycle_cache["bse"]
        rows: list[dict[str, Any]] = []
        page_no = 0
        total_pages = 1
        while page_no < total_pages:
            payload = self._fetch_text_urlopen(
                BSE_STOCK_LIST_URL,
                params=None,
                data={
                    "page": str(page_no),
                    "typejb": "T",
                    "xxfcbj[]": "2",
                    "xxzqdm": "",
                    "sortfield": "xxzqdm",
                    "sorttype": "asc",
                },
                headers={
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Referer": "https://www.bse.cn/nq/listedcompany.html",
                    "User-Agent": "Mozilla/5.0 AxData/0.1",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                context="BSE stock list",
            )
            payload_json = _parse_jsonp_array(payload)
            table_payload = _first_report_table(payload_json)
            total_pages = _parse_positive_int(table_payload.get("totalPages"), default=1)
            page_rows = table_payload.get("content") if isinstance(table_payload, Mapping) else []
            if not isinstance(page_rows, list) or not page_rows:
                break
            for row in page_rows:
                if isinstance(row, dict):
                    rows.append(_normalize_bse_stock_row(row))
            page_no += 1
        self._lifecycle_cache["bse"] = rows
        return rows

    def _fetch_json_urlopen(
        self,
        url: str,
        *,
        params: Mapping[str, str],
        headers: Mapping[str, str],
        context: str,
    ) -> Any:
        payload = self._fetch_text_urlopen(url, params=params, headers=headers, context=context)
        try:
            return json.loads(payload)
        except ValueError as exc:
            raise SourceUnavailableError(f"{context} returned invalid JSON") from exc

    def _fetch_text_urlopen(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None,
        headers: Mapping[str, str],
        context: str,
        data: Mapping[str, str] | None = None,
    ) -> str:
        if self._opener is None:
            try:
                import requests
            except ImportError:
                requests = None

            if requests is not None:
                attempts = 3
                last_error: Exception | None = None
                for attempt in range(attempts):
                    try:
                        if data is None:
                            response = requests.get(
                                url,
                                params=dict(params or {}),
                                headers=dict(headers),
                                timeout=self._timeout,
                            )
                        else:
                            response = requests.post(
                                url,
                                data=dict(data),
                                headers=dict(headers),
                                timeout=self._timeout,
                            )
                        response.raise_for_status()
                        return response.text
                    except requests.RequestException as exc:
                        last_error = exc
                        if attempt + 1 < attempts:
                            time.sleep(0.5 * (attempt + 1))
                raise SourceUnavailableError(f"{context} request failed: {last_error}") from last_error

        request_url = url if params is None else f"{url}?{urlencode(params)}"
        request_body = None if data is None else urlencode(data).encode("utf-8")
        request = Request(request_url, data=request_body, headers=dict(headers), method="POST" if data else "GET")
        try:
            if self._opener is not None:
                response = self._opener(request, timeout=self._timeout)
            else:
                response = urlopen(request, timeout=self._timeout)
            with response:
                return response.read().decode("utf-8")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"{context} request failed: {exc}") from exc

    def _fetch_bytes_urlopen(
        self,
        url: str,
        *,
        params: Mapping[str, str],
        headers: Mapping[str, str],
        context: str,
    ) -> bytes:
        if self._opener is None:
            try:
                import requests
            except ImportError:
                requests = None

            if requests is not None:
                attempts = 3
                last_error: Exception | None = None
                for attempt in range(attempts):
                    try:
                        response = requests.get(
                            url,
                            params=dict(params),
                            headers=dict(headers),
                            timeout=self._timeout,
                        )
                        response.raise_for_status()
                        return response.content
                    except requests.RequestException as exc:
                        last_error = exc
                        if attempt + 1 < attempts:
                            time.sleep(0.5 * (attempt + 1))
                raise SourceUnavailableError(f"{context} request failed: {last_error}") from last_error

        request = Request(f"{url}?{urlencode(params)}", headers=dict(headers))
        try:
            if self._opener is not None:
                response = self._opener(request, timeout=self._timeout)
            else:
                response = urlopen(request, timeout=self._timeout)
            with response:
                return response.read()
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"{context} request failed: {exc}") from exc


def _resolve_date_range(params: Mapping[str, Any]) -> tuple[date, date, str]:
    start_raw = params.get("start_date")
    end_raw = params.get("end_date")
    if start_raw not in (None, "") or end_raw not in (None, ""):
        start = _parse_date(start_raw if start_raw not in (None, "") else end_raw, "start_date")
        end = _parse_date(end_raw if end_raw not in (None, "") else start_raw, "end_date")
        if start > end:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        return start, end, "range"

    year_raw = params.get("year")
    if year_raw not in (None, ""):
        year = _parse_year(year_raw)
        return date(year, 1, 1), date(year, 12, 31), "year"

    today = datetime.now().astimezone().date()
    return date(today.year, 1, 1), date(today.year, 12, 31), "current_year"


def _last_trade_date() -> date:
    """最近一个已收盘的交易日:粗略按工作日回退,周末回退到周五。"""
    today = date.today()
    offset = {0: 3, 6: 2}.get(today.weekday(), 1)
    return today - timedelta(days=offset)


def _resolve_historical_stock_dates(params: Mapping[str, Any]) -> tuple[list[str], str]:
    trade_date_raw = params.get("trade_date")
    start_raw = params.get("start_date")
    end_raw = params.get("end_date")
    has_trade_date = trade_date_raw not in (None, "")
    has_range = start_raw not in (None, "") or end_raw not in (None, "")

    if has_trade_date and has_range:
        raise SourceRequestValidationError("Use either trade_date or start_date/end_date, not both")

    if has_trade_date:
        dates = _parse_date_list(trade_date_raw, "trade_date")
        return dates, "trade_date_list" if len(dates) > 1 else "trade_date"

    if has_range:
        start = _parse_date(start_raw if start_raw not in (None, "") else end_raw, "start_date")
        end = _parse_date(end_raw if end_raw not in (None, "") else start_raw, "end_date")
        if start > end:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        return [_format_date(day) for day in _date_iter(start, end)], "range"

    # 未指定日期时默认最新交易日(与全库统一规则一致)
    return [_format_date(_last_trade_date())], "latest"


def _parse_date_list(value: Any, name: str) -> list[str]:
    if isinstance(value, str):
        parts: Sequence[Any] = [part for part in re.split(r"[,，\s]+", value) if part]
    elif isinstance(value, Sequence):
        parts = value
    else:
        parts = (value,)

    dates: list[str] = []
    for part in parts:
        parsed = _format_date(_parse_date(part, name))
        if parsed not in dates:
            dates.append(parsed)

    if not dates:
        raise SourceRequestValidationError(f"{name} must include at least one date")
    return sorted(dates)


def _parse_exchanges(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        parts: Sequence[Any] = ("SSE", "SZSE", "BSE")
    elif isinstance(value, str):
        parts = [part for part in re.split(r"[,，\s]+", value) if part]
    elif isinstance(value, Sequence):
        parts = value
    else:
        parts = (value,)

    exchanges: list[str] = []
    for part in parts:
        text = str(part).strip()
        if not text:
            continue
        normalized = EXCHANGE_ALIASES.get(text.upper(), EXCHANGE_ALIASES.get(text))
        if normalized is None:
            raise SourceRequestValidationError(
                f"exchange must be one of SSE, SZSE, or BSE. Got {text!r}."
            )
        if normalized not in exchanges:
            exchanges.append(normalized)
    return tuple(exchanges or ("SSE", "SZSE", "BSE"))


def _parse_text_filter_values(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, str):
        parts: Sequence[Any] = [part for part in re.split(r"[,，\s]+", value) if part]
    elif isinstance(value, Sequence):
        parts = value
    else:
        parts = (value,)

    values: list[str] = []
    for part in parts:
        text = str(part).strip()
        if text and text not in values:
            values.append(text)
    return tuple(values)


def _normalize_code_filter(value: str) -> tuple[str | None, str | None]:
    text = value.strip()
    if not text:
        return None, None
    upper = text.upper()
    if upper.endswith(".SH"):
        return upper[:-3], "SSE"
    if upper.endswith(".SZ"):
        return upper[:-3], "SZSE"
    if upper.endswith(".BJ"):
        return upper[:-3], "BSE"
    if len(upper) >= 3 and upper[:2] in {"SH", "SZ", "BJ"}:
        exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}[upper[:2]]
        return upper[2:], exchange
    return text, None


def _row_matches_code_filters(row: Mapping[str, Any], filters: Sequence[str]) -> bool:
    if not filters:
        return True

    row_symbol = str(row.get("symbol") or "").strip()
    row_exchange = str(row.get("exchange") or "").strip()
    row_instrument_id = str(row.get("instrument_id") or "").strip().upper()
    for value in filters:
        symbol, exchange = _normalize_code_filter(value)
        if not symbol:
            continue
        if exchange is not None:
            if row_symbol == symbol and row_exchange == exchange:
                return True
            if row_instrument_id == f"{symbol}.{_exchange_suffix(exchange)}":
                return True
            continue
        if row_symbol == symbol or row_instrument_id == symbol.upper():
            return True
    return False


def _row_matches_name_filters(row: Mapping[str, Any], filters: Sequence[str]) -> bool:
    if not filters:
        return True
    return str(row.get("name") or "") in set(filters)


def _exchange_suffix(exchange: str) -> str:
    return {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}.get(exchange, exchange)


def _parse_year(value: Any) -> int:
    try:
        year = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise SourceRequestValidationError("year must be a four-digit year") from exc
    if year < 1900 or year > 2100:
        raise SourceRequestValidationError("year must be between 1900 and 2100")
    return year


def _parse_date(value: Any, name: str) -> date:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise SourceRequestValidationError(f"{name} must be YYYYMMDD or YYYY-MM-DD")
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError as exc:
        raise SourceRequestValidationError(f"{name} must be a valid date") from exc


def _normalize_source_date(value: Any) -> str | None:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        return None
    return text


def _normalize_any_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text in {"-", "--", "N/A", "n/a", "null", "None"}:
        return None
    digits = re.sub(r"\D", "", text)
    if len(digits) < 8:
        return None
    return digits[:8]


def _format_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def _parse_positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _first_report_table(payload: Any) -> Mapping[str, Any]:
    if isinstance(payload, list) and payload:
        first = payload[0]
        if isinstance(first, Mapping):
            return first
    if isinstance(payload, Mapping):
        return payload
    raise SourceUnavailableError("Exchange source returned unexpected table payload.")


def _report_table_by_name(payload: Any, name: str) -> Mapping[str, Any]:
    if isinstance(payload, list):
        for table in payload:
            if not isinstance(table, Mapping):
                continue
            metadata = table.get("metadata")
            if isinstance(metadata, Mapping) and metadata.get("name") == name:
                return table
    raise SourceUnavailableError(f"Exchange source returned no {name} table.")


def _parse_jsonp_array(text: str) -> Any:
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end < start:
        raise SourceUnavailableError("Exchange source returned unexpected JSONP payload.")
    try:
        return json.loads(text[start : end + 1])
    except ValueError as exc:
        raise SourceUnavailableError("Exchange source returned invalid JSONP payload.") from exc


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"<[^>]+>", "", str(value))
    text = re.sub(r"\s+", " ", text).strip()
    if not text or text in {"-", "--", "N/A", "n/a", "null", "None"}:
        return None
    return text


def _blank_to_none(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"<[^>]+>", "", str(value))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _parse_float(value: Any) -> float | None:
    text = _clean_text(value)
    if text is None:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _parse_share_count_to_100m(value: Any) -> float | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    return parsed / 100_000_000


def _parse_share_value_to_100m(value: Any) -> float | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    if parsed > 10_000:
        return parsed / 100_000_000
    return parsed


def _parse_szse_xlsx_rows(content: bytes) -> list[dict[str, str]]:
    try:
        with zipfile.ZipFile(BytesIO(content)) as archive:
            shared_strings = _read_xlsx_shared_strings(archive)
            sheet_xml = archive.read(_first_xlsx_sheet_path(archive))
            sheet_root = ET.fromstring(sheet_xml)
    except (KeyError, ET.ParseError, zipfile.BadZipFile) as exc:
        raise SourceUnavailableError("SZSE stock list xlsx returned unexpected format.") from exc

    rows = _read_xlsx_rows(sheet_root, shared_strings)
    header_map: dict[int, str] = {}
    data_rows: list[dict[str, str]] = []

    for values in rows:
        if not values:
            continue
        if not header_map:
            header_map = {
                index: field_name
                for index, value in enumerate(values)
                if (field_name := SZSE_XLSX_HEADER_MAP.get(re.sub(r"\s+", "", value)))
            }
            if {"agdm", "agjc"}.issubset(set(header_map.values())):
                continue
            header_map = {}
            continue

        row = {
            field_name: values[index]
            for index, field_name in header_map.items()
            if index < len(values) and values[index] != ""
        }
        symbol = _clean_text(row.get("agdm"))
        if symbol and symbol.isdigit():
            data_rows.append(row)

    if not data_rows:
        raise SourceUnavailableError("SZSE stock list xlsx returned no stock rows.")
    return data_rows


def _read_xlsx_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root.findall(f"{{{XLSX_MAIN_NS}}}si"):
        parts = [
            text_node.text or ""
            for text_node in item.findall(f".//{{{XLSX_MAIN_NS}}}t")
        ]
        strings.append("".join(parts))
    return strings


def _first_xlsx_sheet_path(archive: zipfile.ZipFile) -> str:
    fallback = "xl/worksheets/sheet1.xml"
    if "xl/workbook.xml" not in archive.namelist():
        return fallback

    workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
    first_sheet = workbook_root.find(f".//{{{XLSX_MAIN_NS}}}sheet")
    relationship_id = None if first_sheet is None else first_sheet.get(f"{{{XLSX_REL_NS}}}id")
    if not relationship_id or "xl/_rels/workbook.xml.rels" not in archive.namelist():
        return fallback

    rel_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    for relationship in rel_root.findall(f"{{{XLSX_PACKAGE_REL_NS}}}Relationship"):
        if relationship.get("Id") != relationship_id:
            continue
        target = relationship.get("Target", "")
        if target.startswith("/"):
            return target.lstrip("/")
        return f"xl/{target}"
    return fallback


def _read_xlsx_rows(root: ET.Element, shared_strings: Sequence[str]) -> list[list[str]]:
    sheet_data = root.find(f".//{{{XLSX_MAIN_NS}}}sheetData")
    if sheet_data is None:
        return []

    rows: list[list[str]] = []
    for row_node in sheet_data.findall(f"{{{XLSX_MAIN_NS}}}row"):
        row_values: list[str] = []
        next_index = 0
        for cell in row_node.findall(f"{{{XLSX_MAIN_NS}}}c"):
            column_index = _xlsx_column_index(cell.get("r")) or next_index
            while len(row_values) < column_index:
                row_values.append("")
            row_values.append(_xlsx_cell_text(cell, shared_strings))
            next_index = column_index + 1
        while row_values and row_values[-1] == "":
            row_values.pop()
        rows.append(row_values)
    return rows


def _xlsx_cell_text(cell: ET.Element, shared_strings: Sequence[str]) -> str:
    cell_type = cell.get("t")
    if cell_type == "inlineStr":
        parts = [
            text_node.text or ""
            for text_node in cell.findall(f".//{{{XLSX_MAIN_NS}}}t")
        ]
        return re.sub(r"\s+", " ", "".join(parts)).strip()

    value_node = cell.find(f"{{{XLSX_MAIN_NS}}}v")
    if value_node is None or value_node.text is None:
        return ""

    value = value_node.text
    if cell_type == "s":
        try:
            return shared_strings[int(value)]
        except (IndexError, ValueError):
            return ""
    return re.sub(r"\s+", " ", value).strip()


def _xlsx_column_index(reference: str | None) -> int | None:
    if not reference:
        return None
    match = re.match(r"([A-Z]+)", reference)
    if match is None:
        return None
    index = 0
    for char in match.group(1):
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def _normalize_sse_stock_row(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _clean_text(row.get("A_STOCK_CODE"))
    delist_date = _normalize_any_date(row.get("DELIST_DATE"))
    market_code = _clean_text(row.get("LIST_BOARD"))
    return _project_fields({
        "instrument_id": f"{symbol}.SH" if symbol else None,
        "symbol": symbol,
        "exchange": "SSE",
        "asset_type": "stock",
        "name": _clean_text(row.get("SEC_NAME_CN")),
        "security_full_name": _clean_text(row.get("SEC_NAME_FULL")),
        "market_code": market_code,
        "market": SSE_BOARD_NAMES.get(str(market_code), market_code),
        "industry_code": _clean_text(row.get("CSRC_CODE")),
        "industry": _clean_text(row.get("CSRC_CODE_DESC")),
        "region_code": _clean_text(row.get("AREA_NAME")),
        "region": _clean_text(row.get("AREA_NAME_DESC")),
        "company_code": _clean_text(row.get("COMPANY_CODE")),
        "company_short_name": _clean_text(row.get("COMPANY_ABBR")),
        "company_full_name": _clean_text(row.get("FULL_NAME")),
        "company_short_name_en": _clean_text(row.get("COMPANY_ABBR_EN")),
        "company_full_name_en": _clean_text(row.get("FULL_NAME_IN_ENGLISH")),
        "listing_status": "delisted" if delist_date else "listed",
        "list_date": _normalize_any_date(row.get("LIST_DATE")),
        "delist_date": delist_date,
        "total_share": None,
        "float_share": None,
        "is_profit": None,
        "is_vie": None,
        "has_weighted_voting_rights": None,
        "sponsor": None,
        "share_report_date": None,
    }, STOCK_BASIC_FIELD_NAMES)


def _normalize_szse_stock_row(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _clean_text(row.get("agdm"))
    return _project_fields({
        "instrument_id": f"{symbol}.SZ" if symbol else None,
        "symbol": symbol,
        "exchange": "SZSE",
        "asset_type": "stock",
        "name": _clean_text(row.get("agjc")),
        "security_full_name": None,
        "market_code": None,
        "market": _clean_text(row.get("bk")),
        "industry_code": None,
        "industry": _clean_text(row.get("sshymc")),
        "region_code": None,
        "region": None,
        "company_code": None,
        "company_short_name": None,
        "company_full_name": None,
        "company_short_name_en": None,
        "company_full_name_en": None,
        "listing_status": "listed",
        "list_date": _normalize_any_date(row.get("agssrq")),
        "delist_date": None,
        "total_share": _parse_share_value_to_100m(row.get("agzgb")),
        "float_share": _parse_share_value_to_100m(row.get("agltgb")),
        "is_profit": _blank_to_none(row.get("ylbz")),
        "is_vie": _blank_to_none(row.get("gskzjglx")),
        "has_weighted_voting_rights": _blank_to_none(row.get("sfjybjqcy")),
        "sponsor": None,
        "share_report_date": None,
    }, STOCK_BASIC_FIELD_NAMES)


def _normalize_szse_delisted_row(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _clean_text(row.get("zqdm"))
    return _project_fields({
        "instrument_id": f"{symbol}.SZ" if symbol else None,
        "symbol": symbol,
        "exchange": "SZSE",
        "asset_type": "stock",
        "name": _clean_text(row.get("zqjc")),
        "security_full_name": None,
        "market_code": None,
        "market": None,
        "industry_code": None,
        "industry": None,
        "region_code": None,
        "region": None,
        "company_code": None,
        "company_short_name": None,
        "company_full_name": None,
        "company_short_name_en": None,
        "company_full_name_en": None,
        "listing_status": "delisted",
        "list_date": _normalize_any_date(row.get("ssrq")),
        "delist_date": _normalize_any_date(row.get("zzrq")),
        "total_share": None,
        "float_share": None,
        "is_profit": None,
        "is_vie": None,
        "has_weighted_voting_rights": None,
        "sponsor": None,
        "share_report_date": None,
    }, STOCK_BASIC_FIELD_NAMES)


def _normalize_bse_stock_row(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = _clean_text(row.get("xxzqdm"))
    delist_date = _normalize_any_date(row.get("xxzzrq") or row.get("delist_date"))
    return _project_fields({
        "instrument_id": f"{symbol}.BJ" if symbol else None,
        "symbol": symbol,
        "exchange": "BSE",
        "asset_type": "stock",
        "name": _clean_text(row.get("xxzqjc")),
        "security_full_name": None,
        "market_code": _clean_text(row.get("xxfcbj")),
        "market": "北交所",
        "industry_code": None,
        "industry": _clean_text(row.get("xxhyzl")),
        "region_code": None,
        "region": _clean_text(row.get("xxssdq")),
        "company_code": None,
        "company_short_name": None,
        "company_full_name": None,
        "company_short_name_en": _clean_text(row.get("xxywjc")),
        "company_full_name_en": None,
        "listing_status": "delisted" if delist_date else "listed",
        "list_date": _normalize_any_date(row.get("xxgprq") or row.get("fxssrq")),
        "delist_date": delist_date,
        "total_share": _parse_share_count_to_100m(row.get("xxzgb")),
        "float_share": _parse_share_count_to_100m(row.get("xxfxsgb")),
        "is_profit": None,
        "is_vie": None,
        "has_weighted_voting_rights": None,
        "sponsor": _clean_text(row.get("xxzbqs")),
        "share_report_date": _normalize_any_date(row.get("xxjsrq")),
    }, STOCK_BASIC_FIELD_NAMES)


def _dedupe_by_instrument_id(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        instrument_id = item.get("instrument_id")
        if not instrument_id:
            continue
        key = str(instrument_id)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _project_fields(row: Mapping[str, Any], field_names: Sequence[str]) -> dict[str, Any]:
    return {field_name: row.get(field_name) for field_name in field_names}


def _is_active_on(row: Mapping[str, Any], trade_date: str) -> bool:
    list_date = _normalize_any_date(row.get("list_date"))
    delist_date = _normalize_any_date(row.get("delist_date"))
    if list_date and list_date > trade_date:
        return False
    if delist_date and delist_date < trade_date:
        return False
    return True


def _historical_stock_sort_key(row: Mapping[str, Any]) -> tuple[str, int, str]:
    exchange_order = {"SSE": 0, "SZSE": 1, "BSE": 2}
    return (
        str(row.get("trade_date") or ""),
        exchange_order.get(str(row.get("exchange") or ""), 99),
        str(row.get("symbol") or ""),
    )


def _stock_basic_sort_key(row: Mapping[str, Any]) -> tuple[int, str]:
    exchange_order = {"SSE": 0, "SZSE": 1, "BSE": 2}
    return (
        exchange_order.get(str(row.get("exchange") or ""), 99),
        str(row.get("symbol") or ""),
    )


def _date_iter(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _first_day_of_month(value: date) -> date:
    return date(value.year, value.month, 1)


def _add_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 + months
    return date(month_index // 12, month_index % 12 + 1, 1)


def _previous_open_date(open_dates: Sequence[str], cal_date: str) -> str | None:
    previous = [day for day in open_dates if day < cal_date]
    return previous[-1] if previous else None


def _next_open_date(open_dates: Sequence[str], cal_date: str) -> str | None:
    for day in open_dates:
        if day > cal_date:
            return day
    return None
