"""Tencent Finance source request adapter."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from axdata_core.source_errors import (
    SourceAdapterNotFound,
    SourceRequestValidationError,
    SourceUnavailableError,
)


SUPPORTED_INTERFACES = {
    "tencent_realtime_snapshot",
    "stock_zh_a_spot_tx",
    "stock_zh_a_hist_tx",
    "stock_zh_a_tick_tx_js",
    "stock_zh_index_daily_tx",
    "get_tx_start_year",
}
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q={codes}"
TENCENT_BOARD_RANK_URL = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList"
TENCENT_KLINE_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
TENCENT_TICK_URL = "http://stock.gtimg.cn/data/index.php"
TENCENT_START_YEAR_URL = "https://web.ifzq.gtimg.cn/other/klineweb/klineWeb/weekTrends"


class TencentRequestAdapter:
    """Request Tencent quote snapshots and return AxData fields."""

    source = "tencent"

    def __init__(self, opener: Any | None = None, *, timeout: float = 15.0) -> None:
        self._opener = opener
        self._timeout = timeout
        self.last_meta: dict[str, Any] = {}

    def supports(self, interface_name: str) -> bool:
        return interface_name in SUPPORTED_INTERFACES

    def request(self, interface_name: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if interface_name == "stock_zh_a_spot_tx":
            return self._request_stock_spot(params)
        if interface_name == "stock_zh_a_hist_tx":
            return self._request_daily_kline(params, asset_type="stock")
        if interface_name == "stock_zh_a_tick_tx_js":
            return self._request_stock_tick(params)
        if interface_name == "stock_zh_index_daily_tx":
            return self._request_daily_kline(params, asset_type="index", default_adjust="qfq")
        if interface_name == "get_tx_start_year":
            return self._request_start_year(params)
        if interface_name != "tencent_realtime_snapshot":
            raise SourceAdapterNotFound(
                f"Tencent source adapter does not support interface {interface_name!r}."
            )
        codes = _parse_code_values(params.get("code"))
        quote_codes = [_to_tencent_quote_code(code) for code in codes]
        text = self._fetch_quotes(quote_codes)
        rows = _parse_quote_payload(text)
        self.last_meta = {
            "source_name": "腾讯财经",
            "source_url": TENCENT_QUOTE_URL.format(codes=",".join(quote_codes)),
            "requested_codes": codes,
            "empty_codes": [
                code for code, quote_code in zip(codes, quote_codes) if quote_code not in rows
            ],
        }
        return [rows[quote_code] for quote_code in quote_codes if quote_code in rows]

    def _request_stock_spot(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        sort_type = _enum_value(
            params.get("sort_type"),
            default="price",
            allowed={"price", "change_pct", "volume", "amount"},
            name="sort_type",
        )
        direction = _enum_value(
            params.get("direction"),
            default="down",
            allowed={"down", "up"},
            name="direction",
        )
        offset = _non_negative_int(params.get("offset"), default=0, name="offset")
        limit = min(_positive_int(params.get("limit"), default=20, name="limit"), 200)
        payload = self._fetch_board_rank(
            sort_type=_tencent_spot_sort_key(sort_type),
            direction=direction,
            offset=offset,
            limit=limit,
        )
        rows_payload = payload.get("data", {}).get("rank_list") if isinstance(payload, Mapping) else None
        if not isinstance(rows_payload, list):
            raise SourceUnavailableError("Tencent board rank list returned unexpected payload.")
        rows = [
            _normalize_board_rank_row(item)
            for item in rows_payload
            if isinstance(item, Mapping)
        ]
        rows = [row for row in rows if row is not None]
        self.last_meta = {
            "source_name": "腾讯财经",
            "source_url": TENCENT_BOARD_RANK_URL,
            "board_code": "aStock",
            "sort_type": sort_type,
            "direction": direction,
            "offset": offset,
            "limit": limit,
        }
        return rows

    def _request_stock_tick(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        quote_code = _to_tencent_quote_code(str(params.get("code") or ""))
        page = _non_negative_int(params.get("page"), default=0, name="page")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 500)
        text = self._fetch_tick_page(quote_code, page=page)
        rows = _normalize_tick_rows(text, quote_code=quote_code)
        self.last_meta = {
            "source_name": "腾讯财经",
            "source_url": TENCENT_TICK_URL,
            "requested_code": params.get("code"),
            "quote_code": quote_code,
            "page": page,
            "limit": limit,
        }
        return rows[:limit]

    def _request_start_year(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        quote_code = _to_tencent_quote_code(str(params.get("code") or ""))
        payload = self._fetch_start_year_payload(quote_code)
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            payload = self._fetch_start_year_fallback_payload(quote_code)
            data = payload.get("data", {}).get(quote_code, {}).get("day") if isinstance(payload.get("data"), Mapping) else None
        if not isinstance(data, list) or not data:
            rows: list[dict[str, Any]] = []
        else:
            first = data[0]
            rows = [_normalize_start_year_row(first, quote_code=quote_code)] if isinstance(first, Sequence) else []
        self.last_meta = {
            "source_name": "腾讯财经",
            "source_url": TENCENT_START_YEAR_URL,
            "requested_code": params.get("code"),
            "quote_code": quote_code,
        }
        return [row for row in rows if row is not None]

    def _request_daily_kline(
        self,
        params: Mapping[str, Any],
        *,
        asset_type: str,
        default_adjust: str = "none",
    ) -> list[dict[str, Any]]:
        quote_code = _to_tencent_quote_code(str(params.get("code") or ""))
        explicit_range = params.get("start_date") not in (None, "") or params.get("end_date") not in (None, "")
        start_date = _normalize_date(params.get("start_date"), default=date.today().strftime("%Y%m%d"), name="start_date")
        end_date = _normalize_date(params.get("end_date"), default=start_date, name="end_date")
        if start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        adjust = _enum_value(
            params.get("adjust"),
            default=default_adjust,
            allowed={"none", "qfq", "hfq"},
            name="adjust",
        )
        limit = min(_positive_int(params.get("limit"), default=640, name="limit"), 640)
        source_adjust = "" if adjust == "none" else adjust

        rows: list[dict[str, Any]] = []
        for year in range(int(start_date[:4]), int(end_date[:4]) + 1):
            payload = self._fetch_kline_payload(quote_code, year=year, adjust=source_adjust)
            rows.extend(
                _normalize_kline_rows(
                    payload,
                    quote_code=quote_code,
                    adjust=adjust,
                    asset_type=asset_type,
                )
            )
        if explicit_range:
            rows = [
                row
                for row in rows
                if row["trade_date"] >= start_date and row["trade_date"] <= end_date
            ]
        rows.sort(key=lambda row: row["trade_date"])
        if not explicit_range:
            # 未指定日期时返回最新 limit 根 K 线,而非"今天"这个空区间
            rows = rows[-limit:] if limit else rows
        self.last_meta = {
            "source_name": "腾讯财经",
            "source_url": TENCENT_KLINE_URL,
            "requested_code": params.get("code"),
            "quote_code": quote_code,
            "start_date": start_date,
            "end_date": end_date,
            "adjust": adjust,
            "limit": limit,
        }
        return rows[:limit]

    def _fetch_quotes(self, quote_codes: Sequence[str]) -> str:
        url = TENCENT_QUOTE_URL.format(codes=quote(",".join(quote_codes), safe=","))
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                "Accept": "*/*",
                "Referer": "https://gu.qq.com/",
            },
        )
        try:
            response = self._open(request)
            with response:
                return response.read().decode("gbk", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"Tencent quote request failed: {exc}") from exc

    def _fetch_board_rank(
        self,
        *,
        sort_type: str,
        direction: str,
        offset: int,
        limit: int,
    ) -> Mapping[str, Any]:
        params = {
            "_appver": "11.17.0",
            "board_code": "aStock",
            "sort_type": sort_type,
            "direct": direction,
            "offset": str(offset),
            "count": str(limit),
        }
        url = f"{TENCENT_BOARD_RANK_URL}?{urlencode(params)}"
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                "Accept": "application/json,*/*",
                "Referer": "https://stockapp.finance.qq.com/",
            },
        )
        try:
            response = self._open(request)
            with response:
                text = response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"Tencent board rank list request failed: {exc}") from exc
        try:
            payload = json.loads(text)
        except ValueError as exc:
            raise SourceUnavailableError("Tencent board rank list returned invalid JSON.") from exc
        if payload.get("code") not in (0, "0", None):
            raise SourceUnavailableError(
                f"Tencent board rank list returned error: {payload.get('msg') or payload.get('code')}"
            )
        return payload

    def _fetch_kline_payload(self, quote_code: str, *, year: int, adjust: str) -> Mapping[str, Any]:
        variable = f"kline_day{adjust}{year}"
        params = {
            "_var": variable,
            "param": f"{quote_code},day,{year}-01-01,{year + 1}-12-31,640,{adjust}",
            "r": "0.8205512681390605",
        }
        url = f"{TENCENT_KLINE_URL}?{urlencode(params)}"
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                "Accept": "application/json,text/javascript,*/*",
                "Referer": "https://gu.qq.com/",
            },
        )
        try:
            response = self._open(request)
            with response:
                text = response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"Tencent kline request failed: {exc}") from exc
        json_text = text[text.find("={") + 1 :] if "={" in text else text
        try:
            payload = json.loads(json_text)
        except ValueError as exc:
            raise SourceUnavailableError("Tencent kline returned invalid JSON.") from exc
        if payload.get("code") not in (0, "0", None):
            raise SourceUnavailableError(
                f"Tencent kline returned error: {payload.get('msg') or payload.get('code')}"
            )
        return payload

    def _fetch_tick_page(self, quote_code: str, *, page: int) -> str:
        params = {
            "appn": "detail",
            "action": "data",
            "c": quote_code,
            "p": str(page),
        }
        url = f"{TENCENT_TICK_URL}?{urlencode(params)}"
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                "Accept": "text/javascript,*/*",
                "Referer": "https://gu.qq.com/",
            },
        )
        try:
            response = self._open(request)
            with response:
                return response.read().decode("gbk", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"Tencent tick detail request failed: {exc}") from exc

    def _fetch_start_year_payload(self, quote_code: str) -> Mapping[str, Any]:
        params = {
            "code": quote_code,
            "type": "qfq",
            "_var": "trend_qfq",
            "r": "0.3506048543943414",
        }
        return self._fetch_wrapped_json(
            TENCENT_START_YEAR_URL,
            params=params,
            context="Tencent start-year helper",
        )

    def _fetch_start_year_fallback_payload(self, quote_code: str) -> Mapping[str, Any]:
        params = {
            "_var": "kline_dayqfq",
            "param": f"{quote_code},day,,,320,qfq",
            "r": "0.751892490072597",
        }
        return self._fetch_wrapped_json(
            TENCENT_KLINE_URL,
            params=params,
            context="Tencent start-year fallback",
        )

    def _fetch_wrapped_json(
        self,
        url: str,
        *,
        params: Mapping[str, str],
        context: str,
    ) -> Mapping[str, Any]:
        request = Request(
            f"{url}?{urlencode(params)}",
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                "Accept": "application/json,text/javascript,*/*",
                "Referer": "https://gu.qq.com/",
            },
        )
        try:
            response = self._open(request)
            with response:
                text = response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"{context} request failed: {exc}") from exc
        json_text = text[text.find("={") + 1 :] if "={" in text else text
        try:
            payload = json.loads(json_text)
        except ValueError as exc:
            raise SourceUnavailableError(f"{context} returned invalid JSON.") from exc
        if payload.get("code") not in (0, "0", None):
            raise SourceUnavailableError(
                f"{context} returned error: {payload.get('msg') or payload.get('code')}"
            )
        return payload

    def _open(self, request: Request) -> Any:
        if self._opener is not None:
            return self._opener(request, timeout=self._timeout)
        return urlopen(request, timeout=self._timeout)


def _parse_code_values(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        raise SourceRequestValidationError("code is required")
    if isinstance(value, str):
        parts: Sequence[Any] = [part for part in re.split(r"[,，\s]+", value) if part]
    elif isinstance(value, Sequence):
        parts = value
    else:
        parts = (value,)
    codes: list[str] = []
    for part in parts:
        text = str(part).strip()
        if text and text not in codes:
            codes.append(text)
    if not codes:
        raise SourceRequestValidationError("code is required")
    return tuple(codes)


def _enum_value(value: Any, *, default: str, allowed: set[str], name: str) -> str:
    if value in (None, ""):
        return default
    text = str(value).strip()
    if text not in allowed:
        choices = ", ".join(sorted(allowed))
        raise SourceRequestValidationError(f"{name} must be one of {choices}")
    return text


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


def _non_negative_int(value: Any, *, default: int, name: str) -> int:
    if value in (None, ""):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SourceRequestValidationError(f"{name} must be a non-negative integer") from exc
    if parsed < 0:
        raise SourceRequestValidationError(f"{name} must be a non-negative integer")
    return parsed


def _normalize_date(value: Any, *, default: str, name: str) -> str:
    if value in (None, ""):
        text = default
    else:
        text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise SourceRequestValidationError(f"{name} must be YYYYMMDD or YYYY-MM-DD")
    try:
        datetime.strptime(text, "%Y%m%d")
    except ValueError as exc:
        raise SourceRequestValidationError(f"{name} must be a valid date") from exc
    return text


def _tencent_spot_sort_key(value: str) -> str:
    return {
        "price": "price",
        "change_pct": "zdf",
        "volume": "volume",
        "amount": "turnover",
    }[value]


def _to_tencent_quote_code(value: str) -> str:
    text = value.strip().lower()
    if re.match(r"^(sh|sz|bj)\d{6}$", text):
        return text
    upper = text.upper()
    if upper.endswith(".SH"):
        return "sh" + upper[:-3]
    if upper.endswith(".SZ"):
        return "sz" + upper[:-3]
    if upper.endswith(".BJ"):
        return "bj" + upper[:-3]
    if re.match(r"^\d{6}$", upper):
        if upper.startswith(("6", "5", "9")):
            return "sh" + upper
        if upper.startswith(("4", "8", "92")):
            return "bj" + upper
        return "sz" + upper
    raise SourceRequestValidationError(f"Unsupported Tencent quote code: {value!r}")


def _parse_quote_payload(text: str) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for match in re.finditer(r"v_([a-z]{2}\d{6})=\"([^\"]*)\"", text):
        quote_code = match.group(1)
        raw = match.group(2)
        if not raw or raw.startswith("1~") and raw.count("~") < 3:
            continue
        parts = raw.split("~")
        if len(parts) < 40 or not parts[1] or not parts[2]:
            continue
        row = _normalize_quote_row(quote_code, parts)
        if row is not None:
            rows[quote_code] = row
    return rows


def _normalize_board_rank_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    quote_code = _blank_to_none(row.get("code"))
    symbol = _symbol_from_tencent_market_code(quote_code)
    if symbol is None:
        return None
    exchange = _exchange_from_quote_code(quote_code.lower())
    return {
        "instrument_id": f"{symbol}.{_exchange_suffix(exchange)}",
        "symbol": symbol,
        "exchange": exchange,
        "asset_type": "stock",
        "name": _blank_to_none(row.get("name")),
        "last_price": _parse_float(row.get("pn")),
        "change": _parse_float(row.get("zd")),
        "change_pct": _parse_float(row.get("zdf")),
        "amplitude": _parse_float(row.get("zf")),
        "volume": _parse_float(row.get("volume")),
        "amount": _parse_float(row.get("turnover")),
        "turnover_rate": _parse_float(row.get("hsl")),
        "pe_ttm": _parse_float(row.get("pe_ttm")),
        "total_market_value": _parse_float(row.get("zsz")),
        "float_market_value": _parse_float(row.get("ltsz")),
        "quote_state": _blank_to_none(row.get("state")),
    }


def _symbol_from_tencent_market_code(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip().lower()
    if re.match(r"^(sh|sz|bj)\d{6}$", text):
        return text[2:]
    if re.match(r"^\d{6}$", text):
        return text
    return None


def _normalize_kline_rows(
    payload: Mapping[str, Any],
    *,
    quote_code: str,
    adjust: str,
    asset_type: str,
) -> list[dict[str, Any]]:
    source = payload.get("data", {})
    quote_payload = source.get(quote_code) if isinstance(source, Mapping) else None
    if not isinstance(quote_payload, Mapping):
        return []
    source_key = {"none": "day", "qfq": "qfqday", "hfq": "hfqday"}[adjust]
    raw_rows = (
        quote_payload.get(source_key)
        or quote_payload.get("day")
        or quote_payload.get("qfqday")
        or quote_payload.get("hfqday")
    )
    if not isinstance(raw_rows, list):
        return []
    exchange = _exchange_from_quote_code(quote_code)
    symbol = quote_code[2:]
    instrument_id = f"{symbol}.{_exchange_suffix(exchange)}"
    rows: list[dict[str, Any]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, Sequence) or isinstance(raw_row, (str, bytes, bytearray)):
            continue
        trade_date = _date_from_dash(raw_row[0] if len(raw_row) > 0 else None)
        if trade_date is None:
            continue
        rows.append(
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "exchange": exchange,
                "asset_type": asset_type,
                "trade_date": trade_date,
                "adjust": adjust,
                "open": _parse_float(raw_row[1] if len(raw_row) > 1 else None),
                "close": _parse_float(raw_row[2] if len(raw_row) > 2 else None),
                "high": _parse_float(raw_row[3] if len(raw_row) > 3 else None),
                "low": _parse_float(raw_row[4] if len(raw_row) > 4 else None),
                "volume": _parse_float(raw_row[5] if len(raw_row) > 5 else None),
                "amount": _parse_float(raw_row[8] if len(raw_row) > 8 else None),
            }
        )
    return rows


def _date_from_dash(value: Any) -> str | None:
    text = _blank_to_none(value)
    if text is None:
        return None
    digits = text.replace("-", "")
    if len(digits) == 8 and digits.isdigit():
        return digits
    try:
        return date.fromisoformat(text).strftime("%Y%m%d")
    except ValueError:
        return None


def _normalize_tick_rows(text: str, *, quote_code: str) -> list[dict[str, Any]]:
    match = re.search(r"\[\s*\d+\s*,\s*\"([^\"]*)\"\s*\]", text, flags=re.S)
    if not match:
        return []
    payload = match.group(1)
    exchange = _exchange_from_quote_code(quote_code)
    symbol = quote_code[2:]
    instrument_id = f"{symbol}.{_exchange_suffix(exchange)}"
    rows: list[dict[str, Any]] = []
    side_map = {"B": "buy", "S": "sell", "M": "neutral"}
    for item in payload.split("|"):
        parts = item.split("/")
        if len(parts) < 7:
            continue
        try:
            sequence = int(parts[0])
        except ValueError:
            sequence = None
        rows.append(
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "exchange": exchange,
                "sequence": sequence,
                "trade_time": _blank_to_none(parts[1]),
                "price": _parse_float(parts[2]),
                "change": _parse_float(parts[3]),
                "volume": _parse_float(parts[4]),
                "amount": _parse_float(parts[5]),
                "trade_side": side_map.get(str(parts[6]).strip().upper(), _blank_to_none(parts[6])),
            }
        )
    return rows


def _normalize_start_year_row(row: Sequence[Any], *, quote_code: str) -> dict[str, Any] | None:
    start_date = _date_from_dash(row[0] if len(row) > 0 else None)
    if start_date is None:
        return None
    exchange = _exchange_from_quote_code(quote_code)
    symbol = quote_code[2:]
    return {
        "instrument_id": f"{symbol}.{_exchange_suffix(exchange)}",
        "symbol": symbol,
        "exchange": exchange,
        "asset_type": _asset_type_from_quote_code(quote_code),
        "start_date": start_date,
        "source_value": _parse_float(row[1] if len(row) > 1 else None),
    }


def _normalize_quote_row(quote_code: str, parts: Sequence[str]) -> dict[str, Any] | None:
    symbol = parts[2] if len(parts) > 2 else quote_code[2:]
    exchange = _exchange_from_quote_code(quote_code)
    instrument_id = f"{symbol}.{_exchange_suffix(exchange)}"
    amount = _amount_from_combined(parts[35] if len(parts) > 35 else "")
    quote_time = _blank_to_none(parts[30] if len(parts) > 30 else None)
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "asset_type": _asset_type(parts[61] if len(parts) > 61 else "", symbol),
        "name": _blank_to_none(parts[1] if len(parts) > 1 else None),
        "quote_time": quote_time,
        "last_price": _parse_float(parts[3] if len(parts) > 3 else None),
        "pre_close": _parse_float(parts[4] if len(parts) > 4 else None),
        "open": _parse_float(parts[5] if len(parts) > 5 else None),
        "high": _parse_float(parts[33] if len(parts) > 33 else None),
        "low": _parse_float(parts[34] if len(parts) > 34 else None),
        "change": _parse_float(parts[31] if len(parts) > 31 else None),
        "change_pct": _parse_float(parts[32] if len(parts) > 32 else None),
        "volume": _parse_float(parts[36] if len(parts) > 36 else None),
        "amount": amount,
        "turnover_rate": _parse_float(parts[38] if len(parts) > 38 else None),
        "pe_dynamic": _parse_float(parts[39] if len(parts) > 39 else None),
        "pb": _parse_float(parts[46] if len(parts) > 46 else None),
        "total_market_value": _parse_float(parts[45] if len(parts) > 45 else None),
        "float_market_value": _parse_float(parts[44] if len(parts) > 44 else None),
        "limit_up_price": _parse_float(parts[47] if len(parts) > 47 else None),
        "limit_down_price": _parse_float(parts[48] if len(parts) > 48 else None),
        "currency": _blank_to_none(parts[82] if len(parts) > 82 else None),
    }


def _asset_type_from_quote_code(quote_code: str) -> str:
    symbol = quote_code[2:]
    if symbol.startswith(("15", "16", "50", "51", "56", "58")):
        return "etf"
    if quote_code.startswith("sh000") or quote_code.startswith("sz399"):
        return "index"
    return "stock"


def _amount_from_combined(value: str) -> float | None:
    parts = str(value or "").split("/")
    if len(parts) >= 3:
        return _parse_float(parts[2])
    return None


def _exchange_from_quote_code(quote_code: str) -> str:
    prefix = quote_code[:2]
    if prefix == "sh":
        return "SSE"
    if prefix == "bj":
        return "BSE"
    return "SZSE"


def _exchange_suffix(exchange: str) -> str:
    return {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}[exchange]


def _asset_type(type_flag: str, symbol: str) -> str:
    flag = str(type_flag or "").upper()
    if "ETF" in flag or symbol.startswith(("15", "16", "50", "51", "56", "58")):
        return "etf"
    if "ZS" in flag or symbol.startswith(("000", "399")) and flag == "ZS":
        return "index"
    return "stock"


def _parse_float(value: Any) -> float | None:
    text = _blank_to_none(value)
    if text is None:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _blank_to_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None
