"""KPH source request adapter."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator, Mapping, Sequence
from datetime import date as date_type
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
    "kph_market_emotion",
    "kph_sector_ranking",
    "kph_sector_constituents_history",
    "kph_limit_up_history",
    "kph_limit_down_history",
    "kph_wind_vane_history",
    "kph_limit_ladder",
    "kph_market_review_events",
    "kph_limit_resumption_history",
}

KPH_REALTIME_URL = "https://apphwshhq.kaipanhong.com/w1/api/index.php"
KPH_HISTORY_URL = "https://apphis.kaipanhong.com/w1/api/index.php"

_BASE_PARAMS = {
    "PhoneOSNew": "1",
    "DeviceID": "1a609dd6-b2b8-3bf9-ac40-a77581551454",
    "VerSion": "6.0.6",
    "Token": "0",
    "UserID": "0",
    "Red": "1",
    "apiv": "w45",
}
_SECTOR_TYPE_MAP = {"selected": "7", "industry": "4", "region": "6", "7": "7", "4": "4", "6": "6"}
_LIMIT_PID = {"up": "4", "down": "3", "wind_vane": "6"}
_NO_DATA_ERRCODES = {"1020"}
# 未指定日期时向过去回溯找最新有数据的交易日:覆盖周末与最长长假
_MAX_HISTORY_LOOKBACK_DAYS = 12


class KphRequestAdapter:
    """Request KPH endpoints and return AxData fields."""

    source = "kph"

    def __init__(self, opener: Any | None = None, *, timeout: float = 15.0) -> None:
        self._opener = opener
        self._timeout = timeout
        self.last_meta: dict[str, Any] = {}

    def supports(self, interface_name: str) -> bool:
        return interface_name in SUPPORTED_INTERFACES

    def request(self, interface_name: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if interface_name == "kph_market_emotion":
            return self._request_market_emotion(params)
        if interface_name == "kph_sector_ranking":
            return self._request_sector_ranking(params)
        if interface_name == "kph_sector_constituents_history":
            return self._request_sector_constituents_history(params)
        if interface_name == "kph_limit_up_history":
            return self._request_limit_history(params, kind="up")
        if interface_name == "kph_limit_down_history":
            return self._request_limit_history(params, kind="down")
        if interface_name == "kph_wind_vane_history":
            return self._request_limit_history(params, kind="wind_vane")
        if interface_name == "kph_limit_ladder":
            return self._request_limit_ladder(params)
        if interface_name == "kph_market_review_events":
            return self._request_market_review_events(params)
        if interface_name == "kph_limit_resumption_history":
            return self._request_limit_resumption_history(params)
        raise SourceAdapterNotFound(f"KPH source adapter does not support interface {interface_name!r}.")

    def _request_market_emotion(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        trade_date = _normalize_date(params.get("trade_date"), required=False) or _today_dash()
        is_today = trade_date == _today_dash()
        payload = self._post(
            KPH_REALTIME_URL if is_today else KPH_HISTORY_URL,
            {
                "a": "ZhangFuDetail" if is_today else "HisZhangFuDetail",
                "c": "HomeDingPan" if is_today else "HisHomeDingPan",
                **({} if is_today else {"Day": trade_date}),
            },
            context="KPH market emotion",
        )
        if not payload:
            self.last_meta = {"source_name": "开盘红", "source_url": KPH_REALTIME_URL if is_today else KPH_HISTORY_URL, "trade_date": trade_date.replace("-", "")}
            return []
        info = payload.get("info") if isinstance(payload.get("info"), Mapping) else {}
        row = {
            "trade_date": trade_date.replace("-", ""),
            "limit_up_count": _parse_int(info.get("ZT")),
            "limit_down_count": _parse_int(info.get("DT")),
            "real_limit_up_count": _parse_int(info.get("SJZT")),
            "real_limit_down_count": _parse_int(info.get("SJDT")),
            "st_limit_up_count": _parse_int(info.get("STZT")),
            "st_limit_down_count": _parse_int(info.get("STDT")),
            "rise_count": _parse_int(info.get("SZJS")),
            "fall_count": _parse_int(info.get("XDJS")),
            "flat_count": _parse_int(info.get("0")),
            "market_sign": _clean_text(info.get("sign")),
            "raw_rise_dist": {str(i): _parse_int(info.get(str(i))) for i in range(1, 11)},
            "raw_fall_dist": {str(i): _parse_int(info.get(str(i))) for i in range(-1, -11, -1)},
            "total_amount": _parse_float(info.get("qscln")),
        }
        self.last_meta = {"source_name": "开盘红", "source_url": KPH_REALTIME_URL if is_today else KPH_HISTORY_URL, "trade_date": row["trade_date"]}
        return [row]

    def _request_sector_ranking(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        start_day, explicit = _history_start_day(params.get("trade_date"))
        sector_type = str(params.get("sector_type") or "selected").strip().lower()
        zs_type = _SECTOR_TYPE_MAP.get(sector_type)
        if zs_type is None:
            raise SourceRequestValidationError("sector_type must be selected, industry, or region")
        fetch_all = _parse_bool(params.get("fetch_all"), default=False)
        type_param = "1" if zs_type == "7" else "2"
        page_size = 50
        rows: list[dict[str, Any]] = []
        trade_date = start_day.strftime("%Y-%m-%d")
        for day in _history_day_candidates(start_day, explicit=explicit):
            trade_date = day.strftime("%Y-%m-%d")
            host = KPH_REALTIME_URL if trade_date == _today_dash() else KPH_HISTORY_URL
            index = 0
            rows = []
            while True:
                payload = self._post(
                    host,
                    {
                        "a": "RealRankingInfo",
                        "c": "ZhiShuRanking",
                        "Order": "1",
                        "st": str(page_size),
                        "Index": str(index),
                        "Date": trade_date,
                        "Type": type_param,
                        "ZSType": zs_type,
                    },
                    context="KPH sector ranking",
                )
                batch = _payload_list(payload)
                rows.extend(_parse_sector_row(row, trade_date=trade_date, sector_type=sector_type) for row in batch)
                if not fetch_all or len(batch) < page_size:
                    break
                index += page_size
            if rows or explicit:
                break
        self.last_meta = {"source_name": "开盘红", "source_url": KPH_REALTIME_URL if trade_date == _today_dash() else KPH_HISTORY_URL, "trade_date": trade_date.replace("-", ""), "sector_type": sector_type}
        return rows

    def _request_sector_constituents_history(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        plate_id = _required_text(params.get("plate_id"), "plate_id")
        start_day, explicit = _history_start_day(params.get("trade_date"))
        rows: list[dict[str, Any]] = []
        trade_date = start_day.strftime("%Y-%m-%d")
        for day in _history_day_candidates(start_day, explicit=explicit):
            trade_date = day.strftime("%Y-%m-%d")
            payload = self._post(
                KPH_HISTORY_URL,
                {
                    "a": "ZhiShuStockList_W8",
                    "c": "ZhiShuRanking",
                    "Order": "1",
                    "st": "1000",
                    "old": "1",
                    "Index": "0",
                    "Date": trade_date,
                    "Type": "6",
                    "PlateID": plate_id,
                    "IsZZ": "0",
                    "IsKZZType": "0",
                    "TSZB": "0",
                    "TSZB_Type": "0",
                    "filterType": "0",
                },
                context="KPH sector constituents history",
            )
            rows = [_parse_sector_constituent_row(row, trade_date=trade_date, plate_id=plate_id) for row in _payload_list(payload)]
            if rows or explicit:
                break
        self.last_meta = {"source_name": "开盘红", "source_url": KPH_HISTORY_URL, "trade_date": trade_date.replace("-", ""), "plate_id": plate_id}
        return rows

    def _request_limit_history(self, params: Mapping[str, Any], *, kind: str) -> list[dict[str, Any]]:
        start_day, explicit = _history_start_day(params.get("trade_date"))
        pid = _LIMIT_PID[kind]
        page_size = 50
        rows: list[dict[str, Any]] = []
        trade_date = start_day.strftime("%Y-%m-%d")
        for day in _history_day_candidates(start_day, explicit=explicit):
            trade_date = day.strftime("%Y-%m-%d")
            index = 0
            rows = []
            while True:
                payload = self._post(
                    KPH_HISTORY_URL,
                    {
                        "a": "HisDaBanList",
                        "c": "HisHomeDingPan",
                        "Order": "1",
                        "st": str(page_size),
                        "Index": str(index),
                        "Is_st": "1",
                        "PidType": pid,
                        "Type": "6",
                        "FilterMotherboard": "0",
                        "Filter": "0",
                        "FilterTIB": "0",
                        "FilterGem": "0",
                        "Day": trade_date,
                    },
                    context=f"KPH {kind} history",
                )
                batch = _payload_list(payload)
                rows.extend(_parse_limit_history_row(row, trade_date=trade_date, kind=kind) for row in batch)
                if len(batch) < page_size:
                    break
                index += page_size
            if rows or explicit:
                break
        self.last_meta = {"source_name": "开盘红", "source_url": KPH_HISTORY_URL, "trade_date": trade_date.replace("-", ""), "kind": kind}
        return rows

    def _request_limit_ladder(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        trade_date = _normalize_date(params.get("trade_date"), required=False) or _today_dash()
        payload = self._post(
            KPH_REALTIME_URL if trade_date == _today_dash() else KPH_HISTORY_URL,
            {"a": "GetZhangTingTianTi", "c": "FuPanLa", "Date": trade_date},
            context="KPH limit ladder",
        )
        stock_list = payload.get("StockList") or payload.get("stock_list") or []
        rows: list[dict[str, Any]] = []
        for group in stock_list if isinstance(stock_list, list) else []:
            if isinstance(group, list) and group and isinstance(group[0], list):
                rows.extend(_parse_ladder_row(row, trade_date=trade_date) for row in group)
            elif isinstance(group, list):
                rows.append(_parse_ladder_row(group, trade_date=trade_date))
        self.last_meta = {"source_name": "开盘红", "source_url": KPH_REALTIME_URL if trade_date == _today_dash() else KPH_HISTORY_URL, "trade_date": trade_date.replace("-", "")}
        return rows

    def _request_market_review_events(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        trade_date = _normalize_date(params.get("trade_date"), required=False) or _today_dash()
        limit = _positive_int(params.get("limit"), default=30, name="limit")
        offset = _non_negative_int(params.get("offset"), default=0, name="offset")
        payload = self._post(
            KPH_REALTIME_URL if trade_date == _today_dash() else KPH_HISTORY_URL,
            {"a": "GetPMSL_PMLD", "c": "FuPanLa", "st": str(limit), "Index": str(offset), "Date": trade_date},
            context="KPH market review events",
        )
        items = payload.get("List") or payload.get("list") or []
        self.last_meta = {"source_name": "开盘红", "source_url": KPH_REALTIME_URL if trade_date == _today_dash() else KPH_HISTORY_URL, "trade_date": trade_date.replace("-", "")}
        return [_parse_market_event(item, trade_date=trade_date) for item in items if isinstance(item, Mapping)]

    def _request_limit_resumption_history(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        start_day, explicit = _history_start_day(params.get("trade_date"))
        limit = _positive_int(params.get("limit"), default=100, name="limit")
        offset = _non_negative_int(params.get("offset"), default=0, name="offset")
        rows: list[dict[str, Any]] = []
        trade_date = start_day.strftime("%Y-%m-%d")
        for day in _history_day_candidates(start_day, explicit=explicit):
            trade_date = day.strftime("%Y-%m-%d")
            payload = self._post(
                KPH_HISTORY_URL,
                {"a": "GetPlateInfo_w38", "c": "HisLimitResumption", "st": str(limit), "Index": str(offset), "Date": trade_date},
                context="KPH limit resumption history",
            )
            rows = []
            for plate in payload.get("list", []) if isinstance(payload.get("list"), list) else []:
                if isinstance(plate, Mapping):
                    rows.extend(_parse_resumption_stock(row, plate=plate, trade_date=trade_date) for row in plate.get("StockList", []) if isinstance(row, list))
            if rows or explicit:
                break
        self.last_meta = {"source_name": "开盘红", "source_url": KPH_HISTORY_URL, "trade_date": trade_date.replace("-", "")}
        return rows

    def _post(self, url: str, params: Mapping[str, Any], *, context: str) -> Mapping[str, Any]:
        body = urlencode({**_BASE_PARAMS, **{k: v for k, v in params.items() if v is not None}}).encode("utf-8")
        request = Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "User-Agent": "Dalvik/2.1.0 AxData/0.1",
                "Accept": "application/json,text/plain,*/*",
            },
            method="POST",
        )
        try:
            response = self._open(request)
            with response:
                text = response.read().decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"{context} request failed: {exc}") from exc
        try:
            payload = json.loads(text)
        except ValueError as exc:
            raise SourceUnavailableError(f"{context} returned invalid JSON.") from exc
        if not isinstance(payload, Mapping):
            raise SourceUnavailableError(f"{context} returned unexpected payload.")
        errcode = payload.get("errcode")
        if errcode not in (None, "0", 0):
            if str(errcode) in _NO_DATA_ERRCODES:
                # KPH 对非交易日/无数据日期统一返回 1020(参数出错),视为空结果以便区间补采跳过
                return {}
            raise SourceUnavailableError(f"{context} returned errcode={errcode}")
        return payload

    def _open(self, request: Request) -> Any:
        if self._opener is not None:
            return self._opener(request, timeout=self._timeout)
        return urlopen(request, timeout=self._timeout)


def _parse_sector_row(row: Sequence[Any], *, trade_date: str, sector_type: str) -> dict[str, Any]:
    return {
        "trade_date": trade_date.replace("-", ""),
        "plate_id": _clean_text(_at(row, 0)),
        "plate_name": _clean_text(_at(row, 1)),
        "sector_type": sector_type,
        "amount": _parse_float(_at(row, 2)),
        "change_pct": _parse_float(_at(row, 3)),
        "amplitude": _parse_float(_at(row, 4)),
        "net_inflow": _parse_float(_at(row, 5)),
        "turnover_rate": _parse_float(_at(row, 9)),
        "market_cap": _parse_float(_at(row, 10)),
        "stock_count": _parse_int(_at(row, 17)),
    }


def _parse_sector_constituent_row(row: Sequence[Any], *, trade_date: str, plate_id: str) -> dict[str, Any]:
    return {
        "trade_date": trade_date.replace("-", ""),
        "plate_id": plate_id,
        **_identity_from_symbol(_clean_text(_at(row, 0))),
        "name": _clean_text(_at(row, 1)),
        "tags": _clean_text(_at(row, 4)),
        "last_price": _parse_float(_at(row, 5)),
        "change_pct": _parse_float(_at(row, 6)),
        "amount": _parse_float(_at(row, 7)),
        "turnover_rate": _parse_float(_at(row, 8)),
        "float_market_value": _parse_float(_at(row, 10)),
        "main_net": _parse_float(_at(row, 13)),
        "limit_tag": _clean_text(_at(row, 23)),
        "rank_tag": _clean_text(_at(row, 24)),
        "limit_count": _parse_int(_at(row, 40)),
    }


def _parse_limit_history_row(row: Sequence[Any], *, trade_date: str, kind: str) -> dict[str, Any]:
    # 涨停/跌停共用同一行布局:连板标签、原因等字段按位解析,无值自然为空
    return {
        "trade_date": trade_date.replace("-", ""),
        **_identity_from_symbol(_clean_text(_at(row, 0))),
        "name": _clean_text(_at(row, 1)),
        "limit_time": _parse_int(_at(row, 6)),
        "open_time": _parse_int(_at(row, 7)),
        "seal_amount": _parse_float(_at(row, 8)),
        "limit_tag": _clean_text(_at(row, 9)),
        "limit_count": _parse_int(_at(row, 10)),
        "themes": _clean_text(_at(row, 11)),
        "net_inflow": _parse_float(_at(row, 12)),
        "turnover": _parse_float(_at(row, 13)),
        "turnover_rate": _parse_float(_at(row, 14)),
        "market_cap": _parse_float(_at(row, 15)),
        "reason": _clean_text(_at(row, 16)),
        "seal_money": _parse_float(_at(row, 23)),
        "industry_id": _clean_text(_at(row, 26)),
        "industry_limit_up_count": _parse_int(_at(row, 27)),
    }


def _parse_ladder_row(row: Sequence[Any], *, trade_date: str) -> dict[str, Any]:
    return {
        "trade_date": trade_date.replace("-", ""),
        **_identity_from_symbol(_clean_text(_at(row, 0))),
        "name": _clean_text(_at(row, 1)),
        "limit_count": _parse_int(_at(row, 2)),
        "limit_time": _parse_int(_at(row, 3)),
        "plate_id": _clean_text(_at(row, 4)),
        "plate_name": _clean_text(_at(row, 5)),
        "one_word": _parse_bool_value(_at(row, 6)),
        "popular": _parse_bool_value(_at(row, 7)),
        "plate_limit_up_count": _parse_int(_at(row, 8)),
        "amount": _parse_float(_at(row, 9)),
        "plate_amount": _parse_float(_at(row, 10)),
    }


def _parse_market_event(item: Mapping[str, Any], *, trade_date: str) -> dict[str, Any]:
    return {
        "trade_date": trade_date.replace("-", ""),
        "event_time": _parse_int(item.get("TimeMin")),
        "tag_id": _parse_int(item.get("TagID")),
        "tag_name": _clean_text(item.get("TagName")),
        "tag_attribute": _parse_int(item.get("TagShuXing")),
        "plate_id": _clean_text(item.get("ZSCode")),
        "plate_name": _clean_text(item.get("ZSName")),
        "detail": _clean_text(item.get("Detail")),
        "raw_stock_list": item.get("StockList") if isinstance(item.get("StockList"), list) else [],
    }


def _parse_resumption_stock(row: Sequence[Any], *, plate: Mapping[str, Any], trade_date: str) -> dict[str, Any]:
    return {
        "trade_date": trade_date.replace("-", ""),
        "plate_id": _clean_text(plate.get("ZSCode")),
        "plate_name": _clean_text(plate.get("ZSName")),
        **_identity_from_symbol(_clean_text(_at(row, 0))),
        "name": _clean_text(_at(row, 1)),
        "limit_tag": _clean_text(_at(row, 9)),
        "limit_count": _parse_int(_at(row, 10)),
        "themes": _clean_text(_at(row, 11)),
        "reason_short": _clean_text(_at(row, 16)),
        "reason_detail": _clean_text(_at(row, 17)),
    }


def _payload_list(payload: Mapping[str, Any]) -> list[Sequence[Any]]:
    rows = payload.get("list", [])
    return [row for row in rows if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray))]


def _history_start_day(value: Any) -> tuple[date_type, bool]:
    """解析历史接口日期:显式过去日期直接采用;留空从今天起回溯最新有数据的交易日。"""

    trade_date = _normalize_date(value, required=False)
    if trade_date is None:
        return date_type.today(), False
    parsed = datetime.strptime(trade_date, "%Y-%m-%d").date()
    if parsed >= date_type.today():
        raise SourceRequestValidationError("trade_date must be earlier than today for this KPH historical interface")
    return parsed, True


def _history_day_candidates(start_day: date_type, *, explicit: bool) -> Iterator[date_type]:
    """显式日期只尝试当天;未指定时最多回溯 _MAX_HISTORY_LOOKBACK_DAYS 天。"""

    day = start_day
    for _ in range(1 if explicit else _MAX_HISTORY_LOOKBACK_DAYS):
        yield day
        day -= timedelta(days=1)


def _normalize_date(value: Any, *, required: bool) -> str | None:
    if value in (None, ""):
        if required:
            raise SourceRequestValidationError("trade_date is required")
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) != 8:
        raise SourceRequestValidationError("trade_date must be YYYYMMDD or YYYY-MM-DD")
    try:
        datetime.strptime(digits, "%Y%m%d")
    except ValueError as exc:
        raise SourceRequestValidationError("trade_date must be a valid date") from exc
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def _today_dash() -> str:
    return date_type.today().strftime("%Y-%m-%d")


def _identity_from_symbol(symbol: str | None) -> dict[str, Any]:
    clean = re.sub(r"\D", "", str(symbol or ""))
    if len(clean) != 6:
        return {"instrument_id": None, "symbol": None, "exchange": None}
    if clean.startswith(("6", "5", "9")):
        exchange, suffix = "SSE", "SH"
    elif clean.startswith(("4", "8", "92")):
        exchange, suffix = "BSE", "BJ"
    else:
        exchange, suffix = "SZSE", "SZ"
    return {"instrument_id": f"{clean}.{suffix}", "symbol": clean, "exchange": exchange}


def _at(row: Sequence[Any], index: int) -> Any:
    return row[index] if index < len(row) else None


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


def _parse_bool_value(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip() in {"1", "true", "True"}


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SourceRequestValidationError(f"{name} is required")
    return text


def _parse_float(value: Any) -> float | None:
    text = _clean_text(value)
    if text is None:
        return None
    try:
        return float(text.replace(",", "").replace("%", ""))
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    parsed = _parse_float(value)
    return int(parsed) if parsed is not None else None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text or text in {"-", "--", "null", "None"}:
        return None
    return text
