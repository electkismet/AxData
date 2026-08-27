"""CLS source request adapter."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
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
    "cls_market_emotion",
    "cls_market_wind",
    "cls_market_wind_stocks",
    "cls_market_mainline",
    "cls_sector_industry",
    "cls_sector_heat",
    "cls_sector_popular_stocks",
    "cls_sector_rotation",
    "cls_limit_up_pool",
    "cls_stock_timeline",
    "cls_stock_kline",
    "cls_news_telegraph",
}

CLS_EMOTION_URL = "https://x-quote.cls.cn/v2/quote/a/stock/emotion"
CLS_TUYERE_URL = "https://api3.cls.cn/v2/todayTuyere"
CLS_TUYERE_STOCKS_URL = "https://x-quote.cls.cn/v2/quote/a/plate/tuyere/stocks"
CLS_MAINLINE_URL = "https://api3.cls.cn/v2/dingPan/mainline"
CLS_PLATE_LIST_URL = "https://x-quote.cls.cn/web_quote/plate/plate_list"
CLS_PLATE_HEAT_URL = "https://x-quote.cls.cn/v2/quote/a/plate/plate_heat_list"
CLS_PLATE_POPULAR_URL = "https://x-quote.cls.cn/v2/quote/a/plate/popular_stocks"
CLS_PLATE_ROTATION_URL = "https://x-quote.cls.cn/v2/quote/a/plate/rotation"
CLS_LIMIT_UP_URL = "https://x-quote.cls.cn/quote/index/up_down_analysis"
CLS_TIMELINE_URL = "https://x-quote.cls.cn/quote/stock/tline"
CLS_KLINE_URL = "https://x-quote.cls.cn/quote/stock/kline"
CLS_TELEGRAPH_URL = "https://api3.cls.cn/v1/roll/get_roll_list"

_BASE_APP_PARAMS = {
    "app": "cailianpress",
    "sv": "8.7.4",
    "os": "android",
    "mb": "Xiaomi-2206123SC",
    "ov": "32",
    "channel": "8",
    "motif": "0",
    "net": "",
    "province_code": "3205",
    "token": "",
    "uid": "",
}
_KLINE_TYPE_MAP = {"daily": "fd1", "weekly": "fw", "monthly": "fm", "yearly": "fy"}
_NEWS_CATEGORY_MAP = {"all": "", "important": "red", "company": "announcement"}


class ClsRequestAdapter:
    """Request CLS endpoints and return AxData fields."""

    source = "cls"

    def __init__(self, opener: Any | None = None, *, timeout: float = 15.0) -> None:
        self._opener = opener
        self._timeout = timeout
        self.last_meta: dict[str, Any] = {}

    def supports(self, interface_name: str) -> bool:
        return interface_name in SUPPORTED_INTERFACES

    def request(self, interface_name: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if interface_name == "cls_market_emotion":
            return self._request_market_emotion()
        if interface_name == "cls_market_wind":
            return self._request_market_wind()
        if interface_name == "cls_market_wind_stocks":
            return self._request_market_wind_stocks(params)
        if interface_name == "cls_market_mainline":
            return self._request_market_mainline()
        if interface_name == "cls_sector_industry":
            return self._request_sector_industry()
        if interface_name == "cls_sector_heat":
            return self._request_sector_heat()
        if interface_name == "cls_sector_popular_stocks":
            return self._request_sector_popular_stocks(params)
        if interface_name == "cls_sector_rotation":
            return self._request_sector_rotation(params)
        if interface_name == "cls_limit_up_pool":
            return self._request_limit_up_pool()
        if interface_name == "cls_stock_timeline":
            return self._request_stock_timeline(params)
        if interface_name == "cls_stock_kline":
            return self._request_stock_kline(params)
        if interface_name == "cls_news_telegraph":
            return self._request_news_telegraph(params)
        raise SourceAdapterNotFound(f"CLS source adapter does not support interface {interface_name!r}.")

    def _request_market_emotion(self) -> list[dict[str, Any]]:
        payload = self._fetch_json(
            CLS_EMOTION_URL,
            params={
                "app": "CailianpressWeb",
                "os": "web",
                "sv": "8.4.6",
                "sign": "9f8797a1f4de66c2370f7a03990d2737",
            },
            context="CLS market emotion",
            host="x-quote.cls.cn",
        )
        body = _cls_data(payload, "CLS market emotion")
        up_down = body.get("up_down_dis") if isinstance(body.get("up_down_dis"), Mapping) else {}
        board = body.get("limit_up_board") if isinstance(body.get("limit_up_board"), Mapping) else {}
        row = {
            "market_degree": _parse_float(body.get("market_degree")),
            "shsz_balance": _clean_text(body.get("shsz_balance")),
            "preview_balance": _clean_text(body.get("preview_balance")),
            "up_ratio": _clean_text(body.get("up_ratio")),
            "up_ratio_num": _parse_int(body.get("up_ratio_num")),
            "up_open_num": _parse_int(body.get("up_open_num")),
            "performance": _clean_text(body.get("performance")),
            "rise_num": _parse_int(up_down.get("rise_num")),
            "fall_num": _parse_int(up_down.get("fall_num")),
            "flat_num": _parse_int(up_down.get("flat_num")),
            "up_num": _parse_int(up_down.get("up_num")),
            "down_num": _parse_int(up_down.get("down_num")),
            "raw_up_down_dis": dict(up_down),
            "raw_limit_up_board": dict(board),
        }
        self.last_meta = {"source_name": "财联社", "source_url": CLS_EMOTION_URL}
        return [row]

    def _request_market_wind(self) -> list[dict[str, Any]]:
        payload = self._fetch_json(
            CLS_TUYERE_URL,
            params=_signed_params(),
            context="CLS market wind",
            host="api3.cls.cn",
            mobile=True,
        )
        data = _cls_errno_data(payload, "CLS market wind")
        items = data.get("today_tuyere", []) if isinstance(data, Mapping) else []
        self.last_meta = {"source_name": "财联社", "source_url": CLS_TUYERE_URL}
        return [
            {
                "plate_code": _clean_text(item.get("plate_code")),
                "plate_name": _clean_text(item.get("title")),
                "catalyst": _clean_text(item.get("interpret")),
            }
            for item in items if isinstance(item, Mapping)
        ]

    def _request_market_wind_stocks(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        plate_code = _required_text(params.get("plate_code"), "plate_code")
        payload = self._fetch_json(
            CLS_TUYERE_STOCKS_URL,
            params=_signed_params(plate_code=plate_code),
            context="CLS market wind stocks",
            host="x-quote.cls.cn",
            mobile=True,
        )
        items = _cls_data_list(payload, "CLS market wind stocks")
        self.last_meta = {"source_name": "财联社", "source_url": CLS_TUYERE_STOCKS_URL, "plate_code": plate_code}
        return [_normalize_cls_stock(item) | {"continuous_count": _parse_int(item.get("continuous"))} for item in items]

    def _request_market_mainline(self) -> list[dict[str, Any]]:
        payload = self._fetch_json(
            CLS_MAINLINE_URL,
            params=_signed_params(),
            context="CLS market mainline",
            host="api3.cls.cn",
            mobile=True,
        )
        data = _cls_errno_data(payload, "CLS market mainline")
        rows: list[dict[str, Any]] = []
        if isinstance(data, Mapping):
            for key, value in data.items():
                if isinstance(value, Mapping):
                    rows.append(
                        {
                            "block_key": str(key),
                            "title": _clean_text(value.get("title") or value.get("name")),
                            "summary": _clean_text(value.get("desc") or value.get("summary") or value.get("interpret")),
                            # raw_item 统一序列化为 JSON 字符串,避免 dict/list 混列导致 Parquet 类型冲突
                            "raw_item": json.dumps(value, ensure_ascii=False),
                        }
                    )
                elif isinstance(value, list):
                    rows.append(
                        {
                            "block_key": str(key),
                            "title": str(key),
                            "summary": None,
                            "raw_item": json.dumps(value, ensure_ascii=False),
                        }
                    )
        self.last_meta = {"source_name": "财联社", "source_url": CLS_MAINLINE_URL}
        return rows

    def _request_sector_industry(self) -> list[dict[str, Any]]:
        payload = self._fetch_json(
            CLS_PLATE_LIST_URL,
            params={
                "app": "CailianpressWeb",
                "os": "web",
                "page": "5",
                "rever": "1",
                "sv": "8.4.6",
                "type": "industry",
                "way": "change",
                "sign": "ef1ec7886be706a0b722d7e7bf3c0054",
            },
            context="CLS sector industry",
            host="x-quote.cls.cn",
        )
        data = _cls_data(payload, "CLS sector industry")
        items = data.get("plate_data", []) if isinstance(data, Mapping) else []
        self.last_meta = {"source_name": "财联社", "source_url": CLS_PLATE_LIST_URL}
        return [
            {
                "plate_code": _clean_text(item.get("secu_code")),
                "plate_name": _clean_text(item.get("secu_name")),
                "change_pct": _parse_float(item.get("change")),
                "main_fund_diff": _parse_float(item.get("main_fund_diff")),
                "rise_count": _parse_int(item.get("limit_up")),
                "fall_count": _parse_int(item.get("limit_down")),
                "limit_up_count": _parse_int(item.get("limit_up_num")),
                "limit_down_count": _parse_int(item.get("limit_down_num")),
                "trade_status": _clean_text(item.get("trade_status")),
                "raw_first_stock": item.get("first_stock") if isinstance(item.get("first_stock"), Mapping) else {},
            }
            for item in items if isinstance(item, Mapping)
        ]

    def _request_sector_heat(self) -> list[dict[str, Any]]:
        payload = self._fetch_json(
            CLS_PLATE_HEAT_URL,
            params=_signed_params(),
            context="CLS sector heat",
            host="x-quote.cls.cn",
            mobile=True,
        )
        items = _cls_data_list(payload, "CLS sector heat")
        self.last_meta = {"source_name": "财联社", "source_url": CLS_PLATE_HEAT_URL}
        return [
            {
                "plate_code": _clean_text(item.get("plate_code")),
                "plate_name": _clean_text(item.get("plate_name")),
                "rank": _parse_int(item.get("rank")),
                "cur_heat": _parse_float(item.get("cur_heat")),
                "rank_change": _parse_int(item.get("rank_change")),
                "is_new": _parse_bool_value(item.get("is_new")),
            }
            for item in items
        ]

    def _request_sector_popular_stocks(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        plate_code = _required_text(params.get("plate_code"), "plate_code")
        payload = self._fetch_json(
            CLS_PLATE_POPULAR_URL,
            params=_signed_params(plate_code=plate_code),
            context="CLS sector popular stocks",
            host="x-quote.cls.cn",
            mobile=True,
        )
        items = _cls_data_list(payload, "CLS sector popular stocks")
        self.last_meta = {"source_name": "财联社", "source_url": CLS_PLATE_POPULAR_URL, "plate_code": plate_code}
        return [
            _normalize_cls_stock(item)
            | {
                "change_text": _clean_text(item.get("change")),
                "change_px": _parse_float(item.get("change_px")),
                "board_tag": _clean_text(item.get("tbm")),
                "head_rank": _parse_int(item.get("head_num")),
            }
            for item in items
        ]

    def _request_sector_rotation(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        days = _positive_int(params.get("days"), default=4, name="days")
        payload = self._fetch_json(
            CLS_PLATE_ROTATION_URL,
            params=_signed_params(days=str(days)),
            context="CLS sector rotation",
            host="x-quote.cls.cn",
            mobile=True,
        )
        items = _cls_data_list(payload, "CLS sector rotation")
        rows: list[dict[str, Any]] = []
        for item in items:
            trade_date = _normalize_date_text(item.get("trade_date") or item.get("date"))
            plates = item.get("plates", []) if isinstance(item, Mapping) else []
            for index, plate in enumerate(plates if isinstance(plates, list) else [], start=1):
                if isinstance(plate, Mapping):
                    rows.append(
                        {
                            "trade_date": trade_date,
                            "plate_code": _clean_text(plate.get("plate_code")),
                            "plate_name": _clean_text(plate.get("plate_name")),
                            "change_pct": _parse_float(plate.get("change")),
                            "rank": index,
                        }
                    )
        self.last_meta = {"source_name": "财联社", "source_url": CLS_PLATE_ROTATION_URL, "days": days}
        return rows

    def _request_limit_up_pool(self) -> list[dict[str, Any]]:
        payload = self._fetch_json(
            CLS_LIMIT_UP_URL,
            params={
                "app": "CailianpressWeb",
                "os": "web",
                "rever": "1",
                "sv": "8.4.6",
                "type": "up_pool",
                "way": "last_px",
                "sign": "a6ab28604a6dbe891cdbd7764799eda1",
            },
            context="CLS limit-up pool",
            host="x-quote.cls.cn",
        )
        items = _cls_data_list(payload, "CLS limit-up pool")
        # 源端为实时快照且不接受日期参数:按采集日落 trade_date,
        # 供每日收盘后定时累积成历史(接口本身无法回补历史)
        trade_date = datetime.now().strftime("%Y%m%d")
        self.last_meta = {"source_name": "财联社", "source_url": CLS_LIMIT_UP_URL, "trade_date": trade_date}
        return [
            _normalize_cls_stock(item)
            | {
                "trade_date": trade_date,
                "up_reason": _clean_text(item.get("up_reason")),
            }
            for item in items
        ]

    def _request_stock_timeline(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        secu_code = _to_cls_secu_code(_required_text(params.get("code"), "code"))
        payload = self._fetch_json(
            CLS_TIMELINE_URL,
            params={
                "app": "CailianpressWeb",
                "os": "web",
                "sv": "8.4.6",
                "secu_code": secu_code,
                "fields": "date,minute,last_px,business_balance,business_amount,open_px,preclose_px,av_px",
                "sign": "afad7ec0475a1b9854313502389f3346",
            },
            context="CLS stock timeline",
            host="x-quote.cls.cn",
        )
        data = _cls_data(payload, "CLS stock timeline")
        items = data.get("line", []) if isinstance(data, Mapping) else []
        identity = _identity_from_cls_code(secu_code)
        self.last_meta = {"source_name": "财联社", "source_url": CLS_TIMELINE_URL, "requested_code": secu_code}
        return [
            identity
            | {
                "trade_date": _normalize_date_text(item.get("date")),
                "minute": _clean_text(item.get("minute")),
                "last_price": _parse_float(item.get("last_px")),
                "amount": _parse_float(item.get("business_balance")),
                "volume": _parse_float(item.get("business_amount")),
                "open": _parse_float(item.get("open_px")),
                "pre_close": _parse_float(item.get("preclose_px")),
                "avg_price": _parse_float(item.get("av_px")),
            }
            for item in items if isinstance(item, Mapping)
        ]

    def _request_stock_kline(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        secu_code = _to_cls_secu_code(_required_text(params.get("code"), "code"))
        kline_type = str(params.get("kline_type") or "daily").strip().lower()
        if kline_type not in _KLINE_TYPE_MAP:
            raise SourceRequestValidationError("kline_type must be daily, weekly, monthly, or yearly")
        limit = min(_positive_int(params.get("limit"), default=50, name="limit"), 500)
        offset = _non_negative_int(params.get("offset"), default=0, name="offset")
        payload = self._fetch_json(
            CLS_KLINE_URL,
            params={
                "app": "CailianpressWeb",
                "os": "web",
                "sv": "8.4.6",
                "secu_code": secu_code,
                "type": _KLINE_TYPE_MAP[kline_type],
                "limit": str(limit),
                "offset": str(offset),
                "sign": "d2656d0d3fdc1d489f6f316ea820cc17",
            },
            context="CLS stock kline",
            host="x-quote.cls.cn",
        )
        items = _cls_data_list(payload, "CLS stock kline")
        identity = _identity_from_cls_code(secu_code)
        self.last_meta = {"source_name": "财联社", "source_url": CLS_KLINE_URL, "requested_code": secu_code, "kline_type": kline_type}
        return [
            identity
            | {
                "trade_date": _normalize_date_text(item.get("date")),
                "open": _parse_float(item.get("open")),
                "high": _parse_float(item.get("high")),
                "low": _parse_float(item.get("low")),
                "close": _parse_float(item.get("close")),
                "volume": _parse_float(item.get("volume")),
                "amount": _parse_float(item.get("amount")),
                "change": _parse_float(item.get("change")),
                "change_pct": _parse_float(item.get("change_rate") or item.get("change_pct")),
            }
            for item in items
        ]

    def _request_news_telegraph(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        category = str(params.get("category") or "important").strip().lower()
        if category not in _NEWS_CATEGORY_MAP:
            raise SourceRequestValidationError("category must be all, important, or company")
        date_text = _normalize_date_text(params.get("date")) or datetime.now().strftime("%Y%m%d")
        # limit 为目标总条数;上游单次最多 50 条,超过时按 last_time 滚动翻页
        limit = min(_positive_int(params.get("limit"), default=20, name="limit"), 2000)
        page_size = 50
        day = datetime.strptime(date_text, "%Y%m%d")
        day_start = int(datetime(day.year, day.month, day.day, 0, 0, 0).timestamp())
        day_end = int(datetime(day.year, day.month, day.day, 23, 59, 59).timestamp())
        rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        last_time = day_end
        while len(rows) < limit:
            request_params = _signed_params(refresh_type="1", last_time=str(last_time), rn=str(page_size))
            api_category = _NEWS_CATEGORY_MAP[category]
            if api_category:
                request_params.pop("sign", None)
                request_params["category"] = api_category
                request_params["sign"] = _make_sign(request_params)
            payload = self._fetch_json(
                CLS_TELEGRAPH_URL,
                params=request_params,
                context="CLS news telegraph",
                host="api3.cls.cn",
                mobile=True,
            )
            data = _cls_errno_data(payload, "CLS news telegraph")
            items = data.get("roll_data", []) if isinstance(data, Mapping) else []
            if not items:
                break
            page_min_ctime = last_time
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                ctime = _parse_int(item.get("ctime")) or 0
                if ctime:
                    page_min_ctime = min(page_min_ctime, ctime)
                if ctime < day_start or ctime > day_end:
                    continue
                news_id = _clean_text(item.get("id") or item.get("news_id"))
                if not news_id or news_id in seen_ids:
                    continue
                seen_ids.add(news_id)
                rows.append(
                    {
                        "news_id": news_id,
                        "title": _clean_text(item.get("title")),
                        "content": _clean_text(item.get("content")),
                        "publish_time": datetime.fromtimestamp(ctime).strftime("%Y-%m-%d %H:%M:%S") if ctime else None,
                        # date 列按发布日 YYYYMMDD 派生,用于分区落盘与跨文件去重
                        "date": date_text,
                        "ctime": ctime or None,
                        "category": category,
                    }
                )
                if len(rows) >= limit:
                    break
            # 翻页游标:本页最小 ctime;无进展或已翻过当天则停止
            if page_min_ctime >= last_time or page_min_ctime < day_start:
                break
            last_time = page_min_ctime
        self.last_meta = {"source_name": "财联社", "source_url": CLS_TELEGRAPH_URL, "date": date_text, "category": category, "limit": limit, "rows": len(rows)}
        return rows

    def _fetch_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any],
        context: str,
        host: str,
        mobile: bool = False,
    ) -> Any:
        query = {str(k): str(v) for k, v in params.items() if v is not None}
        request = Request(
            f"{url}?{urlencode(query)}",
            headers={
                "User-Agent": "okhttp/4.9.0" if mobile else "Mozilla/5.0 AxData/0.1",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://www.cls.cn/",
                "Host": host,
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

    def _open(self, request: Request) -> Any:
        if self._opener is not None:
            return self._opener(request, timeout=self._timeout)
        return urlopen(request, timeout=self._timeout)


def _make_sign(params: Mapping[str, Any]) -> str:
    text = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
    sha1 = hashlib.sha1(text.encode()).hexdigest()
    return hashlib.md5(sha1.encode()).hexdigest()


def _signed_params(**extra: Any) -> dict[str, Any]:
    params = {**_BASE_APP_PARAMS, **extra}
    params["sign"] = _make_sign(params)
    return params


def _cls_data(payload: Any, context: str) -> Any:
    if not isinstance(payload, Mapping) or payload.get("code") != 200:
        raise SourceUnavailableError(f"{context} returned unexpected payload.")
    return payload.get("data") or {}


def _cls_data_list(payload: Any, context: str) -> list[Mapping[str, Any]]:
    data = _cls_data(payload, context)
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, Mapping)]


def _cls_errno_data(payload: Any, context: str) -> Any:
    if not isinstance(payload, Mapping) or payload.get("errno") not in (0, "0", None):
        raise SourceUnavailableError(f"{context} returned unexpected payload.")
    return payload.get("data") or {}


def _normalize_cls_stock(item: Mapping[str, Any]) -> dict[str, Any]:
    secu_code = _clean_text(item.get("secu_code"))
    identity = _identity_from_cls_code(secu_code or "")
    return identity | {
        "secu_code": secu_code,
        "secu_name": _clean_text(item.get("secu_name")),
        "last_price": _parse_float(item.get("last_px")),
        "change_pct": _parse_float(item.get("change")),
    }


def _identity_from_cls_code(secu_code: str) -> dict[str, Any]:
    text = str(secu_code or "").strip().lower()
    match = re.match(r"^(sh|sz|bj)(\d{6})$", text)
    if not match:
        # 源端对北交所等代码有时返回 920274.BJ / 300647.SZ 点后缀格式,先归一化再识别
        try:
            text = _to_cls_secu_code(text)
        except SourceRequestValidationError:
            text = ""
        match = re.match(r"^(sh|sz|bj)(\d{6})$", text)
        if not match:
            return {"instrument_id": None, "symbol": None, "exchange": None, "secu_code": secu_code or None, "secu_name": None}
    prefix, symbol = match.groups()
    exchange = {"sh": "SSE", "sz": "SZSE", "bj": "BSE"}[prefix]
    suffix = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}[exchange]
    return {"instrument_id": f"{symbol}.{suffix}", "symbol": symbol, "exchange": exchange, "secu_code": text, "secu_name": None}


def _to_cls_secu_code(value: str) -> str:
    text = str(value or "").strip().lower()
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
    raise SourceRequestValidationError("code must be a six-digit A-share code")


def _normalize_date_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    digits = re.sub(r"\D", "", str(value))
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


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SourceRequestValidationError(f"{name} is required")
    return text


def _parse_float(value: Any) -> float | None:
    text = _clean_text(value)
    if text is None:
        return None
    text = text.replace("%", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    parsed = _parse_float(value)
    return int(parsed) if parsed is not None else None


def _parse_bool_value(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip() in {"1", "true", "True"}


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text or text in {"-", "--", "null", "None"}:
        return None
    return text
