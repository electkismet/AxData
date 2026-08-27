"""Sina Finance source request adapter."""

from __future__ import annotations

import re
import ast
import json
import math
from collections.abc import Mapping
from datetime import date as Date
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from html import unescape
from io import StringIO
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from axdata_core.source_errors import (
    SourceAdapterNotFound,
    SourceRequestValidationError,
    SourceUnavailableError,
)


SUPPORTED_INTERFACES = {
    "bond_cb_profile_sina",
    "bond_cb_summary_sina",
    "bond_gb_zh_sina",
    "bond_gb_us_sina",
    "currency_boc_sina",
    "fund_scale_close_sina",
    "fund_scale_open_sina",
    "fund_scale_structured_sina",
    "futures_display_main_sina",
    "futures_hold_pos_sina",
    "futures_main_sina",
    "futures_zh_daily_sina",
    "futures_zh_minute_sina",
    "rv_from_futures_zh_minute_sina",
    "fund_etf_category_sina",
    "fund_etf_dividend_sina",
    "fund_etf_hist_sina",
    "index_global_hist_sina",
    "index_stock_cons_sina",
    "index_us_stock_sina",
    "option_sse_codes_sina",
    "option_cffex_hs300_daily_sina",
    "option_cffex_hs300_list_sina",
    "option_cffex_hs300_spot_sina",
    "option_cffex_sz50_daily_sina",
    "option_cffex_sz50_list_sina",
    "option_cffex_sz50_spot_sina",
    "option_cffex_zz1000_daily_sina",
    "option_cffex_zz1000_list_sina",
    "option_cffex_zz1000_spot_sina",
    "option_commodity_contract_sina",
    "option_commodity_contract_table_sina",
    "option_commodity_hist_sina",
    "option_finance_minute_sina",
    "option_sse_daily_sina",
    "option_sse_expire_day_sina",
    "option_sse_greeks_sina",
    "option_sse_list_sina",
    "option_sse_minute_sina",
    "option_sse_spot_price_sina",
    "option_sse_underlying_spot_price_sina",
    "stock_financial_report_sina",
    "stock_classify_sina",
    "stock_info_global_sina",
    "stock_intraday_sina",
    "stock_esg_hz_sina",
    "stock_esg_msci_sina",
    "stock_esg_rate_sina",
    "stock_esg_rft_sina",
    "stock_esg_zd_sina",
    "stock_lhb_ggtj_sina",
    "stock_lhb_detail_daily_sina",
    "stock_lhb_jgmx_sina",
    "stock_lhb_jgzz_sina",
    "stock_lhb_yytj_sina",
    "stock_restricted_release_queue_sina",
    "stock_hk_index_daily_sina",
    "stock_hk_index_spot_sina",
    "stock_zh_index_spot_sina",
    "tool_trade_date_hist_sina",
}

FUND_ETF_CATEGORY_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/jsonp.php/"
    "IO.XSRV2.CallbackList['da_yPT46_Ll7K6WD']/Market_Center.getHQNodeDataSimple"
)
FUND_ETF_CATEGORY_MAP = {
    "封闭式基金": "close_fund",
    "ETF基金": "etf_hq_fund",
    "LOF基金": "lof_hq_fund",
}
INDEX_GLOBAL_SINA_SYMBOL_MAP = {
    "英国富时100指数": "UKX",
    "德国DAX 30种股价指数": "DAX",
    "俄罗斯MICEX指数": "INDEXCF",
    "法CAC40指数": "CAC",
    "瑞士股票指数": "SWI20",
    "富时意大利MIB指数": "FTSEMIB",
    "荷兰AEX综合指数": "AEX",
    "西班牙IBEX指数": "IBEX",
    "欧洲Stoxx50指数": "SX5E",
    "加拿大S&P/TSX综合指数": "GSPTSE",
    "墨西哥BOLSA指数": "MXX",
    "巴西BOVESPA股票指数": "IBOV",
    "中国台湾加权指数": "TWJQ",
    "日经225指数": "NKY",
    "首尔综合指数": "KOSPI",
    "印度尼西亚雅加达综合指数": "JCI",
    "印度孟买SENSEX指数": "SENSEX",
    "澳大利亚标准普尔200指数": "AS51",
    "新西兰NZSE 50指数": "NZ250",
    "埃及CASE 30指数": "CASE",
}
INDEX_GLOBAL_HIST_URL = "https://gi.finance.sina.com.cn/hq/daily"
INDEX_US_STOCK_SINA_NAMES = {
    ".INX": "标普500指数",
    ".IXIC": "纳斯达克综合指数",
    ".DJI": "道琼斯工业平均指数",
    ".NDX": "纳斯达克100指数",
}
SINA_HK_INDEX_DAILY_URL_TEMPLATE = "https://finance.sina.com.cn/stock/hkstock/{symbol}/klc2_kl.js"
INDEX_STOCK_CONS_HS300_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "Market_Center.getHQNodeData"
)
INDEX_STOCK_CONS_SIMPLE_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "Market_Center.getHQNodeDataSimple"
)
SINA_STOCK_CLASSIFY_DEFAULT_NODE = "new_blhy"
SINA_STOCK_CLASSIFY_DEFAULT_CATEGORY = "新浪行业"
SINA_STOCK_CLASSIFY_DEFAULT_CLASS_NAME = "玻璃行业"
SINA_OPTION_STOCK_NAME_URL = (
    "https://stock.finance.sina.com.cn/futures/api/openapi.php/"
    "StockOptionService.getStockName"
)
SINA_OPTION_DAILY_URL = (
    "https://stock.finance.sina.com.cn/futures/api/jsonp_v2.php//"
    "StockOptionDaylineService.getSymbolInfo"
)
SINA_OPTION_REMAINDER_DAY_URL = (
    "https://stock.finance.sina.com.cn/futures/api/openapi.php/"
    "StockOptionService.getRemainderDay"
)
SINA_OPTION_MINUTE_URL = (
    "https://stock.finance.sina.com.cn/futures/api/openapi.php/"
    "StockOptionDaylineService.getOptionMinline"
)
SINA_CFFEX_OPTION_DAYLINE_URL = (
    "https://stock.finance.sina.com.cn/futures/api/jsonp.php/"
    "var%20_{symbol}{callback_date}=/FutureOptionAllService.getOptionDayline"
)
SINA_CFFEX_HS300_OPTION_LIST_URL = "https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php"
SINA_CFFEX_SZ50_OPTION_LIST_URL = "https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php/ho/cffex"
SINA_CFFEX_ZZ1000_OPTION_LIST_URL = "https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php/mo/cffex"
SINA_CFFEX_OPTION_DATA_URL = "https://stock.finance.sina.com.cn/futures/api/openapi.php/OptionService.getOptionData"
SINA_COMMODITY_OPTION_PAGE_URL = "https://stock.finance.sina.com.cn/futures/view/optionsDP.php/pg_o/dce"
SINA_FINANCE_REPORT_URL = (
    "https://quotes.sina.cn/cn/api/openapi.php/"
    "CompanyFinanceService.getFinanceReport2022"
)
SINA_LHB_DETAIL_DAILY_URL = "https://vip.stock.finance.sina.com.cn/q/go.php/vInvestConsult/kind/lhb/index.phtml"
SINA_LHB_GGTJ_URL = "https://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/ggtj/index.phtml"
SINA_LHB_JGMX_URL = "https://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/jgmx/index.phtml"
SINA_LHB_JGZZ_URL = "https://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/jgzz/index.phtml"
SINA_LHB_YYTJ_URL = "https://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/yytj/index.phtml"
SINA_RESTRICTED_RELEASE_URL = "https://vip.stock.finance.sina.com.cn/q/go.php/vInvestConsult/kind/xsjj/index.phtml"
SINA_GLOBAL_INFO_FEED_URL = "https://zhibo.sina.com.cn/api/zhibo/feed"
SINA_STOCK_INTRADAY_URL = (
    "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "CN_Bill.GetBillList"
)
SINA_ESG_RFT_URL = "https://global.finance.sina.com.cn/api/openapi.php/EsgService.getRftEsgStocks"
SINA_ESG_MSCI_URL = "https://global.finance.sina.com.cn/api/openapi.php/EsgService.getMsciEsgStocks"
SINA_ESG_RATE_URL = "https://global.finance.sina.com.cn/api/openapi.php/EsgService.getEsgStocks"
SINA_ESG_ZD_URL = "https://global.finance.sina.com.cn/api/openapi.php/EsgService.getZdEsgStocks"
SINA_ESG_HZ_URL = "https://global.finance.sina.com.cn/api/openapi.php/EsgService.getHzEsgStocks"
SINA_TRADE_DATE_HIST_URL = "https://finance.sina.com.cn/realstock/company/klc_td_sh.txt"
SINA_BOND_CB_PROFILE_URL = "https://money.finance.sina.com.cn/bond/info/{symbol}.html"
SINA_BOND_CB_SUMMARY_URL = "https://money.finance.sina.com.cn/bond/quotes/{symbol}.html"
SINA_GB_DAILY_URL = "https://bond.finance.sina.com.cn/hq/gb/daily"
SINA_BOC_FOREX_URL = "http://biz.finance.sina.com.cn/forex/forex.php"
SINA_FUND_SCALE_CLOSE_URL = (
    "http://vip.stock.finance.sina.com.cn/fund_center/data/jsonp.php/"
    "IO.XSRV2.CallbackList['_bjN6KvXOkfPy2Bu']/NetValueReturn_Service.NetValueReturnClose"
)
SINA_FUND_SCALE_OPEN_URL = (
    "http://vip.stock.finance.sina.com.cn/fund_center/data/jsonp.php/"
    "IO.XSRV2.CallbackList['J2cW8KXheoWKdSHc']/NetValueReturn_Service.NetValueReturnOpen"
)
SINA_FUND_SCALE_STRUCTURED_URL = (
    "http://vip.stock.finance.sina.com.cn/fund_center/data/jsonp.php/"
    "IO.XSRV2.CallbackList['cRrwseM7NWX68rDa']/NetValueReturn_Service.NetValueReturnCX"
)
SINA_FUTURES_DATA_URL = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQFuturesData"
SINA_FUTURES_HOLD_POS_URL = "https://vip.stock.finance.sina.com.cn/q/view/vFutures_Positions_cjcc.php"
SINA_FUTURES_DAILY_KLINE_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
    "var%20_{symbol}{callback_date}=/InnerFuturesNewService.getDailyKLine"
)
SINA_FUTURES_KLINE_CALLBACK_DATE = "2021_08_17"
SINA_FUTURES_ZH_DAILY_KLINE_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
    "var%20_V21052021_4_12=/InnerFuturesNewService.getDailyKLine"
)
SINA_FUTURES_ZH_DAILY_QUERY_TYPE = "2021_4_12"
SINA_FUTURES_ZH_MINUTE_KLINE_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/=/"
    "InnerFuturesNewService.getFewMinLine"
)
SINA_HQ_LIST_URL = "https://hq.sinajs.cn/list="
SINA_HK_INDEX_SPOT_DEFAULT_SYMBOLS = ("hkHSI", "hkHSCEI", "hkHSTECH")
SINA_HK_INDEX_SPOT_ALL_SYMBOLS = (
    "hkCES100",
    "hkCES120",
    "hkCES280",
    "hkCES300",
    "hkCESA80",
    "hkCESG10",
    "hkCESHKM",
    "hkCSCMC",
    "hkCSHK100",
    "hkCSHKDIV",
    "hkCSHKLC",
    "hkCSHKLRE",
    "hkCSHKMCS",
    "hkCSHKME",
    "hkCSHKPE",
    "hkCSHKSE",
    "hkCSI300",
    "hkCSRHK50",
    "hkGEM",
    "hkHKL",
    "hkHSCCI",
    "hkHSCEI",
    "hkHSI",
    "hkHSMBI",
    "hkHSMOGI",
    "hkHSMPI",
    "hkHSTECH",
    "hkSSE180",
    "hkSSE180GV",
    "hkSSE380",
    "hkSSE50",
    "hkSSECEQT",
    "hkSSECOMP",
    "hkSSEDIV",
    "hkSSEITOP",
    "hkSSEMCAP",
    "hkSSEMEGA",
    "hkVHSI",
)
SINA_KLC_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
SINA_BOC_CURRENCY_CODES = {
    "美元": "USD",
    "英镑": "GBP",
    "欧元": "EUR",
    "澳门元": "MOP",
    "泰国铢": "THP",
    "菲律宾比索": "PHP",
    "港币": "HKD",
    "瑞士法郎": "CHF",
    "新加坡元": "SGD",
    "瑞典克朗": "SEK",
    "丹麦克朗": "DKK",
    "挪威克朗": "NOK",
    "日元": "JPY",
    "加拿大元": "CAD",
    "澳大利亚元": "AUD",
    "新西兰元": "NZD",
    "韩国元": "KRW",
}
SINA_FUND_SCALE_OPEN_TYPES = {
    "股票型基金": "2",
    "混合型基金": "1",
    "债券型基金": "3",
    "货币型基金": "5",
    "QDII基金": "6",
}
SINA_GB_US_SYMBOLS = {
    "美国1月期国债": "US1MT",
    "美国2月期国债": "US2MT",
    "美国3月期国债": "US3MT",
    "美国4月期国债": "US4MT",
    "美国6月期国债": "US6MT",
    "美国1年期国债": "US1YT",
    "美国2年期国债": "US2YT",
    "美国3年期国债": "US3YT",
    "美国5年期国债": "US5YT",
    "美国7年期国债": "US7YT",
    "美国10年期国债": "US10YT",
    "美国20年期国债": "US20YT",
    "美国30年期国债": "US30YT",
}
SINA_GB_ZH_SYMBOLS = {
    "中国1年期国债": "CN1YT",
    "中国2年期国债": "CN2YT",
    "中国3年期国债": "CN3YT",
    "中国5年期国债": "CN5YT",
    "中国7年期国债": "CN7YT",
    "中国10年期国债": "CN10YT",
    "中国15年期国债": "CN15YT",
    "中国20年期国债": "CN20YT",
    "中国30年期国债": "CN30YT",
}
SSE_OPTION_UNDERLYING_CATEGORIES = {
    "510050": "50ETF",
    "510300": "300ETF",
    "510500": "500ETF",
    "588000": "科创50",
    "588080": "科创板50",
}
SSE_OPTION_CATEGORY_UNDERLYINGS = {
    category: code for code, category in SSE_OPTION_UNDERLYING_CATEGORIES.items()
}
SSE_OPTION_DIRECTIONS = {
    "看涨期权": "UP",
    "认购": "UP",
    "call": "UP",
    "up": "UP",
    "看跌期权": "DOWN",
    "认沽": "DOWN",
    "put": "DOWN",
    "down": "DOWN",
}
SSE_OPTION_DIRECTION_LABELS = {
    "UP": "看涨期权",
    "DOWN": "看跌期权",
}
SINA_FINANCE_REPORT_SOURCES = {
    "资产负债表": ("balance", "资产负债表", "fzb"),
    "balance": ("balance", "资产负债表", "fzb"),
    "利润表": ("income", "利润表", "lrb"),
    "income": ("income", "利润表", "lrb"),
    "现金流量表": ("cashflow", "现金流量表", "llb"),
    "cashflow": ("cashflow", "现金流量表", "llb"),
}


class SinaRequestAdapter:
    """Request Sina Finance source interfaces and return normalized rows."""

    source = "sina"

    def __init__(self, opener: Any | None = None, *, timeout: float = 20.0) -> None:
        self._opener = opener
        self._timeout = timeout
        self.last_meta: dict[str, Any] = {}

    def supports(self, interface_name: str) -> bool:
        return interface_name in SUPPORTED_INTERFACES

    def request(self, interface_name: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if interface_name == "stock_lhb_jgmx_sina":
            return self._request_stock_lhb_jgmx(params)
        if interface_name == "stock_lhb_jgzz_sina":
            return self._request_stock_lhb_jgzz(params)
        if interface_name == "stock_lhb_yytj_sina":
            return self._request_stock_lhb_yytj(params)
        if interface_name == "stock_restricted_release_queue_sina":
            return self._request_stock_restricted_release_queue(params)
        if interface_name == "stock_hk_index_daily_sina":
            return self._request_stock_hk_index_daily(params)
        if interface_name == "stock_hk_index_spot_sina":
            return self._request_stock_hk_index_spot(params)
        if interface_name == "stock_classify_sina":
            return self._request_stock_classify(params)
        if interface_name == "stock_info_global_sina":
            return self._request_stock_info_global(params)
        if interface_name == "stock_intraday_sina":
            return self._request_stock_intraday(params)
        if interface_name == "stock_esg_hz_sina":
            return self._request_stock_esg_hz(params)
        if interface_name == "stock_esg_msci_sina":
            return self._request_stock_esg_msci(params)
        if interface_name == "stock_esg_rate_sina":
            return self._request_stock_esg_rate(params)
        if interface_name == "stock_esg_rft_sina":
            return self._request_stock_esg_rft(params)
        if interface_name == "stock_esg_zd_sina":
            return self._request_stock_esg_zd(params)
        if interface_name == "stock_zh_index_spot_sina":
            return self._request_stock_zh_index_spot(params)
        if interface_name == "tool_trade_date_hist_sina":
            return self._request_tool_trade_date_hist(params)
        if interface_name == "stock_lhb_ggtj_sina":
            return self._request_stock_lhb_ggtj(params)
        if interface_name == "stock_lhb_detail_daily_sina":
            return self._request_stock_lhb_detail_daily(params)
        if interface_name == "stock_financial_report_sina":
            return self._request_stock_financial_report(params)
        if interface_name == "option_sse_underlying_spot_price_sina":
            return self._request_option_sse_underlying_spot_price(params)
        if interface_name == "option_sse_spot_price_sina":
            return self._request_option_sse_spot_price(params)
        if interface_name == "option_sse_minute_sina":
            return self._request_option_sse_minute(params)
        if interface_name == "option_finance_minute_sina":
            return self._request_option_sse_minute(params, interface_name=interface_name)
        if interface_name == "option_sse_list_sina":
            return self._request_option_sse_list(params)
        if interface_name == "option_sse_greeks_sina":
            return self._request_option_sse_greeks(params)
        if interface_name == "option_sse_expire_day_sina":
            return self._request_option_sse_expire_day(params)
        if interface_name == "option_sse_daily_sina":
            return self._request_option_sse_daily(params)
        if interface_name == "option_sse_codes_sina":
            return self._request_option_sse_codes(params)
        if interface_name == "option_cffex_hs300_daily_sina":
            return self._request_option_cffex_daily(
                params,
                interface_name=interface_name,
                default_symbol="io2202P4350",
                symbol_prefix="io",
                underlying_name="沪深300指数",
            )
        if interface_name == "option_cffex_hs300_list_sina":
            return self._request_option_cffex_list(
                params,
                interface_name=interface_name,
                url=SINA_CFFEX_HS300_OPTION_LIST_URL,
                product_code="io",
                underlying_name="沪深300指数",
            )
        if interface_name == "option_cffex_hs300_spot_sina":
            return self._request_option_cffex_spot(
                params,
                interface_name=interface_name,
                default_symbol="io2607",
                product_code="io",
                underlying_name="沪深300指数",
            )
        if interface_name == "option_cffex_sz50_daily_sina":
            return self._request_option_cffex_daily(
                params,
                interface_name=interface_name,
                default_symbol="ho2303P2350",
                symbol_prefix="ho",
                underlying_name="上证50指数",
            )
        if interface_name == "option_cffex_sz50_list_sina":
            return self._request_option_cffex_list(
                params,
                interface_name=interface_name,
                url=SINA_CFFEX_SZ50_OPTION_LIST_URL,
                product_code="ho",
                underlying_name="上证50指数",
            )
        if interface_name == "option_cffex_sz50_spot_sina":
            return self._request_option_cffex_spot(
                params,
                interface_name=interface_name,
                default_symbol="ho2609",
                product_code="ho",
                underlying_name="上证50指数",
            )
        if interface_name == "option_cffex_zz1000_daily_sina":
            return self._request_option_cffex_daily(
                params,
                interface_name=interface_name,
                default_symbol="mo2609P6200",
                symbol_prefix="mo",
                underlying_name="中证1000指数",
            )
        if interface_name == "option_cffex_zz1000_list_sina":
            return self._request_option_cffex_list(
                params,
                interface_name=interface_name,
                url=SINA_CFFEX_ZZ1000_OPTION_LIST_URL,
                product_code="mo",
                underlying_name="中证1000指数",
            )
        if interface_name == "option_cffex_zz1000_spot_sina":
            return self._request_option_cffex_spot(
                params,
                interface_name=interface_name,
                default_symbol="mo2607",
                product_code="mo",
                underlying_name="中证1000指数",
            )
        if interface_name == "option_commodity_contract_sina":
            return self._request_option_commodity_contracts(params)
        if interface_name == "option_commodity_contract_table_sina":
            return self._request_option_commodity_contract_table(params)
        if interface_name == "option_commodity_hist_sina":
            return self._request_option_commodity_hist(params)
        if interface_name == "index_stock_cons_sina":
            return self._request_index_stock_cons(params)
        if interface_name == "index_global_hist_sina":
            return self._request_index_global_hist(params)
        if interface_name == "index_us_stock_sina":
            return self._request_index_us_stock(params)
        if interface_name == "fund_etf_dividend_sina":
            return self._request_fund_etf_dividend(params)
        if interface_name == "fund_etf_hist_sina":
            return self._request_fund_etf_hist(params)
        if interface_name == "bond_cb_profile_sina":
            return self._request_bond_cb_profile(params)
        if interface_name == "bond_cb_summary_sina":
            return self._request_bond_cb_summary(params)
        if interface_name == "bond_gb_us_sina":
            return self._request_bond_gb_us(params)
        if interface_name == "bond_gb_zh_sina":
            return self._request_bond_gb_zh(params)
        if interface_name == "currency_boc_sina":
            return self._request_currency_boc(params)
        if interface_name == "fund_scale_close_sina":
            return self._request_fund_scale_close(params)
        if interface_name == "fund_scale_open_sina":
            return self._request_fund_scale_open(params)
        if interface_name == "fund_scale_structured_sina":
            return self._request_fund_scale_structured(params)
        if interface_name == "futures_display_main_sina":
            return self._request_futures_display_main(params)
        if interface_name == "futures_hold_pos_sina":
            return self._request_futures_hold_pos(params)
        if interface_name == "futures_main_sina":
            return self._request_futures_main(params)
        if interface_name == "futures_zh_daily_sina":
            return self._request_futures_zh_daily(params)
        if interface_name == "futures_zh_minute_sina":
            return self._request_futures_zh_minute(params)
        if interface_name == "rv_from_futures_zh_minute_sina":
            return self._request_rv_from_futures_zh_minute(params)
        if interface_name == "fund_etf_category_sina":
            return self._request_fund_etf_category(params)
        raise SourceAdapterNotFound(
            f"Sina source adapter does not support interface {interface_name!r}."
        )

    def _request_fund_etf_category(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"category", "symbol", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for fund_etf_category_sina: {unknown}")
        category = str(params.get("category", params.get("symbol") or "LOF基金")).strip()
        if category not in FUND_ETF_CATEGORY_MAP:
            raise SourceRequestValidationError("category must be one of 封闭式基金, ETF基金, or LOF基金")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 5000)
        url = _url_with_query(
            FUND_ETF_CATEGORY_URL,
            {
                "page": str(page),
                "num": str(limit),
                "sort": "symbol",
                "asc": "0",
                "node": FUND_ETF_CATEGORY_MAP[category],
                "[object HTMLDivElement]": "qvvne",
            },
        )
        text = self._fetch_text(
            url,
            context="Sina ETF category",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/fund_center/index.html",
            },
        )
        raw_rows = _parse_jsonp_array(text)
        rows = [
            row
            for row in (
                _normalize_fund_etf_category_row(item, category)
                for item in raw_rows
            )
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪基金",
            "source_url": url,
            "category": category,
            "node": FUND_ETF_CATEGORY_MAP[category],
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_fund_etf_dividend(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for fund_etf_dividend_sina: {unknown}")
        code_info = _sina_market_symbol(str(params.get("symbol") or "sh510050"))
        if code_info is None:
            raise SourceRequestValidationError("symbol must be a Sina fund code such as sh510050 or 510050.SH")
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 5000)
        url = f"https://finance.sina.com.cn/realstock/company/{code_info['sina_symbol']}/hfq.js"
        text = self._fetch_text(
            url,
            context="Sina ETF dividend",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": f"https://finance.sina.com.cn/fund/quotes/{code_info['fund_code']}/bc.shtml",
            },
        )
        rows: list[dict[str, Any]] = []
        for item in _parse_sina_js_data_array(text):
            if isinstance(item, Mapping):
                raw_date = item.get("d") or item.get("date")
                raw_dividend = item.get("u")
            elif isinstance(item, (list, tuple)) and len(item) >= 4:
                raw_date = item[0]
                raw_dividend = item[3]
            else:
                continue
            dividend_date = _normalize_report_date(raw_date)
            if dividend_date in (None, "19000101"):
                continue
            if start_date and dividend_date < start_date:
                continue
            if end_date and dividend_date > end_date:
                continue
            rows.append(
                {
                    **code_info,
                    "dividend_date": dividend_date,
                    "accumulated_dividend": _parse_float(raw_dividend),
                }
            )
        rows.sort(key=lambda row: row["dividend_date"])
        self.last_meta = {
            "source_name": "新浪基金",
            "source_url": url,
            "symbol": code_info["sina_symbol"],
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_fund_etf_hist(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for fund_etf_hist_sina: {unknown}")
        code_info = _sina_market_symbol(str(params.get("symbol") or "sh510050"))
        if code_info is None:
            raise SourceRequestValidationError("symbol must be a Sina fund code such as sh510050 or 510050.SH")
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        url = f"https://finance.sina.com.cn/realstock/company/{code_info['sina_symbol']}/hisdata_klc2/klc_kl.js"
        text = self._fetch_text(
            url,
            context="Sina ETF daily KLC",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": f"https://finance.sina.com.cn/fund/quotes/{code_info['fund_code']}/bc.shtml",
            },
        )
        encoded = _extract_sina_encoded_string(text, context="Sina ETF daily KLC")
        rows: list[dict[str, Any]] = []
        for item in _decode_sina_klc_k2_rows(encoded):
            trade_date = item["trade_date"]
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append({**code_info, **item})
        rows.sort(key=lambda row: row["trade_date"])
        self.last_meta = {
            "source_name": "新浪 ETF 行情",
            "source_url": url,
            "symbol": code_info["sina_symbol"],
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": min(len(rows), limit),
            "decoder": "pure_python_sina_klc_k2",
        }
        return rows[-limit:]

    def _request_bond_cb_profile(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for bond_cb_profile_sina: {unknown}")
        code_info = _sina_stock_symbol(str(params.get("symbol") or "sz128039"))
        if code_info is None:
            raise SourceRequestValidationError("symbol must be a six-digit bond code, AxData id, or Sina symbol")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        url = SINA_BOND_CB_PROFILE_URL.format(symbol=code_info["sina_symbol"])
        html = self._fetch_text(
            url,
            context="Sina convertible bond profile",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://money.finance.sina.com.cn/bond/",
            },
            fallback_encoding="gb18030",
        )
        rows = _parse_bond_cb_profile_html(html, code_info=code_info, limit=limit)
        self.last_meta = {
            "source_name": "新浪债券",
            "source_url": url,
            "symbol": code_info["sina_symbol"],
            "instrument_id": code_info["instrument_id"],
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_bond_cb_summary(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for bond_cb_summary_sina: {unknown}")
        code_info = _sina_stock_symbol(str(params.get("symbol") or "sh155255"))
        if code_info is None:
            raise SourceRequestValidationError("symbol must be a six-digit bond code, AxData id, or Sina symbol")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        url = SINA_BOND_CB_SUMMARY_URL.format(symbol=code_info["sina_symbol"])
        html = self._fetch_text(
            url,
            context="Sina convertible bond summary",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://money.finance.sina.com.cn/bond/",
            },
            fallback_encoding="gb18030",
        )
        rows = _parse_bond_cb_summary_html(html, code_info=code_info, limit=limit)
        self.last_meta = {
            "source_name": "新浪债券",
            "source_url": url,
            "symbol": code_info["sina_symbol"],
            "instrument_id": code_info["instrument_id"],
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_bond_gb_us(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for bond_gb_us_sina: {unknown}")
        requested_symbol = str(params.get("symbol") or "美国10年期国债").strip()
        source_symbol = SINA_GB_US_SYMBOLS.get(requested_symbol, requested_symbol.upper())
        if source_symbol not in set(SINA_GB_US_SYMBOLS.values()):
            raise SourceRequestValidationError("symbol must be a supported U.S. Treasury name or Sina code")
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        url = _url_with_query(SINA_GB_DAILY_URL, {"symbol": source_symbol})
        payload = self._fetch_json(
            url,
            context="Sina U.S. government bond yield",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": f"https://stock.finance.sina.com.cn/forex/globalbd/{source_symbol.lower()}.html",
            },
        )
        data = payload.get("result", {}).get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina U.S. government bond yield returned unexpected payload.")
        display_name = _sina_gb_display_name(SINA_GB_US_SYMBOLS, source_symbol)
        rows = []
        for item in data:
            row = _normalize_gb_daily_row(item, source_symbol=source_symbol, display_name=display_name, country="US")
            if row is None:
                continue
            if start_date and row["trade_date"] < start_date:
                continue
            if end_date and row["trade_date"] > end_date:
                continue
            rows.append(row)
        rows.sort(key=lambda row: row["trade_date"])
        returned_rows = rows[:limit] if start_date or end_date else rows[-limit:]
        self.last_meta = {
            "source_name": "新浪债券",
            "source_url": url,
            "requested_symbol": requested_symbol,
            "source_symbol": source_symbol,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": len(returned_rows),
        }
        return returned_rows

    def _request_bond_gb_zh(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for bond_gb_zh_sina: {unknown}")
        requested_symbol = str(params.get("symbol") or "中国10年期国债").strip()
        source_symbol = SINA_GB_ZH_SYMBOLS.get(requested_symbol, requested_symbol.upper())
        if source_symbol not in set(SINA_GB_ZH_SYMBOLS.values()):
            raise SourceRequestValidationError("symbol must be a supported China government bond name or Sina code")
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        url = _url_with_query(SINA_GB_DAILY_URL, {"symbol": source_symbol})
        payload = self._fetch_json(
            url,
            context="Sina China government bond yield",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": f"https://stock.finance.sina.com.cn/forex/globalbd/{source_symbol.lower()}.html",
            },
        )
        data = payload.get("result", {}).get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina China government bond yield returned unexpected payload.")
        display_name = _sina_gb_display_name(SINA_GB_ZH_SYMBOLS, source_symbol)
        rows = []
        for item in data:
            row = _normalize_gb_daily_row(item, source_symbol=source_symbol, display_name=display_name, country="CN")
            if row is None:
                continue
            if start_date and row["trade_date"] < start_date:
                continue
            if end_date and row["trade_date"] > end_date:
                continue
            rows.append(row)
        rows.sort(key=lambda row: row["trade_date"])
        returned_rows = rows[:limit] if start_date or end_date else rows[-limit:]
        self.last_meta = {
            "source_name": "新浪债券",
            "source_url": url,
            "requested_symbol": requested_symbol,
            "source_symbol": source_symbol,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": len(returned_rows),
        }
        return returned_rows

    def _request_currency_boc(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for currency_boc_sina: {unknown}")
        requested_symbol = str(params.get("symbol") or "美元").strip()
        currency_code = SINA_BOC_CURRENCY_CODES.get(requested_symbol, requested_symbol.upper())
        if currency_code not in set(SINA_BOC_CURRENCY_CODES.values()):
            raise SourceRequestValidationError("symbol must be a supported currency Chinese name or ISO code")
        currency_name = _sina_gb_display_name(SINA_BOC_CURRENCY_CODES, currency_code) or requested_symbol
        start_date = _normalize_query_date(params.get("start_date") or _recent_window_start(), "start_date", required=True)
        end_date = _normalize_query_date(params.get("end_date") or _today_yyyymmdd(), "end_date", required=True)
        if start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 100)
        url = _url_with_query(
            SINA_BOC_FOREX_URL,
            {
                "money_code": currency_code,
                "type": "0",
                "startdate": _hyphen_date(start_date),
                "enddate": _hyphen_date(end_date),
                "page": str(page),
                "call_type": "ajax",
            },
        )
        html = self._fetch_text(
            url,
            context="Sina BOC forex quotes",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://finance.sina.com.cn/",
            },
            fallback_encoding="gbk",
        )
        rows = _parse_currency_boc_html(html, currency_code=currency_code, currency_name=currency_name, limit=limit)
        rows.sort(key=lambda row: row["quote_date"])
        self.last_meta = {
            "source_name": "新浪财经",
            "source_url": url,
            "requested_symbol": requested_symbol,
            "currency_code": currency_code,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_fund_scale_close(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for fund_scale_close_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        url = _url_with_query(
            SINA_FUND_SCALE_CLOSE_URL,
            {
                "page": str(page),
                "num": str(limit),
                "sort": "zmjgm",
                "asc": "0",
                "ccode": "",
                "type2": "",
                "type3": "",
            },
        )
        text = self._fetch_text(
            url,
            context="Sina closed-end fund scale",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/fund_center/index.html",
            },
            fallback_encoding="gb18030",
        )
        payload = _parse_jsonp_object(text, context="Sina closed-end fund scale")
        data = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina closed-end fund scale returned unexpected payload.")
        rows = [
            row
            for row in (
                _normalize_fund_scale_row(item, fund_category="封闭式基金", rank=(index + 1) + (page - 1) * limit)
                for index, item in enumerate(data)
            )
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪基金",
            "source_url": url,
            "fund_category": "封闭式基金",
            "page": page,
            "limit": limit,
            "total_num": _parse_int(payload.get("total_num")),
            "count": len(rows),
        }
        return rows[:limit]

    def _request_fund_scale_open(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for fund_scale_open_sina: {unknown}")
        requested_symbol = str(params.get("symbol") or "股票型基金").strip()
        type_code = SINA_FUND_SCALE_OPEN_TYPES.get(requested_symbol, requested_symbol)
        if type_code not in set(SINA_FUND_SCALE_OPEN_TYPES.values()):
            raise SourceRequestValidationError("symbol must be a supported open-end fund type or source type code")
        fund_category = _sina_gb_display_name(SINA_FUND_SCALE_OPEN_TYPES, type_code) or requested_symbol
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        url = _url_with_query(
            SINA_FUND_SCALE_OPEN_URL,
            {
                "page": str(page),
                "num": str(limit),
                "sort": "zmjgm",
                "asc": "0",
                "ccode": "",
                "type2": type_code,
                "type3": "",
            },
        )
        text = self._fetch_text(
            url,
            context="Sina open-end fund scale",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/fund_center/index.html",
            },
            fallback_encoding="gb18030",
        )
        payload = _parse_jsonp_object(text, context="Sina open-end fund scale")
        data = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina open-end fund scale returned unexpected payload.")
        rows = [
            row
            for row in (
                _normalize_fund_scale_row(item, fund_category=fund_category, rank=(index + 1) + (page - 1) * limit)
                for index, item in enumerate(data)
            )
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪基金",
            "source_url": url,
            "fund_category": fund_category,
            "type_code": type_code,
            "page": page,
            "limit": limit,
            "total_num": _parse_int(payload.get("total_num")),
            "count": len(rows),
        }
        return rows[:limit]

    def _request_fund_scale_structured(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for fund_scale_structured_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        url = _url_with_query(
            SINA_FUND_SCALE_STRUCTURED_URL,
            {
                "page": str(page),
                "num": str(limit),
                "sort": "zmjgm",
                "asc": "0",
                "ccode": "",
                "type2": "",
                "type3": "",
            },
        )
        text = self._fetch_text(
            url,
            context="Sina structured fund scale",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/fund_center/index.html",
            },
            fallback_encoding="gb18030",
        )
        payload = _parse_jsonp_object(text, context="Sina structured fund scale")
        data = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina structured fund scale returned unexpected payload.")
        rows = [
            row
            for row in (
                _normalize_fund_scale_row(item, fund_category="分级子基金", rank=(index + 1) + (page - 1) * limit)
                for index, item in enumerate(data)
            )
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪基金",
            "source_url": url,
            "fund_category": "分级子基金",
            "page": page,
            "limit": limit,
            "total_num": _parse_int(payload.get("total_num")),
            "count": len(rows),
        }
        return rows[:limit]

    def _request_futures_display_main(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"node", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for futures_display_main_sina: {unknown}")
        node = str(params.get("node") or "lwg_qh").strip()
        if not re.fullmatch(r"[A-Za-z0-9_]+", node):
            raise SourceRequestValidationError("node must be a Sina futures node such as lwg_qh")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=5, name="limit"), 100)
        url = _url_with_query(
            SINA_FUTURES_DATA_URL,
            {
                "page": str(page),
                "num": str(limit),
                "sort": "position",
                "asc": "0",
                "node": node,
                "base": "futures",
            },
        )
        payload = self._fetch_json(
            url,
            context="Sina futures main contract display",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://finance.sina.com.cn/futuremarket/index.shtml",
            },
        )
        if not isinstance(payload, list):
            raise SourceUnavailableError("Sina futures main contract display returned unexpected payload.")
        rows = [
            row
            for row in (
                _normalize_futures_quote_row(item, source_node=node, rank=(index + 1) + (page - 1) * limit)
                for index, item in enumerate(payload)
            )
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪期货",
            "source_url": url,
            "node": node,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_futures_hold_pos(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "contract", "date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for futures_hold_pos_sina: {unknown}")
        metric = str(params.get("symbol") or "多单持仓").strip()
        metric_to_table = {"成交量": 2, "多单持仓": 3, "空单持仓": 4}
        if metric not in metric_to_table:
            raise SourceRequestValidationError("symbol must be one of 成交量, 多单持仓, or 空单持仓")
        contract = str(params.get("contract") or "OI2501").strip().upper()
        if not re.fullmatch(r"[A-Z]{1,4}\d{3,4}", contract):
            raise SourceRequestValidationError("contract must be a futures contract such as OI2501")
        trade_date = _normalize_query_date(params.get("date") or _today_yyyymmdd(), "date", required=True)
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 100)
        url = _url_with_query(SINA_FUTURES_HOLD_POS_URL, {"t_breed": contract, "t_date": _hyphen_date(trade_date)})
        html = self._fetch_text(
            url,
            context="Sina futures holding position",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/",
            },
            fallback_encoding="gb18030",
        )
        rows = _parse_futures_hold_pos_html(
            html,
            metric=metric,
            table_index=metric_to_table[metric],
            contract=contract,
            trade_date=trade_date,
            limit=limit,
        )
        self.last_meta = {
            "source_name": "新浪期货",
            "source_url": url,
            "metric": metric,
            "contract": contract,
            "trade_date": trade_date,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_futures_main(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for futures_main_sina: {unknown}")
        source_symbol = str(params.get("symbol") or "CF0").strip().upper()
        if not re.fullmatch(r"[A-Z]{1,4}0", source_symbol):
            raise SourceRequestValidationError("symbol must be a Sina main-continuous futures symbol such as CF0")
        start_date = _normalize_query_date(params.get("start_date") or _recent_window_start(), "start_date", required=True)
        end_date = _normalize_query_date(params.get("end_date") or _today_yyyymmdd(), "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        url = _url_with_query(
            SINA_FUTURES_DAILY_KLINE_URL.format(
                symbol=source_symbol,
                callback_date=SINA_FUTURES_KLINE_CALLBACK_DATE,
            ),
            {"symbol": source_symbol, "_": SINA_FUTURES_KLINE_CALLBACK_DATE},
        )
        text = self._fetch_text(
            url,
            context="Sina futures main continuous daily kline",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": "https://finance.sina.com.cn/futuremarket/index.shtml",
            },
        )
        rows: list[dict[str, Any]] = []
        for item in _parse_jsonp_array(text, context="Sina futures main continuous daily kline"):
            row = _normalize_futures_daily_kline_row(item, source_symbol=source_symbol)
            if row is None:
                continue
            trade_date = row["trade_date"]
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        self.last_meta = {
            "source_name": "新浪期货",
            "source_url": url,
            "source_symbol": source_symbol,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_futures_zh_daily(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for futures_zh_daily_sina: {unknown}")
        source_symbol = str(params.get("symbol") or "RB0").strip().upper()
        if not re.fullmatch(r"[A-Z]{1,4}\d{1,4}", source_symbol):
            raise SourceRequestValidationError("symbol must be a Sina futures symbol such as RB0 or RB2410")
        start_date = _normalize_query_date(params.get("start_date") or _recent_window_start(), "start_date", required=True)
        end_date = _normalize_query_date(params.get("end_date") or _today_yyyymmdd(), "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        url = _url_with_query(
            SINA_FUTURES_ZH_DAILY_KLINE_URL,
            {"symbol": source_symbol, "type": SINA_FUTURES_ZH_DAILY_QUERY_TYPE},
        )
        text = self._fetch_text(
            url,
            context="Sina futures daily kline",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": f"https://finance.sina.com.cn/futures/quotes/{source_symbol}.shtml",
            },
        )
        rows: list[dict[str, Any]] = []
        for item in _parse_jsonp_array(text, context="Sina futures daily kline"):
            row = _normalize_futures_daily_kline_row(item, source_symbol=source_symbol)
            if row is None:
                continue
            trade_date = row["trade_date"]
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        self.last_meta = {
            "source_name": "新浪期货",
            "source_url": url,
            "source_symbol": source_symbol,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_futures_zh_minute(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "period", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for futures_zh_minute_sina: {unknown}")
        source_symbol = str(params.get("symbol") or "RB0").strip().upper()
        if not re.fullmatch(r"[A-Z]{1,4}\d{1,4}", source_symbol):
            raise SourceRequestValidationError("symbol must be a Sina futures symbol such as RB0 or IF2008")
        period = str(params.get("period") or "1").strip()
        if period not in {"1", "5", "15", "30", "60"}:
            raise SourceRequestValidationError("period must be one of 1, 5, 15, 30, or 60")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 2000)
        url = _url_with_query(SINA_FUTURES_ZH_MINUTE_KLINE_URL, {"symbol": source_symbol, "type": period})
        text = self._fetch_text(
            url,
            context="Sina futures minute kline",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": f"https://finance.sina.com.cn/futures/quotes/{source_symbol}.shtml",
            },
        )
        rows = [
            row
            for row in (
                _normalize_futures_minute_kline_row(item, source_symbol=source_symbol, period=period)
                for item in _parse_jsonp_array(text, context="Sina futures minute kline")
            )
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪期货",
            "source_url": url,
            "source_symbol": source_symbol,
            "period": period,
            "limit": limit,
            "count": min(len(rows), limit),
        }
        return rows[:limit]

    def _request_rv_from_futures_zh_minute(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "period", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(
                f"Unknown param(s) for rv_from_futures_zh_minute_sina: {unknown}"
            )
        minute_rows = self._request_futures_zh_minute(params)
        minute_meta = dict(self.last_meta)
        close_rows = [
            row for row in sorted(minute_rows, key=lambda item: item["datetime"])
            if row.get("close") is not None and row["close"] > 0
        ]
        if len(close_rows) < 2:
            self.last_meta = {
                **minute_meta,
                "source_name": "新浪期货",
                "derived_from": "futures_zh_minute_sina",
                "count": 0,
                "sample_count": len(close_rows),
                "return_count": 0,
            }
            return []
        returns = [
            math.log(float(current["close"]) / float(previous["close"]))
            for previous, current in zip(close_rows, close_rows[1:])
        ]
        realized_variance = sum(value * value for value in returns)
        row = {
            "source_symbol": close_rows[0]["source_symbol"],
            "period": close_rows[0]["period"],
            "start_datetime": close_rows[0]["datetime"],
            "end_datetime": close_rows[-1]["datetime"],
            "sample_count": len(close_rows),
            "return_count": len(returns),
            "realized_variance": realized_variance,
            "realized_volatility": math.sqrt(realized_variance),
        }
        self.last_meta = {
            **minute_meta,
            "source_name": "新浪期货",
            "derived_from": "futures_zh_minute_sina",
            "count": 1,
            "sample_count": len(close_rows),
            "return_count": len(returns),
        }
        return [row]

    def _request_stock_financial_report(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"stock", "symbol", "limit", "item_limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_financial_report_sina: {unknown}")
        stock_info = _sina_stock_symbol(str(params.get("stock") or "sh600600"))
        if stock_info is None:
            raise SourceRequestValidationError("stock must be a Sina stock symbol such as sh600600 or 600600.SH")
        statement_type, statement_name, source = _sina_finance_report_source(str(params.get("symbol") or "资产负债表"))
        period_limit = min(_parse_positive_int(params.get("limit"), default=1, name="limit"), 100)
        item_limit = min(_parse_positive_int(params.get("item_limit"), default=200, name="item_limit"), 2000)
        url = _url_with_query(
            SINA_FINANCE_REPORT_URL,
            {
                "paperCode": stock_info["sina_symbol"],
                "source": source,
                "type": "0",
                "page": "1",
                "num": str(period_limit),
            },
        )
        payload = self._fetch_json(
            url,
            context="Sina stock financial report",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://quotes.sina.cn/",
            },
        )
        data = payload.get("result", {}).get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, Mapping):
            raise SourceUnavailableError("Sina stock financial report returned unexpected payload.")
        report_dates = data.get("report_date")
        report_list = data.get("report_list")
        if not isinstance(report_dates, list) or not isinstance(report_list, Mapping):
            raise SourceUnavailableError("Sina stock financial report missing report list.")
        rows: list[dict[str, Any]] = []
        for date_info in report_dates[:period_limit]:
            if not isinstance(date_info, Mapping):
                continue
            report_date = _normalize_report_date(date_info.get("date_value"))
            if report_date is None:
                continue
            report = report_list.get(report_date)
            if not isinstance(report, Mapping):
                continue
            items = report.get("data")
            if not isinstance(items, list):
                continue
            for item in items[:item_limit]:
                row = _normalize_finance_report_item(
                    item,
                    report=report,
                    date_info=date_info,
                    report_date=report_date,
                    stock_info=stock_info,
                    statement_type=statement_type,
                    statement_name=statement_name,
                )
                if row is not None:
                    rows.append(row)
        self.last_meta = {
            "source_name": "新浪财经 JSON 财务报表",
            "source_url": url,
            "symbol": stock_info["sina_symbol"],
            "statement_type": statement_type,
            "statement_name": statement_name,
            "period_limit": period_limit,
            "item_limit": item_limit,
            "count": len(rows),
        }
        return rows

    def _request_stock_lhb_detail_daily(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_lhb_detail_daily_sina: {unknown}")
        requested = _normalize_query_date(params.get("date"), "date", required=False)
        if requested is not None and requested >= _today_yyyymmdd():
            raise SourceRequestValidationError("date must be earlier than today for this Sina historical interface")
        limit = min(_parse_positive_int(params.get("limit"), default=200, name="limit"), 5000)
        trade_date = requested or _today_yyyymmdd()
        rows: list[dict[str, Any]] = []
        # 未指定日期时从今天起回溯到最新有数据的交易日(当日榜单收盘后才发布)
        for _ in range(1 if requested is not None else 12):
            url = _url_with_query(SINA_LHB_DETAIL_DAILY_URL, {"tradedate": f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"})
            html = self._fetch_text(
                url,
                context="Sina dragon tiger daily detail",
                headers={
                    "Accept": "text/html,*/*",
                    "Referer": "https://vip.stock.finance.sina.com.cn/",
                },
                fallback_encoding="gb18030",
            )
            rows = _parse_lhb_detail_daily_html(html, trade_date=trade_date, limit=limit)
            if rows or requested is not None:
                break
            previous = Date(int(trade_date[:4]), int(trade_date[4:6]), int(trade_date[6:])) - timedelta(days=1)
            trade_date = previous.strftime("%Y%m%d")
        self.last_meta = {
            "source_name": "新浪龙虎榜",
            "source_url": SINA_LHB_DETAIL_DAILY_URL,
            "trade_date": trade_date,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_stock_lhb_ggtj(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_lhb_ggtj_sina: {unknown}")
        recent_days = str(params.get("symbol") or "5").strip()
        if recent_days not in {"5", "10", "30", "60"}:
            raise SourceRequestValidationError("symbol must be one of 5, 10, 30, or 60")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=40, name="limit"), 1000)
        url = _url_with_query(SINA_LHB_GGTJ_URL, {"last": recent_days, "p": str(page)})
        html = self._fetch_text(
            url,
            context="Sina dragon tiger stock statistics",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/",
            },
            fallback_encoding="gb18030",
        )
        rows = _parse_lhb_ggtj_html(html, recent_days=recent_days, page=page, limit=limit)
        self.last_meta = {
            "source_name": "新浪龙虎榜",
            "source_url": url,
            "recent_days": recent_days,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_stock_lhb_jgmx(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_lhb_jgmx_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=40, name="limit"), 1000)
        url = _url_with_query(SINA_LHB_JGMX_URL, {"p": str(page)})
        html = self._fetch_text(
            url,
            context="Sina dragon tiger institution detail",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/",
            },
            fallback_encoding="gb18030",
        )
        rows = _parse_lhb_jgmx_html(html, page=page, limit=limit)
        self.last_meta = {
            "source_name": "新浪龙虎榜",
            "source_url": url,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_stock_lhb_jgzz(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_lhb_jgzz_sina: {unknown}")
        recent_days = str(params.get("symbol") or "5").strip()
        if recent_days not in {"5", "10", "30", "60"}:
            raise SourceRequestValidationError("symbol must be one of 5, 10, 30, or 60")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=40, name="limit"), 1000)
        url = _url_with_query(SINA_LHB_JGZZ_URL, {"last": recent_days, "p": str(page)})
        html = self._fetch_text(
            url,
            context="Sina dragon tiger institution tracking",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/",
            },
            fallback_encoding="gb18030",
        )
        rows = _parse_lhb_jgzz_html(html, recent_days=recent_days, page=page, limit=limit)
        self.last_meta = {
            "source_name": "新浪龙虎榜",
            "source_url": url,
            "recent_days": recent_days,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_stock_lhb_yytj(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_lhb_yytj_sina: {unknown}")
        recent_days = str(params.get("symbol") or "5").strip()
        if recent_days not in {"5", "10", "30", "60"}:
            raise SourceRequestValidationError("symbol must be one of 5, 10, 30, or 60")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=40, name="limit"), 1000)
        url = _url_with_query(SINA_LHB_YYTJ_URL, {"last": recent_days, "p": str(page)})
        html = self._fetch_text(
            url,
            context="Sina dragon tiger brokerage statistics",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/",
            },
            fallback_encoding="gb18030",
        )
        rows = _parse_lhb_yytj_html(html, recent_days=recent_days, page=page, limit=limit)
        self.last_meta = {
            "source_name": "新浪龙虎榜",
            "source_url": url,
            "recent_days": recent_days,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_tool_trade_date_hist(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for tool_trade_date_hist_sina: {unknown}")
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        payload = self._fetch_text(
            SINA_TRADE_DATE_HIST_URL,
            context="Sina trade date history",
            headers={
                "Accept": "text/plain,*/*",
                "Referer": "https://finance.sina.com.cn/",
            },
        )
        trade_dates = _decode_sina_trade_dates(payload)
        matched: list[dict[str, Any]] = []
        for trade_date in trade_dates:
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            matched.append(
                {
                    "trade_date": trade_date,
                    "exchange": "SSE",
                    "is_open": True,
                    "source_calendar": "Sina KLC_TD_SH",
                }
            )
        # 有起始日期时从头截断(增量语义);无起始日期时取最新 limit 个交易日,
        # 避免默认拿到 1990 年起的最旧 100 天
        rows = matched[:limit] if start_date else matched[-limit:]
        self.last_meta = {
            "source_name": "新浪财经",
            "source_url": SINA_TRADE_DATE_HIST_URL,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_stock_restricted_release_queue(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_restricted_release_queue_sina: {unknown}")
        code_info = _sina_stock_symbol(str(params.get("symbol") or "sh600000"))
        if code_info is None:
            raise SourceRequestValidationError("symbol must be a six-digit A-share code, AxData id, or Sina symbol")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 5000)
        url = _url_with_query(SINA_RESTRICTED_RELEASE_URL, {"symbol": code_info["sina_symbol"]})
        html = self._fetch_text(
            url,
            context="Sina restricted share release queue",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/",
            },
            fallback_encoding="gb18030",
        )
        rows = _parse_restricted_release_html(html, limit=limit)
        self.last_meta = {
            "source_name": "新浪财经",
            "source_url": url,
            "symbol": code_info["sina_symbol"],
            "instrument_id": code_info["instrument_id"],
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_index_global_hist(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for index_global_hist_sina: {unknown}")
        requested_symbol = str(params.get("symbol") or "瑞士股票指数").strip()
        source_symbol = INDEX_GLOBAL_SINA_SYMBOL_MAP.get(requested_symbol, requested_symbol.upper())
        if not re.fullmatch(r"[A-Z0-9_./-]+", source_symbol):
            raise SourceRequestValidationError("symbol must be a known global index name or Sina index code")
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        url = _url_with_query(INDEX_GLOBAL_HIST_URL, {"symbol": source_symbol, "num": "10000"})
        payload = self._fetch_json(
            url,
            context="Sina global index history",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://finance.sina.com.cn/stock/globalindex/",
            },
        )
        data = payload.get("result", {}).get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina global index history returned unexpected payload.")
        rows: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, Mapping):
                continue
            trade_date = _normalize_report_date(item.get("d"))
            if trade_date is None:
                continue
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append(
                {
                    "index_code": source_symbol,
                    "index_name": requested_symbol if requested_symbol in INDEX_GLOBAL_SINA_SYMBOL_MAP else None,
                    "trade_date": trade_date,
                    "open": _parse_float(item.get("o")),
                    "high": _parse_float(item.get("h")),
                    "low": _parse_float(item.get("l")),
                    "close": _parse_float(item.get("c")),
                    "volume": _parse_float(item.get("v")),
                }
            )
        rows.sort(key=lambda row: row["trade_date"])
        self.last_meta = {
            "source_name": "新浪环球市场",
            "source_url": url,
            "requested_symbol": requested_symbol,
            "index_code": source_symbol,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": len(rows),
        }
        return rows[-limit:]

    def _request_index_us_stock(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for index_us_stock_sina: {unknown}")
        source_symbol = str(params.get("symbol") or ".INX").strip().upper()
        if source_symbol not in INDEX_US_STOCK_SINA_NAMES:
            raise SourceRequestValidationError("symbol must be one of .INX, .IXIC, .DJI, or .NDX")
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        url = f"https://finance.sina.com.cn/staticdata/us/{source_symbol}"
        text = self._fetch_text(
            url,
            context=f"Sina US index daily KLC {source_symbol}",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": f"https://stock.finance.sina.com.cn/usstock/quotes/{source_symbol}.html",
            },
        )
        encoded = _extract_sina_encoded_string(text, context=f"Sina US index daily KLC {source_symbol}")
        rows: list[dict[str, Any]] = []
        for item in _decode_sina_klc_k2_rows(encoded):
            trade_date = item["trade_date"]
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append(
                {
                    "source_symbol": source_symbol,
                    "index_name": INDEX_US_STOCK_SINA_NAMES[source_symbol],
                    **item,
                }
            )
        rows.sort(key=lambda row: row["trade_date"])
        self.last_meta = {
            "source_name": "新浪美股指数",
            "source_url": url,
            "source_symbol": source_symbol,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": min(len(rows), limit),
            "decoder": "pure_python_sina_klc_k2",
        }
        return rows[-limit:]

    def _request_stock_zh_index_spot(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_zh_index_spot_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=80, name="limit"), 80)
        url = _url_with_query(
            INDEX_STOCK_CONS_SIMPLE_URL,
            {
                "page": str(page),
                "num": str(limit),
                "sort": "symbol",
                "asc": "1",
                "node": "hs_s",
                "_s_r_a": "page",
            },
        )
        data = self._fetch_json(
            url,
            context="Sina China index spot",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/mkt/",
            },
        )
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina China index spot returned unexpected payload.")
        rows = [
            row
            for row in (_normalize_zh_index_spot_row(item) for item in data)
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪行情中心",
            "source_url": url,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_stock_hk_index_daily(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_hk_index_daily_sina: {unknown}")
        index_code = _parse_hk_index_daily_symbol(params.get("symbol"))
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        url = _url_with_query(SINA_HK_INDEX_DAILY_URL_TEMPLATE.format(symbol=index_code), {"d": "2023_5_01"})
        text = self._fetch_text(
            url,
            context=f"Sina Hong Kong index daily KLC {index_code}",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": f"https://stock.finance.sina.com.cn/hkstock/quotes/{index_code}.html",
            },
        )
        encoded = _extract_sina_encoded_string(text, context=f"Sina Hong Kong index daily KLC {index_code}")
        rows: list[dict[str, Any]] = []
        for item in _decode_sina_klc_k2_rows(encoded):
            trade_date = item["trade_date"]
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append(
                {
                    "index_code": index_code,
                    "sina_symbol": f"hk{index_code}",
                    "market": "HK",
                    "asset_type": "index",
                    **item,
                }
            )
        rows.sort(key=lambda row: row["trade_date"])
        self.last_meta = {
            "source_name": "新浪港股指数",
            "source_url": url,
            "index_code": index_code,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": min(len(rows), limit),
            "decoder": "pure_python_sina_klc_k2",
        }
        return rows[-limit:]

    def _request_stock_hk_index_spot(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbols", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_hk_index_spot_sina: {unknown}")
        symbols = _parse_hk_index_symbols(params.get("symbols"))
        limit = min(_parse_positive_int(params.get("limit"), default=len(symbols), name="limit"), 100)
        url = f"{SINA_HQ_LIST_URL}{','.join(symbols)}"
        text = self._fetch_text(
            url,
            context="Sina Hong Kong index spot",
            headers={
                "Accept": "*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/mkt/",
            },
        )
        rows = [
            row
            for row in (
                _normalize_hk_index_spot_row(sina_symbol, values)
                for sina_symbol, values in _parse_sina_hq_rows(text)
            )
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪行情中心",
            "source_url": url,
            "symbols": symbols,
            "limit": limit,
            "count": min(len(rows), limit),
        }
        return rows[:limit]

    def _request_stock_classify(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "category", "class_name", "node", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_classify_sina: {unknown}")
        category = _clean_text(
            params.get("category")
            or params.get("symbol")
            or SINA_STOCK_CLASSIFY_DEFAULT_CATEGORY
        )
        class_name = _clean_text(params.get("class_name") or SINA_STOCK_CLASSIFY_DEFAULT_CLASS_NAME)
        node = str(params.get("node") or SINA_STOCK_CLASSIFY_DEFAULT_NODE).strip()
        if category is None:
            category = SINA_STOCK_CLASSIFY_DEFAULT_CATEGORY
        if class_name is None:
            class_name = SINA_STOCK_CLASSIFY_DEFAULT_CLASS_NAME
        if not re.fullmatch(r"[A-Za-z0-9_]+", node):
            raise SourceRequestValidationError("node must be a Sina classification node such as new_blhy")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=80, name="limit"), 3000)
        url = _url_with_query(
            INDEX_STOCK_CONS_HS300_URL,
            {
                "page": str(page),
                "num": str(limit),
                "sort": "symbol",
                "asc": "1",
                "node": node,
                "symbol": "",
                "_s_r_a": "init",
            },
        )
        data = self._fetch_json(
            url,
            context="Sina stock classification node",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/mkt/",
            },
        )
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina stock classification node returned unexpected payload.")
        rows = [
            row
            for row in (
                _normalize_stock_classify_row(
                    item,
                    category=category,
                    class_name=class_name,
                    source_node=node,
                    rank=(index + 1) + (page - 1) * limit,
                )
                for index, item in enumerate(data)
            )
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪行情中心",
            "source_url": url,
            "category": category,
            "class_name": class_name,
            "node": node,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_stock_info_global(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "page_size", "limit", "zhibo_id", "tag_id"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_info_global_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        page_size = min(_parse_positive_int(params.get("page_size"), default=20, name="page_size"), 100)
        limit = min(_parse_positive_int(params.get("limit"), default=page_size, name="limit"), page_size)
        zhibo_id = str(params.get("zhibo_id") or "152").strip()
        tag_id = str(params.get("tag_id") or "0").strip()
        if not re.fullmatch(r"\d+", zhibo_id):
            raise SourceRequestValidationError("zhibo_id must be numeric")
        if not re.fullmatch(r"\d+", tag_id):
            raise SourceRequestValidationError("tag_id must be numeric")
        url = _url_with_query(
            SINA_GLOBAL_INFO_FEED_URL,
            {
                "page": page,
                "page_size": page_size,
                "zhibo_id": zhibo_id,
                "tag_id": tag_id,
                "dire": "f",
                "dpc": "1",
                "pagesize": page_size,
                "type": "1",
            },
        )
        payload = self._fetch_json(
            url,
            context="Sina global finance feed",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://finance.sina.com.cn/7x24/",
            },
        )
        feed_rows = (
            payload.get("result", {})
            .get("data", {})
            .get("feed", {})
            .get("list")
            if isinstance(payload, Mapping)
            else None
        )
        if not isinstance(feed_rows, list):
            raise SourceUnavailableError("Sina global finance feed returned unexpected payload.")
        rows = [
            row
            for row in (_normalize_global_info_feed_row(item) for item in feed_rows)
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪财经 7x24",
            "source_url": url,
            "page": page,
            "page_size": page_size,
            "zhibo_id": zhibo_id,
            "tag_id": tag_id,
            "limit": limit,
            "count": min(len(rows), limit),
        }
        return rows[:limit]

    def _request_stock_intraday(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "date", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_intraday_sina: {unknown}")
        code_info = _sina_stock_symbol(str(params.get("symbol") or "sz000001"))
        if code_info is None:
            raise SourceRequestValidationError("symbol must be a Sina A-share symbol such as sz000001 or 000001.SZ")
        trade_date = _normalize_query_date(params.get("date") or _today_yyyymmdd(), "date", required=True)
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=60, name="limit"), 60)
        source_day = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
        url = _url_with_query(
            SINA_STOCK_INTRADAY_URL,
            {
                "symbol": code_info["sina_symbol"],
                "num": limit,
                "page": page,
                "sort": "ticktime",
                "asc": "0",
                "volume": "0",
                "amount": "0",
                "type": "0",
                "day": source_day,
            },
        )
        payload = self._fetch_json(
            url,
            context=f"Sina stock intraday {code_info['sina_symbol']}",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": (
                    "https://vip.stock.finance.sina.com.cn/quotes_service/view/"
                    f"cn_bill.php?symbol={code_info['sina_symbol']}"
                ),
            },
        )
        if not isinstance(payload, list):
            raise SourceUnavailableError(f"Sina stock intraday {code_info['sina_symbol']} returned unexpected payload.")
        rows = [
            row
            for row in (
                _normalize_stock_intraday_row(item, code_info=code_info, trade_date=trade_date)
                for item in payload
            )
            if row is not None
        ]
        rows.sort(key=lambda row: row["tick_time"])
        self.last_meta = {
            "source_name": "新浪逐笔成交",
            "source_url": url,
            "symbol": code_info["sina_symbol"],
            "trade_date": trade_date,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_stock_esg_msci(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_esg_msci_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 100)
        url = _url_with_query(SINA_ESG_MSCI_URL, {"p": page, "num": limit})
        payload = self._fetch_json(
            url,
            context="Sina MSCI ESG ratings",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://finance.sina.com.cn/esg/grade.shtml",
            },
        )
        data = (
            payload.get("result", {}).get("data", {}).get("data")
            if isinstance(payload, Mapping)
            else None
        )
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina MSCI ESG ratings returned unexpected payload.")
        rows = [
            row
            for row in (_normalize_esg_msci_row(item) for item in data)
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪 ESG 评级中心",
            "source_url": url,
            "agency": "MSCI",
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_stock_esg_rate(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_esg_rate_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 200)
        url = _url_with_query(SINA_ESG_RATE_URL, {"page": page, "num": limit})
        payload = self._fetch_json(
            url,
            context="Sina ESG rating history",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://finance.sina.com.cn/esg/grade.shtml",
            },
        )
        stocks = (
            payload.get("result", {}).get("data", {}).get("info", {}).get("stocks")
            if isinstance(payload, Mapping)
            else None
        )
        if not isinstance(stocks, list):
            raise SourceUnavailableError("Sina ESG rating history returned unexpected payload.")
        rows: list[dict[str, Any]] = []
        for stock in stocks:
            if not isinstance(stock, Mapping):
                continue
            symbol = _clean_text(stock.get("symbol"))
            market = _clean_text(stock.get("market"))
            for item in stock.get("esg_info") or []:
                row = _normalize_esg_rate_row(item, symbol=symbol, market=market)
                if row is not None:
                    rows.append(row)
        self.last_meta = {
            "source_name": "新浪 ESG 评级中心",
            "source_url": url,
            "agency": "multi_agency",
            "page": page,
            "limit": limit,
            "count": min(len(rows), limit),
        }
        return rows[:limit]

    def _request_stock_esg_rft(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_esg_rft_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 100)
        url = _url_with_query(SINA_ESG_RFT_URL, {"p": page, "num": limit})
        payload = self._fetch_json(
            url,
            context="Sina Refinitiv ESG ratings",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://finance.sina.com.cn/esg/grade.shtml",
            },
        )
        data = (
            payload.get("result", {}).get("data", {}).get("data")
            if isinstance(payload, Mapping)
            else None
        )
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina Refinitiv ESG ratings returned unexpected payload.")
        rows = [
            row
            for row in (_normalize_esg_rft_row(item) for item in data)
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪 ESG 评级中心",
            "source_url": url,
            "agency": "Refinitiv",
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_stock_esg_zd(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_esg_zd_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 100)
        url = _url_with_query(SINA_ESG_ZD_URL, {"p": page, "num": limit})
        payload = self._fetch_json(
            url,
            context="Sina ZD ESG ratings",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://finance.sina.com.cn/esg/grade.shtml",
            },
        )
        data = (
            payload.get("result", {}).get("data", {}).get("data")
            if isinstance(payload, Mapping)
            else None
        )
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina ZD ESG ratings returned unexpected payload.")
        rows = [
            row
            for row in (_normalize_esg_zd_row(item) for item in data)
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪 ESG 评级中心",
            "source_url": url,
            "agency": "秩鼎",
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_stock_esg_hz(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_esg_hz_sina: {unknown}")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 100)
        url = _url_with_query(SINA_ESG_HZ_URL, {"p": page, "num": limit})
        payload = self._fetch_json(
            url,
            context="Sina Huazheng ESG ratings",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://finance.sina.com.cn/esg/grade.shtml",
            },
        )
        data = (
            payload.get("result", {}).get("data", {}).get("data")
            if isinstance(payload, Mapping)
            else None
        )
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina Huazheng ESG ratings returned unexpected payload.")
        rows = [
            row
            for row in (_normalize_esg_hz_row(item) for item in data)
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪 ESG 评级中心",
            "source_url": url,
            "agency": "华证",
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_option_commodity_contracts(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        unknown_keys = sorted(set(params) - {"limit"})
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for option_commodity_contract_sina: {unknown}")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 200)
        html = self._fetch_text(
            SINA_COMMODITY_OPTION_PAGE_URL,
            context="Sina commodity option contract list",
            headers={
                "Accept": "text/html,*/*",
                "Referer": SINA_COMMODITY_OPTION_PAGE_URL,
            },
            fallback_encoding="gbk",
            force_encoding="gbk",
        )
        rows = _parse_commodity_option_contract_links(html, limit=limit)
        self.last_meta = {
            "source_name": "新浪商品期权",
            "source_url": SINA_COMMODITY_OPTION_PAGE_URL,
            "interface_name": "option_commodity_contract_sina",
            "row_count": len(rows),
        }
        return rows

    def _request_option_commodity_contract_table(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        unknown_keys = sorted(set(params) - {"symbol", "limit"})
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for option_commodity_contract_table_sina: {unknown}")
        symbol = str(params.get("symbol") or "豆粕期权").strip()
        if not symbol:
            raise SourceRequestValidationError("symbol must not be empty")
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 200)
        contracts = self._request_option_commodity_contracts({"limit": 200})
        selected = _select_commodity_option_contract(contracts, symbol)
        if selected is None:
            raise SourceRequestValidationError(f"Unknown commodity option symbol: {symbol}")
        html = self._fetch_text(
            selected["source_url"],
            context=f"Sina commodity option contract table {symbol}",
            headers={
                "Accept": "text/html,*/*",
                "Referer": SINA_COMMODITY_OPTION_PAGE_URL,
            },
            fallback_encoding="gbk",
            force_encoding="gbk",
        )
        rows = _parse_commodity_option_contract_table(
            html,
            option_name=selected["option_name"],
            product_code=selected["product_code"],
            exchange=selected["exchange"],
            source_url=selected["source_url"],
            limit=limit,
        )
        self.last_meta = {
            "source_name": "新浪商品期权",
            "source_url": selected["source_url"],
            "interface_name": "option_commodity_contract_table_sina",
            "row_count": len(rows),
            "option_name": selected["option_name"],
            "product_code": selected["product_code"],
            "exchange": selected["exchange"],
        }
        return rows

    def _request_option_commodity_hist(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "option_name", "start_date", "end_date", "limit"}
        unknown_keys = sorted(set(params) - allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for option_commodity_hist_sina: {unknown}")
        option_name_param = str(params.get("option_name") or "豆粕期权").strip()
        if not option_name_param:
            raise SourceRequestValidationError("option_name must not be empty")
        contracts = self._request_option_commodity_contracts({"limit": 200})
        selected = _select_commodity_option_contract(contracts, option_name_param)
        if selected is None:
            raise SourceRequestValidationError(f"Unknown commodity option: {option_name_param}")
        product_prefix = str(selected["product_code"]).split("_", 1)[0].lower()
        source_symbol = _normalize_commodity_option_symbol(
            str(params.get("symbol") or "m2609P2500"),
            product_prefix=product_prefix,
        )
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 5000)
        today = Date.today()
        callback_date = f"{today.year}_{today.month}_{today.day}"
        url = _url_with_query(
            SINA_CFFEX_OPTION_DAYLINE_URL.format(symbol=source_symbol, callback_date=callback_date),
            {"symbol": source_symbol},
        )
        text = self._fetch_text(
            url,
            context=f"Sina commodity option daily kline {source_symbol}",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": selected["source_url"],
            },
        )
        rows: list[dict[str, Any]] = []
        for item in _parse_jsonp_array(text, context=f"Sina commodity option daily kline {source_symbol}"):
            row = _normalize_commodity_option_daily_row(
                item,
                source_symbol=source_symbol,
                option_name=selected["option_name"],
                product_code=selected["product_code"],
                exchange=selected["exchange"],
            )
            if row is None:
                continue
            trade_date = row["trade_date"]
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        self.last_meta = {
            "source_name": "新浪商品期权",
            "source_url": url,
            "interface_name": "option_commodity_hist_sina",
            "row_count": len(rows),
            "option_name": selected["option_name"],
            "product_code": selected["product_code"],
            "exchange": selected["exchange"],
        }
        return rows

    def _request_index_stock_cons(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "page", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for index_stock_cons_sina: {unknown}")
        index_code = str(params.get("symbol") or "000300").strip()
        if not re.fullmatch(r"\d{6}", index_code):
            raise SourceRequestValidationError("symbol must be a six-digit index code such as 000300")
        page = _parse_positive_int(params.get("page"), default=1, name="page")
        limit = min(_parse_positive_int(params.get("limit"), default=80, name="limit"), 3000)
        if index_code == "000300":
            url = _url_with_query(
                INDEX_STOCK_CONS_HS300_URL,
                {
                    "page": str(page),
                    "num": str(limit),
                    "sort": "symbol",
                    "asc": "1",
                    "node": "hs300",
                    "symbol": "",
                    "_s_r_a": "init",
                },
            )
        else:
            url = _url_with_query(
                INDEX_STOCK_CONS_SIMPLE_URL,
                {
                    "page": str(page),
                    "num": str(limit),
                    "sort": "symbol",
                    "asc": "1",
                    "node": f"zhishu_{index_code}",
                    "_s_r_a": "setlen",
                },
            )
        data = self._fetch_json(
            url,
            context="Sina index constituents",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/mkt/",
            },
        )
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina index constituents returned unexpected payload.")
        rows = [
            row
            for row in (
                _normalize_index_stock_cons_row(item, index_code)
                for item in data
            )
            if row is not None
        ]
        self.last_meta = {
            "source_name": "新浪行情中心",
            "source_url": url,
            "index_code": index_code,
            "page": page,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_option_sse_daily(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for option_sse_daily_sina: {unknown}")
        option_info = _sina_option_symbol(str(params.get("symbol") or "10011799"))
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 10000)
        url = _url_with_query(SINA_OPTION_DAILY_URL, {"symbol": option_info["sina_symbol"]})
        text = self._fetch_text(
            url,
            context="Sina SSE option daily",
            headers={
                "Accept": "*/*",
                "Referer": "https://stock.finance.sina.com.cn/option/quotes.html",
            },
        )
        raw_rows = _parse_jsonp_array(text)
        rows: list[dict[str, Any]] = []
        for item in raw_rows:
            if not isinstance(item, Mapping):
                continue
            trade_date = _normalize_report_date(item.get("d"))
            if trade_date is None:
                continue
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append(
                {
                    **option_info,
                    "trade_date": trade_date,
                    "open": _parse_float(item.get("o")),
                    "high": _parse_float(item.get("h")),
                    "low": _parse_float(item.get("l")),
                    "close": _parse_float(item.get("c")),
                    "volume": _parse_float(item.get("v")),
                }
            )
        rows.sort(key=lambda row: row["trade_date"])
        self.last_meta = {
            "source_name": "新浪上交所期权",
            "source_url": url,
            "symbol": option_info["sina_symbol"],
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": len(rows),
        }
        return rows[-limit:]

    def _request_option_sse_minute(
        self, params: Mapping[str, Any], *, interface_name: str = "option_sse_minute_sina"
    ) -> list[dict[str, Any]]:
        allowed = {"symbol", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for {interface_name}: {unknown}")
        option_info = _sina_option_symbol(str(params.get("symbol") or "10011799"))
        limit = min(_parse_positive_int(params.get("limit"), default=241, name="limit"), 1000)
        url = _url_with_query(SINA_OPTION_MINUTE_URL, {"symbol": option_info["sina_symbol"]})
        payload = self._fetch_json(
            url,
            context="Sina SSE option minute",
            headers={
                "Accept": "*/*",
                "Referer": "https://stock.finance.sina.com.cn/option/quotes.html",
            },
        )
        data = payload.get("result", {}).get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, list):
            raise SourceUnavailableError("Sina SSE option minute returned unexpected payload.")
        rows: list[dict[str, Any]] = []
        current_date: str | None = None
        for item in data:
            if not isinstance(item, Mapping):
                continue
            row_date = _normalize_report_date(item.get("d")) or current_date
            if row_date is None:
                continue
            current_date = row_date
            rows.append(
                {
                    **option_info,
                    "trade_date": current_date,
                    "trade_time": _clean_text(item.get("i")),
                    "price": _parse_float(item.get("p")),
                    "volume": _parse_float(item.get("v")),
                    "open_interest": _parse_float(item.get("t")),
                    "avg_price": _parse_float(item.get("a")),
                }
            )
        self.last_meta = {
            "source_name": "新浪金融期权" if interface_name == "option_finance_minute_sina" else "新浪上交所期权",
            "source_url": url,
            "interface_name": interface_name,
            "symbol": option_info["sina_symbol"],
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_option_sse_spot_price(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for option_sse_spot_price_sina: {unknown}")
        option_info = _sina_option_symbol(str(params.get("symbol") or "10011799"))
        url = f"{SINA_HQ_LIST_URL}{option_info['sina_symbol']}"
        text = self._fetch_text(
            url,
            context="Sina SSE option spot price",
            headers={
                "Accept": "*/*",
                "Referer": "https://stock.finance.sina.com.cn/",
            },
        )
        row = _normalize_option_sse_spot_price_row(_parse_sina_hq_values(text), option_info)
        rows = [] if row is None else [row]
        self.last_meta = {
            "source_name": "新浪上交所期权",
            "source_url": url,
            "symbol": option_info["sina_symbol"],
            "count": len(rows),
        }
        return rows

    def _request_option_sse_underlying_spot_price(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(
                f"Unknown param(s) for option_sse_underlying_spot_price_sina: {unknown}"
            )
        code_info = _sina_market_symbol(str(params.get("symbol") or "sh510050"))
        if code_info is None:
            raise SourceRequestValidationError("symbol must be a Sina underlying symbol such as sh510050 or 510050.SH")
        url = f"{SINA_HQ_LIST_URL}{code_info['sina_symbol']}"
        text = self._fetch_text(
            url,
            context="Sina SSE option underlying spot price",
            headers={
                "Accept": "*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/",
            },
        )
        row = _normalize_option_sse_underlying_spot_row(_parse_sina_hq_values(text), code_info)
        rows = [] if row is None else [row]
        self.last_meta = {
            "source_name": "新浪上交所期权",
            "source_url": url,
            "symbol": code_info["sina_symbol"],
            "count": len(rows),
        }
        return rows

    def _request_option_sse_greeks(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for option_sse_greeks_sina: {unknown}")
        option_info = _sina_option_symbol(str(params.get("symbol") or "10011799"))
        source_symbol = f"CON_SO_{option_info['option_code']}"
        url = f"{SINA_HQ_LIST_URL}{source_symbol}"
        text = self._fetch_text(
            url,
            context="Sina SSE option Greeks",
            headers={
                "Accept": "*/*",
                "Referer": "https://vip.stock.finance.sina.com.cn/",
            },
        )
        row = _normalize_option_sse_greeks_row(_parse_sina_hq_values(text), option_info)
        rows = [] if row is None else [row]
        self.last_meta = {
            "source_name": "新浪上交所期权",
            "source_url": url,
            "symbol": option_info["sina_symbol"],
            "source_symbol": source_symbol,
            "count": len(rows),
        }
        return rows

    def _request_option_sse_list(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for option_sse_list_sina: {unknown}")
        underlying, underlying_name = _sse_option_underlying(str(params.get("symbol") or "50ETF"))
        if underlying_name is None:
            raise SourceRequestValidationError(
                "symbol must be a known SSE option underlying: 50ETF, 300ETF, 500ETF, 科创50, 科创板50, or its code"
            )
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 100)
        data = self._fetch_sse_option_stock_name(underlying_name)
        source_underlying = _clean_text(data.get("stockId"))
        if source_underlying and re.fullmatch(r"\d{6}", source_underlying):
            underlying = source_underlying
        months = _normalize_sse_option_months(data.get("contractMonth"))
        rows = [
            {
                "sequence": sequence,
                "underlying": underlying,
                "underlying_name": underlying_name,
                "expire_month": month,
                "cate_id": _clean_text(data.get("cateId")),
                "exchange": "SSE",
            }
            for sequence, month in enumerate(months, start=1)
        ]
        self.last_meta = {
            "source_name": "新浪上交所期权",
            "source_url": _url_with_query(
                SINA_OPTION_STOCK_NAME_URL,
                {"exchange": "null", "cate": underlying_name},
            ),
            "symbol": underlying_name,
            "underlying": underlying,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_option_sse_expire_day(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "trade_date"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for option_sse_expire_day_sina: {unknown}")
        underlying, underlying_name = _sse_option_underlying(str(params.get("symbol") or "50ETF"))
        if underlying_name is None:
            raise SourceRequestValidationError(
                "symbol must be a known SSE option underlying: 50ETF, 300ETF, 500ETF, 科创50, 科创板50, or its code"
            )
        source_months: list[str] = []
        trade_date_param = params.get("trade_date")
        if trade_date_param in (None, ""):
            stock_name_data = self._fetch_sse_option_stock_name(underlying_name)
            source_months = _normalize_sse_option_months(stock_name_data.get("contractMonth"))
            if not source_months:
                raise SourceUnavailableError("Sina SSE option month list was empty.")
            expire_month = source_months[0]
            source_underlying = _clean_text(stock_name_data.get("stockId"))
            if source_underlying and re.fullmatch(r"\d{6}", source_underlying):
                underlying = source_underlying
        else:
            expire_month = _normalize_expire_month(trade_date_param)
        data, url, used_category = self._fetch_sse_option_remainder_day(underlying_name, expire_month)
        remainder_days = _parse_int(data.get("remainderDays"))
        if remainder_days is not None and remainder_days < 0 and not underlying_name.startswith("XD"):
            data, url, used_category = self._fetch_sse_option_remainder_day(f"XD{underlying_name}", expire_month)
            remainder_days = _parse_int(data.get("remainderDays"))
        other = data.get("other") if isinstance(data.get("other"), Mapping) else {}
        row = {
            "underlying": _clean_text(data.get("stockId")) or underlying,
            "underlying_name": underlying_name,
            "expire_month": expire_month,
            "expire_date": _normalize_report_date(data.get("expireDay")),
            "remainder_days": remainder_days,
            "cate_id": _clean_text(data.get("cateId")),
            "source_category": used_category,
            "underlying_source_name": _clean_text(other.get("name")),
            "underlying_sina_symbol": _clean_text(other.get("symbol")),
            "exchange": "SSE",
        }
        self.last_meta = {
            "source_name": "新浪上交所期权",
            "source_url": url,
            "symbol": underlying_name,
            "underlying": underlying,
            "expire_month": expire_month,
            "source_months": source_months,
            "count": 1,
        }
        return [row]

    def _fetch_sse_option_remainder_day(self, category: str, expire_month: str) -> tuple[Mapping[str, Any], str, str]:
        url = _url_with_query(
            SINA_OPTION_REMAINDER_DAY_URL,
            {
                "exchange": "null",
                "cate": category,
                "date": f"{expire_month[:4]}-{expire_month[4:]}",
            },
        )
        payload = self._fetch_json(
            url,
            context="Sina SSE option remainder day",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://stock.finance.sina.com.cn/option/quotes.html",
            },
        )
        data = payload.get("result", {}).get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, Mapping):
            raise SourceUnavailableError("Sina SSE option remainder day returned unexpected payload.")
        return data, url, category

    def _request_option_sse_codes(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"symbol", "trade_date", "underlying", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for option_sse_codes_sina: {unknown}")
        direction = _sse_option_direction(str(params.get("symbol") or "看涨期权"))
        underlying, underlying_name = _sse_option_underlying(str(params.get("underlying") or "510050"))
        trade_date_param = params.get("trade_date")
        source_months: list[str] = []
        if trade_date_param in (None, ""):
            if underlying_name is None:
                raise SourceRequestValidationError(
                    "underlying must be one of 510050, 510300, 510500, 588000, or 588080 when trade_date is omitted"
                )
            stock_name_data = self._fetch_sse_option_stock_name(underlying_name)
            source_underlying = _clean_text(stock_name_data.get("stockId"))
            if source_underlying and re.fullmatch(r"\d{6}", source_underlying):
                underlying = source_underlying
            source_months = _normalize_sse_option_months(stock_name_data.get("contractMonth"))
            if not source_months:
                raise SourceUnavailableError("Sina SSE option month list was empty.")
            expire_month = source_months[0]
        else:
            expire_month = _normalize_expire_month(trade_date_param)
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        source_symbol = f"OP_{direction}_{underlying}{expire_month[-4:]}"
        url = f"{SINA_HQ_LIST_URL}{source_symbol}"
        text = self._fetch_text(
            url,
            context="Sina SSE option codes",
            headers={
                "Accept": "*/*",
                "Referer": "https://stock.finance.sina.com.cn/",
            },
        )
        rows = [
            {
                "sequence": sequence,
                "option_code": code,
                "sina_symbol": f"CON_OP_{code}",
                "option_type": SSE_OPTION_DIRECTION_LABELS[direction],
                "underlying": underlying,
                "underlying_name": underlying_name,
                "expire_month": expire_month,
                "exchange": "SSE",
            }
            for sequence, code in enumerate(_parse_sse_option_code_list(text), start=1)
        ]
        self.last_meta = {
            "source_name": "新浪上交所期权",
            "source_url": url,
            "source_symbol": source_symbol,
            "option_type": SSE_OPTION_DIRECTION_LABELS[direction],
            "underlying": underlying,
            "underlying_name": underlying_name,
            "expire_month": expire_month,
            "source_months": source_months,
            "limit": limit,
            "count": len(rows),
        }
        return rows[:limit]

    def _request_option_cffex_daily(
        self,
        params: Mapping[str, Any],
        *,
        interface_name: str,
        default_symbol: str,
        symbol_prefix: str,
        underlying_name: str,
    ) -> list[dict[str, Any]]:
        allowed = {"symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for {interface_name}: {unknown}")
        source_symbol = _normalize_cffex_option_symbol(
            str(params.get("symbol") or default_symbol),
            prefix=symbol_prefix,
        )
        start_date = _normalize_query_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_query_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 5000)
        today = Date.today()
        callback_date = f"{today.year}_{today.month}_{today.day}"
        url = _url_with_query(
            SINA_CFFEX_OPTION_DAYLINE_URL.format(symbol=source_symbol, callback_date=callback_date),
            {"symbol": source_symbol},
        )
        text = self._fetch_text(
            url,
            context=f"Sina CFFEX option daily kline {source_symbol}",
            headers={
                "Accept": "application/javascript,text/javascript,*/*",
                "Referer": f"https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php/{symbol_prefix}/cffex",
            },
        )
        rows: list[dict[str, Any]] = []
        for item in _parse_jsonp_array(text, context=f"Sina CFFEX option daily kline {source_symbol}"):
            row = _normalize_cffex_option_daily_row(item, source_symbol=source_symbol, underlying_name=underlying_name)
            if row is None:
                continue
            trade_date = row["trade_date"]
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        self.last_meta = {
            "source_name": "新浪中金所期权",
            "source_url": url,
            "source_symbol": source_symbol,
            "underlying_name": underlying_name,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_option_cffex_list(
        self,
        params: Mapping[str, Any],
        *,
        interface_name: str,
        url: str,
        product_code: str,
        underlying_name: str,
    ) -> list[dict[str, Any]]:
        allowed = {"limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for {interface_name}: {unknown}")
        limit = min(_parse_positive_int(params.get("limit"), default=20, name="limit"), 200)
        html = self._fetch_text(
            url,
            context=f"Sina CFFEX option contract list {product_code}",
            headers={
                "Accept": "text/html,*/*",
                "Referer": "https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php",
            },
            fallback_encoding="utf-8",
        )
        rows = _parse_cffex_option_contract_list_html(
            html,
            product_code=product_code,
            underlying_name=underlying_name,
            limit=limit,
        )
        self.last_meta = {
            "source_name": "新浪中金所期权",
            "source_url": url,
            "product_code": product_code,
            "underlying_name": underlying_name,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _request_option_cffex_spot(
        self,
        params: Mapping[str, Any],
        *,
        interface_name: str,
        default_symbol: str,
        product_code: str,
        underlying_name: str,
    ) -> list[dict[str, Any]]:
        allowed = {"symbol", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for {interface_name}: {unknown}")
        contract_code = _normalize_cffex_option_month_symbol(
            str(params.get("symbol") or default_symbol),
            product_code=product_code,
        )
        limit = min(_parse_positive_int(params.get("limit"), default=100, name="limit"), 200)
        url = _url_with_query(
            SINA_CFFEX_OPTION_DATA_URL,
            {
                "type": "futures",
                "product": product_code,
                "exchange": "cffex",
                "pinzhong": contract_code,
            },
        )
        payload = self._fetch_json(
            url,
            context=f"Sina CFFEX option spot {contract_code}",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": f"https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php/{product_code}/cffex",
            },
        )
        data = payload.get("result", {}).get("data") if isinstance(payload, Mapping) else None
        up_rows = data.get("up") if isinstance(data, Mapping) else None
        down_rows = data.get("down") if isinstance(data, Mapping) else None
        if not isinstance(up_rows, list) or not isinstance(down_rows, list):
            raise SourceUnavailableError(f"Sina CFFEX option spot {contract_code} returned unexpected payload.")
        rows: list[dict[str, Any]] = []
        for index in range(min(len(up_rows), len(down_rows))):
            row = _normalize_cffex_option_spot_row(
                up_rows[index],
                down_rows[index],
                contract_code=contract_code,
                product_code=product_code,
                underlying_name=underlying_name,
            )
            if row is None:
                continue
            rows.append(row)
            if len(rows) >= limit:
                break
        self.last_meta = {
            "source_name": "新浪中金所期权",
            "source_url": url,
            "contract_code": contract_code,
            "product_code": product_code,
            "underlying_name": underlying_name,
            "limit": limit,
            "count": len(rows),
        }
        return rows

    def _fetch_sse_option_stock_name(self, category: str) -> Mapping[str, Any]:
        url = _url_with_query(
            SINA_OPTION_STOCK_NAME_URL,
            {
                "exchange": "null",
                "cate": category,
            },
        )
        payload = self._fetch_json(
            url,
            context="Sina SSE option month list",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://stock.finance.sina.com.cn/option/quotes.html",
            },
        )
        data = payload.get("result", {}).get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, Mapping):
            raise SourceUnavailableError("Sina SSE option month list returned unexpected payload.")
        return data

    def _fetch_text(
        self,
        url: str,
        *,
        context: str,
        headers: Mapping[str, str] | None = None,
        fallback_encoding: str = "utf-8",
        force_encoding: str | None = None,
    ) -> str:
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                **dict(headers or {}),
            },
        )
        try:
            response = self._open(request)
            with response:
                raw = response.read()
                charset = force_encoding or response.headers.get_content_charset() or fallback_encoding
                return raw.decode(charset, errors="replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"{context} request failed: {exc}") from exc

    def _fetch_json(self, url: str, *, context: str, headers: Mapping[str, str] | None = None) -> Any:
        text = self._fetch_text(url, context=context, headers=headers)
        try:
            return json.loads(text)
        except ValueError as exc:
            raise SourceUnavailableError(f"{context} returned invalid JSON.") from exc

    def _open(self, request: Request) -> Any:
        if self._opener is not None:
            return self._opener(request, timeout=self._timeout)
        return urlopen(request, timeout=self._timeout)


def _url_with_query(url: str, params: Mapping[str, Any]) -> str:
    from urllib.parse import urlencode

    return f"{url}?{urlencode(params)}"


def _exchange_from_symbol(symbol: str) -> str:
    if symbol.startswith(("6", "5", "9")):
        return "SSE"
    if symbol.startswith(("4", "8", "92")):
        return "BSE"
    return "SZSE"


def _exchange_suffix(exchange: str) -> str:
    return {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}[exchange]


def _parse_positive_int(value: Any, *, default: int, name: str) -> int:
    if value in (None, ""):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SourceRequestValidationError(f"{name} must be a positive integer") from exc
    if parsed <= 0:
        raise SourceRequestValidationError(f"{name} must be a positive integer")
    return parsed


def _parse_jsonp_array(text: str, *, context: str = "Sina JSONP array") -> list[Any]:
    start = text.find("([")
    end = text.rfind(")")
    if start != -1 and end != -1 and end > start:
        payload = text[start + 1 : end]
    else:
        start = text.find("[")
        end = text.rfind("]")
        if start == -1 or end == -1 or end < start:
            raise SourceUnavailableError(f"{context} returned unexpected JSONP payload.")
        payload = text[start : end + 1]
    try:
        data = json.loads(payload)
    except ValueError:
        try:
            data = ast.literal_eval(payload)
        except (ValueError, SyntaxError) as exc:
            raise SourceUnavailableError(f"{context} returned invalid JSONP data.") from exc
    if not isinstance(data, list):
        raise SourceUnavailableError(f"{context} JSONP data is not a list.")
    return data


def _parse_jsonp_object(text: str, *, context: str) -> dict[str, Any]:
    start = text.find("({")
    end = text.rfind(")")
    if start != -1 and end != -1 and end > start:
        payload = text[start + 1 : end]
    else:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            raise SourceUnavailableError(f"{context} returned unexpected JSONP payload.")
        payload = text[start : end + 1]
    try:
        data = json.loads(payload)
    except ValueError:
        try:
            data = ast.literal_eval(payload)
        except (ValueError, SyntaxError) as exc:
            raise SourceUnavailableError(f"{context} returned invalid JSONP data.") from exc
    if not isinstance(data, dict):
        raise SourceUnavailableError(f"{context} JSONP data is not an object.")
    return data


def _parse_sina_js_data_array(text: str) -> list[Any]:
    start_key = text.find("data")
    if start_key == -1:
        return []
    start = text.find("[", start_key)
    if start == -1:
        return []
    depth = 0
    end = None
    for index in range(start, len(text)):
        char = text[index]
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                end = index + 1
                break
    if end is None:
        raise SourceUnavailableError("Sina ETF dividend returned malformed data array.")
    payload = text[start:end]
    try:
        data = json.loads(payload)
    except ValueError:
        try:
            data = ast.literal_eval(payload)
        except (ValueError, SyntaxError) as exc:
            raise SourceUnavailableError("Sina ETF dividend returned invalid data array.") from exc
    if not isinstance(data, list):
        raise SourceUnavailableError("Sina ETF dividend data is not a list.")
    return data


def _sse_option_direction(value: str) -> str:
    text = value.strip()
    direction = SSE_OPTION_DIRECTIONS.get(text) or SSE_OPTION_DIRECTIONS.get(text.lower())
    if direction is None:
        raise SourceRequestValidationError("symbol must be one of 看涨期权, 看跌期权, call, or put")
    return direction


def _sse_option_underlying(value: str) -> tuple[str, str | None]:
    text = value.strip()
    if text in SSE_OPTION_UNDERLYING_CATEGORIES:
        return text, SSE_OPTION_UNDERLYING_CATEGORIES[text]
    if text in SSE_OPTION_CATEGORY_UNDERLYINGS:
        return SSE_OPTION_CATEGORY_UNDERLYINGS[text], text
    if re.fullmatch(r"\d{6}", text):
        return text, None
    raise SourceRequestValidationError(
        "underlying must be a six-digit SSE option underlying code or one of 50ETF, 300ETF, 500ETF, 科创50, 科创板50"
    )


def _normalize_expire_month(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) != 6:
        raise SourceRequestValidationError("trade_date must be YYYYMM or YYYY-MM")
    month = int(digits[4:6])
    if month < 1 or month > 12:
        raise SourceRequestValidationError("trade_date month must be between 01 and 12")
    return digits


def _normalize_sse_option_months(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    months: list[str] = []
    for item in value:
        try:
            month = _normalize_expire_month(item)
        except SourceRequestValidationError:
            continue
        if month not in months:
            months.append(month)
    return months


def _parse_sina_hq_values(text: str) -> list[str]:
    match = re.search(r'="(.*?)";?\s*$', text.strip(), flags=re.S)
    if match is None:
        return []
    return [item.strip() for item in match.group(1).split(",")]


def _parse_sina_hq_rows(text: str) -> list[tuple[str, list[str]]]:
    rows: list[tuple[str, list[str]]] = []
    for match in re.finditer(r"var\s+hq_str_([^=]+)=\"(.*?)\";?", text, flags=re.S):
        values = [item.strip() for item in match.group(2).split(",")]
        rows.append((match.group(1), values))
    return rows


def _parse_hk_index_symbols(value: Any) -> list[str]:
    if value in (None, ""):
        return list(SINA_HK_INDEX_SPOT_DEFAULT_SYMBOLS)
    text = str(value).strip()
    if text.lower() == "all" or text == "全部":
        return list(SINA_HK_INDEX_SPOT_ALL_SYMBOLS)
    symbols: list[str] = []
    for raw_symbol in re.split(r"[,，\s]+", text):
        raw_symbol = raw_symbol.strip()
        if not raw_symbol:
            continue
        symbol = raw_symbol if raw_symbol.lower().startswith("hk") else f"hk{raw_symbol}"
        symbol = f"hk{symbol[2:].upper()}"
        if not re.fullmatch(r"hk[A-Z0-9]+", symbol):
            raise SourceRequestValidationError(
                "symbols must be comma-separated Sina Hong Kong index symbols such as hkHSI or HSTECH"
            )
        if symbol not in symbols:
            symbols.append(symbol)
    if not symbols:
        raise SourceRequestValidationError("symbols must include at least one Hong Kong index symbol")
    if len(symbols) > 100:
        raise SourceRequestValidationError("symbols must include no more than 100 Hong Kong index symbols")
    return symbols


def _parse_hk_index_daily_symbol(value: Any) -> str:
    text = str(value or "CES100").strip()
    if text.lower().startswith("hk"):
        text = text[2:]
    symbol = text.upper()
    if not re.fullmatch(r"[A-Z0-9]+", symbol):
        raise SourceRequestValidationError("symbol must be a Hong Kong index code such as CES100 or HSI")
    return symbol


def _parse_sse_option_code_list(text: str) -> list[str]:
    codes: list[str] = []
    for value in _parse_sina_hq_values(text):
        if value.startswith("CON_OP_"):
            code = value[7:]
        else:
            code = value
        if re.fullmatch(r"\d+", code):
            codes.append(code)
    return codes


def _sina_option_symbol(value: str) -> dict[str, str]:
    text = value.strip().upper()
    if text.startswith("CON_OP_"):
        option_code = text[7:]
    else:
        option_code = text
    if not re.fullmatch(r"\d+", option_code):
        raise SourceRequestValidationError("symbol must be an option code such as 10011799 or CON_OP_10011799")
    return {
        "option_code": option_code,
        "sina_symbol": f"CON_OP_{option_code}",
    }


def _normalize_option_sse_greeks_row(values: list[str], option_info: Mapping[str, str]) -> dict[str, Any] | None:
    if len(values) < 16 or not values[0]:
        return None
    return {
        **option_info,
        "contract_name": _clean_text(values[0]),
        "volume": _parse_float(values[4]),
        "delta": _parse_float(values[5]),
        "gamma": _parse_float(values[6]),
        "theta": _parse_float(values[7]),
        "vega": _parse_float(values[8]),
        "implied_volatility": _parse_float(values[9]),
        "high": _parse_float(values[10]),
        "low": _parse_float(values[11]),
        "trading_code": _clean_text(values[12]),
        "exercise_price": _parse_float(values[13]),
        "latest_price": _parse_float(values[14]),
        "theoretical_value": _parse_float(values[15]),
        "main_contract_flag": _clean_text(values[16]) if len(values) > 16 else None,
    }


def _normalize_option_sse_spot_price_row(values: list[str], option_info: Mapping[str, str]) -> dict[str, Any] | None:
    if len(values) < 43:
        return None
    return {
        **option_info,
        "buy_volume": _parse_float(values[0]),
        "buy_price": _parse_float(values[1]),
        "latest_price": _parse_float(values[2]),
        "sell_price": _parse_float(values[3]),
        "sell_volume": _parse_float(values[4]),
        "open_interest": _parse_float(values[5]),
        "change_pct": _parse_float(values[6]),
        "exercise_price": _parse_float(values[7]),
        "prev_close": _parse_float(values[8]),
        "open": _parse_float(values[9]),
        "limit_up": _parse_float(values[10]),
        "limit_down": _parse_float(values[11]),
        "quote_time": _clean_text(values[32]),
        "quote_status": _clean_text(values[33]) if len(values) > 33 else None,
        "status_code": _clean_text(values[34]) if len(values) > 34 else None,
        "underlying_type": _clean_text(values[35]) if len(values) > 35 else None,
        "underlying": _clean_text(values[36]) if len(values) > 36 else None,
        "contract_name": _clean_text(values[37]) if len(values) > 37 else None,
        "amplitude": _parse_float(values[38]) if len(values) > 38 else None,
        "high": _parse_float(values[39]) if len(values) > 39 else None,
        "low": _parse_float(values[40]) if len(values) > 40 else None,
        "volume": _parse_float(values[41]) if len(values) > 41 else None,
        "amount": _parse_float(values[42]) if len(values) > 42 else None,
        "main_contract_flag": _clean_text(values[43]) if len(values) > 43 else None,
        "option_type": _clean_text(values[45]) if len(values) > 45 else None,
        "expire_date": _normalize_report_date(values[46]) if len(values) > 46 else None,
        "remaining_days": _parse_int(values[47]) if len(values) > 47 else None,
    }


def _normalize_option_sse_underlying_spot_row(values: list[str], code_info: Mapping[str, str]) -> dict[str, Any] | None:
    if len(values) < 33 or not values[0]:
        return None
    return {
        **code_info,
        "name": _clean_text(values[0]),
        "open": _parse_float(values[1]),
        "prev_close": _parse_float(values[2]),
        "latest_price": _parse_float(values[3]),
        "high": _parse_float(values[4]),
        "low": _parse_float(values[5]),
        "bid": _parse_float(values[6]),
        "ask": _parse_float(values[7]),
        "volume": _parse_float(values[8]),
        "amount": _parse_float(values[9]),
        "bid_volume_1": _parse_float(values[10]),
        "bid_price_1": _parse_float(values[11]),
        "ask_volume_1": _parse_float(values[20]),
        "ask_price_1": _parse_float(values[21]),
        "quote_date": _normalize_report_date(values[30]),
        "quote_time": _clean_text(values[31]),
        "halt_status": _clean_text(values[32]),
    }


def _sina_finance_report_source(value: str) -> tuple[str, str, str]:
    text = value.strip()
    report = SINA_FINANCE_REPORT_SOURCES.get(text) or SINA_FINANCE_REPORT_SOURCES.get(text.lower())
    if report is None:
        raise SourceRequestValidationError("symbol must be one of 资产负债表, 利润表, 现金流量表, balance, income, or cashflow")
    return report


def _normalize_finance_report_item(
    item: Any,
    *,
    report: Mapping[str, Any],
    date_info: Mapping[str, Any],
    report_date: str,
    stock_info: Mapping[str, str],
    statement_type: str,
    statement_name: str,
) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    item_name = _clean_text(item.get("item_title"))
    if item_name is None:
        return None
    return {
        **stock_info,
        "statement_type": statement_type,
        "statement_name": statement_name,
        "report_date": report_date,
        "date_description": _clean_text(date_info.get("date_description")),
        "date_type": _parse_int(date_info.get("date_type")),
        "item_field": _clean_text(item.get("item_field")),
        "item_name": item_name,
        "item_value": _parse_float(item.get("item_value")),
        "item_display_type": _parse_int(item.get("item_display_type")),
        "item_display": _clean_text(item.get("item_display")),
        "item_precision": _clean_text(item.get("item_precision")),
        "item_group_no": _parse_int(item.get("item_group_no")),
        "item_source": _clean_text(item.get("item_source")),
        "item_yoy": _parse_float(item.get("item_tongbi")),
        "data_source": _clean_text(report.get("data_source")),
        "is_audit": _clean_text(report.get("is_audit")),
        "publish_date": _normalize_report_date(report.get("publish_date")),
        "currency": _clean_text(report.get("rCurrency")),
        "report_type": _clean_text(report.get("rType")),
        "update_time": _parse_int(report.get("update_time")),
    }


def _parse_lhb_detail_daily_html(html: str, *, trade_date: str, limit: int) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina dragon tiger parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    rows: list[dict[str, Any]] = []
    for table in tables:
        if table.shape[0] < 3 or table.shape[1] < 7:
            continue
        header_text = " ".join(str(value) for value in table.iloc[1].tolist())
        if "股票代码" not in header_text or "股票名称" not in header_text:
            continue
        indicator = _clean_text(table.iat[0, 0])
        for _, record in table.iloc[2:].iterrows():
            row = _normalize_lhb_detail_row(record.tolist(), trade_date=trade_date, indicator=indicator)
            if row is None:
                continue
            rows.append(row)
            if len(rows) >= limit:
                return rows
    return rows


def _normalize_lhb_detail_row(values: list[Any], *, trade_date: str, indicator: str | None) -> dict[str, Any] | None:
    if len(values) < 7:
        return None
    symbol = _six_digit_code(values[1])
    if symbol is None:
        return None
    exchange = _exchange_from_symbol(symbol)
    return {
        "trade_date": trade_date,
        "rank": _parse_int(values[0]),
        "instrument_id": f"{symbol}.{_exchange_suffix(exchange)}",
        "symbol": symbol,
        "exchange": exchange,
        "name": _clean_text(values[2]),
        "close": _parse_float(values[3]),
        "metric_value": _parse_float(values[4]),
        "volume_10k_shares": _parse_float(values[5]),
        "amount_10k_yuan": _parse_float(values[6]),
        "indicator": indicator,
    }


def _parse_lhb_ggtj_html(html: str, *, recent_days: str, page: int, limit: int) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina dragon tiger parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    if not tables:
        return []
    table = tables[0]
    rows: list[dict[str, Any]] = []
    for index, record in table.iterrows():
        row = _normalize_lhb_ggtj_row(record.tolist(), recent_days=recent_days, page=page, rank=index + 1)
        if row is None:
            continue
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _normalize_lhb_ggtj_row(values: list[Any], *, recent_days: str, page: int, rank: int) -> dict[str, Any] | None:
    if len(values) < 8:
        return None
    symbol = _six_digit_code(values[0])
    if symbol is None:
        return None
    exchange = _exchange_from_symbol(symbol)
    return {
        "recent_days": _parse_int(recent_days),
        "page": page,
        "rank": rank,
        "instrument_id": f"{symbol}.{_exchange_suffix(exchange)}",
        "symbol": symbol,
        "exchange": exchange,
        "name": _clean_text(values[1]),
        "list_count": _parse_int(values[2]),
        "buy_amount_10k_yuan": _parse_float(values[3]),
        "sell_amount_10k_yuan": _parse_float(values[4]),
        "net_amount_10k_yuan": _parse_float(values[5]),
        "buy_seat_count": _parse_int(values[6]),
        "sell_seat_count": _parse_int(values[7]),
    }


def _parse_lhb_jgmx_html(html: str, *, page: int, limit: int) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina dragon tiger parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    if not tables:
        return []
    table = tables[0]
    rows: list[dict[str, Any]] = []
    for index, record in table.iterrows():
        row = _normalize_lhb_jgmx_row(record.tolist(), page=page, rank=index + 1)
        if row is None:
            continue
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _normalize_lhb_jgmx_row(values: list[Any], *, page: int, rank: int) -> dict[str, Any] | None:
    if len(values) < 6:
        return None
    symbol = _six_digit_code(values[0])
    if symbol is None:
        return None
    trade_date = _normalize_report_date(values[2])
    if trade_date is None:
        return None
    exchange = _exchange_from_symbol(symbol)
    return {
        "page": page,
        "rank": rank,
        "instrument_id": f"{symbol}.{_exchange_suffix(exchange)}",
        "symbol": symbol,
        "exchange": exchange,
        "name": _clean_text(values[1]),
        "trade_date": trade_date,
        "institution_buy_amount_10k_yuan": _parse_float(values[3]),
        "institution_sell_amount_10k_yuan": _parse_float(values[4]),
        "trade_type": _clean_text(values[5]),
    }


def _parse_lhb_jgzz_html(html: str, *, recent_days: str, page: int, limit: int) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina dragon tiger parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    if not tables:
        return []
    table = tables[0]
    rows: list[dict[str, Any]] = []
    for index, record in table.iterrows():
        row = _normalize_lhb_jgzz_row(record.tolist(), recent_days=recent_days, page=page, rank=index + 1)
        if row is None:
            continue
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _normalize_lhb_jgzz_row(
    values: list[Any], *, recent_days: str, page: int, rank: int
) -> dict[str, Any] | None:
    if len(values) < 9:
        return None
    symbol = _six_digit_code(values[0])
    if symbol is None:
        return None
    exchange = _exchange_from_symbol(symbol)
    return {
        "recent_days": _parse_int(recent_days),
        "page": page,
        "rank": rank,
        "instrument_id": f"{symbol}.{_exchange_suffix(exchange)}",
        "symbol": symbol,
        "exchange": exchange,
        "name": _clean_text(values[1]),
        "buy_amount_10k_yuan": _parse_float(values[4]),
        "buy_count": _parse_int(values[5]),
        "sell_amount_10k_yuan": _parse_float(values[6]),
        "sell_count": _parse_int(values[7]),
        "net_amount_10k_yuan": _parse_float(values[8]),
    }


def _parse_lhb_yytj_html(html: str, *, recent_days: str, page: int, limit: int) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina dragon tiger parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    if not tables:
        return []
    table = tables[0]
    rows: list[dict[str, Any]] = []
    for index, record in table.iterrows():
        row = _normalize_lhb_yytj_row(record.tolist(), recent_days=recent_days, page=page, rank=index + 1)
        if row is None:
            continue
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _normalize_lhb_yytj_row(
    values: list[Any], *, recent_days: str, page: int, rank: int
) -> dict[str, Any] | None:
    if len(values) < 7:
        return None
    brokerage_name = _clean_text(values[0])
    if brokerage_name is None:
        return None
    return {
        "recent_days": _parse_int(recent_days),
        "page": page,
        "rank": rank,
        "brokerage_name": brokerage_name,
        "list_count": _parse_int(values[1]),
        "buy_amount_10k_yuan": _parse_float(values[2]),
        "buy_seat_count": _parse_int(values[3]),
        "sell_amount_10k_yuan": _parse_float(values[4]),
        "sell_seat_count": _parse_int(values[5]),
        "top_buy_stocks": _clean_text(values[6]),
    }


def _parse_restricted_release_html(html: str, *, limit: int) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina restricted-release parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    if not tables:
        return []
    table = tables[0]
    rows: list[dict[str, Any]] = []
    for record in table.itertuples(index=False):
        row = _normalize_restricted_release_row(list(record))
        if row is None:
            continue
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _normalize_restricted_release_row(values: list[Any]) -> dict[str, Any] | None:
    if len(values) < 7:
        return None
    code_info = _sina_stock_symbol(str(values[0] or ""))
    if code_info is None:
        return None
    release_date = _normalize_report_date(values[2])
    announcement_date = _normalize_report_date(values[6])
    if release_date is None:
        return None
    return {
        **code_info,
        "name": _clean_text(values[1]),
        "release_date": release_date,
        "release_shares_10k": _parse_float(values[3]),
        "release_market_value_100m_yuan": _parse_float(values[4]),
        "batch_no": _parse_int(values[5]),
        "announcement_date": announcement_date,
    }


def _decode_sina_trade_dates(payload: str) -> list[str]:
    match = re.search(r'var\s+datelist\s*=\s*"([^"]+)"', payload)
    if match is None:
        raise SourceUnavailableError("Sina trade date history returned unexpected payload.")
    dates = _decode_sina_klc_date_series(match.group(1))
    dates.add(Date(1992, 5, 4))
    return [item.strftime("%Y%m%d") for item in sorted(dates)]


def _extract_sina_encoded_string(text: str, *, context: str) -> str:
    match = re.search(r'=\s*"([^"]+)"', text)
    if match is None:
        raise SourceUnavailableError(f"{context} returned unexpected encoded payload.")
    return match.group(1)


def _decode_sina_klc_k2_rows(encoded: str) -> list[dict[str, Any]]:
    reader = _SinaKlcBitReader(
        [
            SINA_KLC_ALPHABET.find(char)
            for char in encoded
        ]
    )
    if any(value < 0 for value in reader._values):
        raise SourceUnavailableError("Sina KLC payload contains invalid characters.")
    header = reader.read_values([12, 6])
    if len(header) < 2:
        raise SourceUnavailableError("Sina KLC payload returned truncated header.")
    kind = header[0]
    series_flag = 63 ^ header[1]
    if kind != 3466 or series_flag > 0:
        raise SourceUnavailableError("Sina KLC payload returned unsupported series.")
    state: dict[str, Any] = {
        "b_avp": 1,
        "b_ph": 0,
        "b_phx": 0,
        "b_sep": 0,
        "p_p": 6,
        "p_v": 0,
        "p_a": 0,
        "p_e": 0,
        "p_t": 0,
        "l_o": 3,
        "l_h": 3,
        "l_l": 3,
        "l_c": 3,
        "l_v": 5,
        "l_a": 5,
        "l_e": 3,
        "l_t": 0,
        "u_p": 0,
        "u_v": 0,
        "u_a": 0,
        "wd": 62,
        "d": 0,
    }
    rows: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    while True:
        if reader.exhausted:
            return rows
        record: dict[str, Any] = {"d": 1, "c": 0}
        if reader.read_bit():
            if reader.read_bit():
                if reader.read_bit():
                    record["c"] += 1
                    record["a"] = state["b_avp"]
                    if reader.read_bit():
                        state["b_avp"] ^= int(reader.read_bit())
                        state["b_ph"] ^= int(reader.read_bit())
                        state["b_phx"] ^= int(reader.read_bit())
                        record["s"] = state["b_sep"]
                        state["b_sep"] ^= int(reader.read_bit())
                        if reader.read_bit():
                            state["wd"] = reader.read_values([7])[0]
                        if record.get("s", 0) ^ state["b_sep"]:
                            if record.get("s", 0):
                                state["u_p"] = state.get("u_c", 0)
                            else:
                                state["u_o"] = state["u_h"] = state["u_l"] = state["u_c"] = state["u_p"]
                    for index in range(3 + 2 * state["b_ph"]):
                        if reader.read_bit():
                            key = "pvaet"[index]
                            old_precision = state[f"p_{key}"]
                            state[f"p_{key}"] += reader.read_signed_run_length()
                            state[f"u_{key}"] = _sina_klc_scale(
                                state.get(f"u_{key}", 0), old_precision, state[f"p_{key}"]
                            )
                            if state["b_sep"] and index == 0:
                                for price_key in "ohlc":
                                    state[f"u_{price_key}"] = _sina_klc_scale(
                                        state.get(f"u_{price_key}", 0), old_precision, state["p_p"]
                                    )
                    if not state["b_avp"] and record.get("a"):
                        state["u_a"] = _sina_klc_scale((previous or {}).get("amount", 0), 0, state["p_a"])
                if reader.read_bit():
                    record["c"] += 1
                    for index in range(7 + state["b_ph"] + state["b_phx"]):
                        if reader.read_bit():
                            if index == 6:
                                record["d"] = _sina_klc_read_day_step(reader, state)
                            else:
                                state[f"l_{'ohlcva*et'[index]}"] += reader.read_signed_run_length()
                if reader.read_bit():
                    record["c"] += 1
                    width = state["l_o"] + (reader.read_signed_run_length() if reader.read_bit() else 0)
                    price_delta = reader.read_values([3 * width], signed=[True])[0]
                    if state["b_sep"]:
                        record["p"] = state.get("u_c", 0) + price_delta
                    else:
                        state["u_p"] += price_delta
                        record["p"] = state["u_p"]
                if not record["c"]:
                    break
            else:
                if reader.read_bit():
                    if reader.read_bit():
                        if reader.read_bit():
                            record["d"] = _sina_klc_read_day_step(reader, state)
                        else:
                            state["l_v"] += reader.read_signed_run_length()
                    elif state["b_ph"] and reader.read_bit():
                        suffix_index = 1 if state["b_phx"] and reader.read_bit() else 0
                        state[f"l_{'et'[suffix_index]}"] += reader.read_signed_run_length()
                    else:
                        state["l_a"] += reader.read_signed_run_length()
                else:
                    state[f"l_{'ohlc'[reader.read_values([2])[0]]}"] += reader.read_signed_run_length()
        for index in range(6 + state["b_ph"] + state["b_phx"]):
            key = "ohlcvaet"[index]
            signed = bool(((191 if state["b_sep"] else 185) >> index) & 1)
            record[f"v_{key}"] = reader.read_values([3 * state[f"l_{key}"]], signed=[signed])[0]
        row: dict[str, Any] = {"trade_date": _sina_klc_next_valid_day(state, record["d"]).strftime("%Y%m%d")}
        if record.get("p"):
            row["prev_close"] = _sina_klc_scale(record["p"], state["p_p"])
        if state["b_sep"]:
            state["u_o"] += record["v_o"]
            row["open"] = _sina_klc_scale(state["u_o"], state["p_p"])
            state["u_h"] += record["v_h"]
            row["high"] = _sina_klc_scale(state["u_h"], state["p_p"])
            state["u_l"] += record["v_l"]
            row["low"] = _sina_klc_scale(state["u_l"], state["p_p"])
            state["u_c"] += record["v_c"]
            row["close"] = _sina_klc_scale(state["u_c"], state["p_p"])
        else:
            record["o"] = state["u_p"] + record["v_o"]
            row["open"] = _sina_klc_scale(record["o"], state["p_p"])
            row["high"] = _sina_klc_scale(record["o"] + record["v_h"], state["p_p"])
            row["low"] = _sina_klc_scale(record["o"] - record["v_l"], state["p_p"])
            state["u_p"] = record["o"] + record["v_c"]
            row["close"] = _sina_klc_scale(state["u_p"], state["p_p"])
        state["u_v"] += record["v_v"]
        row["volume"] = _sina_klc_scale(state["u_v"], state["p_v"])
        if state["b_avp"]:
            price_pair = _sina_klc_precision_pair(state["p_p"])
            volume_pair = _sina_klc_precision_pair(state["p_v"])
            if state["b_sep"]:
                average_base = (
                    state.get("u_o", 0)
                    + state.get("u_h", 0)
                    + state.get("u_l", 0)
                    + state.get("u_c", 0)
                ) / 4
            else:
                average_base = record["o"] + (record["v_h"] - record["v_l"] + record["v_c"]) / 4
            amount_base = _sina_klc_scale(
                math.floor(average_base * state["u_v"] + 0.5),
                [price_pair[0] + volume_pair[0], price_pair[1] + volume_pair[1]],
                state["p_a"],
            )
            row["amount"] = _sina_klc_scale(amount_base + record["v_a"], state["p_a"])
        else:
            state["u_a"] += record["v_a"]
            row["amount"] = _sina_klc_scale(state["u_a"], state["p_a"])
        if state["b_ph"]:
            row["post_volume"] = _sina_klc_scale(record.get("v_e", 0), state["p_e"])
            post_amount_base = row["post_volume"] * row["close"]
            if state["b_phx"]:
                post_amount_base += _sina_klc_scale(record.get("v_t", 0), state["p_t"])
            row["post_amount"] = _sina_klc_scale(math.floor(post_amount_base + 0.5), 0)
        rows.append(row)
        previous = row
    return rows


def _decode_sina_klc_date_series(encoded: str) -> set[Date]:
    values: list[int] = []
    for char in encoded:
        index = SINA_KLC_ALPHABET.find(char)
        if index < 0:
            raise SourceUnavailableError("Sina trade date history returned invalid KLC characters.")
        values.append(index)
    reader = _SinaKlcBitReader(values)
    header = reader.read_values([12, 6])
    if len(header) < 2:
        raise SourceUnavailableError("Sina trade date history returned truncated KLC header.")
    kind = header[0]
    series_flag = 63 ^ header[1]
    if kind != 139 or series_flag > 1:
        raise SourceUnavailableError("Sina trade date history returned unsupported KLC series.")
    state = {"d": reader.read_values([18])[0] - 1, "l": 0}
    end_day = reader.read_values([18])[0]
    run_remaining = -1
    result: list[Date] | None = None
    while state["d"] < end_day:
        current = _sina_klc_next_weekday(state)
        if run_remaining <= 0:
            if reader.read_bit():
                state["l"] += reader.read_signed_run_length()
            width = 3 * state["l"]
            run_remaining = reader.read_values([width], signed=[False])[0] + 1
            if result is None:
                result = [current]
                run_remaining -= 1
        else:
            result.append(current)
        run_remaining -= 1
    return set(result or [])


def _sina_klc_read_day_step(reader: "_SinaKlcBitReader", state: dict[str, Any]) -> int:
    step = reader.read_values([3])[0]
    if step == 1:
        state["d"] = reader.read_values([18], signed=[True])[0]
        return 0
    if step == 0:
        return reader.read_values([6])[0]
    return step


def _sina_klc_next_valid_day(state: dict[str, Any], step: int) -> Date:
    weekday_mask = int(state.get("wd") or 62)
    for _ in range(step):
        while True:
            state["d"] += 1
            if weekday_mask & (1 << ((state["d"] % 7 + 10) % 7)):
                break
    return Date(1970, 1, 1) + timedelta(days=7657 + int(state["d"]))


def _sina_klc_precision_pair(value: int) -> list[int]:
    if value == 0:
        return [0, 0]
    if value < 0:
        pair = _sina_klc_precision_pair(-value)
        return [-pair[0], -pair[1]]
    remainder = value % 3
    base = (value - remainder) // 3
    pair = [base, base]
    if remainder:
        pair[remainder - 1] += 1
    return pair


def _sina_klc_scale(
    value: Any,
    source_precision: int | list[int] = 0,
    target_precision: int | None = None,
) -> int | float:
    source_pair = (
        _sina_klc_precision_pair(int(source_precision))
        if isinstance(source_precision, int)
        else [int(source_precision[0]), int(source_precision[1])]
    )
    target_pair = _sina_klc_precision_pair(int(target_precision)) if target_precision is not None else [0, 0]
    exponent_2 = target_pair[0] - source_pair[0]
    exponent_5 = target_pair[1] - source_pair[1]
    with localcontext() as context:
        context.prec = 50
        scaled = Decimal(str(value or 0))
        if exponent_2 >= 0:
            scaled *= Decimal(2) ** exponent_2
        else:
            scaled /= Decimal(2) ** (-exponent_2)
        if exponent_5 >= 0:
            scaled *= Decimal(5) ** exponent_5
        else:
            scaled /= Decimal(5) ** (-exponent_5)
        if target_precision is not None:
            return int(scaled.to_integral_value(rounding=ROUND_HALF_EVEN))
        integral = scaled.to_integral_value()
        if scaled == integral:
            return int(integral)
        return float(scaled)


def _sina_klc_next_weekday(state: dict[str, int]) -> Date:
    state["d"] += 1
    weekday_mod = state["d"] % 7
    if weekday_mod in {3, 4}:
        state["d"] += 5 - weekday_mod
    return Date(1970, 1, 1) + timedelta(days=7657 + state["d"])


class _SinaKlcBitReader:
    def __init__(self, values: list[int]) -> None:
        self._values = values
        self._value_index = 0
        self._bit_index = 0

    @property
    def exhausted(self) -> bool:
        return self._value_index >= len(self._values)

    def read_bit(self) -> bool:
        if self._value_index >= len(self._values):
            return False
        bit = self._values[self._value_index] & (1 << self._bit_index)
        self._bit_index += 1
        if self._bit_index >= 6:
            self._bit_index -= 6
            self._value_index += 1
        return bool(bit)

    def read_values(self, widths: list[int], signed: list[bool] | None = None) -> list[int]:
        signed = signed or []
        result: list[int] = []
        for index, original_width in enumerate(widths):
            width = original_width
            value = 0
            if width <= 0:
                result.append(0)
                continue
            if width > 30:
                is_signed = index < len(signed) and signed[index]
                pair = self.read_values([30, width - 30], signed=[False, is_signed])
                if len(pair) < 2:
                    return result
                result.append(pair[0] + pair[1] * (1 << 30))
                continue
            if self._value_index >= len(self._values):
                return result
            while width > 0:
                take = min(6 - self._bit_index, width)
                mask = (1 << take) - 1
                value |= ((self._values[self._value_index] >> self._bit_index) & mask) << (original_width - width)
                self._bit_index += take
                if self._bit_index >= 6:
                    self._bit_index -= 6
                    self._value_index += 1
                width -= take
            if index < len(signed) and signed[index] and value >= 2 ** (original_width - 1):
                value -= 2 ** original_width
            result.append(value)
        return result

    def read_signed_run_length(self) -> int:
        sign_bit = self.read_bit()
        length = 1
        while True:
            if not self.read_bit():
                return length * (2 * int(sign_bit) - 1)
            length += 1


def _six_digit_code(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return None
    try:
        if re.fullmatch(r"\d+(?:\.0+)?", text):
            return f"{int(float(text)):06d}"
    except ValueError:
        return None
    digits = re.sub(r"\D", "", text)
    if not digits:
        return None
    return digits[-6:].zfill(6)


def _normalize_fund_etf_category_row(item: Any, category: str) -> dict[str, Any] | None:
    values: list[Any]
    if isinstance(item, Mapping):
        values = [
            item.get("symbol") or item.get("代码"),
            item.get("name") or item.get("名称"),
            item.get("trade") or item.get("最新价"),
            item.get("pricechange") or item.get("涨跌额"),
            item.get("changepercent") or item.get("涨跌幅"),
            item.get("buy") or item.get("买入"),
            item.get("sell") or item.get("卖出"),
            item.get("settlement") or item.get("昨收"),
            item.get("open") or item.get("今开"),
            item.get("high") or item.get("最高"),
            item.get("low") or item.get("最低"),
            item.get("volume") or item.get("成交量"),
            item.get("amount") or item.get("成交额"),
        ]
    elif isinstance(item, (list, tuple)):
        values = list(item)
    else:
        return None
    if len(values) < 13:
        return None
    code_info = _sina_market_symbol(str(values[0] or ""))
    if code_info is None:
        return None
    return {
        **code_info,
        "fund_type": category,
        "name": _clean_text(values[1]),
        "latest_price": _parse_float(values[2]),
        "change": _parse_float(values[3]),
        "change_pct": _parse_float(values[4]),
        "bid": _parse_float(values[5]),
        "ask": _parse_float(values[6]),
        "prev_close": _parse_float(values[7]),
        "open": _parse_float(values[8]),
        "high": _parse_float(values[9]),
        "low": _parse_float(values[10]),
        "volume": _parse_float(values[11]),
        "amount": _parse_float(values[12]),
    }


def _parse_bond_cb_profile_html(
    html: str, *, code_info: Mapping[str, str], limit: int
) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina convertible-bond profile parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    if not tables:
        return []
    rows: list[dict[str, Any]] = []
    for index, record in tables[0].iterrows():
        values = record.tolist()
        if len(values) < 2:
            continue
        item_name = _clean_text(values[0])
        if item_name is None:
            continue
        rows.append(
            {
                **code_info,
                "sequence": index + 1,
                "item_name": item_name,
                "item_value": _clean_text(values[1]),
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _parse_bond_cb_summary_html(
    html: str, *, code_info: Mapping[str, str], limit: int
) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina convertible-bond summary parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    summary_table = None
    for table in tables:
        if table.shape[1] < 6 or table.empty:
            continue
        first_column = {_clean_text(value) for value in table.iloc[:, 0].tolist()}
        if "债券类型" in first_column:
            summary_table = table
            break
    if summary_table is None:
        return []
    rows: list[dict[str, Any]] = []
    for _, record in summary_table.iterrows():
        values = record.tolist()
        for pair_offset in (0, 2, 4):
            if len(values) <= pair_offset + 1:
                continue
            item_name = _clean_text(values[pair_offset])
            if item_name is None:
                continue
            rows.append(
                {
                    **code_info,
                    "sequence": len(rows) + 1,
                    "item_name": item_name,
                    "item_value": _clean_text(values[pair_offset + 1]),
                }
            )
            if len(rows) >= limit:
                return rows
    return rows


def _normalize_gb_daily_row(
    item: Any, *, source_symbol: str, display_name: str | None, country: str
) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    trade_date = _normalize_report_date(item.get("d") or item.get("date"))
    if trade_date is None:
        return None
    return {
        "source_symbol": source_symbol,
        "symbol_name": display_name,
        "country": country,
        "tenor": _sina_gb_tenor(source_symbol),
        "trade_date": trade_date,
        "open_yield": _parse_float(item.get("o") or item.get("open")),
        "high_yield": _parse_float(item.get("h") or item.get("high")),
        "low_yield": _parse_float(item.get("l") or item.get("low")),
        "close_yield": _parse_float(item.get("c") or item.get("close")),
        "volume": _parse_float(item.get("v") or item.get("volume")),
    }


def _sina_gb_display_name(symbol_map: Mapping[str, str], source_symbol: str) -> str | None:
    for name, code in symbol_map.items():
        if code == source_symbol:
            return name
    return None


def _sina_gb_tenor(source_symbol: str) -> str:
    match = re.search(r"(\d+)([MY])T$", source_symbol)
    if match is None:
        return source_symbol
    number, unit = match.groups()
    return f"{number}{unit}"


def _parse_currency_boc_html(
    html: str, *, currency_code: str, currency_name: str, limit: int
) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina BOC forex parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html), header=0)
    except ValueError:
        return []
    if not tables:
        return []
    rows: list[dict[str, Any]] = []
    for item in tables[0].to_dict("records"):
        row = _normalize_currency_boc_row(item, currency_code=currency_code, currency_name=currency_name)
        if row is None:
            continue
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _normalize_currency_boc_row(
    item: Mapping[str, Any], *, currency_code: str, currency_name: str
) -> dict[str, Any] | None:
    quote_date = _normalize_report_date(item.get("日期"))
    if quote_date is None:
        return None
    return {
        "currency_code": currency_code,
        "currency_name": currency_name,
        "quote_date": quote_date,
        "fx_buy_rate": _parse_float(item.get("中行汇买价(元)") or item.get("中行汇买价")),
        "cash_buy_rate": _parse_float(item.get("中行钞买价(元)") or item.get("中行钞买价")),
        "cash_sell_rate": _parse_float(item.get("中行钞卖价/汇卖价")),
        "pboc_mid_rate": _parse_float(item.get("央行中间价")),
        "boc_conversion_rate": _parse_float(item.get("中行折算价")),
    }


def _hyphen_date(value: str) -> str:
    return f"{value[:4]}-{value[4:6]}-{value[6:]}"


def _normalize_fund_scale_row(item: Any, *, fund_category: str, rank: int) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    fund_code = _six_digit_code(item.get("symbol"))
    if fund_code is None:
        return None
    return {
        "rank": rank,
        "fund_code": fund_code,
        "fund_name": _clean_text(item.get("sname") or item.get("name")),
        "fund_category": fund_category,
        "unit_nav": _parse_float(item.get("dwjz") or item.get("per_nav")),
        "total_raised_scale": _parse_float(item.get("zmjgm")),
        "latest_total_share": _parse_float(item.get("zjzfe")),
        "established_date": _normalize_report_date(item.get("clrq")),
        "fund_manager": _clean_text(item.get("jjjl")),
        "nav_date": _normalize_report_date(item.get("jzrq")),
    }


def _normalize_futures_quote_row(item: Any, *, source_node: str, rank: int) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    source_symbol = _clean_text(item.get("symbol"))
    if source_symbol is None:
        return None
    return {
        "rank": rank,
        "source_node": source_node,
        "source_symbol": source_symbol,
        "exchange": _clean_text(item.get("exchange")),
        "name": _clean_text(item.get("name")),
        "trade_date": _normalize_report_date(item.get("tradedate")),
        "tick_time": _clean_text(item.get("ticktime")),
        "last_price": _parse_float(item.get("trade")),
        "open": _parse_float(item.get("open")),
        "high": _parse_float(item.get("high")),
        "low": _parse_float(item.get("low")),
        "close": _parse_float(item.get("close")),
        "preclose": _parse_float(item.get("preclose")),
        "settlement": _parse_float(item.get("settlement")),
        "prev_settlement": _parse_float(item.get("prevsettlement") or item.get("presettlement")),
        "bid_price_1": _parse_float(item.get("bidprice1")),
        "ask_price_1": _parse_float(item.get("askprice1")),
        "bid_volume_1": _parse_float(item.get("bidvol1")),
        "ask_volume_1": _parse_float(item.get("askvol1")),
        "volume": _parse_float(item.get("volume")),
        "open_interest": _parse_float(item.get("position")),
        "change_pct": _parse_float(item.get("changepercent")),
    }


def _parse_futures_hold_pos_html(
    html: str,
    *,
    metric: str,
    table_index: int,
    contract: str,
    trade_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - pandas is a core dependency
        raise SourceUnavailableError("Sina futures holding position parsing requires pandas.") from exc
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    if len(tables) <= table_index:
        return []
    table = tables[table_index]
    headers = [_clean_item_name(column) or "" for column in table.columns.tolist()]
    rows: list[dict[str, Any]] = []
    for _, record in table.iterrows():
        row = _normalize_futures_hold_pos_row(
            record.tolist(),
            headers=headers,
            metric=metric,
            contract=contract,
            trade_date=trade_date,
        )
        if row is None:
            continue
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def _normalize_futures_hold_pos_row(
    values: list[Any],
    *,
    headers: list[str],
    metric: str,
    contract: str,
    trade_date: str,
) -> dict[str, Any] | None:
    rank = _parse_int(_table_value(values, headers, ("名次", "排名"), 0))
    if rank is None:
        return None
    member_name = _clean_text(_table_value(values, headers, ("会员简称", "会员名称", "期货公司", "机构名称"), 1))
    if member_name is None:
        return None
    return {
        "trade_date": trade_date,
        "contract": contract,
        "metric": metric,
        "rank": rank,
        "member_name": member_name,
        "value": _parse_float(_table_value(values, headers, (metric,), 2)),
        "change": _parse_float(_table_value(values, headers, ("比上交易增减", "增减", "较上交易日增减"), 3)),
    }


def _table_value(values: list[Any], headers: list[str], aliases: tuple[str, ...], fallback_index: int) -> Any:
    for alias in aliases:
        for index, header in enumerate(headers):
            if header == alias or alias in header:
                if index < len(values):
                    return values[index]
    if fallback_index < len(values):
        return values[fallback_index]
    return None


def _normalize_futures_daily_kline_row(item: Any, *, source_symbol: str) -> dict[str, Any] | None:
    if isinstance(item, Mapping):
        trade_date = _normalize_report_date(item.get("d") or item.get("date"))
        if trade_date is None:
            return None
        return {
            "source_symbol": source_symbol,
            "trade_date": trade_date,
            "open": _parse_float(item.get("o") or item.get("open")),
            "high": _parse_float(item.get("h") or item.get("high")),
            "low": _parse_float(item.get("l") or item.get("low")),
            "close": _parse_float(item.get("c") or item.get("close")),
            "volume": _parse_float(item.get("v") or item.get("volume")),
            "open_interest": _parse_float(item.get("p") or item.get("position") or item.get("hold")),
            "settlement": _parse_float(item.get("s") or item.get("settlement")),
        }
    if isinstance(item, (list, tuple)) and len(item) >= 8:
        trade_date = _normalize_report_date(item[0])
        if trade_date is None:
            return None
        return {
            "source_symbol": source_symbol,
            "trade_date": trade_date,
            "open": _parse_float(item[1]),
            "high": _parse_float(item[2]),
            "low": _parse_float(item[3]),
            "close": _parse_float(item[4]),
            "volume": _parse_float(item[5]),
            "open_interest": _parse_float(item[6]),
            "settlement": _parse_float(item[7]),
        }
    return None


def _normalize_futures_minute_kline_row(
    item: Any, *, source_symbol: str, period: str
) -> dict[str, Any] | None:
    if isinstance(item, Mapping):
        raw_datetime = item.get("d") or item.get("datetime")
        normalized_datetime = _normalize_datetime_text(raw_datetime)
        if normalized_datetime is None:
            return None
        return {
            "source_symbol": source_symbol,
            "period": period,
            "datetime": normalized_datetime,
            "trade_date": normalized_datetime[:8],
            "trade_time": _format_trade_time(normalized_datetime),
            "open": _parse_float(item.get("o") or item.get("open")),
            "high": _parse_float(item.get("h") or item.get("high")),
            "low": _parse_float(item.get("l") or item.get("low")),
            "close": _parse_float(item.get("c") or item.get("close")),
            "volume": _parse_float(item.get("v") or item.get("volume")),
            "open_interest": _parse_float(item.get("p") or item.get("position") or item.get("hold")),
        }
    if isinstance(item, (list, tuple)) and len(item) >= 7:
        normalized_datetime = _normalize_datetime_text(item[0])
        if normalized_datetime is None:
            return None
        return {
            "source_symbol": source_symbol,
            "period": period,
            "datetime": normalized_datetime,
            "trade_date": normalized_datetime[:8],
            "trade_time": _format_trade_time(normalized_datetime),
            "open": _parse_float(item[1]),
            "high": _parse_float(item[2]),
            "low": _parse_float(item[3]),
            "close": _parse_float(item[4]),
            "volume": _parse_float(item[5]),
            "open_interest": _parse_float(item[6]),
        }
    return None


def _normalize_cffex_option_symbol(value: str, *, prefix: str) -> str:
    text = value.strip()
    match = re.fullmatch(rf"({prefix})(\d{{4}})([CP])(\d+)", text, flags=re.IGNORECASE)
    if not match:
        raise SourceRequestValidationError(
            f"symbol must be a CFFEX option symbol such as {prefix}2202P4350"
        )
    prefix_value, expire, option_type, strike = match.groups()
    return f"{prefix_value.lower()}{expire}{option_type.upper()}{strike}"


def _normalize_cffex_option_month_symbol(value: str, *, product_code: str) -> str:
    text = value.strip().lower()
    if not re.fullmatch(rf"{product_code}\d{{4}}", text):
        raise SourceRequestValidationError(
            f"symbol must be a CFFEX option month symbol such as {product_code}2607"
        )
    return text


def _normalize_cffex_option_daily_row(
    item: Any, *, source_symbol: str, underlying_name: str
) -> dict[str, Any] | None:
    if isinstance(item, Mapping):
        trade_date = _normalize_report_date(item.get("d") or item.get("date"))
        if trade_date is None:
            return None
        open_price = item.get("o") or item.get("open")
        high_price = item.get("h") or item.get("high")
        low_price = item.get("l") or item.get("low")
        close_price = item.get("c") or item.get("close")
        volume = item.get("v") or item.get("volume")
    elif isinstance(item, (list, tuple)) and len(item) >= 6:
        open_price, high_price, low_price, close_price, volume, raw_date = item[:6]
        trade_date = _normalize_report_date(raw_date)
        if trade_date is None:
            return None
    else:
        return None
    option_type = "看涨期权" if "C" in source_symbol.upper() else "看跌期权"
    strike_match = re.search(r"[CP](\d+)$", source_symbol.upper())
    return {
        "source_symbol": source_symbol,
        "underlying_name": underlying_name,
        "option_type": option_type,
        "exercise_price": _parse_float(strike_match.group(1) if strike_match else None),
        "trade_date": trade_date,
        "open": _parse_float(open_price),
        "high": _parse_float(high_price),
        "low": _parse_float(low_price),
        "close": _parse_float(close_price),
        "volume": _parse_float(volume),
    }


def _normalize_cffex_option_spot_row(
    call_values: Any,
    put_values: Any,
    *,
    contract_code: str,
    product_code: str,
    underlying_name: str,
) -> dict[str, Any] | None:
    if not isinstance(call_values, (list, tuple)) or not isinstance(put_values, (list, tuple)):
        return None
    if len(call_values) < 9 or len(put_values) < 8:
        return None
    call_symbol = _clean_text(call_values[8])
    put_symbol = _clean_text(put_values[7])
    exercise_price = _parse_float(call_values[7])
    if call_symbol is None or put_symbol is None or exercise_price is None:
        return None
    return {
        "contract_code": contract_code,
        "product_code": product_code,
        "underlying_name": underlying_name,
        "exercise_price": exercise_price,
        "call_symbol": call_symbol,
        "call_bid_volume": _parse_float(call_values[0]),
        "call_bid_price": _parse_float(call_values[1]),
        "call_latest_price": _parse_float(call_values[2]),
        "call_ask_price": _parse_float(call_values[3]),
        "call_ask_volume": _parse_float(call_values[4]),
        "call_open_interest": _parse_float(call_values[5]),
        "call_change": _parse_float(call_values[6]),
        "put_symbol": put_symbol,
        "put_bid_volume": _parse_float(put_values[0]),
        "put_bid_price": _parse_float(put_values[1]),
        "put_latest_price": _parse_float(put_values[2]),
        "put_ask_price": _parse_float(put_values[3]),
        "put_ask_volume": _parse_float(put_values[4]),
        "put_open_interest": _parse_float(put_values[5]),
        "put_change": _parse_float(put_values[6]),
        "exchange": "CFFEX",
    }


def _parse_cffex_option_contract_list_html(
    html: str, *, product_code: str, underlying_name: str, limit: int
) -> list[dict[str, Any]]:
    match = re.search(r'id=["\']option_suffix["\'].*?</div>', html, flags=re.IGNORECASE | re.DOTALL)
    section = match.group(0) if match else html
    contracts: list[str] = []
    for raw_contract in re.findall(r'data-value=["\']([^"\']+)["\']', section, flags=re.IGNORECASE):
        contract = _clean_text(raw_contract)
        if contract is None:
            continue
        contract = contract.lower()
        if not re.fullmatch(rf"{product_code}\d{{4}}", contract):
            continue
        if contract not in contracts:
            contracts.append(contract)
        if len(contracts) >= limit:
            break
    rows: list[dict[str, Any]] = []
    for sequence, contract in enumerate(contracts, start=1):
        rows.append(
            {
                "sequence": sequence,
                "product_code": product_code,
                "underlying_name": underlying_name,
                "contract_code": contract,
                "expire_month": _cffex_contract_expire_month(contract, product_code=product_code),
                "is_main": sequence == 1,
                "exchange": "CFFEX",
            }
        )
    return rows


def _parse_commodity_option_contract_links(html: str, *, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    pattern = re.compile(
        r'<a[^>]+href=["\'](?P<href>[^"\']*optionsDP\.php/(?P<product>[^/"\']+)/(?P<exchange>[^/"\']+))["\'][^>]*>(?P<label>.*?)</a>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        product_code = _clean_text(match.group("product"))
        exchange = _clean_text(match.group("exchange"))
        option_name = _clean_text(re.sub(r"<.*?>", "", match.group("label"), flags=re.DOTALL))
        if product_code is None or exchange is None or option_name is None:
            continue
        key = (product_code, exchange)
        if key in seen:
            continue
        seen.add(key)
        source_path = match.group("href")
        if not source_path.startswith("/"):
            source_path = "/" + source_path
        rows.append(
            {
                "sequence": len(rows) + 1,
                "option_name": option_name,
                "product_code": product_code,
                "exchange": exchange,
                "source_path": source_path,
                "source_url": f"https://stock.finance.sina.com.cn{source_path}",
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _select_commodity_option_contract(
    rows: list[dict[str, Any]], symbol: str
) -> dict[str, Any] | None:
    lookup = symbol.strip().lower()
    for row in rows:
        if row.get("option_name") == symbol:
            return row
        if str(row.get("product_code") or "").lower() == lookup:
            return row
    return None


def _parse_commodity_option_contract_table(
    html: str,
    *,
    option_name: str,
    product_code: str,
    exchange: str,
    source_url: str,
    limit: int,
) -> list[dict[str, Any]]:
    match = re.search(r'id=["\']option_suffix["\'].*?</div>', html, flags=re.IGNORECASE | re.DOTALL)
    section = match.group(0) if match else html
    product_prefix = product_code.split("_", 1)[0].lower()
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_contract in re.findall(r'data-value=["\']([^"\']+)["\']', section, flags=re.IGNORECASE):
        contract_code = _clean_text(raw_contract)
        if contract_code is None:
            continue
        contract_code = contract_code.lower()
        if not contract_code.startswith(product_prefix) or contract_code in seen:
            continue
        seen.add(contract_code)
        rows.append(
            {
                "sequence": len(rows) + 1,
                "option_name": option_name,
                "product_code": product_code,
                "exchange": exchange,
                "contract_code": contract_code,
                "expire_month": _commodity_option_contract_expire_month(contract_code, product_prefix=product_prefix),
                "is_main": len(rows) == 0,
                "source_url": source_url,
            }
        )
        if len(rows) >= limit:
            break
    return rows


def _commodity_option_contract_expire_month(contract_code: str, *, product_prefix: str) -> str | None:
    match = re.fullmatch(rf"{re.escape(product_prefix)}(\d{{2}})(\d{{2}})", contract_code)
    if not match:
        return None
    year, month = match.groups()
    return f"20{year}{month}"


def _normalize_commodity_option_symbol(value: str, *, product_prefix: str) -> str:
    text = value.strip()
    match = re.fullmatch(rf"({re.escape(product_prefix)})(\d{{4}})([CP])(\d+)", text, flags=re.IGNORECASE)
    if not match:
        raise SourceRequestValidationError(
            f"symbol must be a commodity option symbol such as {product_prefix}2609P2500"
        )
    prefix, expire, option_type, strike = match.groups()
    return f"{prefix.lower()}{expire}{option_type.upper()}{strike}"


def _normalize_commodity_option_daily_row(
    item: Any,
    *,
    source_symbol: str,
    option_name: str,
    product_code: str,
    exchange: str,
) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    trade_date = _normalize_report_date(item.get("d") or item.get("date"))
    if trade_date is None:
        return None
    symbol_match = re.fullmatch(r"([a-z]+)(\d{4})([CP])(\d+)", source_symbol, flags=re.IGNORECASE)
    option_type = None
    exercise_price = None
    if symbol_match:
        _prefix, _expire, raw_option_type, strike = symbol_match.groups()
        option_type = "看涨期权" if raw_option_type.upper() == "C" else "看跌期权"
        exercise_price = _parse_float(strike)
    return {
        "source_symbol": source_symbol,
        "option_name": option_name,
        "product_code": product_code,
        "exchange": exchange,
        "option_type": option_type,
        "exercise_price": exercise_price,
        "trade_date": trade_date,
        "open": _parse_float(item.get("o") or item.get("open")),
        "high": _parse_float(item.get("h") or item.get("high")),
        "low": _parse_float(item.get("l") or item.get("low")),
        "close": _parse_float(item.get("c") or item.get("close")),
        "volume": _parse_float(item.get("v") or item.get("volume")),
    }


def _cffex_contract_expire_month(contract: str, *, product_code: str) -> str | None:
    match = re.fullmatch(rf"{product_code}(\d{{2}})(\d{{2}})", contract)
    if not match:
        return None
    year, month = match.groups()
    return f"20{year}{month}"


def _normalize_index_stock_cons_row(item: Any, index_code: str) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    code_info = _sina_stock_symbol(str(item.get("symbol") or item.get("code") or ""))
    if code_info is None:
        return None
    return {
        **code_info,
        "index_code": index_code,
        "name": _clean_text(item.get("name")),
        "latest_price": _parse_float(item.get("trade")),
        "change": _parse_float(item.get("pricechange")),
        "change_pct": _parse_float(item.get("changepercent")),
        "bid": _parse_float(item.get("buy")),
        "ask": _parse_float(item.get("sell")),
        "prev_close": _parse_float(item.get("settlement")),
        "open": _parse_float(item.get("open")),
        "high": _parse_float(item.get("high")),
        "low": _parse_float(item.get("low")),
        "volume": _parse_float(item.get("volume")),
        "amount": _parse_float(item.get("amount")),
        "tick_time": _clean_text(item.get("ticktime")),
        "pe": _parse_float(item.get("per")),
        "pb": _parse_float(item.get("pb")),
        "market_cap": _parse_float(item.get("mktcap")),
        "float_market_cap": _parse_float(item.get("nmc")),
        "turnover_ratio": _parse_float(item.get("turnoverratio")),
    }


def _normalize_stock_classify_row(
    item: Any,
    *,
    category: str,
    class_name: str,
    source_node: str,
    rank: int,
) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    raw_symbol = _clean_text(item.get("symbol") or item.get("code"))
    if raw_symbol is None:
        return None
    code_info = _sina_market_symbol(raw_symbol)
    if code_info is None:
        raw_code = _clean_text(item.get("code"))
        code_info = _sina_market_symbol(raw_code or "")
    if code_info is None:
        return None
    return {
        "category": category,
        "class_name": class_name,
        "source_node": source_node,
        "rank": rank,
        "instrument_id": code_info["instrument_id"],
        "symbol": code_info["fund_code"],
        "sina_symbol": code_info["sina_symbol"],
        "exchange": code_info["exchange"],
        "name": _clean_text(item.get("name")),
        "latest_price": _parse_float(item.get("trade") or item.get("price") or item.get("latest_price")),
        "change": _parse_float(item.get("pricechange") or item.get("change")),
        "change_pct": _parse_float(item.get("changepercent") or item.get("change_pct")),
        "bid": _parse_float(item.get("buy")),
        "ask": _parse_float(item.get("sell")),
        "prev_close": _parse_float(item.get("settlement") or item.get("prev_close")),
        "open": _parse_float(item.get("open")),
        "high": _parse_float(item.get("high")),
        "low": _parse_float(item.get("low")),
        "volume": _parse_float(item.get("volume")),
        "amount": _parse_float(item.get("amount")),
        "tick_time": _clean_text(item.get("ticktime") or item.get("time")),
    }


def _normalize_zh_index_spot_row(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    code_info = _sina_stock_symbol(str(item.get("symbol") or item.get("code") or ""))
    if code_info is None:
        return None
    return {
        **code_info,
        "asset_type": "index",
        "name": _clean_text(item.get("name")),
        "latest_price": _parse_float(item.get("trade")),
        "change": _parse_float(item.get("pricechange")),
        "change_pct": _parse_float(item.get("changepercent")),
        "bid": _parse_float(item.get("buy")),
        "ask": _parse_float(item.get("sell")),
        "prev_close": _parse_float(item.get("settlement")),
        "open": _parse_float(item.get("open")),
        "high": _parse_float(item.get("high")),
        "low": _parse_float(item.get("low")),
        "volume": _parse_float(item.get("volume")),
        "amount": _parse_float(item.get("amount")),
        "tick_time": _clean_text(item.get("ticktime")),
    }


def _normalize_hk_index_spot_row(sina_symbol: str, values: list[str]) -> dict[str, Any] | None:
    if len(values) < 9 or not values[0]:
        return None
    return {
        "sina_symbol": sina_symbol,
        "index_code": _clean_text(values[0]),
        "market": "HK",
        "asset_type": "index",
        "name": _clean_text(values[1]),
        "open": _parse_float(values[2]),
        "prev_close": _parse_float(values[3]),
        "high": _parse_float(values[4]),
        "low": _parse_float(values[5]),
        "latest_price": _parse_float(values[6]),
        "change": _parse_float(values[7]),
        "change_pct": _parse_float(values[8]),
    }


def _normalize_global_info_feed_row(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    content = _strip_html_text(item.get("rich_text") or item.get("text") or item.get("content"))
    create_time = _clean_text(item.get("create_time") or item.get("ctime") or item.get("time"))
    if content is None or create_time is None:
        return None
    return {
        "feed_id": _clean_text(item.get("id") or item.get("feed_id") or item.get("docid")),
        "create_time": create_time,
        "content": content,
    }


def _normalize_stock_intraday_row(
    item: Any, *, code_info: Mapping[str, str], trade_date: str
) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    tick_time = _clean_text(item.get("ticktime") or item.get("time"))
    if tick_time is None:
        return None
    return {
        **code_info,
        "trade_date": trade_date,
        "tick_time": tick_time,
        "price": _parse_float(item.get("price")),
        "volume": _parse_float(item.get("volume")),
        "prev_price": _parse_float(item.get("prev_price")),
        "trade_type": _clean_text(item.get("type") or item.get("kind")),
    }


def _normalize_esg_rft_row(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    symbol = _clean_text(item.get("symbol"))
    if symbol is None:
        return None
    return {
        "symbol": symbol,
        "esg_score": _parse_float(item.get("esg_score")),
        "esg_score_date": _normalize_report_date(item.get("esg_score_date")),
        "env_score": _parse_float(item.get("env_score")),
        "env_score_date": _normalize_report_date(item.get("env_score_date")),
        "social_score": _parse_float(item.get("social_score")),
        "social_score_date": _normalize_report_date(item.get("social_score_date")),
        "governance_score": _parse_float(item.get("governance_score")),
        "governance_score_date": _normalize_report_date(item.get("governance_score_date")),
        "controversy_score": _parse_float(item.get("zy_score")),
        "controversy_score_date": _normalize_report_date(item.get("zy_score_date")),
        "industry": _clean_text(item.get("industry")),
        "exchange": _clean_text(item.get("exchange")),
    }


def _normalize_esg_msci_row(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    symbol = _clean_text(item.get("symbol"))
    if symbol is None:
        return None
    return {
        "symbol": symbol,
        "name": _clean_text(item.get("name")),
        "market": _clean_text(item.get("market")),
        "industry_code": _clean_text(item.get("industry_code")),
        "industry_name": _clean_text(item.get("industry_name")),
        "esg_rating": _clean_text(item.get("esg_rating") or item.get("grade")),
        "env_score": _parse_float(item.get("env_score")),
        "social_score": _parse_float(item.get("social_score")),
        "governance_score": _parse_float(item.get("governance_score")),
        "rating_date": _normalize_report_date(item.get("quarter_date")),
        "updated_time": _clean_text(item.get("updated_time")),
    }


def _normalize_esg_rate_row(item: Any, *, symbol: str | None, market: str | None) -> dict[str, Any] | None:
    if not isinstance(item, Mapping) or symbol is None:
        return None
    return {
        "symbol": symbol,
        "market": market,
        "agency_name": _clean_text(item.get("agency_name")),
        "rating": _clean_text(item.get("esg_score")),
        "rating_period": _clean_text(item.get("esg_dt")),
        "remark": _clean_text(item.get("remark")),
    }


def _normalize_esg_zd_row(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    symbol = _clean_text(item.get("ticker") or item.get("symbol"))
    if symbol is None:
        return None
    return {
        "symbol": symbol,
        "esg_score": _parse_float(item.get("esg_score")),
        "env_score": _parse_float(item.get("environmental_score")),
        "social_score": _parse_float(item.get("social_score")),
        "governance_score": _parse_float(item.get("governance_score")),
        "report_date": _normalize_report_date(item.get("report_date")),
    }


def _normalize_esg_hz_row(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, Mapping):
        return None
    symbol = _clean_text(item.get("symbol"))
    if symbol is None:
        return None
    return {
        "date": _normalize_report_date(item.get("date")),
        "symbol": symbol,
        "market": _clean_text(item.get("market")),
        "name": _clean_text(item.get("name")),
        "esg_score": _parse_float(item.get("esg_score")),
        "esg_grade": _clean_text(item.get("esg_score_grade")),
        "env_score": _parse_float(item.get("e_score")),
        "env_grade": _clean_text(item.get("e_score_grade")),
        "social_score": _parse_float(item.get("s_score")),
        "social_grade": _clean_text(item.get("s_score_grade")),
        "governance_score": _parse_float(item.get("g_score")),
        "governance_grade": _clean_text(item.get("g_score_grade")),
    }


def _sina_market_symbol(value: str) -> dict[str, str] | None:
    original = value.strip()
    upper = original.upper()
    if upper.endswith((".SH", ".SZ", ".BJ")) and len(upper) >= 9:
        suffix = upper[-2:]
        code = upper[:-3]
        if not re.fullmatch(r"\d{6}", code):
            return None
        prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}[suffix]
        exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}[suffix]
        return {
            "instrument_id": f"{code}.{_exchange_suffix(exchange)}",
            "fund_code": code,
            "sina_symbol": f"{prefix}{code}",
            "exchange": exchange,
        }
    text = original.lower()
    match = re.fullmatch(r"(sh|sz|bj)?(\d{6})", text)
    if not match:
        return None
    prefix, code = match.groups()
    if prefix is None:
        exchange = _exchange_from_symbol(code)
        prefix = _exchange_suffix(exchange).lower()
    else:
        exchange = {"sh": "SSE", "sz": "SZSE", "bj": "BSE"}[prefix]
    return {
        "instrument_id": f"{code}.{_exchange_suffix(exchange)}",
        "fund_code": code,
        "sina_symbol": f"{prefix}{code}",
        "exchange": exchange,
    }


def _sina_stock_symbol(value: str) -> dict[str, str] | None:
    info = _sina_market_symbol(value)
    if info is None:
        return None
    return {
        "instrument_id": info["instrument_id"],
        "symbol": info["fund_code"],
        "sina_symbol": info["sina_symbol"],
        "exchange": info["exchange"],
    }


def _normalize_query_date(value: Any, name: str, *, required: bool) -> str | None:
    if value in (None, ""):
        if required:
            raise SourceRequestValidationError(f"{name} is required")
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) != 8:
        raise SourceRequestValidationError(f"{name} must be YYYYMMDD or YYYY-MM-DD")
    return digits


def _normalize_report_date(value: Any) -> str | None:
    text = str(value or "").strip()
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 8:
        return digits[:8]
    return None


def _normalize_datetime_text(value: Any) -> str | None:
    text = str(value or "").strip()
    digits = re.sub(r"\D", "", text)
    if len(digits) == 12:
        return f"{digits}00"
    if len(digits) >= 14:
        return digits[:14]
    return None


def _format_trade_time(datetime_text: str) -> str:
    return f"{datetime_text[8:10]}:{datetime_text[10:12]}:{datetime_text[12:14]}"


def _clean_item_name(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", "", str(value)).strip()
    if not text or text.lower() == "nan" or text == "--":
        return None
    return text


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text or text.lower() == "nan" or text == "--":
        return None
    return text


def _strip_html_text(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    return _clean_text(text)


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    text = re.sub(r",", "", str(value)).strip()
    if not text or text.lower() == "nan" or text == "--":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    return int(parsed)

def _today_yyyymmdd() -> str:
    return Date.today().strftime("%Y%m%d")


def _recent_window_start(days: int = 30) -> str:
    """区间类接口无日期时的统一默认:最近 30 天窗口起点。"""
    return (Date.today() - timedelta(days=days - 1)).strftime("%Y%m%d")
