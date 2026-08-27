"""Cninfo source request adapter."""

import base64
import json
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
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
    "cninfo_announcements",
    "cninfo_announcement_detail",
    "stock_irm_cninfo",
    "stock_irm_ans_cninfo",
    "stock_zh_a_disclosure_report_cninfo",
    "stock_zh_a_disclosure_relation_cninfo",
    "stock_profile_cninfo",
    "stock_allotment_cninfo",
    "stock_dividend_cninfo",
    "stock_hold_change_cninfo",
    "stock_hold_control_cninfo",
    "stock_hold_num_cninfo",
    "stock_industry_category_cninfo",
    "stock_ipo_summary_cninfo",
    "stock_new_gh_cninfo",
    "stock_new_ipo_cninfo",
    "stock_share_change_cninfo",
    "fund_report_asset_allocation_cninfo",
    "bond_corporate_issue_cninfo",
    "bond_cov_issue_cninfo",
    "bond_cov_stock_issue_cninfo",
    "bond_local_government_issue_cninfo",
    "bond_treasure_issue_cninfo",
    "fund_report_industry_allocation_cninfo",
    "fund_report_stock_cninfo",
    "stock_cg_equity_mortgage_cninfo",
    "stock_cg_guarantee_cninfo",
    "stock_cg_lawsuit_cninfo",
    "stock_hold_management_detail_cninfo",
    "stock_industry_change_cninfo",
    "stock_industry_pe_ratio_cninfo",
    "stock_rank_forecast_cninfo",
}

CNINFO_STOCK_INDEX_URL = "https://www.cninfo.com.cn/new/data/szse_stock.json"
CNINFO_ANNOUNCEMENT_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_STATIC_BASE = "https://static.cninfo.com.cn/"
CNINFO_IRM_KEYWORD_URL = "https://irm.cninfo.com.cn/newircs/index/queryKeyboardInfo"
CNINFO_IRM_QUESTION_URL = "https://irm.cninfo.com.cn/newircs/company/question"
CNINFO_IRM_DETAIL_URL = "https://irm.cninfo.com.cn/newircs/question/getQuestionDetail"
CNINFO_WEBAPI_STOCK_PROFILE_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1133"
CNINFO_WEBAPI_STOCK_ALLOTMENT_URL = "https://webapi.cninfo.com.cn/api/stock/p_stock2232"
CNINFO_WEBAPI_STOCK_DIVIDEND_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1139"
CNINFO_WEBAPI_STOCK_HOLD_CHANGE_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1029"
CNINFO_WEBAPI_STOCK_HOLD_CONTROL_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1033"
CNINFO_WEBAPI_STOCK_HOLD_NUM_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1034"
CNINFO_WEBAPI_STOCK_INDUSTRY_CATEGORY_URL = "https://webapi.cninfo.com.cn/api/stock/p_public0002"
CNINFO_WEBAPI_STOCK_INDUSTRY_PE_RATIO_URL = "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1087"
CNINFO_WEBAPI_STOCK_RANK_FORECAST_URL = "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1089"
CNINFO_WEBAPI_STOCK_IPO_SUMMARY_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1134"
CNINFO_WEBAPI_STOCK_NEW_GH_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1098"
CNINFO_WEBAPI_STOCK_NEW_IPO_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1097"
CNINFO_WEBAPI_STOCK_SHARE_CHANGE_URL = "https://webapi.cninfo.com.cn/api/stock/p_stock2215"
CNINFO_WEBAPI_STOCK_INDUSTRY_CHANGE_URL = "https://webapi.cninfo.com.cn/api/stock/p_stock2110"
CNINFO_WEBAPI_FUND_ASSET_ALLOCATION_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1114"
CNINFO_WEBAPI_FUND_INDUSTRY_ALLOCATION_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1113"
CNINFO_WEBAPI_FUND_STOCK_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1112"
CNINFO_WEBAPI_STOCK_CG_EQUITY_MORTGAGE_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1094"
CNINFO_WEBAPI_STOCK_CG_GUARANTEE_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1054"
CNINFO_WEBAPI_STOCK_CG_LAWSUIT_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1055"
CNINFO_WEBAPI_STOCK_HOLD_MANAGEMENT_DETAIL_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1030"
CNINFO_WEBAPI_BOND_CORPORATE_ISSUE_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1122"
CNINFO_WEBAPI_BOND_COV_ISSUE_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1123"
CNINFO_WEBAPI_BOND_COV_STOCK_ISSUE_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1124"
CNINFO_WEBAPI_BOND_LOCAL_GOVERNMENT_ISSUE_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1121"
CNINFO_WEBAPI_BOND_TREASURE_ISSUE_URL = "https://webapi.cninfo.com.cn/api/sysapi/p_sysapi1120"
CNINFO_TZ = timezone(timedelta(hours=8))
CNINFO_ENCKEY_KEY = b"1234567887654321"
CNINFO_ENCKEY_IV = b"1234567887654321"
CNINFO_DISCLOSURE_CATEGORY_MAP = {
    "年报": "category_ndbg_szsh",
    "半年报": "category_bndbg_szsh",
    "一季报": "category_yjdbg_szsh",
    "三季报": "category_sjdbg_szsh",
    "业绩预告": "category_yjygjxz_szsh",
    "权益分派": "category_qyfpxzcs_szsh",
    "董事会": "category_dshgg_szsh",
    "监事会": "category_jshgg_szsh",
    "股东大会": "category_gddh_szsh",
    "日常经营": "category_rcjy_szsh",
    "公司治理": "category_gszl_szsh",
    "中介报告": "category_zj_szsh",
    "首发": "category_sf_szsh",
    "增发": "category_zf_szsh",
    "股权激励": "category_gqjl_szsh",
    "配股": "category_pg_szsh",
    "解禁": "category_jj_szsh",
    "公司债": "category_gszq_szsh",
    "可转债": "category_kzzq_szsh",
    "其他融资": "category_qtrz_szsh",
    "股权变动": "category_gqbd_szsh",
    "补充更正": "category_bcgz_szsh",
    "澄清致歉": "category_cqdq_szsh",
    "风险提示": "category_fxts_szsh",
    "特别处理和退市": "category_tbclts_szsh",
    "退市整理期": "category_tszlq_szsh",
}
CNINFO_HOLD_CHANGE_MARKET_MAP = {
    "深市主板": "012002",
    "沪市": "012001",
    "创业板": "012015",
    "科创板": "012029",
    "北交所": "012046",
    "全部": "",
}
CNINFO_HOLD_CONTROL_TYPE_MAP = {
    "单独控制": "069001",
    "实际控制人": "069002",
    "一致行动人": "069003",
    "家族控制": "069004",
    "全部": "",
}
CNINFO_INDUSTRY_TYPE_MAP = {
    "证监会行业分类标准": "008001",
    "巨潮行业分类标准": "008002",
    "申银万国行业分类标准": "008003",
    "新财富行业分类标准": "008004",
    "国资委行业分类标准": "008005",
    "巨潮产业细分标准": "008006",
    "天相行业分类标准": "008007",
    "全球行业分类标准": "008008",
}
CNINFO_MANAGEMENT_CHANGE_TYPE_MAP = {
    "增持": "B",
    "减持": "S",
    "B": "B",
    "S": "S",
}
CNINFO_INDUSTRY_PE_CLASSIFICATION_MAP = {
    "证监会行业分类": "008001",
    "国证行业分类": "008200",
    "008001": "008001",
    "008200": "008200",
}


@dataclass(frozen=True)
class _WebapiField:
    name: str
    source: str | int
    kind: str = "text"


CNINFO_STOCK_PROFILE_FIELDS = (
    _WebapiField("company_name", 0),
    _WebapiField("english_name", 1),
    _WebapiField("former_short_name", 2),
    _WebapiField("a_share_code", 3),
    _WebapiField("a_share_name", 4),
    _WebapiField("b_share_code", 5),
    _WebapiField("b_share_name", 6),
    _WebapiField("h_share_code", 7),
    _WebapiField("h_share_name", 8),
    _WebapiField("selected_indexes", 9),
    _WebapiField("market", 10),
    _WebapiField("industry", 11),
    _WebapiField("legal_representative", 12),
    _WebapiField("registered_capital", 13, "float"),
    _WebapiField("founded_date", 14, "date"),
    _WebapiField("listing_date", 15, "date"),
    _WebapiField("website", 16),
    _WebapiField("email", 17),
    _WebapiField("phone", 18),
    _WebapiField("fax", 19),
    _WebapiField("registered_address", 20),
    _WebapiField("office_address", 21),
    _WebapiField("postcode", 22),
    _WebapiField("main_business", 23),
    _WebapiField("business_scope", 24),
    _WebapiField("organization_profile", 25),
)

CNINFO_STOCK_ALLOTMENT_FIELDS = (
    _WebapiField("record_id", 0),
    _WebapiField("name", 1),
    _WebapiField("suspend_start_date", 2, "date"),
    _WebapiField("listing_announcement_date", 3, "date"),
    _WebapiField("payment_start_date", 4, "date"),
    _WebapiField("convertible_allotment_shares", 5, "float"),
    _WebapiField("suspend_end_date", 6, "date"),
    _WebapiField("actual_allotment_shares", 7, "float"),
    _WebapiField("allotment_price", 8, "float"),
    _WebapiField("allotment_ratio", 9, "float"),
    _WebapiField("pre_total_share", 10, "float"),
    _WebapiField("transfer_fee_per_share", 11, "float"),
    _WebapiField("legal_person_actual_shares", 12, "float"),
    _WebapiField("raised_funds_net", 13, "float"),
    _WebapiField("major_shareholder_subscribe_method", 14),
    _WebapiField("other_allotment_name", 15),
    _WebapiField("issue_method", 16),
    _WebapiField("failed_refund_date", 17, "date"),
    _WebapiField("ex_right_date", 18, "date"),
    _WebapiField("expected_issue_expense", 19, "float"),
    _WebapiField("issue_result_announcement_date", 20, "date"),
    _WebapiField("warrant_trade_end_date", 22, "date"),
    _WebapiField("other_actual_shares", 23, "float"),
    _WebapiField("state_actual_shares", 24, "float"),
    _WebapiField("entrusted_unit", 25),
    _WebapiField("public_transfer_shares", 26, "float"),
    _WebapiField("other_allotment_code", 27),
    _WebapiField("allotment_target", 28),
    _WebapiField("warrant_trade_start_date", 29, "date"),
    _WebapiField("fund_arrival_date", 30, "date"),
    _WebapiField("organization_name", 31),
    _WebapiField("record_date", 32, "date"),
    _WebapiField("raised_funds_gross", 33, "float"),
    _WebapiField("expected_raised_funds", 34, "float"),
    _WebapiField("major_shareholder_subscribe_shares", 35, "float"),
    _WebapiField("public_actual_shares", 36, "float"),
    _WebapiField("transfer_actual_shares", 37, "float"),
    _WebapiField("underwriting_fee", 38, "float"),
    _WebapiField("legal_person_transfer_shares", 39, "float"),
    _WebapiField("post_float_share", 40, "float"),
    _WebapiField("stock_class", 41),
    _WebapiField("public_allotment_name", 42),
    _WebapiField("issue_method_code", 43),
    _WebapiField("underwriting_method", 44),
    _WebapiField("announcement_date", 45, "date"),
    _WebapiField("allotment_listing_date", 46, "date"),
    _WebapiField("payment_end_date", 47, "date"),
    _WebapiField("underwriting_balance", 48, "float"),
    _WebapiField("expected_allotment_shares", 49, "float"),
    _WebapiField("post_total_share", 50, "float"),
    _WebapiField("employee_actual_shares", 51, "float"),
    _WebapiField("underwriting_method_code", 52),
    _WebapiField("issue_expenses_total", 53, "float"),
    _WebapiField("pre_float_share", 54, "float"),
    _WebapiField("stock_class_code", 55),
    _WebapiField("public_allotment_code", 56),
)

CNINFO_STOCK_DIVIDEND_FIELDS = (
    _WebapiField("announcement_date", "F006D", "date"),
    _WebapiField("dividend_type", "F044V"),
    _WebapiField("bonus_share_ratio", "F011N", "float"),
    _WebapiField("transfer_share_ratio", "F010N", "float"),
    _WebapiField("cash_dividend_ratio", "F012N", "float"),
    _WebapiField("record_date", "F018D", "date"),
    _WebapiField("ex_right_date", "F020D", "date"),
    _WebapiField("dividend_payment_date", "F023D", "date"),
    _WebapiField("share_arrival_date", "F025D", "date"),
    _WebapiField("plan_description", "F007V"),
    _WebapiField("report_period", "F001V"),
)

CNINFO_STOCK_HOLD_CHANGE_FIELDS = (
    _WebapiField("circulated_share", 0, "float"),
    _WebapiField("total_share", 1, "float"),
    _WebapiField("trade_market", 2),
    _WebapiField("name", 3),
    _WebapiField("announcement_date", 4, "date"),
    _WebapiField("change_reason", 5),
    _WebapiField("symbol", 6),
    _WebapiField("change_date", 7, "date"),
    _WebapiField("restricted_share", 8, "float"),
    _WebapiField("circulated_ratio", 9, "float"),
)

CNINFO_STOCK_HOLD_CONTROL_FIELDS = (
    _WebapiField("holding_ratio", 0, "float"),
    _WebapiField("holding_shares", 1, "float"),
    _WebapiField("name", 2),
    _WebapiField("actual_controller_name", 3),
    _WebapiField("direct_controller_name", 4),
    _WebapiField("control_type", 5),
    _WebapiField("symbol", 6),
    _WebapiField("change_date", 7, "date"),
)

CNINFO_STOCK_HOLD_NUM_FIELDS = (
    _WebapiField("avg_holding", 0, "float"),
    _WebapiField("shareholder_count_change_pct", 1, "float"),
    _WebapiField("prev_shareholder_count", 2, "float"),
    _WebapiField("shareholder_count", 3, "float"),
    _WebapiField("name", 4),
    _WebapiField("symbol", 5),
    _WebapiField("avg_holding_change_pct", 6, "float"),
    _WebapiField("change_date", 7, "date"),
    _WebapiField("prev_avg_holding", 8, "float"),
)

CNINFO_STOCK_INDUSTRY_CATEGORY_FIELDS = (
    _WebapiField("parent_code", "PARENTCODE"),
    _WebapiField("category_code", "SORTCODE"),
    _WebapiField("category_name", "SORTNAME"),
    _WebapiField("category_name_en", "F001V"),
    _WebapiField("end_date", "F002D", "date"),
    _WebapiField("industry_type_code", "F003V"),
    _WebapiField("industry_type", "F004V"),
)

CNINFO_STOCK_IPO_SUMMARY_FIELDS = (
    _WebapiField("source_symbol", 0),
    _WebapiField("prospectus_announcement_date", 1, "date"),
    _WebapiField("lottery_rate_announcement_date", 2, "date"),
    _WebapiField("par_value", 3, "float"),
    _WebapiField("total_issue_shares", 4, "float"),
    _WebapiField("nav_per_share_before_issue", 5, "float"),
    _WebapiField("diluted_pe", 6, "float"),
    _WebapiField("raised_funds_net", 7, "float"),
    _WebapiField("online_issue_date", 8, "date"),
    _WebapiField("listing_date", 9, "date"),
    _WebapiField("issue_price", 10, "float"),
    _WebapiField("issue_expenses_total", 11, "float"),
    _WebapiField("nav_per_share_after_issue", 12, "float"),
    _WebapiField("online_lottery_rate", 13, "float"),
    _WebapiField("lead_underwriter", 14),
)

CNINFO_STOCK_NEW_IPO_FIELDS = (
    _WebapiField("lottery_result_announcement_date", 0, "date"),
    _WebapiField("winning_announcement_date", 1, "date"),
    _WebapiField("name", 2),
    _WebapiField("listing_date", 3, "date"),
    _WebapiField("payment_date", 4, "date"),
    _WebapiField("subscription_date", 5, "date"),
    _WebapiField("issue_price", 6, "float"),
    _WebapiField("symbol", 7),
    _WebapiField("online_lottery_rate", 8, "float"),
    _WebapiField("total_issue_shares", 9, "float"),
    _WebapiField("issue_pe", 10, "float"),
    _WebapiField("online_issue_shares", 11, "float"),
    _WebapiField("online_subscription_limit", 12, "float"),
)

CNINFO_STOCK_NEW_GH_FIELDS = (
    _WebapiField("company_name", 0),
    _WebapiField("meeting_date", 1, "date"),
    _WebapiField("review_type", 2),
    _WebapiField("review_content", 3),
    _WebapiField("review_result", 4),
    _WebapiField("announcement_date", 5, "date"),
)

CNINFO_STOCK_SHARE_CHANGE_FIELDS = (
    _WebapiField("symbol", "SECCODE"),
    _WebapiField("name", "SECNAME"),
    _WebapiField("organization_name", "ORGNAME"),
    _WebapiField("announcement_date", "DECLAREDATE", "date"),
    _WebapiField("change_date", "VARYDATE", "date"),
    _WebapiField("change_reason_code", "F001V"),
    _WebapiField("change_reason", "F002V"),
    _WebapiField("total_share", "F003N", "float"),
    _WebapiField("non_circulating_share", "F004N", "float"),
    _WebapiField("promoter_share", "F005N", "float"),
    _WebapiField("state_share", "F006N", "float"),
    _WebapiField("state_owned_legal_person_share", "F007N", "float"),
    _WebapiField("domestic_legal_person_share", "F008N", "float"),
    _WebapiField("foreign_legal_person_share", "F009N", "float"),
    _WebapiField("natural_person_share", "F010N", "float"),
    _WebapiField("raised_legal_person_share", "F011N", "float"),
    _WebapiField("employee_share", "F012N", "float"),
    _WebapiField("transferred_share", "F013N", "float"),
    _WebapiField("other_restricted_share", "F014N", "float"),
    _WebapiField("preferred_share", "F015N", "float"),
    _WebapiField("other_non_circulating_share", "F016N", "float"),
    _WebapiField("circulating_share", "F021N", "float"),
    _WebapiField("a_share", "F022N", "float"),
    _WebapiField("b_share", "F023N", "float"),
    _WebapiField("h_share", "F024N", "float"),
    _WebapiField("executive_share", "F025N", "float"),
    _WebapiField("other_circulating_share", "F026N", "float"),
    _WebapiField("restricted_share", "F028N", "float"),
    _WebapiField("allocated_legal_person_share", "F017N", "float"),
    _WebapiField("strategic_investor_share", "F018N", "float"),
    _WebapiField("securities_investment_fund_share", "F019N", "float"),
    _WebapiField("general_legal_person_share", "F020N", "float"),
    _WebapiField("state_restricted_share", "F029N", "float"),
    _WebapiField("state_owned_legal_person_restricted_share", "F030N", "float"),
    _WebapiField("other_domestic_restricted_share", "F031N", "float"),
    _WebapiField("domestic_legal_person_restricted_share", "F032N", "float"),
    _WebapiField("domestic_natural_person_restricted_share", "F033N", "float"),
    _WebapiField("foreign_restricted_share", "F034N", "float"),
    _WebapiField("foreign_legal_person_restricted_share", "F035N", "float"),
    _WebapiField("foreign_natural_person_restricted_share", "F036N", "float"),
    _WebapiField("restricted_executive_share", "F037N", "float"),
    _WebapiField("restricted_b_share", "F038N", "float"),
    _WebapiField("restricted_h_share", "F040N", "float"),
    _WebapiField("controlling_shareholder_actual_controller_share", "F050N", "float"),
)

CNINFO_FUND_ASSET_ALLOCATION_FIELDS = (
    _WebapiField("report_date", "ENDDATE", "date"),
    _WebapiField("fund_count", "F001N", "float"),
    _WebapiField("equity_asset_pct", "F006N", "float"),
    _WebapiField("bond_asset_pct", "F007N", "float"),
    _WebapiField("cash_asset_pct", "F008N", "float"),
    _WebapiField("fund_market_net_assets", "F005N", "float"),
)

CNINFO_FUND_INDUSTRY_ALLOCATION_FIELDS = (
    _WebapiField("industry_code", "F001V"),
    _WebapiField("industry_name", "F002V"),
    _WebapiField("report_date", "ENDDATE", "date"),
    _WebapiField("fund_count", "F003N", "float"),
    _WebapiField("industry_scale", "F004N", "float"),
    _WebapiField("net_asset_pct", "F005N", "float"),
)

CNINFO_FUND_STOCK_FIELDS = (
    _WebapiField("record_id", "ID"),
    _WebapiField("symbol", "SECCODE"),
    _WebapiField("name", "SECNAME"),
    _WebapiField("report_date", "ENDDATE", "date"),
    _WebapiField("fund_count", "F001N", "float"),
    _WebapiField("holding_shares", "F002N", "float"),
    _WebapiField("holding_market_value", "F003N", "float"),
)

CNINFO_STOCK_CG_EQUITY_MORTGAGE_FIELDS = (
    _WebapiField("released_pledge_shares", 0, "float"),
    _WebapiField("name", 1),
    _WebapiField("announcement_date", 2, "date"),
    _WebapiField("pledge_event", 3),
    _WebapiField("pledgee", 4),
    _WebapiField("pledgor", 5),
    _WebapiField("symbol", 6),
    _WebapiField("pledged_total_share_pct", 7, "float"),
    _WebapiField("cumulative_pledge_total_share_pct", 8, "float"),
    _WebapiField("pledged_shares", 9, "float"),
)

CNINFO_STOCK_CG_GUARANTEE_FIELDS = (
    _WebapiField("announcement_period", 0),
    _WebapiField("guarantee_amount_net_asset_pct", 1, "float"),
    _WebapiField("guarantee_amount", 2, "float"),
    _WebapiField("guarantee_count", 3, "float"),
    _WebapiField("name", 4),
    _WebapiField("symbol", 5),
    _WebapiField("parent_equity", 6, "float"),
)

CNINFO_STOCK_CG_LAWSUIT_FIELDS = (
    _WebapiField("announcement_period", 0),
    _WebapiField("lawsuit_amount", 1, "float"),
    _WebapiField("lawsuit_count", 2, "float"),
    _WebapiField("name", 3),
    _WebapiField("symbol", 4),
)

CNINFO_STOCK_HOLD_MANAGEMENT_DETAIL_FIELDS = (
    _WebapiField("name", 0),
    _WebapiField("announcement_date", 1, "date"),
    _WebapiField("executive_name", 2),
    _WebapiField("ending_market_value", 3, "float"),
    _WebapiField("average_price", 4, "float"),
    _WebapiField("symbol", 5),
    _WebapiField("change_ratio", 6, "float"),
    _WebapiField("change_shares", 7, "float"),
    _WebapiField("end_date", 8, "date"),
    _WebapiField("ending_holding_shares", 9, "float"),
    _WebapiField("beginning_holding_shares", 10, "float"),
    _WebapiField("changer_relation", 11),
    _WebapiField("director_supervisor_senior_position", 12),
    _WebapiField("director_supervisor_senior_name", 13),
    _WebapiField("data_source", 14),
    _WebapiField("change_reason", 15),
)

CNINFO_STOCK_INDUSTRY_CHANGE_FIELDS = (
    _WebapiField("organization_name", "ORGNAME"),
    _WebapiField("symbol", "SECCODE"),
    _WebapiField("name", "SECNAME"),
    _WebapiField("change_date", "VARYDATE", "date"),
    _WebapiField("classification_standard_code", "F001V"),
    _WebapiField("classification_standard", "F002V"),
    _WebapiField("industry_code", "F003V"),
    _WebapiField("industry_sector", "F004V"),
    _WebapiField("industry_subcategory", "F005V"),
    _WebapiField("industry_major", "F006V"),
    _WebapiField("industry_middle", "F007V"),
    _WebapiField("latest_record_flag", "F008C"),
)

CNINFO_STOCK_INDUSTRY_PE_RATIO_FIELDS = (
    _WebapiField("industry_level", 0, "int"),
    _WebapiField("static_pe_mean", 1, "float"),
    _WebapiField("static_pe_median", 2, "float"),
    _WebapiField("static_pe_weighted", 3, "float"),
    _WebapiField("net_profit_static", 4, "float"),
    _WebapiField("industry_name", 5),
    _WebapiField("industry_code", 6),
    _WebapiField("classification", 7),
    _WebapiField("total_market_value_static", 8, "float"),
    _WebapiField("included_company_count", 9, "float"),
    _WebapiField("change_date", 10, "date"),
    _WebapiField("company_count", 11, "float"),
)

CNINFO_STOCK_RANK_FORECAST_FIELDS = (
    _WebapiField("name", 0),
    _WebapiField("publish_date", 1, "date"),
    _WebapiField("previous_rating", 2),
    _WebapiField("rating_change", 3),
    _WebapiField("target_price_high", 4, "float"),
    _WebapiField("is_first_rating", 5),
    _WebapiField("rating", 6),
    _WebapiField("analyst_name", 7),
    _WebapiField("institution_short_name", 8),
    _WebapiField("target_price_low", 9, "float"),
    _WebapiField("symbol", 10),
)

CNINFO_BOND_CORPORATE_ISSUE_FIELDS = (
    _WebapiField("bond_code", "SECCODE"),
    _WebapiField("bond_short_name", "SECNAME"),
    _WebapiField("announcement_date", "DECLAREDATE", "date"),
    _WebapiField("online_issue_start_date", "F003D", "date"),
    _WebapiField("online_issue_end_date", "F004D", "date"),
    _WebapiField("planned_issue_amount", "F005N", "float"),
    _WebapiField("actual_issue_amount", "F006N", "float"),
    _WebapiField("par_value", "F008N", "float"),
    _WebapiField("issue_price", "F007N", "float"),
    _WebapiField("issue_method", "F013V"),
    _WebapiField("issue_target", "F014V"),
    _WebapiField("issue_scope", "F015V"),
    _WebapiField("underwriting_method", "F017V"),
    _WebapiField("min_subscription_unit", "F022N", "float"),
    _WebapiField("fundraising_use", "F023V"),
    _WebapiField("min_subscription_amount", "F052N", "float"),
    _WebapiField("bond_name", "BONDNAME"),
)

CNINFO_BOND_COV_ISSUE_FIELDS = (
    _WebapiField("bond_code", "SECCODE"),
    _WebapiField("bond_short_name", "SECNAME"),
    _WebapiField("announcement_date", "DECLAREDATE", "date"),
    _WebapiField("issue_start_date", "F029D", "date"),
    _WebapiField("issue_end_date", "F003D", "date"),
    _WebapiField("planned_issue_amount", "F005N", "float"),
    _WebapiField("actual_issue_amount", "F006N", "float"),
    _WebapiField("par_value", "F007N", "float"),
    _WebapiField("issue_price", "F052N", "float"),
    _WebapiField("issue_method", "F013V"),
    _WebapiField("issue_target", "F014V"),
    _WebapiField("issue_scope", "F015V"),
    _WebapiField("underwriting_method", "F017V"),
    _WebapiField("fundraising_use", "F021V"),
    _WebapiField("initial_conversion_price", "F026N", "float"),
    _WebapiField("conversion_start_date", "F027D", "date"),
    _WebapiField("conversion_end_date", "F053D", "date"),
    _WebapiField("online_subscription_date", "F051D", "date"),
    _WebapiField("online_subscription_code", "F031V"),
    _WebapiField("online_subscription_short_name", "F032V"),
    _WebapiField("online_subscription_max", "F008N", "float"),
    _WebapiField("online_subscription_min", "F066N", "float"),
    _WebapiField("online_subscription_unit", "F067N", "float"),
    _WebapiField("online_lottery_result_refund_date", "F068D", "date"),
    _WebapiField("priority_subscription_date", "F004D", "date"),
    _WebapiField("allotment_price", "F065N", "float"),
    _WebapiField("bondholder_record_date", "F028D", "date"),
    _WebapiField("priority_subscription_payment_date", "F054D", "date"),
    _WebapiField("conversion_code", "F086V"),
    _WebapiField("trading_market", "F002V"),
    _WebapiField("bond_name", "BONDNAME"),
)

CNINFO_BOND_COV_STOCK_ISSUE_FIELDS = (
    _WebapiField("bond_code", "SECCODE"),
    _WebapiField("bond_short_name", "SECNAME"),
    _WebapiField("announcement_date", "DECLAREDATE", "date"),
    _WebapiField("conversion_code", "F001V"),
    _WebapiField("conversion_short_name", "F002V"),
    _WebapiField("conversion_price", "F003N", "float"),
    _WebapiField("voluntary_conversion_start_date", "F004D", "date"),
    _WebapiField("voluntary_conversion_end_date", "F005D", "date"),
    _WebapiField("underlying_stock", "F017V"),
    _WebapiField("bond_name", "BONDNAME"),
)

CNINFO_BOND_PUBLIC_ISSUE_FIELDS = (
    _WebapiField("bond_code", "SECCODE"),
    _WebapiField("bond_short_name", "SECNAME"),
    _WebapiField("issue_start_date", "F004D", "date"),
    _WebapiField("issue_end_date", "F003D", "date"),
    _WebapiField("planned_issue_amount", "F006N", "float"),
    _WebapiField("actual_issue_amount", "F005N", "float"),
    _WebapiField("issue_price", "F007N", "float"),
    _WebapiField("par_value", "F008N", "float"),
    _WebapiField("payment_date", "F009D", "date"),
    _WebapiField("additional_issue_count", "F028N", "float"),
    _WebapiField("trading_market", "F002V"),
    _WebapiField("issue_method", "F013V"),
    _WebapiField("issue_target", "F014V"),
    _WebapiField("announcement_date", "DECLAREDATE", "date"),
    _WebapiField("bond_name", "BONDNAME"),
)


class CninfoRequestAdapter:
    """Request Cninfo announcement metadata and return AxData fields."""

    source = "cninfo"

    def __init__(self, opener: Any | None = None, *, timeout: float = 20.0) -> None:
        self._opener = opener
        self._timeout = timeout
        self.last_meta: dict[str, Any] = {}
        self._stock_index: dict[str, dict[str, Any]] | None = None

    def supports(self, interface_name: str) -> bool:
        return interface_name in SUPPORTED_INTERFACES

    def request(self, interface_name: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if interface_name == "cninfo_announcements":
            return self._request_announcements(params)
        if interface_name == "cninfo_announcement_detail":
            return self._request_announcement_detail(params)
        if interface_name == "stock_irm_cninfo":
            return self._request_irm_questions(params)
        if interface_name == "stock_irm_ans_cninfo":
            return self._request_irm_answer(params)
        if interface_name == "stock_zh_a_disclosure_report_cninfo":
            return self._request_disclosure(params, tab_name="fulltext")
        if interface_name == "stock_zh_a_disclosure_relation_cninfo":
            return self._request_disclosure(params, tab_name="relation")
        if interface_name == "stock_profile_cninfo":
            return self._request_stock_profile(params)
        if interface_name == "stock_allotment_cninfo":
            return self._request_stock_allotment(params)
        if interface_name == "stock_dividend_cninfo":
            return self._request_stock_dividend(params)
        if interface_name == "stock_hold_change_cninfo":
            return self._request_stock_hold_change(params)
        if interface_name == "stock_hold_control_cninfo":
            return self._request_stock_hold_control(params)
        if interface_name == "stock_hold_num_cninfo":
            return self._request_stock_hold_num(params)
        if interface_name == "stock_industry_category_cninfo":
            return self._request_stock_industry_category(params)
        if interface_name == "stock_ipo_summary_cninfo":
            return self._request_stock_ipo_summary(params)
        if interface_name == "stock_new_gh_cninfo":
            return self._request_stock_new_gh(params)
        if interface_name == "stock_new_ipo_cninfo":
            return self._request_stock_new_ipo(params)
        if interface_name == "stock_share_change_cninfo":
            return self._request_stock_share_change(params)
        if interface_name == "fund_report_asset_allocation_cninfo":
            return self._request_fund_asset_allocation(params)
        if interface_name == "fund_report_industry_allocation_cninfo":
            return self._request_fund_industry_allocation(params)
        if interface_name == "fund_report_stock_cninfo":
            return self._request_fund_stock(params)
        if interface_name == "stock_cg_equity_mortgage_cninfo":
            return self._request_stock_cg_equity_mortgage(params)
        if interface_name == "stock_cg_guarantee_cninfo":
            return self._request_stock_cg_guarantee(params)
        if interface_name == "stock_cg_lawsuit_cninfo":
            return self._request_stock_cg_lawsuit(params)
        if interface_name == "stock_hold_management_detail_cninfo":
            return self._request_stock_hold_management_detail(params)
        if interface_name == "stock_industry_change_cninfo":
            return self._request_stock_industry_change(params)
        if interface_name == "stock_industry_pe_ratio_cninfo":
            return self._request_stock_industry_pe_ratio(params)
        if interface_name == "stock_rank_forecast_cninfo":
            return self._request_stock_rank_forecast(params)
        if interface_name == "bond_corporate_issue_cninfo":
            return self._request_bond_corporate_issue(params)
        if interface_name == "bond_cov_issue_cninfo":
            return self._request_bond_cov_issue(params)
        if interface_name == "bond_cov_stock_issue_cninfo":
            return self._request_bond_cov_stock_issue(params)
        if interface_name == "bond_local_government_issue_cninfo":
            return self._request_bond_public_issue(
                params,
                interface_name=interface_name,
                url=CNINFO_WEBAPI_BOND_LOCAL_GOVERNMENT_ISSUE_URL,
                context="Cninfo local-government bond issue",
                default_start_date="20210911",
                default_end_date="20211110",
            )
        if interface_name == "bond_treasure_issue_cninfo":
            return self._request_bond_public_issue(
                params,
                interface_name=interface_name,
                url=CNINFO_WEBAPI_BOND_TREASURE_ISSUE_URL,
                context="Cninfo treasury bond issue",
                default_start_date="20210910",
                default_end_date="20211109",
            )
        raise SourceAdapterNotFound(
            f"Cninfo source adapter does not support interface {interface_name!r}."
        )

    def _request_announcements(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        codes = _parse_code_values(params.get("code"))
        start_date = _normalize_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        page = _positive_int(params.get("page"), default=1, name="page")
        limit = min(_positive_int(params.get("limit"), default=30, name="limit"), 100)

        rows: list[dict[str, Any]] = []
        empty_codes: list[str] = []
        for code in codes:
            stock = self._resolve_stock(code)
            if stock is None:
                empty_codes.append(code)
                continue
            payload = self._fetch_announcements_for_stock(
                stock,
                start_date=start_date,
                end_date=end_date,
                page=page,
                limit=limit,
            )
            announcements = payload.get("announcements")
            if not isinstance(announcements, list):
                empty_codes.append(code)
                continue
            for item in announcements:
                if isinstance(item, Mapping):
                    rows.append(_normalize_announcement_row(item, stock))

        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_ANNOUNCEMENT_QUERY_URL,
            "requested_codes": codes,
            "empty_codes": empty_codes,
            "page": page,
            "limit": limit,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
        }
        return rows

    def _request_announcement_detail(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        announcement_id = str(params.get("announcement_id") or "").strip()
        adjunct_url = str(params.get("url") or params.get("download_url") or "").strip()
        if not announcement_id and not adjunct_url:
            raise SourceRequestValidationError("announcement_id or url is required")

        if not adjunct_url:
            raise SourceRequestValidationError(
                "url is required when announcement_id is not accompanied by a Cninfo PDF path"
            )
        download_url = _cninfo_download_url(adjunct_url)
        metadata = self._fetch_pdf_metadata(download_url)
        row = {
            "announcement_id": announcement_id or _announcement_id_from_url(download_url),
            "title": _clean_text(params.get("title")),
            "content_type": metadata.get("content_type"),
            "file_size_bytes": metadata.get("file_size_bytes"),
            "download_url": download_url,
        }
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": download_url,
            "content_metadata_only": True,
        }
        return [row]

    def _request_irm_questions(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        symbol = _symbol_from_code(str(params.get("code") or ""))
        if not symbol:
            raise SourceRequestValidationError("code is required and must be a six-digit A-share code")
        page = _positive_int(params.get("page"), default=1, name="page")
        limit = min(_positive_int(params.get("limit"), default=30, name="limit"), 1000)
        start_date = _normalize_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        keyword = str(params.get("keyword") or "").strip()
        org_id = self._fetch_irm_org_id(symbol)
        query = {
            "_t": "1691142650",
            "stockcode": symbol,
            "orgId": org_id,
            "pageSize": str(limit),
            "pageNum": str(page),
            "keyWord": keyword,
            "startDay": _date_dash(start_date) if start_date else "",
            "endDay": _date_dash(end_date) if end_date else "",
        }
        payload = self._fetch_json(
            CNINFO_IRM_QUESTION_URL,
            method="POST",
            query=query,
            headers={
                "Referer": "https://irm.cninfo.com.cn/",
            },
            context="Cninfo IRM question list",
        )
        raw_rows = payload.get("rows") if isinstance(payload, Mapping) else None
        if not isinstance(raw_rows, list):
            raw_rows = []
        rows = [
            _normalize_irm_question_row(item, symbol)
            for item in raw_rows
            if isinstance(item, Mapping)
        ]
        self.last_meta = {
            "source_name": "巨潮互动易",
            "source_url": CNINFO_IRM_QUESTION_URL,
            "requested_code": params.get("code"),
            "page": page,
            "limit": limit,
            "total": payload.get("total") if isinstance(payload, Mapping) else None,
            "total_page": payload.get("totalPage") if isinstance(payload, Mapping) else None,
        }
        return rows

    def _request_disclosure(self, params: Mapping[str, Any], *, tab_name: str) -> list[dict[str, Any]]:
        code = str(params.get("code") or "")
        stock = self._resolve_stock(code)
        if stock is None:
            raise SourceRequestValidationError("code is required and must be a known A-share code")
        market = str(params.get("market") or "沪深京").strip()
        if market != "沪深京":
            raise SourceRequestValidationError("market currently supports only 沪深京")
        category = _disclosure_category(params.get("category"))
        start_date = _normalize_date(params.get("start_date"), "start_date", required=False)
        end_date = _normalize_date(params.get("end_date"), "end_date", required=False)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        page = _positive_int(params.get("page"), default=1, name="page")
        limit = min(_positive_int(params.get("limit"), default=30, name="limit"), 100)
        data = {
            "pageNum": str(page),
            "pageSize": str(limit),
            "column": "szse",
            "tabName": tab_name,
            "plate": "",
            "stock": f"{stock['symbol']},{stock['org_id']}",
            "searchkey": str(params.get("keyword") or "").strip(),
            "secid": "",
            "category": category,
            "trade": "",
            "seDate": _cninfo_date_range(start_date, end_date),
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        payload = self._fetch_json(
            CNINFO_ANNOUNCEMENT_QUERY_URL,
            method="POST",
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": "https://www.cninfo.com.cn",
                "Referer": "https://www.cninfo.com.cn/",
            },
            context="Cninfo disclosure list",
        )
        raw_rows = payload.get("announcements") if isinstance(payload, Mapping) else None
        if not isinstance(raw_rows, list):
            raw_rows = []
        rows = [
            _normalize_announcement_row(item, stock)
            for item in raw_rows
            if isinstance(item, Mapping)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_ANNOUNCEMENT_QUERY_URL,
            "requested_code": code,
            "tab_name": tab_name,
            "page": page,
            "limit": limit,
            "total": payload.get("totalAnnouncement") if isinstance(payload, Mapping) else None,
        }
        return rows

    def _request_irm_answer(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        question_id = str(params.get("question_id") or "").strip()
        if not question_id:
            raise SourceRequestValidationError("question_id is required")
        payload = self._fetch_json(
            CNINFO_IRM_DETAIL_URL,
            query={"questionId": question_id, "_t": "1691146921"},
            headers={"Referer": "https://irm.cninfo.com.cn/"},
            context="Cninfo IRM question detail",
        )
        data = payload.get("data") if isinstance(payload, Mapping) else None
        rows = []
        if isinstance(data, Mapping) and data.get("replyDate") is not None:
            row = _normalize_irm_answer_row(data, question_id)
            if row is not None:
                rows.append(row)
        self.last_meta = {
            "source_name": "巨潮互动易",
            "source_url": CNINFO_IRM_DETAIL_URL,
            "question_id": question_id,
        }
        return rows

    def _request_stock_profile(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        symbol = _symbol_from_code(str(params.get("code") or ""))
        if symbol is None:
            raise SourceRequestValidationError("code is required and must be a six-digit A-share code")
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_PROFILE_URL,
            method="POST",
            query={"scode": symbol},
            context="Cninfo stock profile",
        )
        rows = _webapi_records(payload)
        normalized = [
            {
                "instrument_id": _instrument_id_from_symbol(symbol),
                "symbol": symbol,
                "exchange": _exchange_from_symbol(symbol),
                **_normalize_webapi_row(item, CNINFO_STOCK_PROFILE_FIELDS),
            }
            for item in rows
            if isinstance(item, Mapping)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_PROFILE_URL,
            "requested_code": params.get("code"),
            "count": payload.get("count") if isinstance(payload, Mapping) else len(normalized),
        }
        return normalized

    def _request_stock_allotment(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        symbol = _symbol_from_code(str(params.get("code") or ""))
        if symbol is None:
            raise SourceRequestValidationError("code is required and must be a six-digit A-share code")
        start_date = _normalize_date(params.get("start_date") or _recent_window_start(), "start_date", required=True)
        end_date = _normalize_date(params.get("end_date") or _today_yyyymmdd(), "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_ALLOTMENT_URL,
            method="POST",
            query={
                "scode": symbol,
                "sdate": _date_dash(start_date),
                "edate": _date_dash(end_date),
            },
            context="Cninfo stock allotment",
        )
        rows = _webapi_records(payload)
        normalized = [
            {
                "instrument_id": _instrument_id_from_symbol(symbol),
                "symbol": symbol,
                "exchange": _exchange_from_symbol(symbol),
                **_normalize_webapi_row(item, CNINFO_STOCK_ALLOTMENT_FIELDS),
            }
            for item in rows
            if isinstance(item, Mapping)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_ALLOTMENT_URL,
            "requested_code": params.get("code"),
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(normalized),
        }
        return normalized

    def _request_stock_dividend(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        symbol = _symbol_from_code(str(params.get("code") or ""))
        if symbol is None:
            raise SourceRequestValidationError("code is required and must be a six-digit A-share code")
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_DIVIDEND_URL,
            method="POST",
            query={"scode": symbol},
            context="Cninfo stock dividend",
        )
        rows = [
            {
                "instrument_id": _instrument_id_from_symbol(symbol),
                "symbol": symbol,
                "exchange": _exchange_from_symbol(symbol),
                **_normalize_webapi_row(item, CNINFO_STOCK_DIVIDEND_FIELDS),
            }
            for item in _webapi_records(payload)
            if isinstance(item, Mapping)
        ]
        rows.sort(key=lambda item: item.get("announcement_date") or "")
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_DIVIDEND_URL,
            "requested_code": params.get("code"),
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows

    def _request_stock_hold_change(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        market = _mapped_choice(
            params.get("market"),
            CNINFO_HOLD_CHANGE_MARKET_MAP,
            default="沪市",
            name="market",
        )
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_HOLD_CHANGE_URL,
            method="GET",
            query={"market": market},
            context="Cninfo stock hold change",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_HOLD_CHANGE_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or ""))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_HOLD_CHANGE_URL,
            "requested_market": params.get("market") or "沪市",
            "market_code": market,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows

    def _request_stock_hold_control(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        control_type_code = _mapped_choice(
            params.get("control_type"),
            CNINFO_HOLD_CONTROL_TYPE_MAP,
            default="实际控制人",
            name="control_type",
        )
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_HOLD_CONTROL_URL,
            method="GET",
            query={"ctype": control_type_code},
            context="Cninfo stock hold control",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_HOLD_CONTROL_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or ""))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_HOLD_CONTROL_URL,
            "requested_control_type": params.get("control_type") or "实际控制人",
            "control_type_code": control_type_code,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows

    def _request_stock_hold_num(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        report_date = _normalize_date(params.get("date") or _latest_report_period(), "date", required=True)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_HOLD_NUM_URL,
            method="POST",
            query={"rdate": report_date},
            context="Cninfo stock shareholder count",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_HOLD_NUM_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or ""))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    "report_date": report_date,
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_HOLD_NUM_URL,
            "requested_date": report_date,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows

    def _request_stock_industry_category(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        industry_type_code = _mapped_choice(
            params.get("industry_type"),
            CNINFO_INDUSTRY_TYPE_MAP,
            default="证监会行业分类标准",
            name="industry_type",
        )
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_INDUSTRY_CATEGORY_URL,
            method="GET",
            query={"indcode": "", "indtype": industry_type_code, "format": "json"},
            context="Cninfo industry category",
        )
        rows = [
            _normalize_webapi_row(item, CNINFO_STOCK_INDUSTRY_CATEGORY_FIELDS)
            for item in _webapi_records(payload)
        ]
        _add_industry_levels(rows)
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_INDUSTRY_CATEGORY_URL,
            "requested_industry_type": params.get("industry_type") or "证监会行业分类标准",
            "industry_type_code": industry_type_code,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows

    def _request_stock_ipo_summary(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        symbol = _symbol_from_code(str(params.get("code") or ""))
        if symbol is None:
            raise SourceRequestValidationError("code is required and must be a six-digit A-share code")
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_IPO_SUMMARY_URL,
            method="POST",
            query={"scode": symbol},
            context="Cninfo stock IPO summary",
        )
        rows = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_IPO_SUMMARY_FIELDS)
            values.pop("source_symbol", None)
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "symbol": symbol,
                    "exchange": _exchange_from_symbol(symbol),
                    **values,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_IPO_SUMMARY_URL,
            "requested_code": params.get("code"),
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows

    def _request_stock_new_gh(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_new_gh_cninfo: {unknown}")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_NEW_GH_URL,
            method="POST",
            context="Cninfo IPO meeting approval list",
        )
        rows = [
            _normalize_webapi_row(item, CNINFO_STOCK_NEW_GH_FIELDS)
            for item in _webapi_records(payload)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_NEW_GH_URL,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_stock_new_ipo(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        time_type = str(params.get("time_type") or "36").strip()
        if not time_type:
            raise SourceRequestValidationError("time_type is required")
        market = str(params.get("market") or "ALL").strip().upper()
        if not market:
            raise SourceRequestValidationError("market is required")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_NEW_IPO_URL,
            method="POST",
            query={"timetype": time_type, "market": market},
            context="Cninfo new IPO list",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_NEW_IPO_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or ""))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_NEW_IPO_URL,
            "time_type": time_type,
            "market": market,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_stock_share_change(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        requested_symbol = _symbol_from_code(str(params.get("code") or ""))
        if requested_symbol is None:
            raise SourceRequestValidationError("code is required and must be a six-digit A-share code")
        start_date = _normalize_date(params.get("start_date") or _recent_window_start(), "start_date", required=True)
        end_date = _normalize_date(params.get("end_date") or _today_yyyymmdd(), "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_SHARE_CHANGE_URL,
            method="POST",
            query={
                "scode": requested_symbol,
                "sdate": _date_dash(start_date),
                "edate": _date_dash(end_date),
            },
            context="Cninfo stock share change",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_SHARE_CHANGE_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or requested_symbol))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_SHARE_CHANGE_URL,
            "requested_code": params.get("code"),
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows

    def _request_fund_asset_allocation(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if params:
            unknown = ", ".join(sorted(str(key) for key in params))
            raise SourceRequestValidationError(f"Unknown param(s) for fund_report_asset_allocation_cninfo: {unknown}")
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_FUND_ASSET_ALLOCATION_URL,
            method="POST",
            context="Cninfo fund asset allocation",
        )
        rows = [
            _normalize_webapi_row(item, CNINFO_FUND_ASSET_ALLOCATION_FIELDS)
            for item in _webapi_records(payload)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_FUND_ASSET_ALLOCATION_URL,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows

    def _request_fund_industry_allocation(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for fund_report_industry_allocation_cninfo: {unknown}")
        report_date = _normalize_date(params.get("date") or _latest_report_period(), "date", required=True)
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_FUND_INDUSTRY_ALLOCATION_URL,
            method="POST",
            query={"rdate": _date_dash(report_date)},
            context="Cninfo fund industry allocation",
        )
        rows = [
            _normalize_webapi_row(item, CNINFO_FUND_INDUSTRY_ALLOCATION_FIELDS)
            for item in _webapi_records(payload)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_FUND_INDUSTRY_ALLOCATION_URL,
            "requested_date": report_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_fund_stock(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for fund_report_stock_cninfo: {unknown}")
        report_date = _normalize_date(params.get("date") or _latest_report_period(), "date", required=True)
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_FUND_STOCK_URL,
            method="POST",
            query={"rdate": _date_dash(report_date)},
            context="Cninfo fund heavy stock",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_FUND_STOCK_FIELDS)
            source_symbol = _clean_text(values.get("symbol"))
            symbol = _symbol_from_code(str(source_symbol or "")) or source_symbol
            row = {**values, "symbol": symbol}
            if symbol and re.fullmatch(r"\d{6}", str(symbol)):
                row["instrument_id"] = _instrument_id_from_symbol(str(symbol))
                row["exchange"] = _exchange_from_symbol(str(symbol))
            else:
                row["instrument_id"] = None
                row["exchange"] = None
            rows.append(row)
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_FUND_STOCK_URL,
            "requested_date": report_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_stock_cg_equity_mortgage(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_cg_equity_mortgage_cninfo: {unknown}")
        trade_date = _normalize_date(params.get("date") or "20210930", "date", required=True)
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_CG_EQUITY_MORTGAGE_URL,
            method="POST",
            query={"tdate": _date_dash(trade_date)},
            context="Cninfo equity mortgage",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_CG_EQUITY_MORTGAGE_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or ""))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    "stat_date": trade_date,
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_CG_EQUITY_MORTGAGE_URL,
            "requested_date": trade_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_stock_cg_guarantee(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"market", "symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_cg_guarantee_cninfo: {unknown}")
        requested_market = params.get("market", params.get("symbol"))
        market_code = _mapped_choice(
            requested_market,
            CNINFO_HOLD_CHANGE_MARKET_MAP,
            default="全部",
            name="market",
        )
        start_date = _normalize_date(params.get("start_date") or "20180630", "start_date", required=True)
        end_date = _normalize_date(params.get("end_date") or "20210927", "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_CG_GUARANTEE_URL,
            method="POST",
            query={
                "sdate": _date_dash(start_date),
                "edate": _date_dash(end_date),
                "market": market_code,
            },
            context="Cninfo external guarantee",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_CG_GUARANTEE_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or ""))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_CG_GUARANTEE_URL,
            "requested_market": requested_market or "全部",
            "market_code": market_code,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_stock_cg_lawsuit(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"market", "symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_cg_lawsuit_cninfo: {unknown}")
        requested_market = params.get("market", params.get("symbol"))
        market_code = _mapped_choice(
            requested_market,
            CNINFO_HOLD_CHANGE_MARKET_MAP,
            default="沪市",
            name="market",
        )
        start_date = _normalize_date(params.get("start_date") or "20180630", "start_date", required=True)
        end_date = _normalize_date(params.get("end_date") or "20210927", "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_CG_LAWSUIT_URL,
            method="POST",
            query={
                "sdate": _date_dash(start_date),
                "edate": _date_dash(end_date),
                "market": market_code,
            },
            context="Cninfo company lawsuit",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_CG_LAWSUIT_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or ""))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_CG_LAWSUIT_URL,
            "requested_market": requested_market or "沪市",
            "market_code": market_code,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_stock_hold_management_detail(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"change_type", "symbol", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_hold_management_detail_cninfo: {unknown}")
        requested_type = params.get("change_type", params.get("symbol"))
        change_type_code = _mapped_choice(
            requested_type,
            CNINFO_MANAGEMENT_CHANGE_TYPE_MAP,
            default="增持",
            name="change_type",
        )
        start_date = _normalize_date(params.get("start_date") or "20240101", "start_date", required=True)
        end_date = _normalize_date(params.get("end_date") or "20241231", "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_HOLD_MANAGEMENT_DETAIL_URL,
            method="POST",
            query={
                "sdate": _date_dash(start_date),
                "edate": _date_dash(end_date),
                "varytype": change_type_code,
            },
            context="Cninfo management holding detail",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_HOLD_MANAGEMENT_DETAIL_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or ""))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    "change_type": "增持" if change_type_code == "B" else "减持",
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_HOLD_MANAGEMENT_DETAIL_URL,
            "requested_change_type": requested_type or "增持",
            "change_type_code": change_type_code,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_stock_industry_change(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"code", "start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_industry_change_cninfo: {unknown}")
        requested_symbol = _symbol_from_code(str(params.get("code") or ""))
        if requested_symbol is None:
            raise SourceRequestValidationError("code is required and must be a six-digit A-share code")
        start_date = _normalize_date(params.get("start_date") or "20091227", "start_date", required=True)
        end_date = _normalize_date(params.get("end_date") or "20220713", "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_INDUSTRY_CHANGE_URL,
            method="POST",
            query={
                "scode": requested_symbol,
                "sdate": _date_dash(start_date),
                "edate": _date_dash(end_date),
            },
            context="Cninfo stock industry change",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_INDUSTRY_CHANGE_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or requested_symbol))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_INDUSTRY_CHANGE_URL,
            "requested_code": params.get("code"),
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_stock_industry_pe_ratio(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"classification", "symbol", "date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_industry_pe_ratio_cninfo: {unknown}")
        sort_code = _mapped_choice(
            params.get("classification", params.get("symbol")),
            CNINFO_INDUSTRY_PE_CLASSIFICATION_MAP,
            default="证监会行业分类",
            name="classification",
        )
        trade_date = _normalize_date(params.get("date") or "20240617", "date", required=True)
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_INDUSTRY_PE_RATIO_URL,
            method="POST",
            query={
                "tdate": _date_dash(trade_date),
                "sortcode": sort_code,
            },
            origin="http://webapi.cninfo.com.cn",
            context="Cninfo industry PE ratio",
        )
        rows = [
            _normalize_webapi_row(item, CNINFO_STOCK_INDUSTRY_PE_RATIO_FIELDS)
            for item in _webapi_records(payload)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_INDUSTRY_PE_RATIO_URL,
            "requested_classification": params.get("classification", params.get("symbol")) or "证监会行业分类",
            "sort_code": sort_code,
            "requested_date": trade_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_stock_rank_forecast(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for stock_rank_forecast_cninfo: {unknown}")
        trade_date = _normalize_date(params.get("date") or "20230817", "date", required=True)
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_STOCK_RANK_FORECAST_URL,
            method="POST",
            query={"tdate": _date_dash(trade_date)},
            origin="http://webapi.cninfo.com.cn",
            context="Cninfo stock rank forecast",
        )
        rows: list[dict[str, Any]] = []
        for item in _webapi_records(payload):
            values = _normalize_webapi_row(item, CNINFO_STOCK_RANK_FORECAST_FIELDS)
            symbol = _symbol_from_code(str(values.get("symbol") or ""))
            if symbol is None:
                continue
            rows.append(
                {
                    "instrument_id": _instrument_id_from_symbol(symbol),
                    "exchange": _exchange_from_symbol(symbol),
                    **values,
                    "symbol": symbol,
                }
            )
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_STOCK_RANK_FORECAST_URL,
            "requested_date": trade_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_bond_corporate_issue(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for bond_corporate_issue_cninfo: {unknown}")
        start_date = _normalize_date(params.get("start_date") or "20210911", "start_date", required=True)
        end_date = _normalize_date(params.get("end_date") or "20211110", "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_BOND_CORPORATE_ISSUE_URL,
            method="POST",
            query={
                "sdate": _date_dash(start_date),
                "edate": _date_dash(end_date),
            },
            context="Cninfo corporate bond issue",
        )
        rows = [
            _normalize_webapi_row(item, CNINFO_BOND_CORPORATE_ISSUE_FIELDS)
            for item in _webapi_records(payload)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_BOND_CORPORATE_ISSUE_URL,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_bond_cov_issue(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for bond_cov_issue_cninfo: {unknown}")
        start_date = _normalize_date(params.get("start_date") or "20210913", "start_date", required=True)
        end_date = _normalize_date(params.get("end_date") or "20211112", "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_BOND_COV_ISSUE_URL,
            method="POST",
            query={
                "sdate": _date_dash(start_date),
                "edate": _date_dash(end_date),
            },
            context="Cninfo convertible bond issue",
        )
        rows = [
            _normalize_webapi_row(item, CNINFO_BOND_COV_ISSUE_FIELDS)
            for item in _webapi_records(payload)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_BOND_COV_ISSUE_URL,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_bond_cov_stock_issue(self, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        allowed = {"limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for bond_cov_stock_issue_cninfo: {unknown}")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            CNINFO_WEBAPI_BOND_COV_STOCK_ISSUE_URL,
            method="POST",
            context="Cninfo convertible bond conversion",
        )
        rows = [
            _normalize_webapi_row(item, CNINFO_BOND_COV_STOCK_ISSUE_FIELDS)
            for item in _webapi_records(payload)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": CNINFO_WEBAPI_BOND_COV_STOCK_ISSUE_URL,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _request_bond_public_issue(
        self,
        params: Mapping[str, Any],
        *,
        interface_name: str,
        url: str,
        context: str,
        default_start_date: str,
        default_end_date: str,
    ) -> list[dict[str, Any]]:
        allowed = {"start_date", "end_date", "limit"}
        unknown_keys = sorted(str(key) for key in params if key not in allowed)
        if unknown_keys:
            unknown = ", ".join(unknown_keys)
            raise SourceRequestValidationError(f"Unknown param(s) for {interface_name}: {unknown}")
        start_date = _normalize_date(params.get("start_date") or default_start_date, "start_date", required=True)
        end_date = _normalize_date(params.get("end_date") or default_end_date, "end_date", required=True)
        if start_date and end_date and start_date > end_date:
            raise SourceRequestValidationError("start_date must be before or equal to end_date")
        limit = min(_positive_int(params.get("limit"), default=100, name="limit"), 1000)
        payload = self._fetch_cninfo_webapi_json(
            url,
            method="POST",
            query={
                "sdate": _date_dash(start_date),
                "edate": _date_dash(end_date),
            },
            context=context,
        )
        rows = [
            _normalize_webapi_row(item, CNINFO_BOND_PUBLIC_ISSUE_FIELDS)
            for item in _webapi_records(payload)
        ]
        self.last_meta = {
            "source_name": "巨潮",
            "source_url": url,
            "requested_start_date": start_date,
            "requested_end_date": end_date,
            "limit": limit,
            "count": payload.get("count") if isinstance(payload, Mapping) else len(rows),
        }
        return rows[:limit]

    def _resolve_stock(self, code: str) -> dict[str, Any] | None:
        symbol = _symbol_from_code(code)
        if not symbol:
            return None
        index = self._load_stock_index()
        stock = index.get(symbol)
        if stock is None:
            return None
        return {
            "symbol": symbol,
            "instrument_id": _instrument_id_from_symbol(symbol),
            "exchange": _exchange_from_symbol(symbol),
            "org_id": stock.get("orgId"),
            "name": stock.get("zwjc"),
        }

    def _load_stock_index(self) -> dict[str, dict[str, Any]]:
        if self._stock_index is not None:
            return self._stock_index
        payload = self._fetch_json(CNINFO_STOCK_INDEX_URL, context="Cninfo stock index")
        rows = payload.get("stockList") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise SourceUnavailableError("Cninfo stock index returned unexpected payload.")
        self._stock_index = {
            str(row.get("code")): dict(row)
            for row in rows
            if isinstance(row, Mapping) and row.get("code")
        }
        return self._stock_index

    def _fetch_irm_org_id(self, symbol: str) -> str:
        payload = self._fetch_json(
            CNINFO_IRM_KEYWORD_URL,
            method="POST",
            query={"_t": "1691144074"},
            data={"keyWord": symbol},
            headers={"Referer": "https://irm.cninfo.com.cn/"},
            context="Cninfo IRM organization lookup",
        )
        data = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, list) or not data:
            raise SourceUnavailableError(f"Cninfo IRM did not return organization id for {symbol}.")
        first = data[0]
        if not isinstance(first, Mapping) or not first.get("secid"):
            raise SourceUnavailableError(f"Cninfo IRM organization payload is missing secid for {symbol}.")
        return str(first["secid"])

    def _fetch_announcements_for_stock(
        self,
        stock: Mapping[str, Any],
        *,
        start_date: str | None,
        end_date: str | None,
        page: int,
        limit: int,
    ) -> Mapping[str, Any]:
        symbol = str(stock.get("symbol") or "")
        org_id = str(stock.get("org_id") or "")
        if not symbol or not org_id:
            return {}
        data = {
            "pageNum": str(page),
            "pageSize": str(limit),
            "column": _cninfo_column(symbol),
            "tabName": "fulltext",
            "plate": _cninfo_plate(symbol),
            "stock": f"{symbol},{org_id}",
            "searchkey": "",
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": _cninfo_date_range(start_date, end_date),
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        return self._fetch_json(
            CNINFO_ANNOUNCEMENT_QUERY_URL,
            method="POST",
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": "https://www.cninfo.com.cn",
                "Referer": "https://www.cninfo.com.cn/",
            },
            context="Cninfo announcement list",
        )

    def _fetch_pdf_metadata(self, url: str) -> dict[str, Any]:
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                "Accept": "application/pdf,*/*",
                "Range": "bytes=0-0",
                "Referer": "https://www.cninfo.com.cn/",
            },
            method="HEAD",
        )
        try:
            response = self._open(request)
            with response:
                content_type = response.headers.get("Content-Type")
                content_range = response.headers.get("Content-Range")
                content_length = response.headers.get("Content-Length")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailableError(f"Cninfo PDF metadata request failed: {exc}") from exc
        file_size = _file_size_from_headers(content_range, content_length)
        return {"content_type": content_type, "file_size_bytes": file_size}

    def _fetch_json(
        self,
        url: str,
        *,
        method: str = "GET",
        query: Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        context: str,
    ) -> Any:
        body = urlencode(data or {}).encode("utf-8") if data is not None else None
        request_url = f"{url}?{urlencode(query)}" if query else url
        request = Request(
            request_url,
            data=body,
            headers={
                "User-Agent": "Mozilla/5.0 AxData/0.1",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://www.cninfo.com.cn/",
                **dict(headers or {}),
            },
            method=method,
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

    def _fetch_cninfo_webapi_json(
        self,
        url: str,
        *,
        method: str,
        query: Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | None = None,
        origin: str = "https://webapi.cninfo.com.cn",
        context: str,
    ) -> Mapping[str, Any]:
        origin = origin.rstrip("/")
        payload = self._fetch_json(
            url,
            method=method,
            query=query,
            data=data,
            headers={
                "Accept": "*/*",
                "Accept-Enckey": _cninfo_enckey(),
                "Origin": origin,
                "Referer": f"{origin}/",
                "X-Requested-With": "XMLHttpRequest",
            },
            context=context,
        )
        if not isinstance(payload, Mapping):
            raise SourceUnavailableError(f"{context} returned unexpected payload.")
        result_code = str(payload.get("resultcode") or "").strip()
        if result_code and result_code not in {"200", "0"}:
            message = _clean_text(payload.get("resultmsg")) or "unknown error"
            raise SourceUnavailableError(f"{context} returned resultcode {result_code}: {message}")
        return payload

    def _open(self, request: Request) -> Any:
        if self._opener is not None:
            return self._opener(request, timeout=self._timeout)
        return urlopen(request, timeout=self._timeout)


AES_SBOX = (
    0x63, 0x7C, 0x77, 0x7B, 0xF2, 0x6B, 0x6F, 0xC5, 0x30, 0x01, 0x67, 0x2B, 0xFE, 0xD7, 0xAB, 0x76,
    0xCA, 0x82, 0xC9, 0x7D, 0xFA, 0x59, 0x47, 0xF0, 0xAD, 0xD4, 0xA2, 0xAF, 0x9C, 0xA4, 0x72, 0xC0,
    0xB7, 0xFD, 0x93, 0x26, 0x36, 0x3F, 0xF7, 0xCC, 0x34, 0xA5, 0xE5, 0xF1, 0x71, 0xD8, 0x31, 0x15,
    0x04, 0xC7, 0x23, 0xC3, 0x18, 0x96, 0x05, 0x9A, 0x07, 0x12, 0x80, 0xE2, 0xEB, 0x27, 0xB2, 0x75,
    0x09, 0x83, 0x2C, 0x1A, 0x1B, 0x6E, 0x5A, 0xA0, 0x52, 0x3B, 0xD6, 0xB3, 0x29, 0xE3, 0x2F, 0x84,
    0x53, 0xD1, 0x00, 0xED, 0x20, 0xFC, 0xB1, 0x5B, 0x6A, 0xCB, 0xBE, 0x39, 0x4A, 0x4C, 0x58, 0xCF,
    0xD0, 0xEF, 0xAA, 0xFB, 0x43, 0x4D, 0x33, 0x85, 0x45, 0xF9, 0x02, 0x7F, 0x50, 0x3C, 0x9F, 0xA8,
    0x51, 0xA3, 0x40, 0x8F, 0x92, 0x9D, 0x38, 0xF5, 0xBC, 0xB6, 0xDA, 0x21, 0x10, 0xFF, 0xF3, 0xD2,
    0xCD, 0x0C, 0x13, 0xEC, 0x5F, 0x97, 0x44, 0x17, 0xC4, 0xA7, 0x7E, 0x3D, 0x64, 0x5D, 0x19, 0x73,
    0x60, 0x81, 0x4F, 0xDC, 0x22, 0x2A, 0x90, 0x88, 0x46, 0xEE, 0xB8, 0x14, 0xDE, 0x5E, 0x0B, 0xDB,
    0xE0, 0x32, 0x3A, 0x0A, 0x49, 0x06, 0x24, 0x5C, 0xC2, 0xD3, 0xAC, 0x62, 0x91, 0x95, 0xE4, 0x79,
    0xE7, 0xC8, 0x37, 0x6D, 0x8D, 0xD5, 0x4E, 0xA9, 0x6C, 0x56, 0xF4, 0xEA, 0x65, 0x7A, 0xAE, 0x08,
    0xBA, 0x78, 0x25, 0x2E, 0x1C, 0xA6, 0xB4, 0xC6, 0xE8, 0xDD, 0x74, 0x1F, 0x4B, 0xBD, 0x8B, 0x8A,
    0x70, 0x3E, 0xB5, 0x66, 0x48, 0x03, 0xF6, 0x0E, 0x61, 0x35, 0x57, 0xB9, 0x86, 0xC1, 0x1D, 0x9E,
    0xE1, 0xF8, 0x98, 0x11, 0x69, 0xD9, 0x8E, 0x94, 0x9B, 0x1E, 0x87, 0xE9, 0xCE, 0x55, 0x28, 0xDF,
    0x8C, 0xA1, 0x89, 0x0D, 0xBF, 0xE6, 0x42, 0x68, 0x41, 0x99, 0x2D, 0x0F, 0xB0, 0x54, 0xBB, 0x16,
)
AES_RCON = (0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1B, 0x36)


def _cninfo_enckey(now_seconds: int | None = None) -> str:
    text = str(int(time.time()) if now_seconds is None else int(now_seconds)).encode("utf-8")
    encrypted = _aes_cbc_encrypt_pkcs7(text, key=CNINFO_ENCKEY_KEY, iv=CNINFO_ENCKEY_IV)
    return base64.b64encode(encrypted).decode("ascii")


def _aes_cbc_encrypt_pkcs7(data: bytes, *, key: bytes, iv: bytes) -> bytes:
    if len(key) != 16 or len(iv) != 16:
        raise SourceUnavailableError("Cninfo enckey AES key and IV must be 16 bytes.")
    pad_length = 16 - (len(data) % 16)
    padded = data + bytes([pad_length]) * pad_length
    round_keys = _aes_key_expansion(key)
    previous = iv
    output = bytearray()
    for offset in range(0, len(padded), 16):
        block = bytes(left ^ right for left, right in zip(padded[offset : offset + 16], previous))
        encrypted = _aes_encrypt_block(block, round_keys)
        output.extend(encrypted)
        previous = encrypted
    return bytes(output)


def _aes_key_expansion(key: bytes) -> list[int]:
    expanded = list(key)
    bytes_generated = 16
    rcon_iteration = 1
    temp = [0, 0, 0, 0]
    while bytes_generated < 176:
        temp[:] = expanded[bytes_generated - 4 : bytes_generated]
        if bytes_generated % 16 == 0:
            temp = temp[1:] + temp[:1]
            temp = [AES_SBOX[value] for value in temp]
            temp[0] ^= AES_RCON[rcon_iteration]
            rcon_iteration += 1
        for value in temp:
            expanded.append(expanded[bytes_generated - 16] ^ value)
            bytes_generated += 1
    return expanded


def _aes_encrypt_block(block: bytes, round_keys: Sequence[int]) -> bytes:
    state = list(block)
    _aes_add_round_key(state, round_keys[0:16])
    for round_index in range(1, 10):
        _aes_sub_bytes(state)
        _aes_shift_rows(state)
        _aes_mix_columns(state)
        _aes_add_round_key(state, round_keys[round_index * 16 : (round_index + 1) * 16])
    _aes_sub_bytes(state)
    _aes_shift_rows(state)
    _aes_add_round_key(state, round_keys[160:176])
    return bytes(state)


def _aes_add_round_key(state: list[int], round_key: Sequence[int]) -> None:
    for index, value in enumerate(round_key):
        state[index] ^= value


def _aes_sub_bytes(state: list[int]) -> None:
    for index, value in enumerate(state):
        state[index] = AES_SBOX[value]


def _aes_shift_rows(state: list[int]) -> None:
    state[1], state[5], state[9], state[13] = state[5], state[9], state[13], state[1]
    state[2], state[6], state[10], state[14] = state[10], state[14], state[2], state[6]
    state[3], state[7], state[11], state[15] = state[15], state[3], state[7], state[11]


def _aes_mix_columns(state: list[int]) -> None:
    for offset in range(0, 16, 4):
        a0, a1, a2, a3 = state[offset : offset + 4]
        total = a0 ^ a1 ^ a2 ^ a3
        state[offset] ^= total ^ _aes_xtime(a0 ^ a1)
        state[offset + 1] ^= total ^ _aes_xtime(a1 ^ a2)
        state[offset + 2] ^= total ^ _aes_xtime(a2 ^ a3)
        state[offset + 3] ^= total ^ _aes_xtime(a3 ^ a0)


def _aes_xtime(value: int) -> int:
    return (((value << 1) ^ 0x1B) & 0xFF) if value & 0x80 else ((value << 1) & 0xFF)


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


def _symbol_from_code(value: str) -> str | None:
    text = value.strip().upper()
    if text.endswith((".SH", ".SZ", ".BJ")):
        text = text[:-3]
    if len(text) >= 8 and text[:2] in {"SH", "SZ", "BJ"}:
        text = text[2:]
    if len(text) == 6 and text.isdigit():
        return text
    return None


def _instrument_id_from_symbol(symbol: str) -> str:
    suffix = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}[_exchange_from_symbol(symbol)]
    return f"{symbol}.{suffix}"


def _exchange_from_symbol(symbol: str) -> str:
    if symbol.startswith(("6", "9", "5")):
        return "SSE"
    if symbol.startswith(("4", "8", "92")):
        return "BSE"
    return "SZSE"


def _cninfo_column(symbol: str) -> str:
    if _exchange_from_symbol(symbol) == "BSE":
        return "bj"
    return "sse" if _exchange_from_symbol(symbol) == "SSE" else "szse"


def _cninfo_plate(symbol: str) -> str:
    exchange = _exchange_from_symbol(symbol)
    if exchange == "BSE":
        return "bj"
    return "sh" if exchange == "SSE" else "sz"


def _cninfo_date_range(start_date: str | None, end_date: str | None) -> str:
    if start_date or end_date:
        return f"{_date_dash(start_date) if start_date else ''}~{_date_dash(end_date) if end_date else ''}"
    return ""


def _disclosure_category(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text in CNINFO_DISCLOSURE_CATEGORY_MAP.values():
        return text
    try:
        return CNINFO_DISCLOSURE_CATEGORY_MAP[text]
    except KeyError as exc:
        choices = ", ".join(CNINFO_DISCLOSURE_CATEGORY_MAP)
        raise SourceRequestValidationError(f"category must be one of {choices}") from exc


def _mapped_choice(
    value: Any,
    mapping: Mapping[str, str],
    *,
    default: str,
    name: str,
) -> str:
    text = str(value if value not in (None, "") else default).strip()
    if text in mapping.values():
        return text
    try:
        return mapping[text]
    except KeyError as exc:
        choices = ", ".join(mapping)
        raise SourceRequestValidationError(f"{name} must be one of {choices}") from exc


def _date_dash(value: str | None) -> str:
    if not value:
        return ""
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


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


def _webapi_records(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    records = payload.get("records")
    if records is None:
        return []
    if not isinstance(records, list):
        raise SourceUnavailableError("Cninfo webapi records field returned unexpected payload.")
    return [item for item in records if isinstance(item, Mapping)]


def _normalize_webapi_row(row: Mapping[str, Any], fields: Sequence[_WebapiField]) -> dict[str, Any]:
    return {
        field.name: _normalize_webapi_value(_webapi_value(row, field.source), field.kind)
        for field in fields
    }


def _webapi_value(row: Mapping[str, Any], source: str | int) -> Any:
    if isinstance(source, int):
        values = list(row.values())
        if source >= len(values):
            return None
        return values[source]
    return row.get(source)


def _normalize_webapi_value(value: Any, kind: str) -> Any:
    if kind == "float":
        return _parse_float(value)
    if kind == "int":
        parsed = _parse_float(value)
        return int(parsed) if parsed is not None else None
    if kind == "date":
        return _normalize_source_date(value)
    return _clean_text(value)


def _normalize_source_date(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 8:
        candidate = digits[:8]
        try:
            datetime.strptime(candidate, "%Y%m%d")
        except ValueError:
            return None
        return candidate
    return None


def _add_industry_levels(rows: list[dict[str, Any]]) -> None:
    lengths = sorted(
        {
            len(str(row.get("category_code") or ""))
            for row in rows
            if row.get("category_code")
        }
    )
    levels = {length: index for index, length in enumerate(lengths)}
    for row in rows:
        code = str(row.get("category_code") or "")
        row["level"] = levels.get(len(code))


def _normalize_announcement_row(row: Mapping[str, Any], stock: Mapping[str, Any]) -> dict[str, Any]:
    adjunct_url = str(row.get("adjunctUrl") or "")
    title = _clean_text(row.get("announcementTitle")) or _clean_text(row.get("shortTitle"))
    return {
        "instrument_id": stock.get("instrument_id"),
        "symbol": stock.get("symbol"),
        "exchange": stock.get("exchange"),
        "name": _clean_text(row.get("secName")) or stock.get("name"),
        "announcement_id": _clean_text(row.get("announcementId")),
        "title": title,
        "publish_date": _timestamp_ms_to_date(row.get("announcementTime")),
        "file_type": _clean_text(row.get("adjunctType")),
        "file_size_kb": _parse_float(row.get("adjunctSize")),
        "download_url": _cninfo_download_url(adjunct_url) if adjunct_url else None,
    }


def _normalize_irm_question_row(row: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    exchange = _exchange_from_symbol(symbol)
    trade = row.get("trade")
    board_type = row.get("boardType")
    return {
        "instrument_id": _instrument_id_from_symbol(symbol),
        "symbol": symbol,
        "exchange": exchange,
        "name": _clean_text(row.get("companyShortName")),
        "industry": _first_text(trade),
        "industry_code": _first_text(board_type),
        "question_id": _clean_text(row.get("indexId")),
        "question": _clean_text(row.get("mainContent")),
        "questioner": _clean_text(row.get("authorName")),
        "questioner_id": _clean_text(row.get("author")),
        "source": _irm_source(row.get("pubClient")),
        "question_time": _timestamp_ms_to_datetime(row.get("pubDate")),
        "update_time": _timestamp_ms_to_datetime(row.get("updateDate")),
        "answer_id": _clean_text(row.get("attachedId")),
        "answer": _clean_text(row.get("attachedContent")),
        "answerer": _clean_text(row.get("attachedAuthor")),
    }


def _normalize_irm_answer_row(row: Mapping[str, Any], question_id: str) -> dict[str, Any] | None:
    symbol = _symbol_from_code(str(row.get("stockCode") or ""))
    if symbol is None:
        return None
    return {
        "instrument_id": _instrument_id_from_symbol(symbol),
        "symbol": symbol,
        "exchange": _exchange_from_symbol(symbol),
        "name": _clean_text(row.get("shortName")),
        "question_id": question_id,
        "question": _clean_text(row.get("questionContent")),
        "answer": _clean_text(row.get("replyContent")),
        "questioner": _clean_text(row.get("questioner")),
        "question_time": _timestamp_ms_to_datetime(row.get("questionDate")),
        "answer_time": _timestamp_ms_to_datetime(row.get("replyDate")),
    }


def _first_text(value: Any) -> str | None:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if not value:
            return None
        return _clean_text(value[0])
    return _clean_text(value)


def _irm_source(value: Any) -> str:
    text = str(value or "").strip()
    return {
        "2": "APP",
        "5": "公众号",
        "4": "网站",
    }.get(text, "网站")


def _timestamp_ms_to_date(value: Any) -> str | None:
    try:
        timestamp = int(value) / 1000
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(timestamp, tz=CNINFO_TZ).strftime("%Y%m%d")


def _timestamp_ms_to_datetime(value: Any) -> str | None:
    try:
        timestamp = int(value) / 1000
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(timestamp, tz=CNINFO_TZ).strftime("%Y%m%d%H%M%S")


def _cninfo_download_url(value: str) -> str:
    text = value.strip()
    if text.startswith("http://") or text.startswith("https://"):
        return text.replace("http://static.cninfo.com.cn/", CNINFO_STATIC_BASE)
    return CNINFO_STATIC_BASE + text.lstrip("/")


def _announcement_id_from_url(url: str) -> str | None:
    match = re.search(r"/(\d+)\.PDF$", url, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _file_size_from_headers(content_range: str | None, content_length: str | None) -> int | None:
    if content_range:
        match = re.search(r"/(\d+)$", content_range)
        if match:
            return int(match.group(1))
    if content_length and content_length.isdigit():
        return int(content_length)
    return None


def _parse_float(value: Any) -> float | None:
    text = _clean_text(value)
    if text is None:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"<[^>]+>", "", str(value))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None

def _today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def _recent_window_start(days: int = 30) -> str:
    """区间类接口无日期时的统一默认:最近 30 天窗口起点。"""
    return (datetime.now() - timedelta(days=days - 1)).strftime("%Y%m%d")


def _latest_report_period() -> str:
    """报告期类接口无日期时的统一默认:最新已结束季度末。"""
    today = datetime.now().date()
    for year in (today.year, today.year - 1):
        for month, day in ((12, 31), (9, 30), (6, 30), (3, 31)):
            period = datetime(year, month, day).date()
            if period < today:
                return period.strftime("%Y%m%d")
    return f"{today.year - 1}1231"
