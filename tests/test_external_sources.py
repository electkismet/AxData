from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from axdata_core import get_request_interface, request_interface
from axdata_core.adapters.cninfo import CninfoRequestAdapter
from axdata_core.adapters.cls import ClsRequestAdapter
from axdata_core.adapters.eastmoney import EastmoneyRequestAdapter
from axdata_core.adapters.kph import KphRequestAdapter
from axdata_core.adapters.sina import SinaRequestAdapter
from axdata_core.adapters.tencent import TencentRequestAdapter
from axdata_core.source_errors import SourceAdapterNotFound, SourceRequestValidationError


REPO_ROOT = Path(__file__).resolve().parents[1]
SINA_KLC_K2_FIXTURE = "K2/IjACQAQfAAAG"


class FakeResponse:
    def __init__(self, payload, *, headers=None, encoding="utf-8", status=200):
        self.payload = payload
        self.status = status
        self.headers = FakeHeaders(headers or {}, encoding=encoding)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return None

    def read(self):
        if isinstance(self.payload, bytes):
            return self.payload
        return str(self.payload).encode(self.headers.encoding)


class FakeHeaders(dict):
    def __init__(self, *args, encoding="utf-8", **kwargs):
        super().__init__(*args, **kwargs)
        self.encoding = encoding

    def get_content_charset(self):
        return self.encoding


def test_external_adapter_imports_do_not_load_source_request_gateway():
    code = (
        "import sys\n"
        "import axdata_core.adapters.cninfo.request\n"
        "import axdata_core.adapters.cls.request\n"
        "import axdata_core.adapters.eastmoney.request\n"
        "import axdata_core.adapters.exchange.request\n"
        "import axdata_core.adapters.kph.request\n"
        "import axdata_core.adapters.sina.request\n"
        "import axdata_core.adapters.tencent.request\n"
        "print('cninfo=' + str('axdata_core.adapters.cninfo.request' in sys.modules))\n"
        "print('cls=' + str('axdata_core.adapters.cls.request' in sys.modules))\n"
        "print('eastmoney=' + str('axdata_core.adapters.eastmoney.request' in sys.modules))\n"
        "print('exchange=' + str('axdata_core.adapters.exchange.request' in sys.modules))\n"
        "print('kph=' + str('axdata_core.adapters.kph.request' in sys.modules))\n"
        "print('sina=' + str('axdata_core.adapters.sina.request' in sys.modules))\n"
        "print('tencent=' + str('axdata_core.adapters.tencent.request' in sys.modules))\n"
        "print('source_errors=' + str('axdata_core.source_errors' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "cninfo=True" in result.stdout
    assert "cls=True" in result.stdout
    assert "eastmoney=True" in result.stdout
    assert "exchange=True" in result.stdout
    assert "kph=True" in result.stdout
    assert "sina=True" in result.stdout
    assert "tencent=True" in result.stdout
    assert "source_errors=True" in result.stdout
    assert "source_request=False" in result.stdout


def test_external_adapter_package_imports_are_lightweight():
    code = (
        "import sys\n"
        "import axdata_core.adapters.cninfo as cninfo\n"
        "import axdata_core.adapters.cls as cls\n"
        "import axdata_core.adapters.eastmoney as eastmoney\n"
        "import axdata_core.adapters.exchange as exchange\n"
        "import axdata_core.adapters.kph as kph\n"
        "import axdata_core.adapters.sina as sina\n"
        "import axdata_core.adapters.tencent as tencent\n"
        "tracked = [\n"
        "    'axdata_core.adapters.cninfo.request',\n"
        "    'axdata_core.adapters.cls.request',\n"
        "    'axdata_core.adapters.eastmoney.request',\n"
        "    'axdata_core.adapters.exchange.request',\n"
        "    'axdata_core.adapters.kph.request',\n"
        "    'axdata_core.adapters.sina.request',\n"
        "    'axdata_core.adapters.tencent.request',\n"
        "]\n"
        "print('before=' + ','.join(name for name in tracked if name in sys.modules))\n"
        "_ = cninfo.CninfoRequestAdapter\n"
        "_ = cls.ClsRequestAdapter\n"
        "_ = eastmoney.EastmoneyRequestAdapter\n"
        "_ = exchange.ExchangeRequestAdapter\n"
        "_ = kph.KphRequestAdapter\n"
        "_ = sina.SinaRequestAdapter\n"
        "_ = tencent.TencentRequestAdapter\n"
        "print('after=' + ','.join(name for name in tracked if name in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "before=\n" in result.stdout
    assert "after=axdata_core.adapters.cninfo.request" in result.stdout
    assert "axdata_core.adapters.cls.request" in result.stdout
    assert "axdata_core.adapters.eastmoney.request" in result.stdout
    assert "axdata_core.adapters.exchange.request" in result.stdout
    assert "axdata_core.adapters.kph.request" in result.stdout
    assert "axdata_core.adapters.sina.request" in result.stdout
    assert "axdata_core.adapters.tencent.request" in result.stdout
    assert "source_request=False" in result.stdout


class CninfoOpener:
    def __call__(self, request, timeout):
        parsed = urlparse(request.full_url)
        if parsed.path.endswith("/queryKeyboardInfo"):
            return FakeResponse(
                json.dumps(
                    {
                        "statusCode": 200,
                        "message": "success",
                        "data": [
                            {
                                "stockCode": "002594",
                                "shortName": "比亚迪",
                                "stockType": "S",
                                "secid": "gshk0001211",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/company/question"):
            query = parse_qs(parsed.query)
            assert query["stockcode"] == ["002594"]
            assert query["pageSize"] == ["1"]
            return FakeResponse(
                json.dumps(
                    {
                        "pageNo": 1,
                        "pageSize": 1,
                        "total": 282,
                        "totalPage": 282,
                        "rows": [
                            {
                                "indexId": "2301823909845012480",
                                "trade": ["制造业"],
                                "mainContent": "尊敬的董秘你好",
                                "boardType": ["012002"],
                                "pubDate": 1782703572000,
                                "stockCode": "002594",
                                "companyShortName": "比亚迪",
                                "author": "2227137287091273728",
                                "authorName": "irm3229095",
                                "pubClient": "2",
                                "attachedId": None,
                                "attachedContent": None,
                                "attachedAuthor": None,
                                "updateDate": 1783061889000,
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/getQuestionDetail"):
            query = parse_qs(parsed.query)
            assert query["questionId"] == ["1495108801386602496"]
            return FakeResponse(
                json.dumps(
                    {
                        "statusCode": 200,
                        "message": "获取问答详情成功",
                        "data": {
                            "questionContent": "建议比亚迪公司研究研究老车主付费升级辅助驾驶系统",
                            "questioner": "irm1541214",
                            "questionDate": 1688789573000,
                            "replyDate": 1689122071000,
                            "replyContent": "您好！您的建议我们已经收到，感谢您对公司的关注！",
                            "stockCode": "002594",
                            "shortName": "比亚迪",
                        },
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1133"):
            query = parse_qs(parsed.query)
            assert query["scode"] == ["600030"]
            headers = {key.lower(): value for key, value in request.headers.items()}
            assert headers.get("accept-enckey")
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "F001V": "中信证券股份有限公司",
                                "F002V": "CITIC Securities Company Limited",
                                "F003V": "中信证券",
                                "F004V": "600030",
                                "F005V": "中信证券",
                                "F006V": None,
                                "F007V": None,
                                "F008V": "06030",
                                "F009V": "中信证券",
                                "F010V": "上证180指数",
                                "F011V": "上海证券交易所主板",
                                "F012V": "资本市场服务",
                                "F013V": "张佑君",
                                "F014N": "1482054.6829",
                                "F015D": "1995-10-25 00:00:00",
                                "F016D": "2003-01-06 00:00:00",
                                "F017V": "www.citics.com",
                                "F018V": "ir@citics.com",
                                "F019V": "0755-23835888",
                                "F020V": "0755-23835861",
                                "F021V": "广东省深圳市福田区中心三路8号卓越时代广场（二期）北座",
                                "F022V": "北京市朝阳区亮马桥路48号中信证券大厦",
                                "F023V": "100026",
                                "F024V": "证券经纪、证券投资咨询、证券承销与保荐等",
                                "F025V": "证券业务；证券投资基金托管。",
                                "F026V": "中信证券是中国证监会核准的综合类证券公司。",
                                "ROWNO": 1,
                                "SECCODE": "600030",
                                "SECNAME": "中信证券",
                                "ORGID": "gssh0600030",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1139"):
            query = parse_qs(parsed.query)
            assert query["scode"] == ["600009"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 2,
                        "records": [
                            {
                                "F006D": "2024-06-12",
                                "F044V": "年度分红",
                                "F011N": "0",
                                "F010N": "0",
                                "F012N": "2.8",
                                "F018D": "2024-06-18",
                                "F020D": "2024-06-19",
                                "F023D": "2024-06-19",
                                "F025D": None,
                                "F007V": "10派2.8元(含税)",
                                "F001V": "2023年度",
                            },
                            {
                                "F006D": "2023-06-08",
                                "F044V": "年度分红",
                                "F011N": "0",
                                "F010N": "0",
                                "F012N": "2.6",
                                "F018D": "2023-06-14",
                                "F020D": "2023-06-15",
                                "F023D": "2023-06-15",
                                "F025D": None,
                                "F007V": "10派2.6元(含税)",
                                "F001V": "2022年度",
                            },
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1134"):
            query = parse_qs(parsed.query)
            assert query["scode"] == ["600030"]
            values = [
                "600030",
                "2002-12-13",
                "2002-12-27",
                "1",
                "400000000",
                "2.31",
                "15.0",
                "1800000000",
                "2002-12-24",
                "2003-01-06",
                "4.5",
                "60000000",
                "3.25",
                "0.18",
                "中信证券股份有限公司",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1097"):
            query = parse_qs(parsed.query)
            assert query["timetype"] == ["36"]
            assert query["market"] == ["ALL"]
            values = [
                "2024-01-18",
                "2024-01-17",
                "雪祺电气",
                "2024-01-11",
                "2024-01-04",
                "2024-01-03",
                "15.38",
                "001387",
                "0.03",
                "34190000",
                "22.53",
                "13676000",
                "13500",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1098"):
            values = [
                "样例科技股份有限公司",
                "2024-01-18",
                "首发",
                "首发上市申请",
                "通过",
                "2024-01-19",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1114"):
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "ENDDATE": "2021-06-30",
                                "F001N": "9123",
                                "F006N": "23.4",
                                "F007N": "41.2",
                                "F008N": "5.6",
                                "F005N": "123456.78",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1113"):
            query = parse_qs(parsed.query)
            assert query["rdate"] == ["2021-06-30"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "F001V": "C",
                                "F002V": "制造业",
                                "ENDDATE": "2021-06-30",
                                "F003N": "3210",
                                "F004N": "123456.7",
                                "F005N": "18.9",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1112"):
            query = parse_qs(parsed.query)
            assert query["rdate"] == ["2021-06-30"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "ID": "1",
                                "SECCODE": "600519",
                                "SECNAME": "贵州茅台",
                                "ENDDATE": "2021-06-30",
                                "F001N": "2500",
                                "F002N": "12345.67",
                                "F003N": "987654.32",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1094"):
            query = parse_qs(parsed.query)
            assert query["tdate"] == ["2021-09-30"]
            values = [
                "1000",
                "平安银行",
                "2021-09-29",
                "股权质押",
                "招商证券",
                "中国平安",
                "000001",
                "1.23",
                "12.34",
                "5000",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1054"):
            query = parse_qs(parsed.query)
            assert query["sdate"] == ["2018-06-30"]
            assert query["edate"] == ["2021-09-27"]
            assert query.get("market", [""]) == [""]
            values = [
                "2018-06-30~2021-09-27",
                "12.5",
                "123456.78",
                "3",
                "平安银行",
                "000001",
                "987654.32",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1055"):
            query = parse_qs(parsed.query)
            assert query["sdate"] == ["2018-06-30"]
            assert query["edate"] == ["2021-09-27"]
            assert query.get("market", [""]) == ["012001"]
            values = [
                "2018-06-30---2021-09-27",
                "123456.78",
                "5",
                "浦发银行",
                "600000",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1030"):
            query = parse_qs(parsed.query)
            assert query["sdate"] == ["2024-01-01"]
            assert query["edate"] == ["2024-12-31"]
            assert query["varytype"] == ["B"]
            values = [
                "平安银行",
                "2024-06-01",
                "张三",
                "1234.56",
                "12.34",
                "000001",
                "0.12",
                "10000",
                "2024-05-31",
                "20000",
                "10000",
                "本人",
                "董事",
                "张三",
                "交易所",
                "二级市场买入",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1122"):
            query = parse_qs(parsed.query)
            assert query["sdate"] == ["2021-09-11"]
            assert query["edate"] == ["2021-11-10"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "SECCODE": "188001",
                                "SECNAME": "21企业01",
                                "DECLAREDATE": "2021-09-13",
                                "F003D": "2021-09-15",
                                "F004D": "2021-09-16",
                                "F005N": "20",
                                "F006N": "20",
                                "F008N": "100",
                                "F007N": "100",
                                "F013V": "网上发行",
                                "F014V": "合格投资者",
                                "F015V": "面向专业投资者",
                                "F017V": "余额包销",
                                "F022N": "10",
                                "F023V": "补充营运资金",
                                "F052N": "1000",
                                "BONDNAME": "2021年企业债券第一期",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1123"):
            query = parse_qs(parsed.query)
            assert query["sdate"] == ["2021-09-13"]
            assert query["edate"] == ["2021-11-12"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "SECCODE": "113050",
                                "SECNAME": "南银转债",
                                "DECLAREDATE": "2021-09-28",
                                "F029D": "2021-10-15",
                                "F003D": "2021-10-19",
                                "F005N": "200",
                                "F006N": "200",
                                "F007N": "100",
                                "F052N": "100",
                                "F013V": "网上发行",
                                "F014V": "原股东和社会公众投资者",
                                "F015V": "上交所",
                                "F017V": "余额包销",
                                "F021V": "补充核心一级资本",
                                "F026N": "10.10",
                                "F027D": "2022-04-21",
                                "F053D": "2027-10-14",
                                "F051D": "2021-10-15",
                                "F031V": "783009",
                                "F032V": "南银发债",
                                "F008N": "10000",
                                "F066N": "1",
                                "F067N": "10",
                                "F068D": "2021-10-19",
                                "F004D": "2021-10-15",
                                "F065N": "100",
                                "F028D": "2021-10-14",
                                "F054D": "2021-10-15",
                                "F086V": "601009",
                                "F002V": "上海证券交易所",
                                "BONDNAME": "南京银行股份有限公司公开发行A股可转换公司债券",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1124"):
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "SECCODE": "113050",
                                "SECNAME": "南银转债",
                                "DECLAREDATE": "2022-04-20",
                                "F001V": "601009",
                                "F002V": "南京银行",
                                "F003N": "10.10",
                                "F004D": "2022-04-21",
                                "F005D": "2027-10-14",
                                "F017V": "南京银行",
                                "BONDNAME": "南京银行股份有限公司公开发行A股可转换公司债券",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1121"):
            query = parse_qs(parsed.query)
            assert query["sdate"] == ["2021-09-11"]
            assert query["edate"] == ["2021-11-10"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "SECCODE": "2105201",
                                "SECNAME": "21湖南债01",
                                "F004D": "2021-09-15",
                                "F003D": "2021-09-15",
                                "F006N": "50",
                                "F005N": "50",
                                "F007N": "100",
                                "F008N": "100",
                                "F009D": "2021-09-16",
                                "F028N": "0",
                                "F002V": "银行间市场",
                                "F013V": "公开招标",
                                "F014V": "承销团成员",
                                "DECLAREDATE": "2021-09-10",
                                "BONDNAME": "2021年湖南省政府一般债券一期",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1120"):
            query = parse_qs(parsed.query)
            assert query["sdate"] == ["2021-09-10"]
            assert query["edate"] == ["2021-11-09"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "SECCODE": "019654",
                                "SECNAME": "21国债06",
                                "F004D": "2021-09-10",
                                "F003D": "2021-09-13",
                                "F006N": "650",
                                "F005N": "650",
                                "F007N": "100",
                                "F008N": "100",
                                "F009D": "2021-09-14",
                                "F028N": "0",
                                "F002V": "上海证券交易所",
                                "F013V": "承销团余额包销",
                                "F014V": "承销团成员",
                                "DECLAREDATE": "2021-09-08",
                                "BONDNAME": "2021年记账式附息(六期)国债",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1029"):
            query = parse_qs(parsed.query)
            assert query["market"] == ["012001"]
            values = [
                "19405918194",
                "19405918198",
                "深市主板",
                "平安银行",
                "2024-01-10",
                "限售股上市",
                "000001",
                "2024-01-12",
                "4",
                "99.99",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1033"):
            query = parse_qs(parsed.query)
            assert query["ctype"] == ["069002"]
            values = [
                "58.00",
                "1123456789",
                "平安银行",
                "中国平安保险(集团)股份有限公司",
                "中国平安保险(集团)股份有限公司",
                "实际控制人",
                "000001",
                "2024-12-31",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1034"):
            query = parse_qs(parsed.query)
            assert query["rdate"] == ["20210630"]
            values = [
                "19500.5",
                "-1.25",
                "512345",
                "505941",
                "平安银行",
                "000001",
                "2.5",
                "2021-06-30",
                "19024.2",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_public0002"):
            query = parse_qs(parsed.query)
            assert query["indtype"] == ["008001"]
            assert query["format"] == ["json"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 2,
                        "records": [
                            {
                                "PARENTCODE": "",
                                "SORTCODE": "10",
                                "SORTNAME": "工业",
                                "F001V": "Industry",
                                "F002D": None,
                                "F003V": "008001",
                                "F004V": "证监会行业分类标准",
                            },
                            {
                                "PARENTCODE": "10",
                                "SORTCODE": "1010",
                                "SORTNAME": "能源",
                                "F001V": "Energy",
                                "F002D": None,
                                "F003V": "008001",
                                "F004V": "证监会行业分类标准",
                            },
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1087"):
            query = parse_qs(parsed.query)
            assert query["tdate"] == ["2021-09-10"]
            assert query["sortcode"] == ["008001"]
            values = [
                "1",
                "22.5",
                "18.3",
                "20.1",
                "123456.7",
                "制造业",
                "C",
                "证监会行业分类",
                "987654.3",
                "120",
                "2021-09-10",
                "150",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_sysapi1089"):
            query = parse_qs(parsed.query)
            assert query["tdate"] == ["2023-08-17"]
            values = [
                "平安银行",
                "2023-08-17",
                "增持",
                "上调",
                "15.5",
                "否",
                "买入",
                "张三",
                "中信证券",
                "12.5",
                "000001",
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_stock2215"):
            query = parse_qs(parsed.query)
            assert query["scode"] == ["002594"]
            assert query["sdate"] == ["2009-12-27"]
            assert query["edate"] == ["2024-10-21"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "SECCODE": "002594",
                                "SECNAME": "比亚迪",
                                "ORGNAME": "比亚迪股份有限公司",
                                "DECLAREDATE": "2024-03-26",
                                "VARYDATE": "2023-12-31",
                                "F001V": "0101",
                                "F002V": "定期报告",
                                "F003N": "2911142855",
                                "F004N": "0",
                                "F005N": "0",
                                "F006N": "0",
                                "F007N": "0",
                                "F008N": "0",
                                "F009N": "0",
                                "F010N": "0",
                                "F011N": "0",
                                "F012N": "0",
                                "F013N": "0",
                                "F014N": "0",
                                "F015N": "0",
                                "F016N": "0",
                                "F017N": "0",
                                "F018N": "0",
                                "F019N": "0",
                                "F020N": "0",
                                "F021N": "2911142855",
                                "F022N": "1814263855",
                                "F023N": "0",
                                "F024N": "1096879000",
                                "F025N": "0",
                                "F026N": "0",
                                "F028N": "0",
                                "F029N": "0",
                                "F030N": "0",
                                "F031N": "0",
                                "F032N": "0",
                                "F033N": "0",
                                "F034N": "0",
                                "F035N": "0",
                                "F036N": "0",
                                "F037N": "0",
                                "F038N": "0",
                                "F040N": "0",
                                "F050N": "0",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_stock2110"):
            query = parse_qs(parsed.query)
            assert query["scode"] == ["002594"]
            assert query["sdate"] == ["2009-12-27"]
            assert query["edate"] == ["2022-07-13"]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [
                            {
                                "ORGNAME": "比亚迪股份有限公司",
                                "SECCODE": "002594",
                                "SECNAME": "比亚迪",
                                "VARYDATE": "2021-01-01",
                                "F001V": "008001",
                                "F002V": "证监会行业分类标准",
                                "F003V": "C36",
                                "F004V": "制造业",
                                "F005V": "汽车制造业",
                                "F006V": "汽车制造",
                                "F007V": "新能源车",
                                "F008C": "1",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/p_stock2232"):
            query = parse_qs(parsed.query)
            assert query["scode"] == ["600030"]
            assert query["sdate"] == ["1990-01-01"]
            assert query["edate"] == ["2024-10-22"]
            values = [
                "PG6000302000",
                "中信证券",
                None,
                "2003-01-05",
                "2000-02-01",
                "0",
                None,
                "40000000",
                "4.5",
                "0.3",
                "248150000",
                "0",
                "0",
                "180000000",
                "现金",
                None,
                "向原股东配售",
                None,
                "2000-01-28",
                "12000000",
                "2000-02-20",
                "600030",
                None,
                "0",
                "0",
                "中信证券承销团",
                "0",
                None,
                "全体股东",
                None,
                "2000-02-15",
                "中信证券股份有限公司",
                "2000-01-27",
                "192000000",
                "200000000",
                "12000000",
                "28000000",
                "0",
                "8000000",
                "0",
                "170000000",
                "A股",
                None,
                "PG",
                "余额包销",
                "2000-01-20",
                "2000-03-01",
                "2000-02-10",
                "0",
                "40000000",
                "288150000",
                "0",
                "YEBX",
                "12000000",
                "130000000",
                "A",
                None,
            ]
            return FakeResponse(
                json.dumps(
                    {
                        "count": 1,
                        "records": [{f"F{index:03d}": value for index, value in enumerate(values)}],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/szse_stock.json"):
            return FakeResponse(
                json.dumps(
                    {
                        "stockList": [
                            {
                                "code": "000001",
                                "orgId": "gssz0000001",
                                "zwjc": "平安银行",
                            }
                        ]
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/hisAnnouncement/query"):
            body = parse_qs(request.data.decode("utf-8"))
            if body.get("tabName") == ["relation"]:
                assert body["seDate"] == ["2023-06-19~2023-12-20"]
                return FakeResponse(
                    json.dumps(
                        {
                            "totalAnnouncement": 10,
                            "announcements": [
                                {
                                    "secCode": "000001",
                                    "secName": "平安银行",
                                    "orgId": "gssz0000001",
                                    "announcementId": "1218282445",
                                    "announcementTitle": "投资者关系活动记录表",
                                    "announcementTime": 1699437670000,
                                    "adjunctUrl": "finalpage/2023-11-08/1218282445.PDF",
                                    "adjunctSize": 257,
                                    "adjunctType": "PDF",
                                }
                            ],
                        },
                        ensure_ascii=False,
                    )
                )
            assert body["seDate"] == ["2024-01-01~2024-01-31"]
            return FakeResponse(
                json.dumps(
                    {
                        "totalRecordNum": 1,
                        "announcements": [
                            {
                                "secCode": "000001",
                                "secName": "平安银行",
                                "announcementId": "1218968511",
                                "announcementTitle": "关联交易公告",
                                "announcementTime": 1705939200000,
                                "adjunctUrl": "finalpage/2024-01-23/1218968511.PDF",
                                "adjunctSize": 156,
                                "adjunctType": "PDF",
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/1218968511.PDF"):
            return FakeResponse(
                b"",
                headers={
                    "Content-Type": "application/pdf",
                    "Content-Range": "bytes 0-0/158287",
                },
            )
        raise AssertionError(request.full_url)


def test_cninfo_adapter_normalizes_announcements_and_detail():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "cninfo_announcements",
        {"code": "000001.SZ", "start_date": "20240101", "end_date": "20240131", "limit": 1},
    )
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "exchange": "SZSE",
            "name": "平安银行",
            "announcement_id": "1218968511",
            "title": "关联交易公告",
            "publish_date": "20240123",
            "file_type": "PDF",
            "file_size_kb": 156.0,
            "download_url": "https://static.cninfo.com.cn/finalpage/2024-01-23/1218968511.PDF",
        }
    ]

    detail = adapter.request(
        "cninfo_announcement_detail",
        {
            "announcement_id": "1218968511",
            "url": "https://static.cninfo.com.cn/finalpage/2024-01-23/1218968511.PDF",
        },
    )
    assert detail == [
        {
            "announcement_id": "1218968511",
            "title": None,
            "content_type": "application/pdf",
            "file_size_bytes": 158287,
            "download_url": "https://static.cninfo.com.cn/finalpage/2024-01-23/1218968511.PDF",
        }
    ]


class TencentOpener:
    def __call__(self, request, timeout):
        if "getBoardRankList" in request.full_url:
            return FakeResponse(
                json.dumps(
                    {
                        "code": 0,
                        "msg": "ok",
                        "data": {
                            "rank_list": [
                                {
                                    "code": "sh688808",
                                    "name": "联讯仪器",
                                    "pn": "79.45",
                                    "zd": "140.00",
                                    "zdf": "6.57",
                                    "zf": "13.94",
                                    "volume": "19239.58",
                                    "turnover": "424746",
                                    "hsl": "9.97",
                                    "pe_ttm": "852.97",
                                    "zsz": "2330",
                                    "ltsz": "438.02",
                                    "state": "",
                                }
                            ]
                        },
                    },
                    ensure_ascii=False,
                )
            )
        if "newfqkline/get" in request.full_url:
            if "sh000001" in request.full_url:
                return FakeResponse(
                    "kline_dayqfq2024="
                    + json.dumps(
                        {
                            "code": 0,
                            "msg": "",
                            "data": {
                                "sh000001": {
                                    "day": [
                                        [
                                            "2024-01-02",
                                            "2972.78",
                                            "2962.28",
                                            "2976.27",
                                            "2962.28",
                                            "304141793.00",
                                            {},
                                            "0.66",
                                            "34595072.92",
                                            "0.00",
                                            "0.00",
                                        ]
                                    ]
                                }
                            },
                        },
                        ensure_ascii=False,
                    )
                )
            return FakeResponse(
                "kline_day2024="
                + json.dumps(
                    {
                        "code": 0,
                        "msg": "",
                        "data": {
                            "sz000001": {
                                "day": [
                                    [
                                        "2024-01-02",
                                        "9.39",
                                        "9.21",
                                        "9.42",
                                        "9.21",
                                        "1158366.00",
                                        {},
                                        "0.60",
                                        "107574.23",
                                        "0.00",
                                        "0.00",
                                    ]
                                ]
                            }
                        },
                    },
                    ensure_ascii=False,
                )
            )
        if "stock.gtimg.cn/data/index.php" in request.full_url:
            return FakeResponse(
                'v_detail_data_sz000001=[0,"0/09:25:00/10.29/0.00/10899/11215071/S|'
                '1/09:30:00/10.28/-0.01/3455/3551465/S"];',
                encoding="gbk",
            )
        if "klineWeb/weekTrends" in request.full_url:
            return FakeResponse(
                'trend_qfq={"code":0,"msg":"ok","data":[["1990-12-19","113.1000"]]}'
            )
        parts = [""] * 88
        values = {
            0: "51",
            1: "平安银行",
            2: "000001",
            3: "10.64",
            4: "10.52",
            5: "10.52",
            30: "20260622144633",
            31: "0.12",
            32: "1.14",
            33: "10.67",
            34: "10.42",
            35: "10.64/1208120/1273923383",
            36: "1208120",
            38: "0.62",
            39: "4.80",
            44: "2064.76",
            45: "2064.79",
            46: "0.45",
            47: "11.57",
            48: "9.47",
            61: "GP-A",
            82: "CNY",
        }
        for index, value in values.items():
            parts[index] = value
        text = f'v_sz000001="{"~".join(parts)}";v_pv_none_match="1";'
        return FakeResponse(text, encoding="gbk")


def test_tencent_adapter_normalizes_snapshot_and_skips_invalid_quote():
    adapter = TencentRequestAdapter(opener=TencentOpener())

    rows = adapter.request("tencent_realtime_snapshot", {"code": ["000001.SZ", "999999.SZ"]})

    assert len(rows) == 1
    assert rows[0]["instrument_id"] == "000001.SZ"
    assert rows[0]["asset_type"] == "stock"
    assert rows[0]["last_price"] == 10.64
    assert rows[0]["amount"] == 1273923383.0
    assert adapter.last_meta["empty_codes"] == ["999999.SZ"]


def test_tencent_adapter_normalizes_stock_spot_rank_list():
    adapter = TencentRequestAdapter(opener=TencentOpener())

    rows = adapter.request("stock_zh_a_spot_tx", {"sort_type": "price", "limit": 1})

    assert rows == [
        {
            "instrument_id": "688808.SH",
            "symbol": "688808",
            "exchange": "SSE",
            "asset_type": "stock",
            "name": "联讯仪器",
            "last_price": 79.45,
            "change": 140.0,
            "change_pct": 6.57,
            "amplitude": 13.94,
            "volume": 19239.58,
            "amount": 424746.0,
            "turnover_rate": 9.97,
            "pe_ttm": 852.97,
            "total_market_value": 2330.0,
            "float_market_value": 438.02,
            "quote_state": None,
        }
    ]


def test_cninfo_adapter_normalizes_disclosure_report_and_relation():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    report = adapter.request(
        "stock_zh_a_disclosure_report_cninfo",
        {"code": "000001.SZ", "start_date": "20240101", "end_date": "20240131", "limit": 1},
    )
    assert report[0]["announcement_id"] == "1218968511"
    assert report[0]["download_url"].endswith("1218968511.PDF")

    relation = adapter.request(
        "stock_zh_a_disclosure_relation_cninfo",
        {"code": "000001.SZ", "start_date": "20230619", "end_date": "20231220", "limit": 1},
    )
    assert relation == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "exchange": "SZSE",
            "name": "平安银行",
            "announcement_id": "1218282445",
            "title": "投资者关系活动记录表",
            "publish_date": "20231108",
            "file_type": "PDF",
            "file_size_kb": 257.0,
            "download_url": "https://static.cninfo.com.cn/finalpage/2023-11-08/1218282445.PDF",
        }
    ]


def test_cninfo_adapter_normalizes_irm_questions():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_irm_cninfo", {"code": "002594.SZ", "page": 1, "limit": 1})

    assert rows == [
        {
            "instrument_id": "002594.SZ",
            "symbol": "002594",
            "exchange": "SZSE",
            "name": "比亚迪",
            "industry": "制造业",
            "industry_code": "012002",
            "question_id": "2301823909845012480",
            "question": "尊敬的董秘你好",
            "questioner": "irm3229095",
            "questioner_id": "2227137287091273728",
            "source": "APP",
            "question_time": "20260629112612",
            "update_time": "20260703145809",
            "answer_id": None,
            "answer": None,
            "answerer": None,
        }
    ]
    assert adapter.last_meta["total"] == 282


def test_cninfo_adapter_normalizes_irm_answer_detail():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_irm_ans_cninfo", {"question_id": "1495108801386602496"})

    assert rows == [
        {
            "instrument_id": "002594.SZ",
            "symbol": "002594",
            "exchange": "SZSE",
            "name": "比亚迪",
            "question_id": "1495108801386602496",
            "question": "建议比亚迪公司研究研究老车主付费升级辅助驾驶系统",
            "answer": "您好！您的建议我们已经收到，感谢您对公司的关注！",
            "questioner": "irm1541214",
            "question_time": "20230708121253",
            "answer_time": "20230712083431",
        }
    ]
    assert adapter.last_meta["question_id"] == "1495108801386602496"


def test_cninfo_adapter_normalizes_stock_profile_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_profile_cninfo", {"code": "600030.SH"})

    assert rows == [
        {
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
            "company_name": "中信证券股份有限公司",
            "english_name": "CITIC Securities Company Limited",
            "former_short_name": "中信证券",
            "a_share_code": "600030",
            "a_share_name": "中信证券",
            "b_share_code": None,
            "b_share_name": None,
            "h_share_code": "06030",
            "h_share_name": "中信证券",
            "selected_indexes": "上证180指数",
            "market": "上海证券交易所主板",
            "industry": "资本市场服务",
            "legal_representative": "张佑君",
            "registered_capital": 1482054.6829,
            "founded_date": "19951025",
            "listing_date": "20030106",
            "website": "www.citics.com",
            "email": "ir@citics.com",
            "phone": "0755-23835888",
            "fax": "0755-23835861",
            "registered_address": "广东省深圳市福田区中心三路8号卓越时代广场（二期）北座",
            "office_address": "北京市朝阳区亮马桥路48号中信证券大厦",
            "postcode": "100026",
            "main_business": "证券经纪、证券投资咨询、证券承销与保荐等",
            "business_scope": "证券业务；证券投资基金托管。",
            "organization_profile": "中信证券是中国证监会核准的综合类证券公司。",
        }
    ]

    result = request_interface(
        "stock_profile_cninfo",
        params={"code": "600030.SH"},
        fields=["instrument_id", "company_name", "listing_date"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "600030.SH",
            "company_name": "中信证券股份有限公司",
            "listing_date": "20030106",
        }
    ]


def test_cninfo_adapter_normalizes_stock_allotment_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "stock_allotment_cninfo",
        {"code": "600030.SH", "start_date": "19900101", "end_date": "20241022"},
    )

    assert len(rows) == 1
    assert rows[0]["instrument_id"] == "600030.SH"
    assert rows[0]["symbol"] == "600030"
    assert rows[0]["name"] == "中信证券"
    assert rows[0]["listing_announcement_date"] == "20030105"
    assert rows[0]["payment_start_date"] == "20000201"
    assert rows[0]["allotment_price"] == 4.5
    assert rows[0]["allotment_ratio"] == 0.3
    assert rows[0]["actual_allotment_shares"] == 40000000.0
    assert rows[0]["raised_funds_gross"] == 192000000.0
    assert rows[0]["record_date"] == "20000127"
    assert rows[0]["payment_end_date"] == "20000210"

    result = request_interface(
        "stock_allotment_cninfo",
        params={"code": "600030.SH", "start_date": "19900101", "end_date": "20241022"},
        fields=["instrument_id", "record_date", "allotment_price"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "600030.SH",
            "record_date": "20000127",
            "allotment_price": 4.5,
        }
    ]


def test_cninfo_adapter_normalizes_stock_dividend_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_dividend_cninfo", {"code": "600009.SH"})

    assert rows == [
        {
            "instrument_id": "600009.SH",
            "symbol": "600009",
            "exchange": "SSE",
            "announcement_date": "20230608",
            "dividend_type": "年度分红",
            "bonus_share_ratio": 0.0,
            "transfer_share_ratio": 0.0,
            "cash_dividend_ratio": 2.6,
            "record_date": "20230614",
            "ex_right_date": "20230615",
            "dividend_payment_date": "20230615",
            "share_arrival_date": None,
            "plan_description": "10派2.6元(含税)",
            "report_period": "2022年度",
        },
        {
            "instrument_id": "600009.SH",
            "symbol": "600009",
            "exchange": "SSE",
            "announcement_date": "20240612",
            "dividend_type": "年度分红",
            "bonus_share_ratio": 0.0,
            "transfer_share_ratio": 0.0,
            "cash_dividend_ratio": 2.8,
            "record_date": "20240618",
            "ex_right_date": "20240619",
            "dividend_payment_date": "20240619",
            "share_arrival_date": None,
            "plan_description": "10派2.8元(含税)",
            "report_period": "2023年度",
        },
    ]

    result = request_interface(
        "stock_dividend_cninfo",
        params={"code": "600009.SH"},
        fields=["instrument_id", "announcement_date", "cash_dividend_ratio"],
        adapter=adapter,
    )
    assert result.records[0] == {
        "instrument_id": "600009.SH",
        "announcement_date": "20230608",
        "cash_dividend_ratio": 2.6,
    }


def test_cninfo_adapter_normalizes_stock_hold_change_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_hold_change_cninfo", {"market": "沪市"})

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "exchange": "SZSE",
            "circulated_share": 19405918194.0,
            "total_share": 19405918198.0,
            "trade_market": "深市主板",
            "name": "平安银行",
            "announcement_date": "20240110",
            "change_reason": "限售股上市",
            "symbol": "000001",
            "change_date": "20240112",
            "restricted_share": 4.0,
            "circulated_ratio": 99.99,
        }
    ]

    result = request_interface(
        "stock_hold_change_cninfo",
        params={"market": "沪市"},
        fields=["instrument_id", "change_date", "total_share"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "change_date": "20240112",
            "total_share": 19405918198.0,
        }
    ]


def test_cninfo_adapter_normalizes_stock_hold_control_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_hold_control_cninfo", {"control_type": "实际控制人"})

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "exchange": "SZSE",
            "holding_ratio": 58.0,
            "holding_shares": 1123456789.0,
            "name": "平安银行",
            "actual_controller_name": "中国平安保险(集团)股份有限公司",
            "direct_controller_name": "中国平安保险(集团)股份有限公司",
            "control_type": "实际控制人",
            "symbol": "000001",
            "change_date": "20241231",
        }
    ]

    result = request_interface(
        "stock_hold_control_cninfo",
        params={"control_type": "实际控制人"},
        fields=["instrument_id", "actual_controller_name", "holding_ratio"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "actual_controller_name": "中国平安保险(集团)股份有限公司",
            "holding_ratio": 58.0,
        }
    ]


def test_cninfo_adapter_normalizes_stock_hold_num_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_hold_num_cninfo", {"date": "20210630"})

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "exchange": "SZSE",
            "report_date": "20210630",
            "avg_holding": 19500.5,
            "shareholder_count_change_pct": -1.25,
            "prev_shareholder_count": 512345.0,
            "shareholder_count": 505941.0,
            "name": "平安银行",
            "symbol": "000001",
            "avg_holding_change_pct": 2.5,
            "change_date": "20210630",
            "prev_avg_holding": 19024.2,
        }
    ]

    result = request_interface(
        "stock_hold_num_cninfo",
        params={"date": "20210630"},
        fields=["instrument_id", "report_date", "shareholder_count"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "report_date": "20210630",
            "shareholder_count": 505941.0,
        }
    ]


def test_cninfo_adapter_normalizes_stock_industry_category_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_industry_category_cninfo", {"industry_type": "证监会行业分类标准"})

    assert rows == [
        {
            "parent_code": None,
            "category_code": "10",
            "category_name": "工业",
            "category_name_en": "Industry",
            "end_date": None,
            "industry_type_code": "008001",
            "industry_type": "证监会行业分类标准",
            "level": 0,
        },
        {
            "parent_code": "10",
            "category_code": "1010",
            "category_name": "能源",
            "category_name_en": "Energy",
            "end_date": None,
            "industry_type_code": "008001",
            "industry_type": "证监会行业分类标准",
            "level": 1,
        },
    ]

    result = request_interface(
        "stock_industry_category_cninfo",
        params={"industry_type": "证监会行业分类标准"},
        fields=["category_code", "category_name", "level"],
        adapter=adapter,
    )
    assert result.records == [
        {"category_code": "10", "category_name": "工业", "level": 0},
        {"category_code": "1010", "category_name": "能源", "level": 1},
    ]


def test_cninfo_adapter_normalizes_stock_ipo_summary_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_ipo_summary_cninfo", {"code": "600030.SH"})

    assert rows == [
        {
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
            "prospectus_announcement_date": "20021213",
            "lottery_rate_announcement_date": "20021227",
            "par_value": 1.0,
            "total_issue_shares": 400000000.0,
            "nav_per_share_before_issue": 2.31,
            "diluted_pe": 15.0,
            "raised_funds_net": 1800000000.0,
            "online_issue_date": "20021224",
            "listing_date": "20030106",
            "issue_price": 4.5,
            "issue_expenses_total": 60000000.0,
            "nav_per_share_after_issue": 3.25,
            "online_lottery_rate": 0.18,
            "lead_underwriter": "中信证券股份有限公司",
        }
    ]

    result = request_interface(
        "stock_ipo_summary_cninfo",
        params={"code": "600030.SH"},
        fields=["instrument_id", "listing_date", "issue_price"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "600030.SH",
            "listing_date": "20030106",
            "issue_price": 4.5,
        }
    ]


def test_cninfo_adapter_normalizes_stock_new_ipo_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_new_ipo_cninfo", {"time_type": "36", "market": "ALL", "limit": 1})

    assert rows == [
        {
            "instrument_id": "001387.SZ",
            "exchange": "SZSE",
            "lottery_result_announcement_date": "20240118",
            "winning_announcement_date": "20240117",
            "name": "雪祺电气",
            "listing_date": "20240111",
            "payment_date": "20240104",
            "subscription_date": "20240103",
            "issue_price": 15.38,
            "symbol": "001387",
            "online_lottery_rate": 0.03,
            "total_issue_shares": 34190000.0,
            "issue_pe": 22.53,
            "online_issue_shares": 13676000.0,
            "online_subscription_limit": 13500.0,
        }
    ]

    result = request_interface(
        "stock_new_ipo_cninfo",
        params={"time_type": "36", "market": "ALL", "limit": 1},
        fields=["instrument_id", "subscription_date", "issue_price"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "001387.SZ",
            "subscription_date": "20240103",
            "issue_price": 15.38,
        }
    ]


def test_cninfo_adapter_normalizes_stock_new_gh_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_new_gh_cninfo", {"limit": 1})

    assert rows == [
        {
            "company_name": "样例科技股份有限公司",
            "meeting_date": "20240118",
            "review_type": "首发",
            "review_content": "首发上市申请",
            "review_result": "通过",
            "announcement_date": "20240119",
        }
    ]

    result = request_interface(
        "stock_new_gh_cninfo",
        params={"limit": 1},
        fields=["company_name", "meeting_date", "review_result"],
        adapter=adapter,
    )
    assert result.records == [
        {"company_name": "样例科技股份有限公司", "meeting_date": "20240118", "review_result": "通过"}
    ]


def test_cninfo_adapter_normalizes_stock_share_change_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "stock_share_change_cninfo",
        {"code": "002594.SZ", "start_date": "20091227", "end_date": "20241021"},
    )

    assert len(rows) == 1
    assert rows[0]["instrument_id"] == "002594.SZ"
    assert rows[0]["symbol"] == "002594"
    assert rows[0]["name"] == "比亚迪"
    assert rows[0]["announcement_date"] == "20240326"
    assert rows[0]["change_date"] == "20231231"
    assert rows[0]["change_reason"] == "定期报告"
    assert rows[0]["total_share"] == 2911142855.0
    assert rows[0]["circulating_share"] == 2911142855.0
    assert rows[0]["a_share"] == 1814263855.0
    assert rows[0]["h_share"] == 1096879000.0

    result = request_interface(
        "stock_share_change_cninfo",
        params={"code": "002594.SZ", "start_date": "20091227", "end_date": "20241021"},
        fields=["instrument_id", "change_date", "total_share"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "002594.SZ",
            "change_date": "20231231",
            "total_share": 2911142855.0,
        }
    ]


def test_cninfo_adapter_normalizes_fund_asset_allocation_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("fund_report_asset_allocation_cninfo", {})

    assert rows == [
        {
            "report_date": "20210630",
            "fund_count": 9123.0,
            "equity_asset_pct": 23.4,
            "bond_asset_pct": 41.2,
            "cash_asset_pct": 5.6,
            "fund_market_net_assets": 123456.78,
        }
    ]

    result = request_interface(
        "fund_report_asset_allocation_cninfo",
        params={},
        fields=["report_date", "equity_asset_pct"],
        adapter=adapter,
    )
    assert result.records == [{"report_date": "20210630", "equity_asset_pct": 23.4}]


def test_cninfo_adapter_normalizes_fund_industry_allocation_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("fund_report_industry_allocation_cninfo", {"date": "20210630", "limit": 1})

    assert rows == [
        {
            "industry_code": "C",
            "industry_name": "制造业",
            "report_date": "20210630",
            "fund_count": 3210.0,
            "industry_scale": 123456.7,
            "net_asset_pct": 18.9,
        }
    ]

    result = request_interface(
        "fund_report_industry_allocation_cninfo",
        params={"date": "20210630", "limit": 1},
        fields=["industry_code", "industry_name", "net_asset_pct"],
        adapter=adapter,
    )
    assert result.records == [
        {"industry_code": "C", "industry_name": "制造业", "net_asset_pct": 18.9}
    ]


def test_cninfo_adapter_normalizes_fund_stock_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("fund_report_stock_cninfo", {"date": "20210630", "limit": 1})

    assert rows == [
        {
            "record_id": "1",
            "symbol": "600519",
            "name": "贵州茅台",
            "report_date": "20210630",
            "fund_count": 2500.0,
            "holding_shares": 12345.67,
            "holding_market_value": 987654.32,
            "instrument_id": "600519.SH",
            "exchange": "SSE",
        }
    ]

    result = request_interface(
        "fund_report_stock_cninfo",
        params={"date": "20210630", "limit": 1},
        fields=["instrument_id", "name", "holding_market_value"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "600519.SH", "name": "贵州茅台", "holding_market_value": 987654.32}
    ]


def test_cninfo_adapter_normalizes_stock_cg_equity_mortgage_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_cg_equity_mortgage_cninfo", {"date": "20210930", "limit": 1})

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "exchange": "SZSE",
            "stat_date": "20210930",
            "released_pledge_shares": 1000.0,
            "name": "平安银行",
            "announcement_date": "20210929",
            "pledge_event": "股权质押",
            "pledgee": "招商证券",
            "pledgor": "中国平安",
            "symbol": "000001",
            "pledged_total_share_pct": 1.23,
            "cumulative_pledge_total_share_pct": 12.34,
            "pledged_shares": 5000.0,
        }
    ]

    result = request_interface(
        "stock_cg_equity_mortgage_cninfo",
        params={"date": "20210930", "limit": 1},
        fields=["instrument_id", "announcement_date", "pledged_shares"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "000001.SZ", "announcement_date": "20210929", "pledged_shares": 5000.0}
    ]


def test_cninfo_adapter_normalizes_stock_cg_guarantee_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "stock_cg_guarantee_cninfo",
        {"market": "全部", "start_date": "20180630", "end_date": "20210927", "limit": 1},
    )

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "exchange": "SZSE",
            "announcement_period": "2018-06-30~2021-09-27",
            "guarantee_amount_net_asset_pct": 12.5,
            "guarantee_amount": 123456.78,
            "guarantee_count": 3.0,
            "name": "平安银行",
            "symbol": "000001",
            "parent_equity": 987654.32,
        }
    ]

    result = request_interface(
        "stock_cg_guarantee_cninfo",
        params={"market": "全部", "start_date": "20180630", "end_date": "20210927", "limit": 1},
        fields=["instrument_id", "guarantee_count", "guarantee_amount"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "000001.SZ", "guarantee_count": 3.0, "guarantee_amount": 123456.78}
    ]


def test_cninfo_adapter_normalizes_stock_cg_lawsuit_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "stock_cg_lawsuit_cninfo",
        {"market": "沪市", "start_date": "20180630", "end_date": "20210927", "limit": 1},
    )

    assert rows == [
        {
            "instrument_id": "600000.SH",
            "exchange": "SSE",
            "announcement_period": "2018-06-30---2021-09-27",
            "lawsuit_amount": 123456.78,
            "lawsuit_count": 5.0,
            "name": "浦发银行",
            "symbol": "600000",
        }
    ]

    result = request_interface(
        "stock_cg_lawsuit_cninfo",
        params={"market": "沪市", "start_date": "20180630", "end_date": "20210927", "limit": 1},
        fields=["instrument_id", "lawsuit_count", "lawsuit_amount"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "600000.SH", "lawsuit_count": 5.0, "lawsuit_amount": 123456.78}
    ]


def test_cninfo_adapter_normalizes_stock_hold_management_detail_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "stock_hold_management_detail_cninfo",
        {"change_type": "增持", "start_date": "20240101", "end_date": "20241231", "limit": 1},
    )

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "exchange": "SZSE",
            "change_type": "增持",
            "name": "平安银行",
            "announcement_date": "20240601",
            "executive_name": "张三",
            "ending_market_value": 1234.56,
            "average_price": 12.34,
            "symbol": "000001",
            "change_ratio": 0.12,
            "change_shares": 10000.0,
            "end_date": "20240531",
            "ending_holding_shares": 20000.0,
            "beginning_holding_shares": 10000.0,
            "changer_relation": "本人",
            "director_supervisor_senior_position": "董事",
            "director_supervisor_senior_name": "张三",
            "data_source": "交易所",
            "change_reason": "二级市场买入",
        }
    ]

    result = request_interface(
        "stock_hold_management_detail_cninfo",
        params={"change_type": "增持", "start_date": "20240101", "end_date": "20241231", "limit": 1},
        fields=["instrument_id", "change_type", "change_shares"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "000001.SZ", "change_type": "增持", "change_shares": 10000.0}
    ]


def test_cninfo_adapter_normalizes_stock_industry_change_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "stock_industry_change_cninfo",
        {"code": "002594.SZ", "start_date": "20091227", "end_date": "20220713", "limit": 1},
    )

    assert rows == [
        {
            "instrument_id": "002594.SZ",
            "exchange": "SZSE",
            "organization_name": "比亚迪股份有限公司",
            "symbol": "002594",
            "name": "比亚迪",
            "change_date": "20210101",
            "classification_standard_code": "008001",
            "classification_standard": "证监会行业分类标准",
            "industry_code": "C36",
            "industry_sector": "制造业",
            "industry_subcategory": "汽车制造业",
            "industry_major": "汽车制造",
            "industry_middle": "新能源车",
            "latest_record_flag": "1",
        }
    ]

    result = request_interface(
        "stock_industry_change_cninfo",
        params={"code": "002594.SZ", "start_date": "20091227", "end_date": "20220713", "limit": 1},
        fields=["instrument_id", "change_date", "industry_code"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "002594.SZ", "change_date": "20210101", "industry_code": "C36"}
    ]


def test_cninfo_adapter_normalizes_stock_industry_pe_ratio_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "stock_industry_pe_ratio_cninfo",
        {"classification": "证监会行业分类", "date": "20210910", "limit": 1},
    )

    assert rows == [
        {
            "industry_level": 1,
            "static_pe_mean": 22.5,
            "static_pe_median": 18.3,
            "static_pe_weighted": 20.1,
            "net_profit_static": 123456.7,
            "industry_name": "制造业",
            "industry_code": "C",
            "classification": "证监会行业分类",
            "total_market_value_static": 987654.3,
            "included_company_count": 120.0,
            "change_date": "20210910",
            "company_count": 150.0,
        }
    ]

    result = request_interface(
        "stock_industry_pe_ratio_cninfo",
        params={"classification": "证监会行业分类", "date": "20210910", "limit": 1},
        fields=["industry_code", "change_date", "static_pe_weighted"],
        adapter=adapter,
    )
    assert result.records == [
        {"industry_code": "C", "change_date": "20210910", "static_pe_weighted": 20.1}
    ]


def test_cninfo_adapter_normalizes_stock_rank_forecast_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("stock_rank_forecast_cninfo", {"date": "20230817", "limit": 1})

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "exchange": "SZSE",
            "name": "平安银行",
            "publish_date": "20230817",
            "previous_rating": "增持",
            "rating_change": "上调",
            "target_price_high": 15.5,
            "is_first_rating": "否",
            "rating": "买入",
            "analyst_name": "张三",
            "institution_short_name": "中信证券",
            "target_price_low": 12.5,
            "symbol": "000001",
        }
    ]

    result = request_interface(
        "stock_rank_forecast_cninfo",
        params={"date": "20230817", "limit": 1},
        fields=["instrument_id", "rating", "target_price_high"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "000001.SZ", "rating": "买入", "target_price_high": 15.5}
    ]


def test_cninfo_adapter_normalizes_bond_corporate_issue_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "bond_corporate_issue_cninfo",
        {"start_date": "20210911", "end_date": "20211110", "limit": 1},
    )

    assert rows == [
        {
            "bond_code": "188001",
            "bond_short_name": "21企业01",
            "announcement_date": "20210913",
            "online_issue_start_date": "20210915",
            "online_issue_end_date": "20210916",
            "planned_issue_amount": 20.0,
            "actual_issue_amount": 20.0,
            "par_value": 100.0,
            "issue_price": 100.0,
            "issue_method": "网上发行",
            "issue_target": "合格投资者",
            "issue_scope": "面向专业投资者",
            "underwriting_method": "余额包销",
            "min_subscription_unit": 10.0,
            "fundraising_use": "补充营运资金",
            "min_subscription_amount": 1000.0,
            "bond_name": "2021年企业债券第一期",
        }
    ]

    result = request_interface(
        "bond_corporate_issue_cninfo",
        params={"start_date": "20210911", "end_date": "20211110", "limit": 1},
        fields=["bond_code", "announcement_date", "issue_price"],
        adapter=adapter,
    )
    assert result.records == [
        {"bond_code": "188001", "announcement_date": "20210913", "issue_price": 100.0}
    ]


def test_cninfo_adapter_normalizes_bond_cov_issue_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "bond_cov_issue_cninfo",
        {"start_date": "20210913", "end_date": "20211112", "limit": 1},
    )

    assert rows == [
        {
            "bond_code": "113050",
            "bond_short_name": "南银转债",
            "announcement_date": "20210928",
            "issue_start_date": "20211015",
            "issue_end_date": "20211019",
            "planned_issue_amount": 200.0,
            "actual_issue_amount": 200.0,
            "par_value": 100.0,
            "issue_price": 100.0,
            "issue_method": "网上发行",
            "issue_target": "原股东和社会公众投资者",
            "issue_scope": "上交所",
            "underwriting_method": "余额包销",
            "fundraising_use": "补充核心一级资本",
            "initial_conversion_price": 10.1,
            "conversion_start_date": "20220421",
            "conversion_end_date": "20271014",
            "online_subscription_date": "20211015",
            "online_subscription_code": "783009",
            "online_subscription_short_name": "南银发债",
            "online_subscription_max": 10000.0,
            "online_subscription_min": 1.0,
            "online_subscription_unit": 10.0,
            "online_lottery_result_refund_date": "20211019",
            "priority_subscription_date": "20211015",
            "allotment_price": 100.0,
            "bondholder_record_date": "20211014",
            "priority_subscription_payment_date": "20211015",
            "conversion_code": "601009",
            "trading_market": "上海证券交易所",
            "bond_name": "南京银行股份有限公司公开发行A股可转换公司债券",
        }
    ]

    result = request_interface(
        "bond_cov_issue_cninfo",
        params={"start_date": "20210913", "end_date": "20211112", "limit": 1},
        fields=["bond_code", "online_subscription_date", "initial_conversion_price"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "bond_code": "113050",
            "online_subscription_date": "20211015",
            "initial_conversion_price": 10.1,
        }
    ]


def test_cninfo_adapter_normalizes_bond_cov_stock_issue_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request("bond_cov_stock_issue_cninfo", {"limit": 1})

    assert rows == [
        {
            "bond_code": "113050",
            "bond_short_name": "南银转债",
            "announcement_date": "20220420",
            "conversion_code": "601009",
            "conversion_short_name": "南京银行",
            "conversion_price": 10.1,
            "voluntary_conversion_start_date": "20220421",
            "voluntary_conversion_end_date": "20271014",
            "underlying_stock": "南京银行",
            "bond_name": "南京银行股份有限公司公开发行A股可转换公司债券",
        }
    ]

    result = request_interface(
        "bond_cov_stock_issue_cninfo",
        params={"limit": 1},
        fields=["bond_code", "conversion_code", "conversion_price"],
        adapter=adapter,
    )
    assert result.records == [
        {"bond_code": "113050", "conversion_code": "601009", "conversion_price": 10.1}
    ]


def test_cninfo_adapter_normalizes_bond_local_government_issue_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "bond_local_government_issue_cninfo",
        {"start_date": "20210911", "end_date": "20211110", "limit": 1},
    )

    assert rows == [
        {
            "bond_code": "2105201",
            "bond_short_name": "21湖南债01",
            "issue_start_date": "20210915",
            "issue_end_date": "20210915",
            "planned_issue_amount": 50.0,
            "actual_issue_amount": 50.0,
            "issue_price": 100.0,
            "par_value": 100.0,
            "payment_date": "20210916",
            "additional_issue_count": 0.0,
            "trading_market": "银行间市场",
            "issue_method": "公开招标",
            "issue_target": "承销团成员",
            "announcement_date": "20210910",
            "bond_name": "2021年湖南省政府一般债券一期",
        }
    ]

    result = request_interface(
        "bond_local_government_issue_cninfo",
        params={"start_date": "20210911", "end_date": "20211110", "limit": 1},
        fields=["bond_code", "issue_start_date", "planned_issue_amount"],
        adapter=adapter,
    )
    assert result.records == [
        {"bond_code": "2105201", "issue_start_date": "20210915", "planned_issue_amount": 50.0}
    ]


def test_cninfo_adapter_normalizes_bond_treasure_issue_and_gateway_fields():
    adapter = CninfoRequestAdapter(opener=CninfoOpener())

    rows = adapter.request(
        "bond_treasure_issue_cninfo",
        {"start_date": "20210910", "end_date": "20211109", "limit": 1},
    )

    assert rows == [
        {
            "bond_code": "019654",
            "bond_short_name": "21国债06",
            "issue_start_date": "20210910",
            "issue_end_date": "20210913",
            "planned_issue_amount": 650.0,
            "actual_issue_amount": 650.0,
            "issue_price": 100.0,
            "par_value": 100.0,
            "payment_date": "20210914",
            "additional_issue_count": 0.0,
            "trading_market": "上海证券交易所",
            "issue_method": "承销团余额包销",
            "issue_target": "承销团成员",
            "announcement_date": "20210908",
            "bond_name": "2021年记账式附息(六期)国债",
        }
    ]

    result = request_interface(
        "bond_treasure_issue_cninfo",
        params={"start_date": "20210910", "end_date": "20211109", "limit": 1},
        fields=["bond_code", "payment_date", "actual_issue_amount"],
        adapter=adapter,
    )
    assert result.records == [
        {"bond_code": "019654", "payment_date": "20210914", "actual_issue_amount": 650.0}
    ]


def test_tencent_adapter_normalizes_stock_daily_kline():
    adapter = TencentRequestAdapter(opener=TencentOpener())

    rows = adapter.request(
        "stock_zh_a_hist_tx",
        {
            "code": "000001.SZ",
            "start_date": "20240102",
            "end_date": "20240102",
            "limit": 1,
        },
    )

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "exchange": "SZSE",
            "asset_type": "stock",
            "trade_date": "20240102",
            "adjust": "none",
            "open": 9.39,
            "close": 9.21,
            "high": 9.42,
            "low": 9.21,
            "volume": 1158366.0,
            "amount": 107574.23,
        }
    ]
    assert adapter.last_meta["adjust"] == "none"


def test_tencent_adapter_normalizes_index_daily_kline():
    adapter = TencentRequestAdapter(opener=TencentOpener())

    rows = adapter.request(
        "stock_zh_index_daily_tx",
        {
            "code": "000001.SH",
            "start_date": "20240102",
            "end_date": "20240102",
            "limit": 1,
        },
    )

    assert rows == [
        {
            "instrument_id": "000001.SH",
            "symbol": "000001",
            "exchange": "SSE",
            "asset_type": "index",
            "trade_date": "20240102",
            "adjust": "qfq",
            "open": 2972.78,
            "close": 2962.28,
            "high": 2976.27,
            "low": 2962.28,
            "volume": 304141793.0,
            "amount": 34595072.92,
        }
    ]
    assert adapter.last_meta["adjust"] == "qfq"


def test_tencent_adapter_normalizes_stock_tick_detail():
    adapter = TencentRequestAdapter(opener=TencentOpener())

    rows = adapter.request("stock_zh_a_tick_tx_js", {"code": "000001.SZ", "page": 0, "limit": 1})

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "exchange": "SZSE",
            "sequence": 0,
            "trade_time": "09:25:00",
            "price": 10.29,
            "change": 0.0,
            "volume": 10899.0,
            "amount": 11215071.0,
            "trade_side": "sell",
        }
    ]
    assert adapter.last_meta["page"] == 0


def test_tencent_adapter_normalizes_start_year_helper():
    adapter = TencentRequestAdapter(opener=TencentOpener())

    rows = adapter.request("get_tx_start_year", {"code": "000001.SH"})

    assert rows == [
        {
            "instrument_id": "000001.SH",
            "symbol": "000001",
            "exchange": "SSE",
            "asset_type": "index",
            "start_date": "19901219",
            "source_value": 113.1,
        }
    ]


class EastmoneyOpener:
    def __call__(self, request, timeout):
        parsed = urlparse(request.full_url)
        query = parse_qs(parsed.query)
        report_name = query.get("reportName", [""])[0]
        if parsed.path.endswith("/api/qt/clist/get"):
            fs = query.get("fs", [""])[0]
            if fs == "b:MK0010":
                return FakeResponse(
                    json.dumps(
                        {
                            "rc": 0,
                            "data": {
                                "diff": [
                                    {
                                        "f12": "000001",
                                        "f14": "上证指数",
                                        "f2": 320050,
                                        "f3": 35,
                                        "f4": 1120,
                                        "f5": 100,
                                        "f6": 200,
                                        "f15": 321000,
                                        "f16": 319000,
                                        "f17": 319500,
                                        "f18": 318930,
                                    }
                                ]
                            },
                        },
                        ensure_ascii=False,
                    )
                )
            if fs.startswith("b:BK1033"):
                return FakeResponse(
                    json.dumps(
                        {
                            "rc": 0,
                            "data": {
                                "diff": [
                                    {
                                        "f12": "000049",
                                        "f14": "德赛电池",
                                        "f2": 32.1,
                                        "f3": 10.01,
                                        "f4": 2.92,
                                        "f5": 1000,
                                        "f6": 3210000,
                                        "f7": 11.2,
                                        "f8": 5.1,
                                        "f9": 20.0,
                                        "f10": 1.5,
                                        "f15": 32.1,
                                        "f16": 28.0,
                                        "f17": 29.0,
                                        "f18": 29.18,
                                        "f20": 100000000,
                                        "f21": 90000000,
                                        "f23": 2.1,
                                    }
                                ]
                            },
                        },
                        ensure_ascii=False,
                    )
                )
            if fs.startswith("m:90"):
                return FakeResponse(
                    json.dumps(
                        {
                            "rc": 0,
                            "data": {
                                "diff": [
                                    {
                                        "f12": "BK1033",
                                        "f14": "电池",
                                        "f2": 1161.9,
                                        "f3": 4.98,
                                        "f4": 55.07,
                                        "f5": 7468144,
                                        "f6": 37967558656,
                                        "f7": 7.75,
                                        "f8": 2.99,
                                        "f20": 2106511600000,
                                        "f62": 2673646592,
                                        "f128": "德赛电池",
                                        "f140": "000049",
                                        "f136": 10.01,
                                        "f104": 40,
                                        "f105": 0,
                                    }
                                ]
                            },
                        },
                        ensure_ascii=False,
                    )
                )
            return FakeResponse(
                json.dumps(
                    {
                        "rc": 0,
                        "data": {
                            "diff": [
                                {
                                    "f12": "000001",
                                    "f14": "平安银行",
                                    "f2": 10.64,
                                    "f3": 1.14,
                                    "f4": 0.12,
                                    "f5": 1208120,
                                    "f6": 1273923383,
                                    "f7": 2.3,
                                    "f8": 0.62,
                                    "f9": 4.8,
                                    "f10": 1.1,
                                    "f15": 10.67,
                                    "f16": 10.42,
                                    "f17": 10.52,
                                    "f18": 10.52,
                                    "f20": 206479000000,
                                    "f21": 206476000000,
                                    "f23": 0.45,
                                }
                            ]
                        },
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/api/qt/ulist.np/get"):
            fields = query.get("fields", [""])[0]
            if "f100" in fields:
                return FakeResponse(json.dumps({"rc": 0, "data": {"diff": [{"f12": "000001", "f14": "平安银行", "f100": "银行"}]}}, ensure_ascii=False))
            return FakeResponse(json.dumps({"rc": 0, "data": {"diff": [{"f12": "000001", "f14": "平安银行", "f2": 10.64, "f3": 1.14}]}}))
        if parsed.path.endswith("/getTopicZTPool"):
            return FakeResponse(json.dumps({"rc": 0, "data": {"pool": [{"c": "000001", "n": "平安银行", "m": "0", "p": 10640, "zdp": 10.0, "fbt": 93000, "lbt": 150000, "lbc": 1, "hybk": "银行"}]}}, ensure_ascii=False))
        if parsed.path.endswith("/getTopicDTPool"):
            return FakeResponse(json.dumps({"rc": 0, "data": {"pool": [{"c": "603126", "n": "中材节能", "m": "1", "p": 8920, "zdp": -9.99, "lbt": 145209, "days": 1, "hybk": "环保设备"}]}}, ensure_ascii=False))
        if parsed.path.endswith("/getYesterdayZTPool"):
            return FakeResponse(json.dumps({"rc": 0, "data": {"pool": [{"c": "600172", "n": "黄河旋风", "m": "1", "p": 11830, "ztp": 13020, "zdp": -0.08, "zf": 7.85, "zs": 0.94, "yfbt": 133733, "ylbc": 3}]}}))
        if parsed.path.endswith("/getAllStockChanges"):
            return FakeResponse(json.dumps({"rc": 0, "data": {"allstock": [{"c": "000001", "n": "平安银行", "m": "0", "tm": 93000, "i": 4.4}]}}))
        if parsed.path.endswith("/getStockChanges"):
            return FakeResponse(json.dumps({"rc": 0, "data": {"data": [{"tm": 93000, "t": 8201, "p": 1050, "u": 4.4, "v": 1234}]}}))
        if report_name == "RPT_DAILYBILLBOARD_DETAILS":
            return FakeResponse(
                json.dumps(
                    {
                        "success": True,
                        "message": "ok",
                        "result": {
                            "pages": 1,
                            "count": 1,
                            "data": [
                                {
                                    "TRADE_DATE": "2024-01-02 00:00:00",
                                    "SECUCODE": "000595.SZ",
                                    "SECURITY_CODE": "000595",
                                    "SECURITY_NAME_ABBR": "*ST宝实",
                                    "EXPLANATION": "连续三个交易日内，涨幅偏离值累计达到20%的证券",
                                    "CLOSE_PRICE": 5.7,
                                    "CHANGE_RATE": 10.0386,
                                    "TURNOVERRATE": 5.0939,
                                    "BILLBOARD_BUY_AMT": 79387344,
                                    "BILLBOARD_SELL_AMT": 41045941.7,
                                    "BILLBOARD_NET_AMT": 38341402.3,
                                    "BILLBOARD_DEAL_AMT": 120433285.7,
                                    "TRADE_MARKET": "深交所风险警示板",
                                }
                            ],
                        },
                    },
                    ensure_ascii=False,
                )
            )
        if report_name == "RPTA_WEB_RZRQ_GGMX":
            if "999999" in request.full_url:
                return FakeResponse(json.dumps({"success": False, "message": "返回数据为空"}))
            return FakeResponse(
                json.dumps(
                    {
                        "success": True,
                        "message": "ok",
                        "result": {
                            "pages": 1,
                            "count": 1,
                            "data": [
                                {
                                    "DATE": "2024-01-05 00:00:00",
                                    "SECUCODE": "000001.SZ",
                                    "SCODE": "000001",
                                    "SECNAME": "平安银行",
                                    "TRADE_MARKET": "深交所主板",
                                    "SPJ": 9.27,
                                    "ZDF": 1.7563,
                                    "RZYE": 5105063089,
                                    "RZMRE": 266715372,
                                    "RZCHE": 325760031,
                                    "RZJME": -59044659,
                                    "RQYE": 106309509,
                                    "RQMCL": 564400,
                                    "RQCHL": 201500,
                                    "RQJMG": 362900,
                                    "RZRQYE": 5211372598,
                                    "SZ": 179889420226.5,
                                }
                            ],
                        },
                    },
                    ensure_ascii=False,
                )
            )
        raise AssertionError(request.full_url)


class EastmoneyReportOpener:
    def __call__(self, request, timeout):
        return FakeResponse(
            json.dumps(
                {
                    "hits": 1,
                    "TotalPage": 1,
                    "data": [
                        {
                            "infoCode": "AP202410221640398395",
                            "stockCode": "000001",
                            "stockName": "平安银行",
                            "market": "SHENZHEN",
                            "title": "2024年三季报点评",
                            "publishDate": "2024-10-22 00:00:00.000",
                            "orgName": "东兴证券股份有限公司",
                            "emRatingName": "买入",
                            "ratingChange": 3,
                            "researcher": "林瑾璐,田馨宇",
                            "predictThisYearEps": "2.6000000000",
                            "predictThisYearPe": "4.5400000000",
                            "attachSize": 960,
                            "attachPages": 8,
                        }
                    ],
                },
                ensure_ascii=False,
            )
        )


class ClsOpener:
    def __call__(self, request, timeout):
        parsed = urlparse(request.full_url)
        if parsed.path.endswith("/stock/emotion"):
            return FakeResponse(
                json.dumps(
                    {
                        "code": 200,
                        "data": {
                            "market_degree": "59",
                            "shsz_balance": "3.14万亿",
                            "preview_balance": "3.14万亿",
                            "up_ratio": "76.00%",
                            "up_ratio_num": "100",
                            "up_open_num": "31",
                            "performance": "3.81%",
                            "up_down_dis": {
                                "rise_num": 3520,
                                "fall_num": 1832,
                                "flat_num": 161,
                                "up_num": 127,
                                "down_num": 54,
                            },
                            "limit_up_board": {"row1": ["一板"], "row2": [90]},
                        },
                    },
                    ensure_ascii=False,
                )
            )
        if parsed.path.endswith("/todayTuyere"):
            return FakeResponse(json.dumps({"errno": 0, "data": {"today_tuyere": [{"plate_code": "cls80198", "title": "锂电池", "interpret": "政策利好"}]}}, ensure_ascii=False))
        if parsed.path.endswith("/plate/tuyere/stocks"):
            return FakeResponse(json.dumps({"code": 200, "data": [{"secu_code": "sh603929", "secu_name": "亚翔集成", "last_px": 11.5, "change": 0.1, "continuous": 3}]}, ensure_ascii=False))
        if parsed.path.endswith("/dingPan/mainline"):
            return FakeResponse(json.dumps({"errno": 0, "data": {"faucet_1": {"title": "AI主线", "summary": "算力扩散"}}}, ensure_ascii=False))
        if parsed.path.endswith("/plate/plate_list"):
            return FakeResponse(json.dumps({"code": 200, "data": {"plate_data": [{"secu_name": "通信", "secu_code": "cls82074", "change": 0.0321, "main_fund_diff": 957, "limit_up": 102, "limit_down": 14, "limit_up_num": 7, "limit_down_num": 0, "trade_status": "ENDTR", "first_stock": {"secu_name": "中际旭创"}}]}}, ensure_ascii=False))
        if parsed.path.endswith("/plate/plate_heat_list"):
            return FakeResponse(json.dumps({"code": 200, "data": [{"plate_code": "cls80195", "plate_name": "半导体芯片", "rank": 1, "cur_heat": 99.1, "rank_change": 2, "is_new": 0}]}, ensure_ascii=False))
        if parsed.path.endswith("/plate/popular_stocks"):
            return FakeResponse(json.dumps({"code": 200, "data": [{"secu_code": "sz002552", "secu_name": "宝鼎科技", "change": "+10.00%", "change_px": 1.0, "tbm": "12天7板", "head_num": 1}]}, ensure_ascii=False))
        if parsed.path.endswith("/plate/rotation"):
            return FakeResponse(json.dumps({"code": 200, "data": [{"trade_date": "2026-05-22", "plates": [{"plate_code": "cls80424", "plate_name": "MLCC", "change": 3.2}]}]}, ensure_ascii=False))
        if parsed.path.endswith("/up_down_analysis"):
            return FakeResponse(json.dumps({"code": 200, "data": [{"secu_code": "sh603929", "secu_name": "亚翔集成", "last_px": 11.5, "change": 0.1, "up_reason": "新能源汽车概念"}]}, ensure_ascii=False))
        if parsed.path.endswith("/stock/tline"):
            return FakeResponse(json.dumps({"code": 200, "data": {"line": [{"date": "20260522", "minute": "0930", "last_px": 11.5, "business_balance": 123456, "business_amount": 12345, "open_px": 11.45, "preclose_px": 11.46, "av_px": 11.48}]}}, ensure_ascii=False))
        if parsed.path.endswith("/stock/kline"):
            return FakeResponse(json.dumps({"code": 200, "data": [{"date": "20260522", "open": 11.45, "close": 11.5, "high": 11.6, "low": 11.4, "volume": 1234567, "amount": 12345678, "change": 0.04, "change_rate": 0.35}]}, ensure_ascii=False))
        if parsed.path.endswith("/roll/get_roll_list"):
            return FakeResponse(json.dumps({"errno": 0, "data": {"roll_data": [{"id": "1", "title": "重要快讯", "content": "正文", "ctime": int(time.time())}]}}, ensure_ascii=False))
        raise AssertionError(request.full_url)


class KphOpener:
    def __init__(self):
        self.his_daban_calls = 0

    def __call__(self, request, timeout):
        body = parse_qs(request.data.decode("utf-8"))
        action = body.get("a", [""])[0]
        controller = body.get("c", [""])[0]
        if action in {"ZhangFuDetail", "HisZhangFuDetail"}:
            return FakeResponse(json.dumps({"errcode": "0", "info": {"ZT": 112, "DT": 2, "SJZT": 95, "SJDT": 2, "STZT": 17, "STDT": 0, "SZJS": 3036, "XDJS": 2008, "0": 139, "sign": "题材炒作热度高", "qscln": 3140000000000, "1": 100, "-1": 80}}, ensure_ascii=False))
        if action == "RealRankingInfo":
            return FakeResponse(json.dumps({"errcode": "0", "list": [["801807", "算力", 123456789, 2.88, 3.5, 1000, 0, 0, 0, 5.21, 999999, 1.5, None, None, None, None, None, 50]]}, ensure_ascii=False))
        if action == "ZhiShuStockList_W8":
            return FakeResponse(json.dumps({"errcode": "0", "list": [["000001", "平安银行", None, None, "银行", 11.5, 0.35, 123456789, 0.21, None, 220000000000, 1, 2, 3, None, None, None, None, None, None, None, None, None, "首板", "龙一", None, None, None, None, None, None, None, None, None, 5.5, None, None, None, None, None, None, 2]]}, ensure_ascii=False))
        if action == "HisDaBanList":
            # 首页返回满页 50 条,次页返回 2 条,验证翻页聚合
            self.his_daban_calls += 1
            row = ["601126", "四方股份", None, None, None, None, 1778655105, 0, 75845056, "首板", 1, "智能电网", 27697971, 2581487973, 6.65, 40196945337, "智能电网", None, None, None, None, None, None, 160656208, None, None, "801346", 11]
            count = 50 if self.his_daban_calls == 1 else 2
            return FakeResponse(json.dumps({"errcode": "0", "list": [row] * count}, ensure_ascii=False))
        if action == "GetZhangTingTianTi":
            return FakeResponse(json.dumps({"errcode": "0", "StockList": [[["000001", "平安银行", 2, 1778655105, "801001", "芯片", 1, 0, 8, 100000, 200000]]]}, ensure_ascii=False))
        if action == "GetPMSL_PMLD":
            return FakeResponse(json.dumps({"errcode": "0", "List": [{"TimeMin": 1778655105, "TagID": 1, "TagName": "直线拉升", "TagShuXing": 2, "ZSCode": "801001", "ZSName": "芯片", "Detail": "板块拉升", "StockList": [["000001", "平安银行"]]}]}, ensure_ascii=False))
        if action == "GetPlateInfo_w38" and controller == "HisLimitResumption":
            return FakeResponse(json.dumps({"errcode": "0", "list": [{"ZSCode": "801001", "ZSName": "芯片", "StockList": [["000001", "平安银行", None, None, None, None, None, None, None, "首板", 1, "芯片", None, None, None, None, "AI", "算力订单催化"]]}]}, ensure_ascii=False))
        raise AssertionError(request.full_url + " " + str(body))


def test_eastmoney_adapter_normalizes_all_supported_interfaces():
    adapter = EastmoneyRequestAdapter(opener=EastmoneyOpener())

    indices = adapter.request("eastmoney_market_index_realtime", {"scope": "default"})
    assert indices[0]["index_name"] == "上证指数"
    assert indices[0]["last_price"] == 3200.5

    snapshot = adapter.request("eastmoney_stock_realtime_snapshot", {"code": "000001.SZ"})
    assert snapshot[0]["instrument_id"] == "000001.SZ"
    assert snapshot[0]["last_price"] == 10.64

    sectors = adapter.request("eastmoney_sector_realtime", {"sector_type": "industry"})
    assert sectors[0]["sector_code"] == "BK1033"
    assert sectors[0]["lead_stock_symbol"] == "000049"

    constituents = adapter.request("eastmoney_sector_constituents", {"sector_code": "BK1033"})
    assert constituents[0]["instrument_id"] == "000049.SZ"

    belongs = adapter.request("eastmoney_stock_sector_belong", {"code": "000001.SZ"})
    assert belongs[0]["sector_name"] == "银行"

    limit_up = adapter.request("eastmoney_limit_up_pool", {"trade_date": "20260514"})
    assert limit_up[0]["instrument_id"] == "000001.SZ"
    assert limit_up[0]["last_price"] == 10.64

    limit_down = adapter.request("eastmoney_limit_down_pool", {"trade_date": "20260514"})
    assert limit_down[0]["instrument_id"] == "603126.SH"

    yesterday = adapter.request("eastmoney_yesterday_limit_up_pool", {"trade_date": "20260514"})
    assert yesterday[0]["limit_price"] == 13.02

    changes = adapter.request("eastmoney_stock_changes", {"change_type": "8201"})
    assert changes[0]["change_type_name"] == "火箭发射"

    detail = adapter.request("eastmoney_stock_change_detail", {"code": "000001.SZ", "trade_date": "20260514"})
    assert detail[0]["price"] == 10.5

    dragon = adapter.request("eastmoney_dragon_tiger_daily", {"trade_date": "20240102"})
    assert dragon[0]["instrument_id"] == "000595.SZ"
    assert dragon[0]["buy_amount"] == 79387344.0

    margin = adapter.request(
        "eastmoney_margin_trading",
        {"code": "000001.SZ", "start_date": "20240102", "end_date": "20240105"},
    )
    assert margin[0]["instrument_id"] == "000001.SZ"
    assert margin[0]["short_sell_volume"] == 564400.0

    empty = adapter.request("eastmoney_margin_trading", {"code": "999999.SZ"})
    assert empty == []

    reports = EastmoneyRequestAdapter(opener=EastmoneyReportOpener()).request(
        "eastmoney_research_reports",
        {"code": "000001.SZ", "start_date": "20240101", "end_date": "20241231"},
    )
    assert reports[0]["report_id"] == "AP202410221640398395"
    assert reports[0]["eps_forecast_this_year"] == 2.6


def test_cls_adapter_normalizes_supported_interfaces():
    adapter = ClsRequestAdapter(opener=ClsOpener())

    emotion = adapter.request("cls_market_emotion", {})
    assert emotion[0]["market_degree"] == 59.0
    assert emotion[0]["up_num"] == 127

    wind = adapter.request("cls_market_wind", {})
    assert wind[0]["plate_code"] == "cls80198"

    wind_stocks = adapter.request("cls_market_wind_stocks", {"plate_code": "cls80198"})
    assert wind_stocks[0]["instrument_id"] == "603929.SH"
    assert wind_stocks[0]["continuous_count"] == 3

    mainline = adapter.request("cls_market_mainline", {})
    assert mainline[0]["block_key"] == "faucet_1"

    industry = adapter.request("cls_sector_industry", {})
    assert industry[0]["plate_code"] == "cls82074"

    heat = adapter.request("cls_sector_heat", {})
    assert heat[0]["plate_name"] == "半导体芯片"

    popular = adapter.request("cls_sector_popular_stocks", {"plate_code": "cls80025"})
    assert popular[0]["instrument_id"] == "002552.SZ"
    assert popular[0]["head_rank"] == 1

    rotation = adapter.request("cls_sector_rotation", {"days": 4})
    assert rotation[0]["trade_date"] == "20260522"
    assert rotation[0]["rank"] == 1

    limit_up = adapter.request("cls_limit_up_pool", {})
    assert limit_up[0]["up_reason"] == "新能源汽车概念"

    timeline = adapter.request("cls_stock_timeline", {"code": "002664.SZ"})
    assert timeline[0]["instrument_id"] == "002664.SZ"
    assert timeline[0]["avg_price"] == 11.48

    kline = adapter.request("cls_stock_kline", {"code": "002664.SZ"})
    assert kline[0]["trade_date"] == "20260522"
    assert kline[0]["close"] == 11.5

    news = adapter.request("cls_news_telegraph", {"category": "important"})
    assert news[0]["title"] == "重要快讯"


def test_kph_adapter_normalizes_supported_interfaces():
    adapter = KphRequestAdapter(opener=KphOpener())

    emotion = adapter.request("kph_market_emotion", {"trade_date": "20260513"})
    assert emotion[0]["trade_date"] == "20260513"
    assert emotion[0]["limit_up_count"] == 112

    ranking = adapter.request("kph_sector_ranking", {"trade_date": "20260513", "sector_type": "selected"})
    assert ranking[0]["plate_id"] == "801807"
    assert ranking[0]["stock_count"] == 50

    constituents = adapter.request(
        "kph_sector_constituents_history",
        {"plate_id": "801001", "trade_date": "20260513"},
    )
    assert constituents[0]["instrument_id"] == "000001.SZ"
    assert constituents[0]["limit_tag"] == "首板"

    limit_up = adapter.request("kph_limit_up_history", {"trade_date": "20260513"})
    assert len(limit_up) == 52
    assert limit_up[0]["instrument_id"] == "601126.SH"
    assert limit_up[0]["reason"] == "智能电网"

    limit_down = adapter.request("kph_limit_down_history", {"trade_date": "20260513"})
    assert limit_down[0]["instrument_id"] == "601126.SH"

    wind_vane = adapter.request("kph_wind_vane_history", {"trade_date": "20260513"})
    assert wind_vane[0]["themes"] == "智能电网"

    ladder = adapter.request("kph_limit_ladder", {"trade_date": "20260513"})
    assert ladder[0]["instrument_id"] == "000001.SZ"
    assert ladder[0]["limit_count"] == 2

    events = adapter.request("kph_market_review_events", {"trade_date": "20260513"})
    assert events[0]["tag_name"] == "直线拉升"

    resumption = adapter.request("kph_limit_resumption_history", {"trade_date": "20260513"})
    assert resumption[0]["reason_detail"] == "算力订单催化"


class KphNoDataOpener:
    def __call__(self, request, timeout):
        return FakeResponse(json.dumps({"errcode": "1020", "errmsg": "参数出错"}, ensure_ascii=False))


def test_kph_adapter_treats_no_data_errcode_as_empty():
    adapter = KphRequestAdapter(opener=KphNoDataOpener())

    assert adapter.request("kph_limit_up_history", {"trade_date": "20260103"}) == []
    assert adapter.request("kph_limit_down_history", {"trade_date": "20260103"}) == []
    assert adapter.request("kph_wind_vane_history", {"trade_date": "20260103"}) == []
    assert adapter.request("kph_market_emotion", {"trade_date": "20260103"}) == []
    assert adapter.request("kph_sector_ranking", {"trade_date": "20260103"}) == []


class KphLatestDayOpener:
    """HisDaBanList 对 2026-08-26 及更早返回数据,更新的日期返回 1020(无数据)。"""

    def __init__(self):
        self.requested_days: list[str] = []

    def __call__(self, request, timeout):
        body = parse_qs(request.data.decode("utf-8"))
        action = body.get("a", [""])[0]
        if action == "HisDaBanList":
            day = body.get("Day", [""])[0]
            self.requested_days.append(day)
            if day >= "2026-08-27":
                return FakeResponse(json.dumps({"errcode": "1020", "errmsg": "参数出错"}, ensure_ascii=False))
            row = ["601126", "四方股份", None, None, None, None, 1778655105, 0, 75845056, "首板", 1, "智能电网", 27697971, 2581487973, 6.65, 40196945337, "智能电网", None, None, None, None, None, None, 160656208, None, None, "801346", 11]
            return FakeResponse(json.dumps({"errcode": "0", "list": [row]}, ensure_ascii=False))
        raise AssertionError(request.full_url + " " + str(body))


def test_kph_limit_history_without_date_walks_back_to_latest_trade_day(monkeypatch):
    import datetime as datetime_module

    import axdata_core.adapters.kph.request as kph_request

    class FrozenDate:
        @staticmethod
        def today():
            return datetime_module.date(2026, 8, 27)

    monkeypatch.setattr(kph_request, "date_type", FrozenDate)
    opener = KphLatestDayOpener()
    adapter = KphRequestAdapter(opener=opener)

    rows = adapter.request("kph_limit_up_history", {})

    assert opener.requested_days[0] == "2026-08-27"
    assert opener.requested_days[-1] == "2026-08-26"
    assert len(rows) == 1
    assert rows[0]["trade_date"] == "20260826"
    assert adapter.last_meta["trade_date"] == "20260826"

    # 显式指定过去日期:只请求当天,不做回溯
    opener.requested_days.clear()
    rows = adapter.request("kph_limit_up_history", {"trade_date": "20260105"})
    assert opener.requested_days == ["2026-01-05"]
    assert rows[0]["trade_date"] == "20260105"


class SinaOpener:
    def __call__(self, request, timeout):
        if "money.finance.sina.com.cn/bond/info/sz128039.html" in request.full_url:
            html = """
            <html><body>
              <table>
                <tr><td>债券名称</td><td>2018年三力士股份有限公司公开发行可转换公司债券</td></tr>
                <tr><td>债券简称</td><td>三力转债</td></tr>
              </table>
            </body></html>
            """
            return FakeResponse(html, encoding="gb2312")
        if "money.finance.sina.com.cn/bond/quotes/sh155255.html" in request.full_url:
            html = """
            <html><body>
              <table>
                <tr>
                  <td>债券类型</td><td>普通企业债</td>
                  <td>发行价格（元）</td><td>100</td>
                  <td>全价（元）</td><td>--</td>
                </tr>
                <tr>
                  <td>计息方式</td><td>固定利率</td>
                  <td>发行规模（亿元）</td><td>17</td>
                  <td>剩余年限（年）</td><td>9.24</td>
                </tr>
              </table>
            </body></html>
            """
            return FakeResponse(html, encoding="gb2312")
        if "bond.finance.sina.com.cn/hq/gb/daily" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            symbol = query["symbol"][0]
            if symbol == "US10YT":
                rows = [
                    {"d": "2022-08-24", "o": "3.048", "h": "3.126", "l": "3.022", "c": "3.1057", "v": "0"},
                    {"d": "2022-08-25", "o": "3.11", "h": "3.128", "l": "3.022", "c": "3.024", "v": "0"},
                ]
            else:
                assert symbol == "CN10YT"
                rows = [
                    {"d": "2022-08-02", "o": "2.734", "h": "2.738", "l": "2.724", "c": "2.73", "v": "0"},
                    {"d": "2022-08-03", "o": "2.747", "h": "2.747", "l": "2.716", "c": "2.716", "v": "0"},
                ]
            payload = {
                "result": {
                    "data": rows
                }
            }
            return FakeResponse(json.dumps(payload), encoding="utf-8")
        if "biz.finance.sina.com.cn/forex/forex.php" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["money_code"] == ["USD"]
            html = """
            <html><body>
              <table>
                <tr>
                  <th>日期</th><th>中行汇买价(元)</th><th>中行钞买价(元)</th>
                  <th>中行钞卖价/汇卖价</th><th>央行中间价</th><th>中行折算价</th>
                </tr>
                <tr>
                  <td>2023-03-10</td><td>689.7</td><td>684.09</td>
                  <td>692.62</td><td>696.55</td><td>696.55</td>
                </tr>
              </table>
            </body></html>
            """
            return FakeResponse(html, encoding="gbk")
        if "futures/view/optionsCffexDP.php" in request.full_url:
            if "/ho/cffex" in request.full_url:
                html = """
                <html><body>
                  <div class="js-select" id="option_symbol">
                    <span class="selected">上证50指数</span>
                    <ul>
                      <li class="active" data-value="ho">上证50指数</li>
                      <li data-value="io">沪深300指数</li>
                      <li data-value="mo">中证1000指数</li>
                    </ul>
                  </div>
                  <div class="js-select" id="option_suffix" data-value="ho2607">
                    <span class="selected">ho2607</span>
                    <ul>
                      <li class="active" data-value="ho2607">ho2607</li>
                      <li data-value="ho2609">ho2609</li>
                      <li data-value="ho2703">ho2703</li>
                    </ul>
                  </div>
                </body></html>
                """
                return FakeResponse(html, encoding="utf-8")
            if "/mo/cffex" in request.full_url:
                html = """
                <html><body>
                  <div class="js-select" id="option_symbol">
                    <span class="selected">中证1000指数</span>
                    <ul>
                      <li data-value="ho">上证50指数</li>
                      <li data-value="io">沪深300指数</li>
                      <li class="active" data-value="mo">中证1000指数</li>
                    </ul>
                  </div>
                  <div class="js-select" id="option_suffix" data-value="mo2609">
                    <span class="selected">mo2609</span>
                    <ul>
                      <li class="active" data-value="mo2609">mo2609</li>
                      <li data-value="mo2612">mo2612</li>
                      <li data-value="mo2703">mo2703</li>
                    </ul>
                  </div>
                </body></html>
                """
                return FakeResponse(html, encoding="utf-8")
            html = """
            <html><body>
              <div class="js-select" id="option_symbol">
                <span class="selected">沪深300指数</span>
                <ul>
                  <li class="active" data-value="ho">上证50指数</li>
                  <li class="active" data-value="io">沪深300指数</li>
                  <li class="active" data-value="mo">中证1000指数</li>
                </ul>
              </div>
              <div class="js-select" id="option_suffix" data-value="io2607">
                <span class="selected">io2607</span>
                <ul>
                  <li class="active" data-value="io2607">io2607</li>
                  <li data-value="io2609">io2609</li>
                  <li data-value="io2703">io2703</li>
                </ul>
              </div>
            </body></html>
            """
            return FakeResponse(html, encoding="utf-8")
        if "futures/view/optionsDP.php" in request.full_url:
            if "/m_o/dce" in request.full_url:
                html = """
                <html><body>
                  <ul class="clearfix">
                    <li class="active"><a href="/futures/view/optionsDP.php/m_o/dce">豆粕期权</a></li>
                    <li class="active"><a href="/futures/view/optionsDP.php/c_o/dce">玉米期权</a></li>
                  </ul>
                  <div class="js-select" id="option_suffix" data-value="m2609">
                    <span class="selected">m2609</span>
                    <ul>
                      <li class="active" data-value="m2609">m2609</li>
                      <li data-value="m2608">m2608</li>
                      <li data-value="m2701">m2701</li>
                    </ul>
                  </div>
                </body></html>
                """
                return FakeResponse(html, encoding="gbk")
            html = """
            <html><body>
              <ul class="clearfix">
                <li class="active"><a href="/futures/view/optionsDP.php/m_o/dce">豆粕期权</a></li>
                <li class="active"><a href="/futures/view/optionsDP.php/c_o/dce">玉米期权</a></li>
                <li class="active"><a href="/futures/view/optionsDP.php/i_o/dce">铁矿石期权</a></li>
              </ul>
            </body></html>
            """
            return FakeResponse(html, encoding="gbk")
        if "OptionService.getOptionData" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            if query["product"] == ["ho"]:
                assert query["pinzhong"] == ["ho2609"]
                payload = {
                    "result": {
                        "status": {"code": 0},
                        "data": {
                            "up": [
                                ["1", "395.800", "408.800", "410.000", "1", "191", 5.09, "2500", "ho2609C2500"],
                                ["2", "352.000", "361.400", "362.800", "4", "230", 4.7, "2550", "ho2609C2550"],
                            ],
                            "down": [
                                ["2", "12.600", "12.400", "13.600", "1", "1587", -7.46, "ho2609P2500"],
                                ["3", "16.000", "16.200", "17.000", "5", "1440", -6.8, "ho2609P2550"],
                            ],
                        },
                    }
                }
                return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            if query["product"] == ["mo"]:
                assert query["pinzhong"] == ["mo2607"]
                payload = {
                    "result": {
                        "status": {"code": 0},
                        "data": {
                            "up": [
                                ["1", "1352.000", "1393.600", "1398.800", "2", "123", -0.16, "7200", "mo2607C7200"],
                                ["2", "1250.000", "1280.000", "1288.000", "3", "150", -0.22, "7400", "mo2607C7400"],
                            ],
                            "down": [
                                ["2", "2.600", "2.600", "2.800", "5", "2568", -27.78, "mo2607P7200"],
                                ["3", "3.000", "3.200", "3.400", "6", "2100", -24.5, "mo2607P7400"],
                            ],
                        },
                    }
                }
                return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            assert query["product"] == ["io"]
            assert query["pinzhong"] == ["io2607"]
            payload = {
                "result": {
                    "status": {"code": 0},
                    "data": {
                        "up": [
                            ["1", "605.800", "626.000", "627.800", "1", "83", 5.89, "4200", "io2607C4200"],
                            ["2", "520.000", "531.000", "533.000", "3", "120", 4.2, "4300", "io2607C4300"],
                        ],
                        "down": [
                            ["7", "1.200", "1.200", "1.400", "5", "818", -50, "io2607P4200"],
                            ["6", "1.600", "1.800", "2.000", "4", "780", -42, "io2607P4300"],
                        ],
                    },
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "NetValueReturn_Service.NetValueReturnClose" in request.full_url:
            payload = {
                "total_num": 185,
                "data": [
                    {
                        "symbol": "001369",
                        "sname": "兴业稳固收益两年理财债券",
                        "zmjgm": "779912",
                        "clrq": "2015-06-10 00:00:00",
                        "jjjl": "唐丁祥、宁瑶",
                        "dwjz": "1.0299",
                        "jzrq": "20260703",
                        "zjzfe": 7799120000,
                    }
                ],
            }
            return FakeResponse(
                "IO.XSRV2.CallbackList['_bjN6KvXOkfPy2Bu'](" + json.dumps(payload, ensure_ascii=False) + ");",
                encoding="utf-8",
            )
        if "NetValueReturn_Service.NetValueReturnOpen" in request.full_url:
            payload = {
                "total_num": 6752,
                "data": [
                    {
                        "symbol": 510300,
                        "sname": "华泰柏瑞沪深300ETF",
                        "zmjgm": "3296860",
                        "clrq": "2012-05-04 00:00:00",
                        "jjjl": "柳军",
                        "dwjz": "4.8814",
                        "jzrq": "2026-07-03 00:00:00",
                        "zjzfe": 44828600000,
                    }
                ],
            }
            return FakeResponse(
                "IO.XSRV2.CallbackList['J2cW8KXheoWKdSHc'](" + json.dumps(payload, ensure_ascii=False) + ");",
                encoding="utf-8",
            )
        if "NetValueReturn_Service.NetValueReturnCX" in request.full_url:
            payload = {
                "total_num": 402,
                "data": [
                    {
                        "symbol": 150006,
                        "sname": "长盛同庆封闭A",
                        "zmjgm": "1468670",
                        "clrq": "2009-05-12 00:00:00",
                        "jjjl": "王宁",
                        "dwjz": "",
                        "jzrq": "",
                        "zjzfe": "",
                    }
                ],
            }
            return FakeResponse(
                "IO.XSRV2.CallbackList['cRrwseM7NWX68rDa'](" + json.dumps(payload, ensure_ascii=False) + ");",
                encoding="utf-8",
            )
        if "zhibo.sina.com.cn/api/zhibo/feed" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["page"] == ["1"]
            assert query["page_size"] == ["2"]
            assert query["zhibo_id"] == ["152"]
            payload = {
                "result": {
                    "data": {
                        "feed": {
                            "list": [
                                {
                                    "id": "1001",
                                    "create_time": "2026-07-05 09:30:00",
                                    "rich_text": "<p>全球财经快讯样例</p>",
                                },
                                {
                                    "id": "1002",
                                    "create_time": "2026-07-05 09:31:00",
                                    "rich_text": "第二条&nbsp;快讯",
                                },
                            ]
                        }
                    }
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "CN_Bill.GetBillList" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["symbol"] == ["sz000001"]
            assert query["day"] == ["2026-07-03"]
            assert query["num"] == ["2"]
            payload = [
                {
                    "ticktime": "09:31:00",
                    "price": "12.35",
                    "volume": "2000",
                    "prev_price": "12.34",
                    "type": "卖盘",
                },
                {
                    "ticktime": "09:30:00",
                    "price": "12.34",
                    "volume": "1000",
                    "prev_price": "12.33",
                    "type": "买盘",
                },
            ]
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "EsgService.getRftEsgStocks" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["p"] == ["1"]
            assert query["num"] == ["2"]
            payload = {
                "result": {
                    "data": {
                        "data": [
                            {
                                "symbol": "000001",
                                "esg_score": "72",
                                "esg_score_date": "2024-06-30",
                                "env_score": "70",
                                "env_score_date": "2024-06-30",
                                "social_score": "73",
                                "social_score_date": "2024-06-30",
                                "governance_score": "74",
                                "governance_score_date": "2024-06-30",
                                "zy_score": "60",
                                "zy_score_date": "2024-06-30",
                                "industry": "银行",
                                "exchange": "SZSE",
                            }
                        ]
                    }
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "EsgService.getMsciEsgStocks" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["p"] == ["1"]
            assert query["num"] == ["2"]
            payload = {
                "result": {
                    "data": {
                        "data": [
                            {
                                "symbol": "000001",
                                "name": "平安银行",
                                "market": "CN",
                                "industry_code": "4010",
                                "industry_name": "银行",
                                "esg_rating": "AA",
                                "env_score": "7.1",
                                "social_score": "6.8",
                                "governance_score": "7.5",
                                "quarter_date": "2024-06-30",
                                "updated_time": "2024-07-01 10:00:00",
                            }
                        ]
                    }
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "EsgService.getEsgStocks" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["page"] == ["1"]
            assert query["num"] == ["2"]
            payload = {
                "result": {
                    "data": {
                        "info": {
                            "stocks": [
                                {
                                    "symbol": "000001",
                                    "market": "CN",
                                    "esg_info": [
                                        {
                                            "agency_name": "评级机构A",
                                            "esg_score": "A",
                                            "esg_dt": "2024Q2",
                                            "remark": "样例",
                                        }
                                    ],
                                }
                            ]
                        }
                    }
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "EsgService.getZdEsgStocks" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["p"] == ["1"]
            assert query["num"] == ["2"]
            payload = {
                "result": {
                    "data": {
                        "data": [
                            {
                                "ticker": "000001",
                                "esg_score": "81.2",
                                "environmental_score": "78.5",
                                "social_score": "82.0",
                                "governance_score": "83.1",
                                "report_date": "2024-06-30",
                            }
                        ]
                    }
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "EsgService.getHzEsgStocks" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["p"] == ["1"]
            assert query["num"] == ["2"]
            payload = {
                "result": {
                    "data": {
                        "data": [
                            {
                                "date": "2024-06-30",
                                "symbol": "000001",
                                "market": "CN",
                                "name": "平安银行",
                                "esg_score": "79.6",
                                "esg_score_grade": "AA",
                                "e_score": "78.0",
                                "e_score_grade": "A",
                                "s_score": "80.0",
                                "s_score_grade": "AA",
                                "g_score": "81.0",
                                "g_score_grade": "AA",
                            }
                        ]
                    }
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "Market_Center.getHQFuturesData" in request.full_url:
            payload = [
                {
                    "symbol": "RB0",
                    "exchange": "shfe",
                    "name": "螺纹钢连续",
                    "trade": "3077.00",
                    "settlement": "0.00",
                    "presettlement": "3065.000",
                    "open": "3066.00",
                    "high": "3079.00",
                    "low": "3058.00",
                    "close": "0.00",
                    "bidprice1": "3075.000",
                    "askprice1": "3078.000",
                    "bidvol1": "352",
                    "askvol1": "521",
                    "volume": "227333",
                    "position": "2086185",
                    "ticktime": "23:00:00",
                    "tradedate": "2026-07-06",
                    "preclose": "3062.000",
                    "changepercent": "0.0039152",
                    "prevsettlement": "3065.00",
                }
            ]
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "vFutures_Positions_cjcc.php" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["t_breed"] == ["OI2501"]
            assert query["t_date"] == ["2024-10-16"]
            html = """
            <html><body>
              <table><tr><td>查询条件</td></tr></table>
              <table><tr><td>合约资料</td></tr></table>
              <table>
                <tr><th>名次</th><th>会员简称</th><th>成交量</th><th>比上交易增减</th></tr>
                <tr><td>1</td><td>中信期货（代客）</td><td>78197</td><td>-15458</td></tr>
                <tr><td>总计</td><td></td><td>78197</td><td>-15458</td></tr>
              </table>
              <table>
                <tr><th>名次</th><th>会员简称</th><th>多单持仓</th><th>比上交易增减</th></tr>
                <tr><td>1</td><td>中信期货（代客）</td><td>31024</td><td>-1288</td></tr>
                <tr><td>总计</td><td></td><td>31024</td><td>-1288</td></tr>
              </table>
              <table>
                <tr><th>名次</th><th>会员简称</th><th>空单持仓</th><th>比上交易增减</th></tr>
                <tr><td>1</td><td>中粮期货（代客）</td><td>25648</td><td>557</td></tr>
                <tr><td>总计</td><td></td><td>25648</td><td>557</td></tr>
              </table>
            </body></html>
            """
            return FakeResponse(html, encoding="gb2312")
        if "InnerFuturesNewService.getFewMinLine" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["symbol"] == ["RB0"]
            assert query["type"] == ["1"]
            payload = [
                {"d": "2026-07-01 09:13:00", "o": "3075.000", "h": "3075.000", "l": "3073.000", "c": "3073.000", "v": "5640", "p": "1998668"},
                {"d": "2026-07-01 09:14:00", "o": "3073.000", "h": "3076.000", "l": "3073.000", "c": "3075.000", "v": "4200", "p": "1999000"},
            ]
            return FakeResponse(
                "var _=(" + json.dumps(payload, ensure_ascii=False) + ");",
                encoding="utf-8",
            )
        if "InnerFuturesNewService.getDailyKLine" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            source_symbol = query["symbol"][0]
            if source_symbol == "CF0":
                payload = [
                    {
                        "d": "2024-01-23",
                        "o": "15800.000",
                        "h": "15900.000",
                        "l": "15750.000",
                        "c": "15870.000",
                        "v": "250000",
                        "p": "600000",
                        "s": "15840.000",
                    },
                    {
                        "d": "2024-01-24",
                        "o": "15920.000",
                        "h": "16010.000",
                        "l": "15830.000",
                        "c": "15975.000",
                        "v": "276741",
                        "p": "603842",
                        "s": "15930.000",
                    },
                    {
                        "d": "2024-01-25",
                        "o": "15960.000",
                        "h": "16020.000",
                        "l": "15890.000",
                        "c": "15910.000",
                        "v": "289000",
                        "p": "604100",
                        "s": "15970.000",
                    },
                ]
            else:
                assert source_symbol == "RB0"
                payload = [
                    {
                        "d": "2024-01-01",
                        "o": "3990.000",
                        "h": "4010.000",
                        "l": "3970.000",
                        "c": "4000.000",
                        "v": "800000",
                        "p": "1500000",
                        "s": "3995.000",
                    },
                    {
                        "d": "2024-01-02",
                        "o": "4005.000",
                        "h": "4058.000",
                        "l": "3983.000",
                        "c": "4047.000",
                        "v": "970394",
                        "p": "1541082",
                        "s": "4036.000",
                    },
                    {
                        "d": "2024-01-03",
                        "o": "4047.000",
                        "h": "4050.000",
                        "l": "4010.000",
                        "c": "4020.000",
                        "v": "920000",
                        "p": "1539000",
                        "s": "4025.000",
                    },
                ]
            return FakeResponse(
                f"var _{source_symbol}2021_08_17=(" + json.dumps(payload, ensure_ascii=False) + ");",
                encoding="utf-8",
            )
        if "hisdata_klc2/klc_kl.js" in request.full_url:
            payload = f'var KLC_K2_sh510050="{SINA_KLC_K2_FIXTURE}";'
            return FakeResponse(payload, encoding="utf-8")
        if "staticdata/us/.INX" in request.full_url:
            payload = f'var KLC_K2__INX="{SINA_KLC_K2_FIXTURE}";'
            return FakeResponse(payload, encoding="utf-8")
        if "stock/hkstock/CES100/klc2_kl.js" in request.full_url:
            payload = f'var KLC_K2_CES100="{SINA_KLC_K2_FIXTURE}";'
            return FakeResponse(payload, encoding="utf-8")
        if "klc_td_sh.txt" in request.full_url:
            payload = (
                'var datelist="'
                "LC/AAApNDXCw6mHbaPgkryxXv10eAJP1LW0SD39aT7+NV44Xba3PxCgTdrp5BkYVAc11hWvg0c/19UAc7jNtHQyWBAu2xmGuZI1NVAc3FepphjnTBw1X4hmGu+ypVAcvFenpBXPqCc6F4ZmGueLFwbIN8QTDXPsCc1FepphjvOoCc8FepphjvcgFO3CP00wxXXWhrkUdZrIJpw9X3ThrlEp6hlGc88Kcem0VeFpZM46VV4MrTC2KScKc811U4aLXUdlzINc9lTrwFW3T52KPj0mDueVFuUR1RtiEoCXfdgFOOSGRXnUhrXWhb0kt6Rk2pU44JV4SrTyU9wSDHPwCnXdP1FuiUM44r7qwdKqcYrIZpw1DqgrlU5IrHRawxjrwBaqcbrIt9gr3UhDtOpyVNjEnCHPnC3royNWvi0gjHXBXYdRlLbFpdJFueSFcqkK30sSDO+68K46IVOwVkaBX/"
                '";var KLC_TD_SH=datelist;'
            )
            return FakeResponse(payload, encoding="utf-8")
        if "vInvestConsult/kind/xsjj/index.phtml" in request.full_url:
            html = """
            <html><body>
              <table>
                <tr>
                  <th>代码</th><th>名称</th><th>解禁日期</th><th>解禁数量(万股)</th>
                  <th>解禁股流通市值(亿元)</th><th>上市批次</th><th>公告日期</th>
                </tr>
                <tr>
                  <td>600000</td><td>浦发银行</td><td>2020-09-04</td><td>124831.65</td>
                  <td>127.0786</td><td>10</td><td>2017-09-06</td>
                </tr>
              </table>
            </body></html>
            """
            return FakeResponse(html, encoding="gb2312")
        if "vInvestConsult/kind/lhb/index.phtml" in request.full_url:
            html = """
            <html><body>
              <table class="list_table">
                <tr>
                  <td>振幅值达15%的证券</td><td>振幅值达15%的证券</td>
                  <td>振幅值达15%的证券</td><td>振幅值达15%的证券</td>
                  <td>振幅值达15%的证券</td><td>振幅值达15%的证券</td>
                  <td>振幅值达15%的证券</td><td>振幅值达15%的证券</td>
                </tr>
                <tr>
                  <td>序号</td><td>股票代码</td><td>股票名称</td><td>收盘价(元)</td>
                  <td>对应值(%)</td><td>成交量(万股)</td><td>成交额(万元)</td><td>查看详情</td>
                </tr>
                <tr>
                  <td>1</td><td>000017</td><td>深中华A</td><td>11.68</td>
                  <td>15.88</td><td>15353.7649</td><td>182261.0964</td><td>查看交易详情</td>
                </tr>
              </table>
              <table><tr><td>交易营业所</td><td>买入金额(万元)</td><td>卖出金额(万元)</td><td>净买入(万元)</td></tr></table>
            </body></html>
            """
            return FakeResponse(html, encoding="gb2312")
        if "vLHBData/kind/ggtj/index.phtml" in request.full_url:
            html = """
            <html><body>
              <table>
                <tr>
                  <th>股票代码</th><th>股票名称</th><th>上榜次数</th><th>累积购买额(万)</th>
                  <th>累积卖出额(万)</th><th>净额(万)</th><th>买入席位数</th><th>卖出席位数</th>
                </tr>
                <tr>
                  <td>2407</td><td>多氟多</td><td>4</td><td>1334069.58</td>
                  <td>901157.03</td><td>432912.55</td><td>9</td><td>10</td>
                </tr>
              </table>
            </body></html>
            """
            return FakeResponse(html, encoding="gb2312")
        if "vLHBData/kind/jgmx/index.phtml" in request.full_url:
            html = """
            <html><body>
              <table>
                <tr>
                  <th>股票代码</th><th>股票名称</th><th>交易日期</th>
                  <th>机构席位买入额(万)</th><th>机构席位卖出额(万)</th><th>类型</th>
                </tr>
                <tr>
                  <td>688578</td><td>艾力斯</td><td>2026-07-03</td>
                  <td>44085.10</td><td>0.0</td><td>--</td>
                </tr>
              </table>
            </body></html>
            """
            return FakeResponse(html, encoding="gb2312")
        if "vLHBData/kind/jgzz/index.phtml" in request.full_url:
            html = """
            <html><body>
              <table>
                <tr>
                  <th>股票代码</th><th>股票名称</th><th>当前价</th><th>涨跌幅</th>
                  <th>累积买入额(万)</th><th>买入次数</th>
                  <th>累积卖出额(万)</th><th>卖出次数</th><th>净额(万)</th>
                </tr>
                <tr>
                  <td>725</td><td>京东方A</td><td>--</td><td>--</td>
                  <td>721552.5</td><td>4</td><td>457528.1</td><td>3</td><td>264024.4</td>
                </tr>
              </table>
            </body></html>
            """
            return FakeResponse(html, encoding="gb2312")
        if "vLHBData/kind/yytj/index.phtml" in request.full_url:
            html = """
            <html><body>
              <table>
                <tr>
                  <th>营业部名称</th><th>上榜次数</th><th>累积购买额(万)</th>
                  <th>买入席位数</th><th>累积卖出额(万)</th>
                  <th>卖出席位数</th><th>买入前三股票</th>
                </tr>
                <tr>
                  <td>招商证券股份有限公司深圳建安路证券营业部</td><td>1</td>
                  <td>0.0</td><td>0</td><td>15.12</td><td>1</td><td>国华退</td>
                </tr>
              </table>
            </body></html>
            """
            return FakeResponse(html, encoding="gb2312")
        if "CompanyFinanceService.getFinanceReport2022" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            assert query["paperCode"] == ["sh600600"]
            assert query["source"] == ["fzb"]
            payload = {
                "result": {
                    "status": {"code": 0},
                    "data": {
                        "report_count": "1",
                        "report_date": [
                            {"date_value": "20260331", "date_description": "2026一季报", "date_type": 1}
                        ],
                        "report_list": {
                            "20260331": {
                                "rType": "合并期末",
                                "rCurrency": "CNY",
                                "data_source": "定期报告",
                                "is_audit": "未审计",
                                "publish_date": "20260428",
                                "update_time": 1777285085,
                                "data": [
                                    {
                                        "item_field": "",
                                        "item_title": "流动资产",
                                        "item_value": "",
                                        "item_display_type": 1,
                                        "item_display": "大类",
                                        "item_precision": "f2",
                                        "item_group_no": 1,
                                        "item_source": "fzb",
                                        "item_tongbi": "",
                                    },
                                    {
                                        "item_field": "CURFDS",
                                        "item_title": "货币资金",
                                        "item_value": "13254301120.000000",
                                        "item_display_type": 2,
                                        "item_display": "小类",
                                        "item_precision": "f2",
                                        "item_group_no": 1,
                                        "item_source": "fzb",
                                        "item_tongbi": -0.18663,
                                    },
                                ],
                            }
                        },
                    },
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "FutureOptionAllService.getOptionDayline" in request.full_url:
            query = parse_qs(urlparse(request.full_url).query)
            source_symbol = query["symbol"][0]
            if source_symbol == "io2202P4350":
                payload = [
                    {"o": "15.0000", "h": "17.0000", "l": "15.0000", "c": "15.8000", "v": "49", "d": "2021-11-29"},
                    {"o": "16.0000", "h": "16.4000", "l": "12.0000", "c": "12.2000", "v": "80", "d": "2021-11-30"},
                ]
            elif source_symbol == "ho2303P2350":
                assert source_symbol == "ho2303P2350"
                payload = [
                    {"o": "16.8000", "h": "16.8000", "l": "16.8000", "c": "16.8000", "v": "6", "d": "2022-12-21"},
                    {"o": "12.0000", "h": "13.0000", "l": "10.0000", "c": "11.0000", "v": "9", "d": "2022-12-22"},
                ]
            elif source_symbol == "mo2609P6200":
                assert source_symbol == "mo2609P6200"
                payload = [
                    {"o": "411.4000", "h": "461.8000", "l": "411.4000", "c": "424.8000", "v": "477", "d": "2025-11-24"},
                    {"o": "432.0000", "h": "450.0000", "l": "420.0000", "c": "430.0000", "v": "390", "d": "2025-11-25"},
                ]
            else:
                assert source_symbol == "m2609P2500"
                payload = [
                    {"o": "8.0000", "h": "39.0000", "l": "8.0000", "c": "39.0000", "v": "98", "d": "2025-09-30"},
                    {"o": "36.0000", "h": "40.0000", "l": "34.0000", "c": "35.0000", "v": "80", "d": "2025-10-09"},
                ]
            return FakeResponse(
                f"var _{source_symbol}=(" + json.dumps(payload, ensure_ascii=False) + ");",
                encoding="utf-8",
            )
        if "StockOptionDaylineService.getOptionMinline" in request.full_url:
            payload = {
                "result": {
                    "status": {"code": 0},
                    "data": [
                        {"i": "09:30:00", "p": "0.0000", "v": "0", "t": "847", "a": "0.0000", "d": "2026-07-03"},
                        {"i": "09:31:00", "p": "0.3082", "v": "0", "t": "851", "a": "0.3055"},
                    ],
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "StockOptionDaylineService.getSymbolInfo" in request.full_url:
            payload = (
                "/*<script>location.href='//sina.com';</script>*/\n"
                "(["
                "{\"d\":\"2026-06-09\",\"o\":\"0.2109\",\"h\":\"0.2227\",\"l\":\"0.2095\",\"c\":\"0.2227\",\"v\":\"54451\"},"
                "{\"d\":\"2026-07-03\",\"o\":\"0.5898\",\"h\":\"0.6948\",\"l\":\"0.5800\",\"c\":\"0.6760\",\"v\":\"397212\"}"
                "])"
            )
            return FakeResponse(payload, encoding="utf-8")
        if "StockOptionService.getRemainderDay" in request.full_url:
            payload = {
                "result": {
                    "status": {"code": 0},
                    "data": {
                        "expireDay": "2026-07-22",
                        "remainderDays": 17,
                        "stockId": "510050",
                        "cateId": "510050C2607",
                        "zhulikanzhang": "",
                        "zhulikandie": "",
                        "other": {
                            "name": "华夏上证50ETF",
                            "type": 10,
                            "symbol": "s_sh510050",
                            "url": "http://finance.sina.com.cn/fund/quotes/510050/bc.shtml",
                        },
                    },
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "StockOptionService.getStockName" in request.full_url:
            payload = {
                "result": {
                    "status": {"code": 0},
                    "data": {
                        "cateList": ["50ETF", "300ETF", "500ETF"],
                        "contractMonth": ["2026-07", "2026-07", "2026-08"],
                        "stockId": "510050",
                        "cateId": "510050C2607",
                    },
                }
            }
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "hq.sinajs.cn/list=CON_SO_10011799" in request.full_url:
            payload = (
                'var hq_str_CON_SO_10011799="'
                "50ETF购7月2700,,,,84,0.9983,0.0448,-0.114,0.0036,0.001,"
                "0.3407,0.2928,510050C2607M02700,2.7000,0.3267,0.3284,M"
                '";'
            )
            return FakeResponse(payload, encoding="gbk")
        if "hq.sinajs.cn/list=CON_OP_10011799" in request.full_url:
            payload = (
                'var hq_str_CON_OP_10011799="'
                "1,0.3171,0.3267,0.3203,1,838,7.82,2.7000,0.3028,0.3005,"
                "0.6033,0.0027,0.3308,1,0.3268,2,0.3221,1,0.3210,5,"
                "0.3203,1,0.3171,1,0.3154,1,0.3113,2,0.3086,1,0.3084,1,"
                "2026-07-03 14:26:17,0,E 00,EBS,510050,50ETF购7月2700,"
                "15.82,0.3407,0.2928,84,272948.00,M,0.3030,C,2026-07-22,18,2,0.323,0.0037"
                '";'
            )
            return FakeResponse(payload, encoding="gbk")
        if "hq.sinajs.cn/list=sh510050" in request.full_url:
            payload = (
                'var hq_str_sh510050="'
                "上证50ETF华夏,3.003,3.003,3.023,3.048,3.003,3.023,3.024,"
                "536339530,1621596612.000,543700,3.023,316900,3.022,1939200,"
                "3.021,819300,3.020,202200,3.019,690700,3.024,168000,3.025,"
                "110100,3.026,2700,3.027,69600,3.028,2026-07-03,15:00:01,00,"
                '";'
            )
            return FakeResponse(payload, encoding="gbk")
        if "hq.sinajs.cn/list=hkHSI" in request.full_url:
            payload = (
                'var hq_str_hkHSI="'
                "HSI,恒生指数,24000.00,23900.00,24100.00,23800.00,24050.00,150.00,0.6276,"
                ",,,,,,,,,"
                '";\n'
                'var hq_str_hkHSTECH="'
                "HSTECH,恒生科技指数,5100.00,5000.00,5150.00,4980.00,5125.00,125.00,2.5000,"
                ",,,,,,,,,"
                '";'
            )
            return FakeResponse(payload, encoding="gbk")
        if "hq.sinajs.cn/list=OP_UP_5100502607" in request.full_url:
            payload = (
                'var hq_str_OP_UP_5100502607="'
                "CON_OP_10011799,CON_OP_10011781,"
                '";'
            )
            return FakeResponse(payload, encoding="gbk")
        if "gi.finance.sina.com.cn/hq/daily" in request.full_url:
            payload = {
                "result": {
                    "data": [
                        {"d": "2024-01-02", "o": "100.1", "h": "101.2", "l": "99.8", "c": "100.5", "v": "12345"},
                        {"d": "2024-01-03", "o": "100.5", "h": "102.0", "l": "100.0", "c": "101.5", "v": "23456"},
                    ]
                }
            }
            return FakeResponse(json.dumps(payload), encoding="utf-8")
        if "Market_Center.getHQNodeData?" in request.full_url:
            payload = [
                {
                    "symbol": "sh600000",
                    "code": "600000",
                    "name": "浦发银行",
                    "trade": "8.690",
                    "pricechange": -0.01,
                    "changepercent": -0.115,
                    "buy": "8.690",
                    "sell": "8.700",
                    "settlement": "8.700",
                    "open": "8.690",
                    "high": "8.820",
                    "low": "8.590",
                    "volume": 69513349,
                    "amount": 604291542,
                    "ticktime": "15:00:02",
                    "per": 5.717,
                    "pb": 0.384,
                    "mktcap": 28942773.4827,
                    "nmc": 28942773.4827,
                    "turnoverratio": 0.20871,
                }
            ]
            return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        if "Market_Center.getHQNodeDataSimple" in request.full_url:
            parsed = urlparse(request.full_url)
            query = parse_qs(parsed.query)
            if query.get("node", [""])[0] == "hs_s":
                payload = [
                    {
                        "symbol": "sh000001",
                        "code": "000001",
                        "name": "上证指数",
                        "trade": "4043.6432",
                        "pricechange": "14.739",
                        "changepercent": "0.366",
                        "buy": "0",
                        "sell": "0",
                        "settlement": "4028.9038",
                        "open": "4031.3351",
                        "high": "4073.8802",
                        "low": "4027.2553",
                        "volume": 602009738,
                        "amount": 1465563104854,
                        "ticktime": "15:30:39",
                    }
                ]
                return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            if query.get("node", [""])[0].startswith("zhishu_"):
                payload = [
                    {
                        "symbol": "sz399001",
                        "name": "深证成指",
                        "trade": "10490.35",
                        "pricechange": "12.34",
                        "changepercent": "0.118",
                        "buy": "0.000",
                        "sell": "0.000",
                        "settlement": "10478.01",
                        "open": "10480.00",
                        "high": "10510.00",
                        "low": "10400.00",
                        "volume": "12345678",
                        "amount": "987654321",
                        "ticktime": "15:00:03",
                        "per": "0",
                        "pb": "0",
                        "mktcap": "0",
                        "nmc": "0",
                        "turnoverratio": "0",
                    }
                ]
                return FakeResponse(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            payload = (
                "IO.XSRV2.CallbackList['da_yPT46_Ll7K6WD']("
                "[[\"sh510050\",\"50ETF\",\"2.500\",\"0.010\",\"0.40\",\"2.499\",\"2.500\","
                "\"2.490\",\"2.495\",\"2.510\",\"2.480\",\"1234567\",\"3086419\"]]);"
            )
            return FakeResponse(payload)
        if request.full_url.endswith("/hfq.js"):
            payload = (
                "var fund_hfq={data:[[\"1900-01-01\",\"1\",\"0\",\"0\"],"
                "[\"2024-01-15\",\"1\",\"0\",\"0.05\"]]};"
            )
            return FakeResponse(payload)
        html = """
        <table>
          <tr><td>平安银行(000001) 利润表单位：万元</td><td>平安银行(000001) 利润表单位：万元</td><td>平安银行(000001) 利润表单位：万元</td></tr>
          <tr><td>报表日期</td><td>2024-12-31</td><td>2024-09-30</td></tr>
          <tr><td>一、营业收入</td><td>14669500.00</td><td>11158200.00</td></tr>
          <tr><td>利息净收入</td><td>9342700.00</td><td>7253600.00</td></tr>
        </table>
        """
        return FakeResponse(html, encoding="gb2312")


def test_sina_adapter_no_longer_exposes_legacy_financial_statement_interface():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    assert adapter.supports("sina_financial_statement") is False
    with pytest.raises(SourceAdapterNotFound, match="sina_financial_statement"):
        adapter.request(
            "sina_financial_statement",
            {"code": "000001.SZ", "statement_type": "income", "year": 2024, "limit": 2},
        )


def test_sina_adapter_normalizes_fund_etf_category_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("fund_etf_category_sina", {"category": "ETF基金", "page": 1, "limit": 1})

    assert rows == [
        {
            "instrument_id": "510050.SH",
            "fund_code": "510050",
            "sina_symbol": "sh510050",
            "exchange": "SSE",
            "fund_type": "ETF基金",
            "name": "50ETF",
            "latest_price": 2.5,
            "change": 0.01,
            "change_pct": 0.4,
            "bid": 2.499,
            "ask": 2.5,
            "prev_close": 2.49,
            "open": 2.495,
            "high": 2.51,
            "low": 2.48,
            "volume": 1234567.0,
            "amount": 3086419.0,
        }
    ]

    result = request_interface(
        "fund_etf_category_sina",
        params={"category": "ETF基金", "page": 1, "limit": 1},
        fields=["instrument_id", "name", "latest_price"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "510050.SH", "name": "50ETF", "latest_price": 2.5}
    ]


def test_sina_adapter_normalizes_fund_etf_dividend_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("fund_etf_dividend_sina", {"symbol": "sh510050", "limit": 1})

    assert rows == [
        {
            "instrument_id": "510050.SH",
            "fund_code": "510050",
            "sina_symbol": "sh510050",
            "exchange": "SSE",
            "dividend_date": "20240115",
            "accumulated_dividend": 0.05,
        }
    ]

    result = request_interface(
        "fund_etf_dividend_sina",
        params={"symbol": "sh510050", "limit": 1},
        fields=["instrument_id", "dividend_date", "accumulated_dividend"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "510050.SH", "dividend_date": "20240115", "accumulated_dividend": 0.05}
    ]


def test_sina_adapter_decodes_fund_etf_hist_klc_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("fund_etf_hist_sina", {"symbol": "sh510050", "limit": 1})

    assert rows == [
        {
            "instrument_id": "510050.SH",
            "fund_code": "510050",
            "sina_symbol": "sh510050",
            "exchange": "SSE",
            "trade_date": "19901220",
            "open": 1.0,
            "high": 1.02,
            "low": 0.99,
            "close": 1.01,
            "volume": 1000,
            "amount": 1005,
        }
    ]

    result = request_interface(
        "fund_etf_hist_sina",
        params={"symbol": "sh510050", "limit": 1},
        fields=["instrument_id", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "510050.SH", "trade_date": "19901220", "close": 1.01}
    ]


def test_sina_adapter_normalizes_bond_cb_profile_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("bond_cb_profile_sina", {"symbol": "128039.SZ", "limit": 1})

    assert rows == [
        {
            "instrument_id": "128039.SZ",
            "symbol": "128039",
            "sina_symbol": "sz128039",
            "exchange": "SZSE",
            "sequence": 1,
            "item_name": "债券名称",
            "item_value": "2018年三力士股份有限公司公开发行可转换公司债券",
        }
    ]

    result = request_interface(
        "bond_cb_profile_sina",
        params={"symbol": "sz128039", "limit": 2},
        fields=["instrument_id", "item_name", "item_value"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "128039.SZ",
            "item_name": "债券名称",
            "item_value": "2018年三力士股份有限公司公开发行可转换公司债券",
        },
        {"instrument_id": "128039.SZ", "item_name": "债券简称", "item_value": "三力转债"},
    ]


def test_sina_adapter_normalizes_bond_cb_summary_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("bond_cb_summary_sina", {"symbol": "155255.SH", "limit": 3})

    assert rows == [
        {
            "instrument_id": "155255.SH",
            "symbol": "155255",
            "sina_symbol": "sh155255",
            "exchange": "SSE",
            "sequence": 1,
            "item_name": "债券类型",
            "item_value": "普通企业债",
        },
        {
            "instrument_id": "155255.SH",
            "symbol": "155255",
            "sina_symbol": "sh155255",
            "exchange": "SSE",
            "sequence": 2,
            "item_name": "发行价格（元）",
            "item_value": "100",
        },
        {
            "instrument_id": "155255.SH",
            "symbol": "155255",
            "sina_symbol": "sh155255",
            "exchange": "SSE",
            "sequence": 3,
            "item_name": "全价（元）",
            "item_value": None,
        },
    ]

    result = request_interface(
        "bond_cb_summary_sina",
        params={"symbol": "sh155255", "limit": 2},
        fields=["instrument_id", "item_name", "item_value"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "155255.SH", "item_name": "债券类型", "item_value": "普通企业债"},
        {"instrument_id": "155255.SH", "item_name": "发行价格（元）", "item_value": "100"},
    ]


def test_sina_adapter_normalizes_bond_gb_us_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("bond_gb_us_sina", {"symbol": "美国10年期国债", "start_date": "20220824", "limit": 1})

    assert rows == [
        {
            "source_symbol": "US10YT",
            "symbol_name": "美国10年期国债",
            "country": "US",
            "tenor": "10Y",
            "trade_date": "20220824",
            "open_yield": 3.048,
            "high_yield": 3.126,
            "low_yield": 3.022,
            "close_yield": 3.1057,
            "volume": 0.0,
        }
    ]

    result = request_interface(
        "bond_gb_us_sina",
        params={"symbol": "US10YT", "start_date": "20220824", "limit": 1},
        fields=["source_symbol", "trade_date", "close_yield"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "US10YT", "trade_date": "20220824", "close_yield": 3.1057}
    ]


def test_sina_adapter_normalizes_bond_gb_zh_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("bond_gb_zh_sina", {"symbol": "中国10年期国债", "start_date": "20220802", "limit": 1})

    assert rows == [
        {
            "source_symbol": "CN10YT",
            "symbol_name": "中国10年期国债",
            "country": "CN",
            "tenor": "10Y",
            "trade_date": "20220802",
            "open_yield": 2.734,
            "high_yield": 2.738,
            "low_yield": 2.724,
            "close_yield": 2.73,
            "volume": 0.0,
        }
    ]

    result = request_interface(
        "bond_gb_zh_sina",
        params={"symbol": "CN10YT", "start_date": "20220802", "limit": 1},
        fields=["source_symbol", "trade_date", "close_yield"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "CN10YT", "trade_date": "20220802", "close_yield": 2.73}
    ]


def test_sina_adapter_normalizes_currency_boc_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request(
        "currency_boc_sina",
        {"symbol": "美元", "start_date": "20230304", "end_date": "20230310", "limit": 1},
    )

    assert rows == [
        {
            "currency_code": "USD",
            "currency_name": "美元",
            "quote_date": "20230310",
            "fx_buy_rate": 689.7,
            "cash_buy_rate": 684.09,
            "cash_sell_rate": 692.62,
            "pboc_mid_rate": 696.55,
            "boc_conversion_rate": 696.55,
        }
    ]

    result = request_interface(
        "currency_boc_sina",
        params={"symbol": "USD", "start_date": "20230304", "end_date": "20230310", "limit": 1},
        fields=["currency_code", "quote_date", "fx_buy_rate"],
        adapter=adapter,
    )
    assert result.records == [
        {"currency_code": "USD", "quote_date": "20230310", "fx_buy_rate": 689.7}
    ]


def test_sina_adapter_normalizes_fund_scale_close_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("fund_scale_close_sina", {"page": 1, "limit": 1})

    assert rows == [
        {
            "rank": 1,
            "fund_code": "001369",
            "fund_name": "兴业稳固收益两年理财债券",
            "fund_category": "封闭式基金",
            "unit_nav": 1.0299,
            "total_raised_scale": 779912.0,
            "latest_total_share": 7799120000.0,
            "established_date": "20150610",
            "fund_manager": "唐丁祥、宁瑶",
            "nav_date": "20260703",
        }
    ]

    result = request_interface(
        "fund_scale_close_sina",
        params={"page": 1, "limit": 1},
        fields=["fund_code", "fund_name", "total_raised_scale"],
        adapter=adapter,
    )
    assert result.records == [
        {"fund_code": "001369", "fund_name": "兴业稳固收益两年理财债券", "total_raised_scale": 779912.0}
    ]


def test_sina_adapter_normalizes_fund_scale_open_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("fund_scale_open_sina", {"symbol": "股票型基金", "page": 1, "limit": 1})

    assert rows == [
        {
            "rank": 1,
            "fund_code": "510300",
            "fund_name": "华泰柏瑞沪深300ETF",
            "fund_category": "股票型基金",
            "unit_nav": 4.8814,
            "total_raised_scale": 3296860.0,
            "latest_total_share": 44828600000.0,
            "established_date": "20120504",
            "fund_manager": "柳军",
            "nav_date": "20260703",
        }
    ]

    result = request_interface(
        "fund_scale_open_sina",
        params={"symbol": "2", "page": 1, "limit": 1},
        fields=["fund_code", "fund_name", "total_raised_scale"],
        adapter=adapter,
    )
    assert result.records == [
        {"fund_code": "510300", "fund_name": "华泰柏瑞沪深300ETF", "total_raised_scale": 3296860.0}
    ]


def test_sina_adapter_normalizes_fund_scale_structured_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("fund_scale_structured_sina", {"page": 1, "limit": 1})

    assert rows == [
        {
            "rank": 1,
            "fund_code": "150006",
            "fund_name": "长盛同庆封闭A",
            "fund_category": "分级子基金",
            "unit_nav": None,
            "total_raised_scale": 1468670.0,
            "latest_total_share": None,
            "established_date": "20090512",
            "fund_manager": "王宁",
            "nav_date": None,
        }
    ]

    result = request_interface(
        "fund_scale_structured_sina",
        params={"page": 1, "limit": 1},
        fields=["fund_code", "fund_name", "total_raised_scale"],
        adapter=adapter,
    )
    assert result.records == [
        {"fund_code": "150006", "fund_name": "长盛同庆封闭A", "total_raised_scale": 1468670.0}
    ]


def test_sina_adapter_normalizes_futures_display_main_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("futures_display_main_sina", {"node": "lwg_qh", "page": 1, "limit": 1})

    assert rows == [
        {
            "rank": 1,
            "source_node": "lwg_qh",
            "source_symbol": "RB0",
            "exchange": "shfe",
            "name": "螺纹钢连续",
            "trade_date": "20260706",
            "tick_time": "23:00:00",
            "last_price": 3077.0,
            "open": 3066.0,
            "high": 3079.0,
            "low": 3058.0,
            "close": 0.0,
            "preclose": 3062.0,
            "settlement": 0.0,
            "prev_settlement": 3065.0,
            "bid_price_1": 3075.0,
            "ask_price_1": 3078.0,
            "bid_volume_1": 352.0,
            "ask_volume_1": 521.0,
            "volume": 227333.0,
            "open_interest": 2086185.0,
            "change_pct": 0.0039152,
        }
    ]

    result = request_interface(
        "futures_display_main_sina",
        params={"node": "lwg_qh", "page": 1, "limit": 1},
        fields=["source_symbol", "trade_date", "last_price"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "RB0", "trade_date": "20260706", "last_price": 3077.0}
    ]


def test_sina_adapter_normalizes_futures_hold_pos_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request(
        "futures_hold_pos_sina",
        {"symbol": "多单持仓", "contract": "OI2501", "date": "20241016", "limit": 1},
    )

    assert rows == [
        {
            "trade_date": "20241016",
            "contract": "OI2501",
            "metric": "多单持仓",
            "rank": 1,
            "member_name": "中信期货（代客）",
            "value": 31024.0,
            "change": -1288.0,
        }
    ]

    result = request_interface(
        "futures_hold_pos_sina",
        params={"symbol": "多单持仓", "contract": "OI2501", "date": "20241016", "limit": 1},
        fields=["contract", "member_name", "value"],
        adapter=adapter,
    )
    assert result.records == [
        {"contract": "OI2501", "member_name": "中信期货（代客）", "value": 31024.0}
    ]


def test_sina_adapter_normalizes_futures_main_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request(
        "futures_main_sina",
        {"symbol": "CF0", "start_date": "20240124", "end_date": "20240301", "limit": 1},
    )

    assert rows == [
        {
            "source_symbol": "CF0",
            "trade_date": "20240124",
            "open": 15920.0,
            "high": 16010.0,
            "low": 15830.0,
            "close": 15975.0,
            "volume": 276741.0,
            "open_interest": 603842.0,
            "settlement": 15930.0,
        }
    ]

    result = request_interface(
        "futures_main_sina",
        params={"symbol": "CF0", "start_date": "20240124", "end_date": "20240301", "limit": 1},
        fields=["source_symbol", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "CF0", "trade_date": "20240124", "close": 15975.0}
    ]


def test_sina_adapter_normalizes_futures_zh_daily_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request(
        "futures_zh_daily_sina",
        {"symbol": "RB0", "start_date": "20240102", "end_date": "20240105", "limit": 1},
    )

    assert rows == [
        {
            "source_symbol": "RB0",
            "trade_date": "20240102",
            "open": 4005.0,
            "high": 4058.0,
            "low": 3983.0,
            "close": 4047.0,
            "volume": 970394.0,
            "open_interest": 1541082.0,
            "settlement": 4036.0,
        }
    ]

    result = request_interface(
        "futures_zh_daily_sina",
        params={"symbol": "RB0", "start_date": "20240102", "end_date": "20240105", "limit": 1},
        fields=["source_symbol", "trade_date", "settlement"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "RB0", "trade_date": "20240102", "settlement": 4036.0}
    ]


def test_sina_adapter_normalizes_futures_zh_minute_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("futures_zh_minute_sina", {"symbol": "RB0", "period": "1", "limit": 1})

    assert rows == [
        {
            "source_symbol": "RB0",
            "period": "1",
            "datetime": "20260701091300",
            "trade_date": "20260701",
            "trade_time": "09:13:00",
            "open": 3075.0,
            "high": 3075.0,
            "low": 3073.0,
            "close": 3073.0,
            "volume": 5640.0,
            "open_interest": 1998668.0,
        }
    ]

    result = request_interface(
        "futures_zh_minute_sina",
        params={"symbol": "RB0", "period": "1", "limit": 1},
        fields=["source_symbol", "datetime", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "RB0", "datetime": "20260701091300", "close": 3073.0}
    ]


def test_sina_adapter_calculates_rv_from_futures_zh_minute_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("rv_from_futures_zh_minute_sina", {"symbol": "RB0", "period": "1", "limit": 2})

    assert len(rows) == 1
    assert rows[0] == {
        "source_symbol": "RB0",
        "period": "1",
        "start_datetime": "20260701091300",
        "end_datetime": "20260701091400",
        "sample_count": 2,
        "return_count": 1,
        "realized_variance": pytest.approx(4.2330392523384376e-07),
        "realized_volatility": pytest.approx(0.0006506181101336204),
    }

    result = request_interface(
        "rv_from_futures_zh_minute_sina",
        params={"symbol": "RB0", "period": "1", "limit": 2},
        fields=["source_symbol", "return_count", "realized_volatility"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "source_symbol": "RB0",
            "return_count": 1,
            "realized_volatility": pytest.approx(0.0006506181101336204),
        }
    ]


def test_sina_adapter_normalizes_option_cffex_hs300_daily_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_cffex_hs300_daily_sina", {"symbol": "io2202P4350", "limit": 1})

    assert rows == [
        {
            "source_symbol": "io2202P4350",
            "underlying_name": "沪深300指数",
            "option_type": "看跌期权",
            "exercise_price": 4350.0,
            "trade_date": "20211129",
            "open": 15.0,
            "high": 17.0,
            "low": 15.0,
            "close": 15.8,
            "volume": 49.0,
        }
    ]

    result = request_interface(
        "option_cffex_hs300_daily_sina",
        params={"symbol": "io2202P4350", "limit": 1},
        fields=["source_symbol", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "io2202P4350", "trade_date": "20211129", "close": 15.8}
    ]


def test_sina_adapter_normalizes_option_cffex_sz50_daily_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_cffex_sz50_daily_sina", {"symbol": "ho2303P2350", "limit": 1})

    assert rows == [
        {
            "source_symbol": "ho2303P2350",
            "underlying_name": "上证50指数",
            "option_type": "看跌期权",
            "exercise_price": 2350.0,
            "trade_date": "20221221",
            "open": 16.8,
            "high": 16.8,
            "low": 16.8,
            "close": 16.8,
            "volume": 6.0,
        }
    ]

    result = request_interface(
        "option_cffex_sz50_daily_sina",
        params={"symbol": "ho2303P2350", "limit": 1},
        fields=["source_symbol", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "ho2303P2350", "trade_date": "20221221", "close": 16.8}
    ]


def test_sina_adapter_normalizes_option_cffex_zz1000_daily_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_cffex_zz1000_daily_sina", {"symbol": "mo2609P6200", "limit": 1})

    assert rows == [
        {
            "source_symbol": "mo2609P6200",
            "underlying_name": "中证1000指数",
            "option_type": "看跌期权",
            "exercise_price": 6200.0,
            "trade_date": "20251124",
            "open": 411.4,
            "high": 461.8,
            "low": 411.4,
            "close": 424.8,
            "volume": 477.0,
        }
    ]

    result = request_interface(
        "option_cffex_zz1000_daily_sina",
        params={"symbol": "mo2609P6200", "limit": 1},
        fields=["source_symbol", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "mo2609P6200", "trade_date": "20251124", "close": 424.8}
    ]


def test_sina_adapter_normalizes_option_cffex_hs300_list_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_cffex_hs300_list_sina", {"limit": 2})

    assert rows == [
        {
            "sequence": 1,
            "product_code": "io",
            "underlying_name": "沪深300指数",
            "contract_code": "io2607",
            "expire_month": "202607",
            "is_main": True,
            "exchange": "CFFEX",
        },
        {
            "sequence": 2,
            "product_code": "io",
            "underlying_name": "沪深300指数",
            "contract_code": "io2609",
            "expire_month": "202609",
            "is_main": False,
            "exchange": "CFFEX",
        },
    ]

    result = request_interface(
        "option_cffex_hs300_list_sina",
        params={"limit": 1},
        fields=["contract_code", "expire_month", "is_main"],
        adapter=adapter,
    )
    assert result.records == [
        {"contract_code": "io2607", "expire_month": "202607", "is_main": True}
    ]


def test_sina_adapter_normalizes_option_cffex_sz50_list_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_cffex_sz50_list_sina", {"limit": 2})

    assert rows == [
        {
            "sequence": 1,
            "product_code": "ho",
            "underlying_name": "上证50指数",
            "contract_code": "ho2607",
            "expire_month": "202607",
            "is_main": True,
            "exchange": "CFFEX",
        },
        {
            "sequence": 2,
            "product_code": "ho",
            "underlying_name": "上证50指数",
            "contract_code": "ho2609",
            "expire_month": "202609",
            "is_main": False,
            "exchange": "CFFEX",
        },
    ]

    result = request_interface(
        "option_cffex_sz50_list_sina",
        params={"limit": 1},
        fields=["contract_code", "expire_month", "is_main"],
        adapter=adapter,
    )
    assert result.records == [
        {"contract_code": "ho2607", "expire_month": "202607", "is_main": True}
    ]


def test_sina_adapter_normalizes_option_cffex_zz1000_list_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_cffex_zz1000_list_sina", {"limit": 2})

    assert rows == [
        {
            "sequence": 1,
            "product_code": "mo",
            "underlying_name": "中证1000指数",
            "contract_code": "mo2609",
            "expire_month": "202609",
            "is_main": True,
            "exchange": "CFFEX",
        },
        {
            "sequence": 2,
            "product_code": "mo",
            "underlying_name": "中证1000指数",
            "contract_code": "mo2612",
            "expire_month": "202612",
            "is_main": False,
            "exchange": "CFFEX",
        },
    ]

    result = request_interface(
        "option_cffex_zz1000_list_sina",
        params={"limit": 1},
        fields=["contract_code", "expire_month", "is_main"],
        adapter=adapter,
    )
    assert result.records == [
        {"contract_code": "mo2609", "expire_month": "202609", "is_main": True}
    ]


def test_sina_adapter_normalizes_option_commodity_contract_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_commodity_contract_sina", {"limit": 2})

    assert rows == [
        {
            "sequence": 1,
            "option_name": "豆粕期权",
            "product_code": "m_o",
            "exchange": "dce",
            "source_path": "/futures/view/optionsDP.php/m_o/dce",
            "source_url": "https://stock.finance.sina.com.cn/futures/view/optionsDP.php/m_o/dce",
        },
        {
            "sequence": 2,
            "option_name": "玉米期权",
            "product_code": "c_o",
            "exchange": "dce",
            "source_path": "/futures/view/optionsDP.php/c_o/dce",
            "source_url": "https://stock.finance.sina.com.cn/futures/view/optionsDP.php/c_o/dce",
        },
    ]

    result = request_interface(
        "option_commodity_contract_sina",
        params={"limit": 1},
        fields=["option_name", "product_code", "exchange"],
        adapter=adapter,
    )
    assert result.records == [
        {"option_name": "豆粕期权", "product_code": "m_o", "exchange": "dce"}
    ]


def test_sina_adapter_normalizes_option_commodity_contract_table_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_commodity_contract_table_sina", {"symbol": "豆粕期权", "limit": 2})

    assert rows == [
        {
            "sequence": 1,
            "option_name": "豆粕期权",
            "product_code": "m_o",
            "exchange": "dce",
            "contract_code": "m2609",
            "expire_month": "202609",
            "is_main": True,
            "source_url": "https://stock.finance.sina.com.cn/futures/view/optionsDP.php/m_o/dce",
        },
        {
            "sequence": 2,
            "option_name": "豆粕期权",
            "product_code": "m_o",
            "exchange": "dce",
            "contract_code": "m2608",
            "expire_month": "202608",
            "is_main": False,
            "source_url": "https://stock.finance.sina.com.cn/futures/view/optionsDP.php/m_o/dce",
        },
    ]

    result = request_interface(
        "option_commodity_contract_table_sina",
        params={"symbol": "m_o", "limit": 1},
        fields=["option_name", "contract_code", "expire_month"],
        adapter=adapter,
    )
    assert result.records == [
        {"option_name": "豆粕期权", "contract_code": "m2609", "expire_month": "202609"}
    ]


def test_sina_adapter_normalizes_option_commodity_hist_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_commodity_hist_sina", {"symbol": "m2609P2500", "option_name": "豆粕期权", "limit": 1})

    assert rows == [
        {
            "source_symbol": "m2609P2500",
            "option_name": "豆粕期权",
            "product_code": "m_o",
            "exchange": "dce",
            "option_type": "看跌期权",
            "exercise_price": 2500.0,
            "trade_date": "20250930",
            "open": 8.0,
            "high": 39.0,
            "low": 8.0,
            "close": 39.0,
            "volume": 98.0,
        }
    ]

    result = request_interface(
        "option_commodity_hist_sina",
        params={"symbol": "m2609P2500", "option_name": "m_o", "limit": 1},
        fields=["source_symbol", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": "m2609P2500", "trade_date": "20250930", "close": 39.0}
    ]


def test_sina_adapter_normalizes_option_cffex_hs300_spot_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_cffex_hs300_spot_sina", {"symbol": "io2607", "limit": 1})

    assert rows == [
        {
            "contract_code": "io2607",
            "product_code": "io",
            "underlying_name": "沪深300指数",
            "exercise_price": 4200.0,
            "call_symbol": "io2607C4200",
            "call_bid_volume": 1.0,
            "call_bid_price": 605.8,
            "call_latest_price": 626.0,
            "call_ask_price": 627.8,
            "call_ask_volume": 1.0,
            "call_open_interest": 83.0,
            "call_change": 5.89,
            "put_symbol": "io2607P4200",
            "put_bid_volume": 7.0,
            "put_bid_price": 1.2,
            "put_latest_price": 1.2,
            "put_ask_price": 1.4,
            "put_ask_volume": 5.0,
            "put_open_interest": 818.0,
            "put_change": -50.0,
            "exchange": "CFFEX",
        }
    ]

    result = request_interface(
        "option_cffex_hs300_spot_sina",
        params={"symbol": "io2607", "limit": 1},
        fields=["call_symbol", "exercise_price", "put_symbol"],
        adapter=adapter,
    )
    assert result.records == [
        {"call_symbol": "io2607C4200", "exercise_price": 4200.0, "put_symbol": "io2607P4200"}
    ]


def test_sina_adapter_normalizes_option_cffex_sz50_spot_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_cffex_sz50_spot_sina", {"symbol": "ho2609", "limit": 1})

    assert rows == [
        {
            "contract_code": "ho2609",
            "product_code": "ho",
            "underlying_name": "上证50指数",
            "exercise_price": 2500.0,
            "call_symbol": "ho2609C2500",
            "call_bid_volume": 1.0,
            "call_bid_price": 395.8,
            "call_latest_price": 408.8,
            "call_ask_price": 410.0,
            "call_ask_volume": 1.0,
            "call_open_interest": 191.0,
            "call_change": 5.09,
            "put_symbol": "ho2609P2500",
            "put_bid_volume": 2.0,
            "put_bid_price": 12.6,
            "put_latest_price": 12.4,
            "put_ask_price": 13.6,
            "put_ask_volume": 1.0,
            "put_open_interest": 1587.0,
            "put_change": -7.46,
            "exchange": "CFFEX",
        }
    ]

    result = request_interface(
        "option_cffex_sz50_spot_sina",
        params={"symbol": "ho2609", "limit": 1},
        fields=["call_symbol", "exercise_price", "put_symbol"],
        adapter=adapter,
    )
    assert result.records == [
        {"call_symbol": "ho2609C2500", "exercise_price": 2500.0, "put_symbol": "ho2609P2500"}
    ]


def test_sina_adapter_normalizes_option_cffex_zz1000_spot_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_cffex_zz1000_spot_sina", {"symbol": "mo2607", "limit": 1})

    assert rows == [
        {
            "contract_code": "mo2607",
            "product_code": "mo",
            "underlying_name": "中证1000指数",
            "exercise_price": 7200.0,
            "call_symbol": "mo2607C7200",
            "call_bid_volume": 1.0,
            "call_bid_price": 1352.0,
            "call_latest_price": 1393.6,
            "call_ask_price": 1398.8,
            "call_ask_volume": 2.0,
            "call_open_interest": 123.0,
            "call_change": -0.16,
            "put_symbol": "mo2607P7200",
            "put_bid_volume": 2.0,
            "put_bid_price": 2.6,
            "put_latest_price": 2.6,
            "put_ask_price": 2.8,
            "put_ask_volume": 5.0,
            "put_open_interest": 2568.0,
            "put_change": -27.78,
            "exchange": "CFFEX",
        }
    ]

    result = request_interface(
        "option_cffex_zz1000_spot_sina",
        params={"symbol": "mo2607", "limit": 1},
        fields=["call_symbol", "exercise_price", "put_symbol"],
        adapter=adapter,
    )
    assert result.records == [
        {"call_symbol": "mo2607C7200", "exercise_price": 7200.0, "put_symbol": "mo2607P7200"}
    ]


def test_sina_adapter_normalizes_index_global_hist_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("index_global_hist_sina", {"symbol": "瑞士股票指数", "limit": 1})

    assert rows == [
        {
            "index_code": "SWI20",
            "index_name": "瑞士股票指数",
            "trade_date": "20240103",
            "open": 100.5,
            "high": 102.0,
            "low": 100.0,
            "close": 101.5,
            "volume": 23456.0,
        }
    ]

    result = request_interface(
        "index_global_hist_sina",
        params={"symbol": "瑞士股票指数", "limit": 1},
        fields=["index_code", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"index_code": "SWI20", "trade_date": "20240103", "close": 101.5}
    ]


def test_sina_adapter_decodes_index_us_stock_klc_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("index_us_stock_sina", {"symbol": ".INX", "limit": 1})

    assert rows == [
        {
            "source_symbol": ".INX",
            "index_name": "标普500指数",
            "trade_date": "19901220",
            "open": 1.0,
            "high": 1.02,
            "low": 0.99,
            "close": 1.01,
            "volume": 1000,
            "amount": 1005,
        }
    ]

    result = request_interface(
        "index_us_stock_sina",
        params={"symbol": ".INX", "limit": 1},
        fields=["source_symbol", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_symbol": ".INX", "trade_date": "19901220", "close": 1.01}
    ]


def test_sina_adapter_normalizes_index_stock_cons_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("index_stock_cons_sina", {"symbol": "000300", "page": 1, "limit": 1})

    assert rows == [
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "sina_symbol": "sh600000",
            "exchange": "SSE",
            "index_code": "000300",
            "name": "浦发银行",
            "latest_price": 8.69,
            "change": -0.01,
            "change_pct": -0.115,
            "bid": 8.69,
            "ask": 8.7,
            "prev_close": 8.7,
            "open": 8.69,
            "high": 8.82,
            "low": 8.59,
            "volume": 69513349.0,
            "amount": 604291542.0,
            "tick_time": "15:00:02",
            "pe": 5.717,
            "pb": 0.384,
            "market_cap": 28942773.4827,
            "float_market_cap": 28942773.4827,
            "turnover_ratio": 0.20871,
        }
    ]

    result = request_interface(
        "index_stock_cons_sina",
        params={"symbol": "000300", "page": 1, "limit": 1},
        fields=["instrument_id", "index_code", "name", "latest_price"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "600000.SH", "index_code": "000300", "name": "浦发银行", "latest_price": 8.69}
    ]


def test_sina_adapter_normalizes_stock_classify_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request(
        "stock_classify_sina",
        {"symbol": "新浪行业", "class_name": "玻璃行业", "node": "new_blhy", "page": 1, "limit": 1},
    )

    assert rows == [
        {
            "category": "新浪行业",
            "class_name": "玻璃行业",
            "source_node": "new_blhy",
            "rank": 1,
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "sina_symbol": "sh600000",
            "exchange": "SSE",
            "name": "浦发银行",
            "latest_price": 8.69,
            "change": -0.01,
            "change_pct": -0.115,
            "bid": 8.69,
            "ask": 8.7,
            "prev_close": 8.7,
            "open": 8.69,
            "high": 8.82,
            "low": 8.59,
            "volume": 69513349.0,
            "amount": 604291542.0,
            "tick_time": "15:00:02",
        }
    ]

    result = request_interface(
        "stock_classify_sina",
        params={"node": "new_blhy", "class_name": "玻璃行业", "limit": 1},
        fields=["source_node", "instrument_id", "name"],
        adapter=adapter,
    )
    assert result.records == [
        {"source_node": "new_blhy", "instrument_id": "600000.SH", "name": "浦发银行"}
    ]


def test_sina_adapter_normalizes_stock_zh_index_spot_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_zh_index_spot_sina", {"page": 1, "limit": 1})

    assert rows == [
        {
            "instrument_id": "000001.SH",
            "symbol": "000001",
            "sina_symbol": "sh000001",
            "exchange": "SSE",
            "asset_type": "index",
            "name": "上证指数",
            "latest_price": 4043.6432,
            "change": 14.739,
            "change_pct": 0.366,
            "bid": 0.0,
            "ask": 0.0,
            "prev_close": 4028.9038,
            "open": 4031.3351,
            "high": 4073.8802,
            "low": 4027.2553,
            "volume": 602009738.0,
            "amount": 1465563104854.0,
            "tick_time": "15:30:39",
        }
    ]

    result = request_interface(
        "stock_zh_index_spot_sina",
        params={"page": 1, "limit": 1},
        fields=["instrument_id", "name", "latest_price"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "000001.SH", "name": "上证指数", "latest_price": 4043.6432}
    ]


def test_sina_adapter_decodes_stock_hk_index_daily_klc_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_hk_index_daily_sina", {"symbol": "CES100", "limit": 1})

    assert rows == [
        {
            "index_code": "CES100",
            "sina_symbol": "hkCES100",
            "market": "HK",
            "asset_type": "index",
            "trade_date": "19901220",
            "open": 1.0,
            "high": 1.02,
            "low": 0.99,
            "close": 1.01,
            "volume": 1000,
            "amount": 1005,
        }
    ]

    result = request_interface(
        "stock_hk_index_daily_sina",
        params={"symbol": "hkCES100", "limit": 1},
        fields=["sina_symbol", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"sina_symbol": "hkCES100", "trade_date": "19901220", "close": 1.01}
    ]


def test_sina_adapter_normalizes_stock_hk_index_spot_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_hk_index_spot_sina", {"symbols": "hkHSI,HSTECH", "limit": 2})

    assert rows == [
        {
            "sina_symbol": "hkHSI",
            "index_code": "HSI",
            "market": "HK",
            "asset_type": "index",
            "name": "恒生指数",
            "open": 24000.0,
            "prev_close": 23900.0,
            "high": 24100.0,
            "low": 23800.0,
            "latest_price": 24050.0,
            "change": 150.0,
            "change_pct": 0.6276,
        },
        {
            "sina_symbol": "hkHSTECH",
            "index_code": "HSTECH",
            "market": "HK",
            "asset_type": "index",
            "name": "恒生科技指数",
            "open": 5100.0,
            "prev_close": 5000.0,
            "high": 5150.0,
            "low": 4980.0,
            "latest_price": 5125.0,
            "change": 125.0,
            "change_pct": 2.5,
        },
    ]

    result = request_interface(
        "stock_hk_index_spot_sina",
        params={"symbols": "hkHSI,HSTECH", "limit": 1},
        fields=["sina_symbol", "index_code", "latest_price"],
        adapter=adapter,
    )
    assert result.records == [
        {"sina_symbol": "hkHSI", "index_code": "HSI", "latest_price": 24050.0}
    ]


def test_sina_adapter_normalizes_stock_info_global_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_info_global_sina", {"page": 1, "page_size": 2, "limit": 2})

    assert rows == [
        {
            "feed_id": "1001",
            "create_time": "2026-07-05 09:30:00",
            "content": "全球财经快讯样例",
        },
        {
            "feed_id": "1002",
            "create_time": "2026-07-05 09:31:00",
            "content": "第二条 快讯",
        },
    ]

    result = request_interface(
        "stock_info_global_sina",
        params={"page": 1, "page_size": 2, "limit": 1},
        fields=["create_time", "content"],
        adapter=adapter,
    )
    assert result.records == [
        {"create_time": "2026-07-05 09:30:00", "content": "全球财经快讯样例"}
    ]


def test_sina_adapter_normalizes_stock_intraday_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_intraday_sina", {"symbol": "sz000001", "date": "20260703", "limit": 2})

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "sina_symbol": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20260703",
            "tick_time": "09:30:00",
            "price": 12.34,
            "volume": 1000.0,
            "prev_price": 12.33,
            "trade_type": "买盘",
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "sina_symbol": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20260703",
            "tick_time": "09:31:00",
            "price": 12.35,
            "volume": 2000.0,
            "prev_price": 12.34,
            "trade_type": "卖盘",
        },
    ]

    result = request_interface(
        "stock_intraday_sina",
        params={"symbol": "sz000001", "date": "20260703", "limit": 2},
        fields=["instrument_id", "tick_time", "price"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "000001.SZ", "tick_time": "09:30:00", "price": 12.34},
        {"instrument_id": "000001.SZ", "tick_time": "09:31:00", "price": 12.35},
    ]


def test_sina_adapter_normalizes_stock_esg_msci_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_esg_msci_sina", {"page": 1, "limit": 2})

    assert rows == [
        {
            "symbol": "000001",
            "name": "平安银行",
            "market": "CN",
            "industry_code": "4010",
            "industry_name": "银行",
            "esg_rating": "AA",
            "env_score": 7.1,
            "social_score": 6.8,
            "governance_score": 7.5,
            "rating_date": "20240630",
            "updated_time": "2024-07-01 10:00:00",
        }
    ]

    result = request_interface(
        "stock_esg_msci_sina",
        params={"page": 1, "limit": 2},
        fields=["symbol", "esg_rating", "rating_date"],
        adapter=adapter,
    )
    assert result.records == [
        {"symbol": "000001", "esg_rating": "AA", "rating_date": "20240630"}
    ]


def test_sina_adapter_normalizes_stock_esg_rate_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_esg_rate_sina", {"page": 1, "limit": 2})

    assert rows == [
        {
            "symbol": "000001",
            "market": "CN",
            "agency_name": "评级机构A",
            "rating": "A",
            "rating_period": "2024Q2",
            "remark": "样例",
        }
    ]

    result = request_interface(
        "stock_esg_rate_sina",
        params={"page": 1, "limit": 2},
        fields=["symbol", "agency_name", "rating"],
        adapter=adapter,
    )
    assert result.records == [
        {"symbol": "000001", "agency_name": "评级机构A", "rating": "A"}
    ]


def test_sina_adapter_normalizes_stock_esg_rft_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_esg_rft_sina", {"page": 1, "limit": 2})

    assert rows == [
        {
            "symbol": "000001",
            "esg_score": 72.0,
            "esg_score_date": "20240630",
            "env_score": 70.0,
            "env_score_date": "20240630",
            "social_score": 73.0,
            "social_score_date": "20240630",
            "governance_score": 74.0,
            "governance_score_date": "20240630",
            "controversy_score": 60.0,
            "controversy_score_date": "20240630",
            "industry": "银行",
            "exchange": "SZSE",
        }
    ]

    result = request_interface(
        "stock_esg_rft_sina",
        params={"page": 1, "limit": 2},
        fields=["symbol", "esg_score", "industry"],
        adapter=adapter,
    )
    assert result.records == [
        {"symbol": "000001", "esg_score": 72.0, "industry": "银行"}
    ]


def test_sina_adapter_normalizes_stock_esg_zd_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_esg_zd_sina", {"page": 1, "limit": 2})

    assert rows == [
        {
            "symbol": "000001",
            "esg_score": 81.2,
            "env_score": 78.5,
            "social_score": 82.0,
            "governance_score": 83.1,
            "report_date": "20240630",
        }
    ]

    result = request_interface(
        "stock_esg_zd_sina",
        params={"page": 1, "limit": 2},
        fields=["symbol", "esg_score", "report_date"],
        adapter=adapter,
    )
    assert result.records == [
        {"symbol": "000001", "esg_score": 81.2, "report_date": "20240630"}
    ]


def test_sina_adapter_normalizes_stock_esg_hz_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_esg_hz_sina", {"page": 1, "limit": 2})

    assert rows == [
        {
            "date": "20240630",
            "symbol": "000001",
            "market": "CN",
            "name": "平安银行",
            "esg_score": 79.6,
            "esg_grade": "AA",
            "env_score": 78.0,
            "env_grade": "A",
            "social_score": 80.0,
            "social_grade": "AA",
            "governance_score": 81.0,
            "governance_grade": "AA",
        }
    ]

    result = request_interface(
        "stock_esg_hz_sina",
        params={"page": 1, "limit": 2},
        fields=["symbol", "esg_score", "esg_grade"],
        adapter=adapter,
    )
    assert result.records == [
        {"symbol": "000001", "esg_score": 79.6, "esg_grade": "AA"}
    ]


def test_sina_adapter_decodes_tool_trade_date_hist_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("tool_trade_date_hist_sina", {"start_date": "19901219", "limit": 2})

    assert rows == [
        {
            "trade_date": "19901219",
            "exchange": "SSE",
            "is_open": True,
            "source_calendar": "Sina KLC_TD_SH",
        },
        {
            "trade_date": "19901220",
            "exchange": "SSE",
            "is_open": True,
            "source_calendar": "Sina KLC_TD_SH",
        },
    ]

    result = request_interface(
        "tool_trade_date_hist_sina",
        params={"start_date": "19920504", "end_date": "19920504", "limit": 1},
        fields=["trade_date", "exchange"],
        adapter=adapter,
    )
    assert result.records == [{"trade_date": "19920504", "exchange": "SSE"}]


def test_sina_adapter_normalizes_option_sse_codes_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request(
        "option_sse_codes_sina",
        {"symbol": "看涨期权", "underlying": "510050", "trade_date": "202607", "limit": 2},
    )

    assert rows == [
        {
            "sequence": 1,
            "option_code": "10011799",
            "sina_symbol": "CON_OP_10011799",
            "option_type": "看涨期权",
            "underlying": "510050",
            "underlying_name": "50ETF",
            "expire_month": "202607",
            "exchange": "SSE",
        },
        {
            "sequence": 2,
            "option_code": "10011781",
            "sina_symbol": "CON_OP_10011781",
            "option_type": "看涨期权",
            "underlying": "510050",
            "underlying_name": "50ETF",
            "expire_month": "202607",
            "exchange": "SSE",
        },
    ]

    result = request_interface(
        "option_sse_codes_sina",
        params={"symbol": "看涨期权", "underlying": "510050", "trade_date": "202607", "limit": 1},
        fields=["sequence", "option_code", "sina_symbol"],
        adapter=adapter,
    )
    assert result.records == [
        {"sequence": 1, "option_code": "10011799", "sina_symbol": "CON_OP_10011799"}
    ]


def test_sina_adapter_normalizes_option_sse_daily_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_sse_daily_sina", {"symbol": "10011799", "limit": 1})

    assert rows == [
        {
            "option_code": "10011799",
            "sina_symbol": "CON_OP_10011799",
            "trade_date": "20260703",
            "open": 0.5898,
            "high": 0.6948,
            "low": 0.58,
            "close": 0.676,
            "volume": 397212.0,
        }
    ]

    result = request_interface(
        "option_sse_daily_sina",
        params={"symbol": "CON_OP_10011799", "limit": 1},
        fields=["option_code", "trade_date", "close"],
        adapter=adapter,
    )
    assert result.records == [
        {"option_code": "10011799", "trade_date": "20260703", "close": 0.676}
    ]


def test_sina_adapter_normalizes_option_sse_minute_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_sse_minute_sina", {"symbol": "10011799", "limit": 2})

    assert rows == [
        {
            "option_code": "10011799",
            "sina_symbol": "CON_OP_10011799",
            "trade_date": "20260703",
            "trade_time": "09:30:00",
            "price": 0.0,
            "volume": 0.0,
            "open_interest": 847.0,
            "avg_price": 0.0,
        },
        {
            "option_code": "10011799",
            "sina_symbol": "CON_OP_10011799",
            "trade_date": "20260703",
            "trade_time": "09:31:00",
            "price": 0.3082,
            "volume": 0.0,
            "open_interest": 851.0,
            "avg_price": 0.3055,
        },
    ]

    result = request_interface(
        "option_sse_minute_sina",
        params={"symbol": "CON_OP_10011799", "limit": 1},
        fields=["option_code", "trade_time", "avg_price"],
        adapter=adapter,
    )
    assert result.records == [
        {"option_code": "10011799", "trade_time": "09:30:00", "avg_price": 0.0}
    ]


def test_sina_adapter_normalizes_option_finance_minute_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_finance_minute_sina", {"symbol": "10011799", "limit": 1})

    assert rows == [
        {
            "option_code": "10011799",
            "sina_symbol": "CON_OP_10011799",
            "trade_date": "20260703",
            "trade_time": "09:30:00",
            "price": 0.0,
            "volume": 0.0,
            "open_interest": 847.0,
            "avg_price": 0.0,
        }
    ]

    result = request_interface(
        "option_finance_minute_sina",
        params={"symbol": "CON_OP_10011799", "limit": 1},
        fields=["option_code", "trade_time", "avg_price"],
        adapter=adapter,
    )
    assert result.records == [
        {"option_code": "10011799", "trade_time": "09:30:00", "avg_price": 0.0}
    ]


def test_sina_adapter_normalizes_option_sse_spot_price_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_sse_spot_price_sina", {"symbol": "10011799"})

    assert rows == [
        {
            "option_code": "10011799",
            "sina_symbol": "CON_OP_10011799",
            "buy_volume": 1.0,
            "buy_price": 0.3171,
            "latest_price": 0.3267,
            "sell_price": 0.3203,
            "sell_volume": 1.0,
            "open_interest": 838.0,
            "change_pct": 7.82,
            "exercise_price": 2.7,
            "prev_close": 0.3028,
            "open": 0.3005,
            "limit_up": 0.6033,
            "limit_down": 0.0027,
            "quote_time": "2026-07-03 14:26:17",
            "quote_status": "0",
            "status_code": "E 00",
            "underlying_type": "EBS",
            "underlying": "510050",
            "contract_name": "50ETF购7月2700",
            "amplitude": 15.82,
            "high": 0.3407,
            "low": 0.2928,
            "volume": 84.0,
            "amount": 272948.0,
            "main_contract_flag": "M",
            "option_type": "C",
            "expire_date": "20260722",
            "remaining_days": 18,
        }
    ]

    result = request_interface(
        "option_sse_spot_price_sina",
        params={"symbol": "CON_OP_10011799"},
        fields=["option_code", "latest_price", "underlying"],
        adapter=adapter,
    )
    assert result.records == [
        {"option_code": "10011799", "latest_price": 0.3267, "underlying": "510050"}
    ]


def test_sina_adapter_normalizes_option_sse_underlying_spot_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_sse_underlying_spot_price_sina", {"symbol": "sh510050"})

    assert rows == [
        {
            "instrument_id": "510050.SH",
            "fund_code": "510050",
            "sina_symbol": "sh510050",
            "exchange": "SSE",
            "name": "上证50ETF华夏",
            "open": 3.003,
            "prev_close": 3.003,
            "latest_price": 3.023,
            "high": 3.048,
            "low": 3.003,
            "bid": 3.023,
            "ask": 3.024,
            "volume": 536339530.0,
            "amount": 1621596612.0,
            "bid_volume_1": 543700.0,
            "bid_price_1": 3.023,
            "ask_volume_1": 690700.0,
            "ask_price_1": 3.024,
            "quote_date": "20260703",
            "quote_time": "15:00:01",
            "halt_status": "00",
        }
    ]

    result = request_interface(
        "option_sse_underlying_spot_price_sina",
        params={"symbol": "510050.SH"},
        fields=["instrument_id", "latest_price", "quote_date"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "510050.SH", "latest_price": 3.023, "quote_date": "20260703"}
    ]


def test_sina_adapter_normalizes_stock_financial_report_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request(
        "stock_financial_report_sina",
        {"stock": "sh600600", "symbol": "资产负债表", "limit": 1, "item_limit": 2},
    )

    assert rows == [
        {
            "instrument_id": "600600.SH",
            "symbol": "600600",
            "sina_symbol": "sh600600",
            "exchange": "SSE",
            "statement_type": "balance",
            "statement_name": "资产负债表",
            "report_date": "20260331",
            "date_description": "2026一季报",
            "date_type": 1,
            "item_field": None,
            "item_name": "流动资产",
            "item_value": None,
            "item_display_type": 1,
            "item_display": "大类",
            "item_precision": "f2",
            "item_group_no": 1,
            "item_source": "fzb",
            "item_yoy": None,
            "data_source": "定期报告",
            "is_audit": "未审计",
            "publish_date": "20260428",
            "currency": "CNY",
            "report_type": "合并期末",
            "update_time": 1777285085,
        },
        {
            "instrument_id": "600600.SH",
            "symbol": "600600",
            "sina_symbol": "sh600600",
            "exchange": "SSE",
            "statement_type": "balance",
            "statement_name": "资产负债表",
            "report_date": "20260331",
            "date_description": "2026一季报",
            "date_type": 1,
            "item_field": "CURFDS",
            "item_name": "货币资金",
            "item_value": 13254301120.0,
            "item_display_type": 2,
            "item_display": "小类",
            "item_precision": "f2",
            "item_group_no": 1,
            "item_source": "fzb",
            "item_yoy": -0.18663,
            "data_source": "定期报告",
            "is_audit": "未审计",
            "publish_date": "20260428",
            "currency": "CNY",
            "report_type": "合并期末",
            "update_time": 1777285085,
        },
    ]

    result = request_interface(
        "stock_financial_report_sina",
        params={"stock": "sh600600", "symbol": "资产负债表", "limit": 1, "item_limit": 2},
        fields=["instrument_id", "report_date", "item_name", "item_value"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "600600.SH", "report_date": "20260331", "item_name": "流动资产", "item_value": None},
        {"instrument_id": "600600.SH", "report_date": "20260331", "item_name": "货币资金", "item_value": 13254301120.0},
    ]


def test_sina_adapter_normalizes_stock_lhb_detail_daily_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_lhb_detail_daily_sina", {"date": "20240222", "limit": 1})

    assert rows == [
        {
            "trade_date": "20240222",
            "rank": 1,
            "instrument_id": "000017.SZ",
            "symbol": "000017",
            "exchange": "SZSE",
            "name": "深中华A",
            "close": 11.68,
            "metric_value": 15.88,
            "volume_10k_shares": 15353.7649,
            "amount_10k_yuan": 182261.0964,
            "indicator": "振幅值达15%的证券",
        }
    ]

    result = request_interface(
        "stock_lhb_detail_daily_sina",
        params={"date": "20240222", "limit": 1},
        fields=["instrument_id", "indicator", "metric_value"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "000017.SZ", "indicator": "振幅值达15%的证券", "metric_value": 15.88}
    ]


def test_sina_adapter_normalizes_stock_lhb_ggtj_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_lhb_ggtj_sina", {"symbol": "5", "page": 1, "limit": 1})

    assert rows == [
        {
            "recent_days": 5,
            "page": 1,
            "rank": 1,
            "instrument_id": "002407.SZ",
            "symbol": "002407",
            "exchange": "SZSE",
            "name": "多氟多",
            "list_count": 4,
            "buy_amount_10k_yuan": 1334069.58,
            "sell_amount_10k_yuan": 901157.03,
            "net_amount_10k_yuan": 432912.55,
            "buy_seat_count": 9,
            "sell_seat_count": 10,
        }
    ]

    result = request_interface(
        "stock_lhb_ggtj_sina",
        params={"symbol": "5", "page": 1, "limit": 1},
        fields=["instrument_id", "list_count", "net_amount_10k_yuan"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "002407.SZ", "list_count": 4, "net_amount_10k_yuan": 432912.55}
    ]


def test_sina_adapter_normalizes_stock_lhb_jgmx_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_lhb_jgmx_sina", {"page": 1, "limit": 1})

    assert rows == [
        {
            "page": 1,
            "rank": 1,
            "instrument_id": "688578.SH",
            "symbol": "688578",
            "exchange": "SSE",
            "name": "艾力斯",
            "trade_date": "20260703",
            "institution_buy_amount_10k_yuan": 44085.1,
            "institution_sell_amount_10k_yuan": 0.0,
            "trade_type": None,
        }
    ]

    result = request_interface(
        "stock_lhb_jgmx_sina",
        params={"page": 1, "limit": 1},
        fields=["instrument_id", "trade_date", "institution_buy_amount_10k_yuan"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "instrument_id": "688578.SH",
            "trade_date": "20260703",
            "institution_buy_amount_10k_yuan": 44085.1,
        }
    ]


def test_sina_adapter_normalizes_stock_lhb_jgzz_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_lhb_jgzz_sina", {"symbol": "5", "page": 1, "limit": 1})

    assert rows == [
        {
            "recent_days": 5,
            "page": 1,
            "rank": 1,
            "instrument_id": "000725.SZ",
            "symbol": "000725",
            "exchange": "SZSE",
            "name": "京东方A",
            "buy_amount_10k_yuan": 721552.5,
            "buy_count": 4,
            "sell_amount_10k_yuan": 457528.1,
            "sell_count": 3,
            "net_amount_10k_yuan": 264024.4,
        }
    ]

    result = request_interface(
        "stock_lhb_jgzz_sina",
        params={"symbol": "5", "page": 1, "limit": 1},
        fields=["instrument_id", "buy_count", "net_amount_10k_yuan"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "000725.SZ", "buy_count": 4, "net_amount_10k_yuan": 264024.4}
    ]


def test_sina_adapter_normalizes_stock_lhb_yytj_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_lhb_yytj_sina", {"symbol": "5", "page": 1, "limit": 1})

    assert rows == [
        {
            "recent_days": 5,
            "page": 1,
            "rank": 1,
            "brokerage_name": "招商证券股份有限公司深圳建安路证券营业部",
            "list_count": 1,
            "buy_amount_10k_yuan": 0.0,
            "buy_seat_count": 0,
            "sell_amount_10k_yuan": 15.12,
            "sell_seat_count": 1,
            "top_buy_stocks": "国华退",
        }
    ]

    result = request_interface(
        "stock_lhb_yytj_sina",
        params={"symbol": "5", "page": 1, "limit": 1},
        fields=["brokerage_name", "sell_amount_10k_yuan", "top_buy_stocks"],
        adapter=adapter,
    )
    assert result.records == [
        {
            "brokerage_name": "招商证券股份有限公司深圳建安路证券营业部",
            "sell_amount_10k_yuan": 15.12,
            "top_buy_stocks": "国华退",
        }
    ]


def test_sina_adapter_normalizes_stock_restricted_release_queue_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("stock_restricted_release_queue_sina", {"symbol": "600000.SH", "limit": 1})

    assert rows == [
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "sina_symbol": "sh600000",
            "exchange": "SSE",
            "name": "浦发银行",
            "release_date": "20200904",
            "release_shares_10k": 124831.65,
            "release_market_value_100m_yuan": 127.0786,
            "batch_no": 10,
            "announcement_date": "20170906",
        }
    ]

    result = request_interface(
        "stock_restricted_release_queue_sina",
        params={"symbol": "sh600000", "limit": 1},
        fields=["instrument_id", "release_date", "release_shares_10k"],
        adapter=adapter,
    )
    assert result.records == [
        {"instrument_id": "600000.SH", "release_date": "20200904", "release_shares_10k": 124831.65}
    ]


def test_sina_adapter_normalizes_option_sse_expire_day_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_sse_expire_day_sina", {"symbol": "50ETF", "trade_date": "202607"})

    assert rows == [
        {
            "underlying": "510050",
            "underlying_name": "50ETF",
            "expire_month": "202607",
            "expire_date": "20260722",
            "remainder_days": 17,
            "cate_id": "510050C2607",
            "source_category": "50ETF",
            "underlying_source_name": "华夏上证50ETF",
            "underlying_sina_symbol": "s_sh510050",
            "exchange": "SSE",
        }
    ]

    result = request_interface(
        "option_sse_expire_day_sina",
        params={"symbol": "510050", "trade_date": "202607"},
        fields=["underlying", "expire_date", "remainder_days"],
        adapter=adapter,
    )
    assert result.records == [
        {"underlying": "510050", "expire_date": "20260722", "remainder_days": 17}
    ]


def test_sina_adapter_normalizes_option_sse_greeks_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_sse_greeks_sina", {"symbol": "10011799"})

    assert rows == [
        {
            "option_code": "10011799",
            "sina_symbol": "CON_OP_10011799",
            "contract_name": "50ETF购7月2700",
            "volume": 84.0,
            "delta": 0.9983,
            "gamma": 0.0448,
            "theta": -0.114,
            "vega": 0.0036,
            "implied_volatility": 0.001,
            "high": 0.3407,
            "low": 0.2928,
            "trading_code": "510050C2607M02700",
            "exercise_price": 2.7,
            "latest_price": 0.3267,
            "theoretical_value": 0.3284,
            "main_contract_flag": "M",
        }
    ]

    result = request_interface(
        "option_sse_greeks_sina",
        params={"symbol": "CON_OP_10011799"},
        fields=["option_code", "delta", "implied_volatility"],
        adapter=adapter,
    )
    assert result.records == [
        {"option_code": "10011799", "delta": 0.9983, "implied_volatility": 0.001}
    ]


def test_sina_adapter_normalizes_option_sse_list_and_gateway_fields():
    adapter = SinaRequestAdapter(opener=SinaOpener())

    rows = adapter.request("option_sse_list_sina", {"symbol": "50ETF", "limit": 2})

    assert rows == [
        {
            "sequence": 1,
            "underlying": "510050",
            "underlying_name": "50ETF",
            "expire_month": "202607",
            "cate_id": "510050C2607",
            "exchange": "SSE",
        },
        {
            "sequence": 2,
            "underlying": "510050",
            "underlying_name": "50ETF",
            "expire_month": "202608",
            "cate_id": "510050C2607",
            "exchange": "SSE",
        },
    ]

    result = request_interface(
        "option_sse_list_sina",
        params={"symbol": "510050", "limit": 1},
        fields=["underlying", "expire_month"],
        adapter=adapter,
    )
    assert result.records == [
        {"underlying": "510050", "expire_month": "202607"}
    ]


def test_external_source_catalog_and_gateway_registration():
    names = {
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
        "fund_report_industry_allocation_cninfo",
        "fund_report_stock_cninfo",
        "stock_cg_equity_mortgage_cninfo",
        "stock_cg_guarantee_cninfo",
        "stock_cg_lawsuit_cninfo",
        "stock_hold_management_detail_cninfo",
        "stock_industry_change_cninfo",
        "stock_industry_pe_ratio_cninfo",
        "stock_rank_forecast_cninfo",
        "bond_corporate_issue_cninfo",
        "bond_cov_issue_cninfo",
        "bond_cov_stock_issue_cninfo",
        "bond_local_government_issue_cninfo",
        "bond_treasure_issue_cninfo",
        "stock_zh_a_spot_tx",
        "stock_zh_a_hist_tx",
        "stock_zh_a_tick_tx_js",
        "stock_zh_index_daily_tx",
        "get_tx_start_year",
        "tencent_realtime_snapshot",
        "bond_cb_profile_sina",
        "bond_cb_summary_sina",
        "bond_gb_us_sina",
        "bond_gb_zh_sina",
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
        "index_us_stock_sina",
        "index_stock_cons_sina",
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
        "option_sse_codes_sina",
        "option_sse_daily_sina",
        "option_sse_expire_day_sina",
        "option_sse_greeks_sina",
        "option_sse_list_sina",
        "option_sse_minute_sina",
        "option_sse_spot_price_sina",
        "option_sse_underlying_spot_price_sina",
        "stock_financial_report_sina",
        "stock_classify_sina",
        "stock_lhb_detail_daily_sina",
        "stock_lhb_ggtj_sina",
        "stock_lhb_jgmx_sina",
        "stock_lhb_jgzz_sina",
        "stock_lhb_yytj_sina",
        "stock_restricted_release_queue_sina",
        "stock_hk_index_daily_sina",
        "stock_hk_index_spot_sina",
        "stock_info_global_sina",
        "stock_intraday_sina",
        "stock_esg_hz_sina",
        "stock_esg_msci_sina",
        "stock_esg_rate_sina",
        "stock_esg_rft_sina",
        "stock_esg_zd_sina",
        "stock_zh_index_spot_sina",
        "tool_trade_date_hist_sina",
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
    for name in names:
        interface = get_request_interface(name)
        assert interface.name == name
        assert interface.source_code in {"cninfo", "tencent", "eastmoney", "cls", "kph", "sina"}
        assert interface.request_mode == "source_request"

    adapter = TencentRequestAdapter(opener=TencentOpener())
    result = request_interface(
        "stock_zh_a_spot_tx",
        params={"limit": 1},
        fields=["instrument_id", "last_price"],
        adapter=adapter,
    )
    assert result.records == [{"instrument_id": "688808.SH", "last_price": 79.45}]
    assert result.meta["source"] == "tencent"


def test_external_adapters_validate_params():
    with pytest.raises(SourceRequestValidationError, match="code is required"):
        CninfoRequestAdapter(opener=CninfoOpener()).request("cninfo_announcements", {})
    with pytest.raises(SourceAdapterNotFound, match="sina_financial_statement"):
        SinaRequestAdapter(opener=SinaOpener()).request(
            "sina_financial_statement",
            {"code": "000001.SZ", "statement_type": "bad"},
        )
    with pytest.raises(SourceRequestValidationError, match="sort_type"):
        TencentRequestAdapter(opener=TencentOpener()).request(
            "stock_zh_a_spot_tx",
            {"sort_type": "bad"},
        )
