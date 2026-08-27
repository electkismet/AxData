# axdata-collector-kph

开盘红采集器(AxData collector-only plugin),由 `scripts/generate_collector_suite.py`
从接口目录自动生成,共 9 个采集器。

## 安装

```powershell
.\.venv\Scripts\python -m pip install -e packages\axdata-collector-kph
```

安装后在 Web 控制台「插件管理」启用 `axdata.collector.kph`,
再到「采集」页为任意采集器创建任务。
