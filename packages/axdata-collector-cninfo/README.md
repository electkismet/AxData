# axdata-collector-cninfo

巨潮采集器(AxData collector-only plugin),由 `scripts/generate_collector_suite.py`
从接口目录自动生成,共 32 个采集器。

## 安装

```powershell
.\.venv\Scripts\python -m pip install -e packages\axdata-collector-cninfo
```

安装后在 Web 控制台「插件管理」启用 `axdata.collector.cninfo`,
再到「采集」页为任意采集器创建任务。
