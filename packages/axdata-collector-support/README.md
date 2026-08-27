# axdata-collector-support

AxData 内置 Web 数据源采集器插件的共享通用 runner。

提供 `make_runner(source_code)`,为各数据源插件包生成符合 Collector Runner
契约的运行入口:直接实例化 `axdata_core.adapters.{source}.provider_bridge`
的源适配器,返回 records + meta,写盘、质量检查、进度、运行历史全部交给
AxData Collector Runner。

支持日期型采集器(声明 `quality.date_field == "trade_date"`)的
`start_date` / `end_date` 区间补采。

本项目是一个普通依赖库,不注册任何插件入口点。
