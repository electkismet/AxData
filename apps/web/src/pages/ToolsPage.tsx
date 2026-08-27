import { useEffect, useMemo, useRef, useState } from "react";
import { AlertCircle, CheckCircle2, Code2, Database, Download, ExternalLink, Eye, EyeOff, FileArchive, Key as KeyIcon, Loader2, PlayCircle, Power, RefreshCw, RotateCcw, Save, ServerCog, Settings, Shield, Trash2, Upload } from "lucide-react";
import type { ApiTokenRecord, AxpInstallResult, AxpPreview, CatalogItem, CollectorPluginGroup, CollectorStatus, GuidanceFields, HealthPayload, InfoPage, IngestionPlan, InstalledPlugin, LocalDiagnosticCheck, LocalDiagnosticsPayload, PluginAbilityFilter, ProviderStatus, RuntimeConfig, TableRow } from "../types";
import { CodeBlock, DataTable, InfoPageView, Metric } from "../components/common";
import { apiFetch } from "../api";
import { readLocalRuntimeDraft, saveLocalApiPort, saveLocalRuntimeDraft } from "../config";
import { compactCollectorTaskTitle, formatAuthState } from "../utils";
import { InterfaceNotice } from "./DataInterfacesPage";

const DOWNLOAD_CONFIG_STORAGE_PREFIX = "axdata.collector.config.";
const COLLECTOR_RECENT_RUN_LIMIT = 3;

type CollectorBuilderReference = {
  paramsNote: string;
  params: TableRow[];
  fields: TableRow[];
};

const COLLECTOR_PARAM_PLACEHOLDERS: Record<string, string> = {
  code: "000001.SZ",
  adjust: "none",
  anchor_date: "YYYYMMDD 或 YYYY-MM-DD"
};

const OUTPUT_FORMAT_LABELS: Record<string, string> = {
  csv: "CSV",
  duckdb: "DuckDB",
  jsonl: "JSONL",
  parquet: "Parquet"
};

const COLLECTOR_FORMAT_ORDER = ["parquet", "csv", "duckdb", "jsonl"];

const COLLECTOR_RESOURCE_GROUP_LABELS: Record<string, string> = {
  default: "默认通道",
  "tdx.quote": "通达信行情通道",
  "tdx.f10": "通达信 F10 通道",
  "tdx.file": "通达信文件通道",
  "exchange.http": "交易所公开接口",
  "sample.local": "本地样例通道"
};

const RUN_STAGE_LABELS: Record<string, string> = {
  queued: "进入队列",
  started: "开始执行",
  params_resolved: "确认参数",
  request_planned: "准备采集",
  provider_resolved: "定位采集器",
  downloaded: "读取数据",
  written: "写入文件",
  quality_checked: "质量检查",
  metadata_recorded: "保存记录",
  finished: "完成",
  failed: "失败",
  cancelled: "取消",
  skipped: "跳过"
};

const RUN_EVENT_MESSAGE_LABELS: Record<string, string> = {
  "Collector run queued.": "任务已进入采集队列。",
  "Collector run started.": "本次采集已开始执行。",
  "Collector run params resolved.": "已确认本次运行使用的参数。",
  "Collector run request planned.": "已准备好本次采集请求。",
  "Collector runner entry resolved.": "已找到独立采集器入口。",
  "Collector provider and downloader profile resolved.": "已找到采集插件和下载配置。",
  "Collector runner produced records.": "采集器已产出数据记录。",
  "Source data downloaded.": "源端数据已读取完成。",
  "Collector output files written.": "采集结果已写入本地文件。",
  "Collector run metadata recorded.": "运行历史已保存。",
  "Collector run finished.": "本次采集已完成。"
};

const QUALITY_FIELD_LABELS: Record<string, string> = {
  quality_status: "质量状态",
  row_count: "行数检查",
  row_count_value: "记录数",
  required_columns_present: "必要字段",
  missing_required_columns: "缺失字段",
  unexpected_columns: "额外字段",
  duplicate_key_count: "重复主键",
  duplicate_rows_dropped: "去重行数",
  rows_written: "写入行数",
  rows_after: "写后总行数",
  write_mode: "写入方式",
  write_primary_key: "写入主键",
  date_field: "日期字段",
  date_range: "日期范围",
  actual_date_count: "实际日期数",
  calendar_check_applied: "交易日历检查",
  calendar_coverage_status: "交易日历覆盖",
  calendar_date_range: "日历覆盖范围",
  expected_trading_day_count: "应有交易日",
  actual_trading_day_count: "实际交易日",
  date_gap_count: "缺失交易日",
  missing_trading_dates: "缺失日期样例",
  extra_non_trading_dates: "非交易日样例",
  calendar_next_action: "处理建议",
  suspension_coverage_status: "停牌解释",
  quality_warnings: "质量提醒",
  quality_errors: "质量错误",
  price_ohlc_anomaly_count: "价格异常",
  negative_volume_count: "负成交量",
  negative_amount_count: "负成交额"
};

const QUALITY_DISPLAY_ORDER = [
  "quality_status",
  "row_count_value",
  "row_count",
  "required_columns_present",
  "missing_required_columns",
  "duplicate_key_count",
  "write_mode",
  "write_primary_key",
  "date_field",
  "date_range",
  "actual_date_count",
  "calendar_check_applied",
  "calendar_coverage_status",
  "expected_trading_day_count",
  "actual_trading_day_count",
  "date_gap_count",
  "missing_trading_dates",
  "extra_non_trading_dates",
  "calendar_next_action",
  "quality_warnings",
  "quality_errors",
  "price_ohlc_anomaly_count",
  "negative_volume_count",
  "negative_amount_count"
];

function IngestionPage({ activeDoc, plan }: { activeDoc: CatalogItem; plan: IngestionPlan }) {
  const ActiveIcon = activeDoc.icon;

  return (
    <>
      <section className="doc-hero single">
        <div className="doc-heading">
          <div className="endpoint-line">
            <span className="section-eyebrow">采集配置</span>
            <code>{plan.task}</code>
            <span className={plan.status === "enabled" ? "ready-badge" : "ready-badge example"}>
              <CheckCircle2 size={15} />
              {plan.status === "enabled" ? "已启用" : "示例配置"}
            </span>
          </div>

          <div className="title-row">
            <span className="title-icon">
              <ActiveIcon size={26} />
            </span>
            <h1>{activeDoc.title}</h1>
          </div>
          <p>{plan.summary}</p>

          <dl className="summary-list">
            <div>
              <dt>任务</dt>
              <dd>{plan.task}</dd>
            </div>
            <div>
              <dt>调度</dt>
              <dd>{plan.schedule}</dd>
            </div>
            <div>
              <dt>时区</dt>
              <dd>{plan.timezone}</dd>
            </div>
            <div>
              <dt>模式</dt>
              <dd>{plan.mode}</dd>
            </div>
            <div>
              <dt>写入</dt>
              <dd>{plan.writePolicy}</dd>
            </div>
            <div>
              <dt>输出</dt>
              <dd>{plan.output}</dd>
            </div>
          </dl>
        </div>
      </section>

      <section className="doc-section">
        <div className="section-title">
          <Database size={20} />
          <h2>口径输入</h2>
        </div>
        <DataTable columns={["接口口径", "覆盖范围", "写入位置", "格式"]} rows={plan.inputs} />
      </section>

      <section className="doc-section">
        <div className="section-title">
          <RefreshCw size={20} />
          <h2>处理流程</h2>
        </div>
        <DataTable columns={["层级", "处理", "说明"]} rows={plan.pipeline} />
      </section>

      <section className="doc-section code-grid">
        <div>
          <div className="section-title">
            <Settings size={20} />
            <h2>任务配置</h2>
          </div>
          <DataTable columns={["配置项", "值", "说明"]} rows={plan.settings} />
        </div>
        <div>
          <div className="section-title">
            <Code2 size={20} />
            <h2>配置文件</h2>
          </div>
          <CodeBlock code={plan.code} language="yaml" />
        </div>
      </section>
    </>
  );
}

type DownloadResult = {
  interface_name?: string;
  job_id?: string;
  status?: string;
  progress_pct?: number;
  message?: string;
  eta_ms?: number | null;
  progress_current?: number | null;
  progress_total?: number | null;
  progress_unit?: string | null;
  progress_label?: string | null;
  trigger_type?: string;
  provider_id?: string | null;
  resource_group?: string | null;
  resource_requested?: number;
  resource_granted?: number;
  resource_used?: number;
  resource_limit?: number;
  resource_wait_reason?: string | null;
  queue_position?: number;
  output_lock_key?: string | null;
  output_wait_reason?: string | null;
  output_queue_position?: number;
  run_signature?: string;
  schedule_run_key?: string | null;
  duplicate_job_id?: string | null;
  skip_reason?: string | null;
  backoff_until?: string | null;
  result?: DownloadResult | null;
  error?: { message?: string } | null;
  row_count?: number;
  snapshot_date?: string;
  snapshot_date_source?: string;
  collection_time?: string;
  file_stem?: string;
  connection_mode?: string;
  connection_count?: number;
  concurrency?: Record<string, number | string>;
  output_formats?: string[];
  output_paths?: Record<string, string>;
  output_path?: string;
  log_path?: string;
  duration_ms?: number;
  duration_breakdown_ms?: Record<string, number>;
  source_meta?: Record<string, unknown>;
};

type DownloaderConcurrencyProfile = {
  mode?: string;
  mode_editable?: boolean;
  runtime_source_server_count?: number;
  default_source_server_count?: number;
  source_server_count_editable?: boolean;
  max_source_server_count?: number;
  default_connections_per_server?: number;
  connections_per_server_editable?: boolean;
  max_connections_per_server?: number;
  default_max_concurrent_tasks?: number;
  max_concurrent_tasks_editable?: boolean;
  max_max_concurrent_tasks?: number;
  default_batch_size?: number;
  batch_size_editable?: boolean;
  max_batch_size?: number;
  default_request_interval_ms?: number;
  request_interval_ms_editable?: boolean;
  min_request_interval_ms?: number;
  max_request_interval_ms?: number;
  default_retry_count?: number;
  retry_count_editable?: boolean;
  max_retry_count?: number;
  default_timeout_ms?: number;
  timeout_ms_editable?: boolean;
  min_timeout_ms?: number;
  max_timeout_ms?: number;
  default_connection_count?: number;
  connection_count_editable?: boolean;
  max_connection_count?: number;
  description?: string;
};

type DownloaderProfile = {
  interface_name: string;
  default_connection_mode?: string;
  default_connection_count?: number;
  connection_count_editable?: boolean;
  max_connection_count?: number;
  concurrency?: DownloaderConcurrencyProfile;
  params?: TableRow[] | null;
  supported_formats?: string[];
};

type ConcurrencySettings = {
  mode: string;
  sourceServerCount: number;
  connectionsPerServer: number;
  maxConcurrentTasks: number;
  batchSize: number;
  requestIntervalMs: number;
  retryCount: number;
  timeoutMs: number;
};

type CollectorExecutionSettings = {
  mode: "recommended" | "custom";
  sourceServerCount: number;
  connectionsPerServer: number;
  maxConcurrentTasks: number;
};

const CONCURRENCY_PRESET_OPTIONS: [string, string][] = [
  ["low", "低"],
  ["medium", "中（推荐）"],
  ["high", "高"],
  ["custom", "自定义"]
];

const F10_TOPIC_WORKER_INTERFACES = new Set(["stock_limit_ladder_tdx", "stock_theme_strength_rank_tdx"]);
const DEFAULT_F10_TOPIC_WORKERS = 6;
const DEFAULT_F10_TOPIC_REFILL_WORKERS = 6;
const DEFAULT_F10_TOPIC_REFILL_ROUNDS = 1;

type CollectionMode = "manual" | "auto";

type CollectionSchedule = {
  frequency: "daily" | "trade_day" | "weekly";
  time: string;
  weekday: string;
};

const DEFAULT_COLLECTION_SCHEDULE: CollectionSchedule = {
  frequency: "trade_day",
  time: "18:00",
  weekday: "1"
};

type DownloadScheduleStatus = {
  interface_name?: string;
  enabled?: boolean;
  frequency?: CollectionSchedule["frequency"];
  time?: string;
  weekday?: string;
  timezone?: string;
  last_run_key?: string | null;
  last_job_id?: string | null;
  last_checked_at?: string | null;
  updated_at?: string | null;
};

type TradeCalendarCacheStatus = {
  path?: string;
  exists?: boolean;
  row_count?: number;
  open_count?: number;
  start_date?: string | null;
  end_date?: string | null;
  today?: string;
  today_is_open?: boolean | null;
  covers_today?: boolean;
  updated_at?: string | null;
  requested_start_date?: string;
  requested_end_date?: string;
  fetched_ranges?: Array<{ start_date: string; end_date: string }>;
  fetched_row_count?: number;
};

type TradeCalendarMaintenanceConfig = {
  enabled?: boolean;
  time?: string;
  past_days?: number;
  future_days?: number;
  recheck_past_days?: number;
  path?: string;
};

type TdxServerKind = "quote" | "extended";

type TdxServerRow = {
  name: string;
  host: string;
  port: number | string;
  enabled: boolean;
  priority?: number;
  latency_ms?: number | null;
  last_checked_at?: string | null;
  last_error?: string | null;
  address?: string;
};

type TdxServerStatus = {
  kind: TdxServerKind;
  source: string;
  config_path: string;
  server_count: number;
  enabled_count: number;
  servers: TdxServerRow[];
};

type TdxServerProbeSchedule = {
  enabled: boolean;
  frequency: "daily" | "weekly";
  time: string;
  weekday: string;
  timeout: number;
  max_workers: number;
  kinds: TdxServerKind[];
  last_run_key?: string | null;
  last_checked_at?: string | null;
  last_result?: Record<string, unknown> | null;
  updated_at?: string | null;
};

type CollectorTaskStatus = {
  task_id: string;
  collector_name: string;
  name?: string;
  template_id?: string | null;
  created_by?: string | null;
  enabled: boolean;
  trigger_type: string;
  interval_seconds?: number | null;
  daily_time?: string | null;
  interface_name?: string | null;
  provider_id?: string | null;
  downloader_profile?: string | null;
  resource_group?: string;
  expected_layer?: string | null;
  schedule_hint?: string | null;
  write_mode?: string | null;
  partition_by?: string[];
  primary_key?: string[];
  date_field?: string | null;
  required_datasets?: string[];
  required_plugin?: string | null;
  dependency?: { provider_id?: string | null; required?: boolean; required_datasets?: string[] } | null;
  dependency_status?: string | null;
  dependency_message?: string | null;
  dependency_errors?: Array<Record<string, unknown>>;
  category?: string | null;
  tags?: string[];
  max_retries?: number | null;
  backoff_seconds?: number | null;
  next_run_at?: string | null;
  backoff_until?: string | null;
  last_run_id?: string | null;
  last_status?: string | null;
  last_success_at?: string | null;
  last_failure_at?: string | null;
  last_error?: string | null;
  last_error_summary?: string | null;
  queue_status?: string | null;
  can_run_now?: boolean;
  blocked_reason?: string | null;
  updated_at?: string | null;
} & GuidanceFields;

type CollectorRunStatus = {
  run_id: string;
  task_id: string;
  collector_name: string;
  trigger_type: string;
  status: string;
  provider_id?: string | null;
  downloader_profile?: string | null;
  resource_group?: string;
  output_paths?: Record<string, string>;
  started_at?: string | null;
  finished_at?: string | null;
  duration_ms?: number | null;
  error?: string | null;
  skip_reason?: string | null;
  retry_count?: number | null;
  records_read?: number | null;
  rows_written?: number | null;
  write_mode?: string | null;
  partition_by?: string[];
  primary_key?: string[];
  rows_before?: number | null;
  rows_after?: number | null;
  duplicate_rows_dropped?: number | null;
  partitions_touched?: string[];
  params_override?: Record<string, unknown>;
  next_run_at?: string | null;
  backoff_until?: string | null;
  result?: Record<string, unknown>;
  quality?: Record<string, unknown>;
  events?: CollectorRunEvent[];
  stage_timings?: Record<string, number | null>;
  error_category?: string | null;
  error_summary?: string | null;
  metadata?: Record<string, unknown>;
  queue_status?: string | null;
  can_run_now?: boolean;
  blocked_reason?: string | null;
  last_error_summary?: string | null;
} & GuidanceFields;

type CollectorRunEvent = {
  event_id?: string;
  sequence?: number;
  timestamp?: string;
  stage?: string;
  level?: string;
  message?: string;
  category?: string | null;
  details?: Record<string, unknown>;
};

type CollectorSchedulerStatus = {
  task_count: number;
  enabled_task_count: number;
  run_count: number;
  active_run_count: number;
  active_runs: CollectorRunStatus[];
  latest_runs: Record<string, CollectorRunStatus>;
};

type CollectorTaskTemplateStatus = {
  template_id: string;
  title: string;
  description: string;
  collector_name: string;
  interface_name: string;
  provider: string;
  default_params?: Record<string, unknown>;
  fields?: string[] | null;
  formats?: string[] | null;
  trigger_type: string;
  interval_seconds?: number | null;
  daily_time?: string | null;
  schedule_hint?: string | null;
  resource_group?: string;
  expected_layer?: string;
  write_mode?: string | null;
  partition_by?: string[];
  primary_key?: string[];
  date_field?: string | null;
  required_datasets?: string[];
  dependency?: Record<string, unknown>;
  safety_limits?: Record<string, unknown>;
  required_plugin?: string | null;
  enabled_by_default?: boolean;
  system_default?: boolean;
  category?: string | null;
  tags?: string[];
  task_id?: string | null;
  available: boolean;
  unavailable_reason?: string | null;
  next_action?: string | null;
  action_command?: string | null;
};

type LocalDiagnosticsState = {
  status: LocalDiagnosticsPayload | null;
  doctor: LocalDiagnosticsPayload | null;
  error: string | null;
};

export function ToolsPage({
  activeCollector,
  activePluginId,
  activePluginGroup,
  apiBase,
  health,
  runtimeCatalogItems
}: {
  activeCollector: CollectorStatus | null;
  activePluginId: string;
  activePluginGroup: CollectorPluginGroup | null;
  apiBase: string;
  health: HealthPayload | null;
  runtimeCatalogItems: CatalogItem[];
}) {
  const [collectorTasks, setCollectorTasks] = useState<CollectorTaskStatus[]>([]);
  const [collectorTemplates, setCollectorTemplates] = useState<CollectorTaskTemplateStatus[]>([]);
  const [collectorStatus, setCollectorStatus] = useState<CollectorSchedulerStatus | null>(null);
  const [collectorRuns, setCollectorRuns] = useState<CollectorRunStatus[]>([]);
  const [collectorBusyTaskId, setCollectorBusyTaskId] = useState<string | null>(null);
  const [collectorError, setCollectorError] = useState<string | null>(null);
  const [collectorMessage, setCollectorMessage] = useState<string | null>(null);
  const [isCollectorInputOpen, setIsCollectorInputOpen] = useState(true);
  const [isCollectorOutputOpen, setIsCollectorOutputOpen] = useState(true);
  const [collectorTaskName, setCollectorTaskName] = useState("");
  const [collectorStorageFormats, setCollectorStorageFormats] = useState<string[]>(["parquet"]);
  const [collectorParamValues, setCollectorParamValues] = useState<Record<string, string>>({});
  const [collectorExecutionSettings, setCollectorExecutionSettings] = useState<CollectorExecutionSettings>(
    defaultCollectorExecutionSettings(null)
  );
  const collectorName = activeCollector ? collectorCatalogId(activeCollector) : "";

  async function refreshCollectorTasks() {
    const [tasks, templates, status, runs] = await Promise.all([
      fetchCollectorTasks(apiBase),
      fetchCollectorTaskTemplates(apiBase),
      fetchCollectorStatus(apiBase),
      fetchCollectorRuns(apiBase)
    ]);
    setCollectorTasks(tasks);
    setCollectorTemplates(templates);
    setCollectorStatus(status);
    setCollectorRuns(runs);
  }

  useEffect(() => {
    let isActive = true;
    async function refreshOnce() {
      try {
        const [tasks, templates, status, runs] = await Promise.all([
          fetchCollectorTasks(apiBase),
          fetchCollectorTaskTemplates(apiBase),
          fetchCollectorStatus(apiBase),
          fetchCollectorRuns(apiBase)
        ]);
        if (!isActive) {
          return;
        }
        setCollectorTasks(tasks);
        setCollectorTemplates(templates);
        setCollectorStatus(status);
        setCollectorRuns(runs);
        setCollectorError(null);
      } catch (error) {
        if (isActive) {
          setCollectorError(error instanceof Error ? error.message : "Collector task 状态读取失败");
        }
      }
    }
    refreshOnce();
    const intervalId = window.setInterval(refreshOnce, 5000);
    return () => {
      isActive = false;
      window.clearInterval(intervalId);
    };
  }, [apiBase]);

  const relatedTasks = useMemo(() => {
    if (collectorName) {
      return collectorTasks.filter((task) => task.collector_name === collectorName);
    }
    if (activePluginId) {
      return collectorTasks.filter(
        (task) =>
          task.required_plugin === activePluginId ||
          task.provider_id === activePluginId ||
          String(task.dependency?.provider_id || "") === activePluginId
      );
    }
    return [];
  }, [activePluginId, collectorName, collectorTasks]);
  const relatedTaskIds = useMemo(() => new Set(relatedTasks.map((task) => task.task_id)), [relatedTasks]);
  const relatedTemplates = useMemo(
    () => collectorTemplates.filter((template) => template.collector_name === collectorName),
    [collectorName, collectorTemplates]
  );
  const relatedRuns = useMemo(
    () => collectorRuns.filter((run) => run.collector_name === collectorName || relatedTaskIds.has(run.task_id)),
    [collectorName, collectorRuns, relatedTaskIds]
  );
  const scopedSchedulerStatus = useMemo(
    () => scopeCollectorSchedulerStatus(collectorStatus, relatedTasks, relatedRuns),
    [collectorStatus, relatedRuns, relatedTasks]
  );
  const latestRun = relatedRuns[0] ?? null;
  const collectorBuilderReference = useMemo(
    () => (activeCollector && isCollectorTaskBuilderEnabled(activeCollector) ? buildCollectorBuilderReference(activeCollector, runtimeCatalogItems) : null),
    [activeCollector, runtimeCatalogItems]
  );
  const collectorBuilderSupportedFormats = useMemo(
    () => (activeCollector ? collectorSupportedFormats(activeCollector) : ["parquet"]),
    [activeCollector]
  );
  const collectorBuilderFormatOptions = useMemo(
    () => collectorStorageFormatOptions(collectorBuilderSupportedFormats),
    [collectorBuilderSupportedFormats]
  );
  const collectorStoragePaths = activeCollector
    ? collectorOutputPathPreviews(activeCollector, health, collectorStorageFormats)
    : [];
  const isCreatingCollectorTask = collectorBusyTaskId === "collector:create";

  useEffect(() => {
    const normalized = normalizeCollectorStorageFormats(collectorStorageFormats, collectorBuilderSupportedFormats);
    if (normalized.join("|") !== collectorStorageFormats.join("|")) {
      setCollectorStorageFormats(normalized);
    }
  }, [collectorBuilderSupportedFormats, collectorStorageFormats]);

  useEffect(() => {
    if (!activeCollector || !collectorBuilderReference) {
      return;
    }
    setCollectorTaskName(defaultCollectorTaskName(activeCollector));
    setCollectorStorageFormats(normalizeCollectorStorageFormats(["parquet"], collectorSupportedFormats(activeCollector)));
    setCollectorParamValues(defaultCollectorParamValues(activeCollector));
    setCollectorExecutionSettings(defaultCollectorExecutionSettings(activeCollector));
  }, [activeCollector, collectorBuilderReference, collectorName]);

  function toggleCollectorStorageFormat(format: string, checked: boolean) {
    const normalizedFormat = format.toLowerCase();
    if (normalizedFormat === "parquet" && !checked) {
      return;
    }
    const next = checked
      ? [...collectorStorageFormats, normalizedFormat]
      : collectorStorageFormats.filter((item) => item !== normalizedFormat);
    setCollectorStorageFormats(normalizeCollectorStorageFormats(next, collectorBuilderSupportedFormats));
  }

  function updateCollectorParamValue(name: string, value: string) {
    setCollectorParamValues((current) => ({ ...current, [name]: value }));
  }

  function updateCollectorExecutionSetting(name: keyof CollectorExecutionSettings, value: number | string) {
    if (!activeCollector) {
      return;
    }
    setCollectorExecutionSettings((current) =>
      normalizeCollectorExecutionSettings({ ...current, [name]: value }, activeCollector)
    );
  }

  async function createCollectorBuilderTaskAction() {
    if (!activeCollector || !collectorBuilderReference) {
      return;
    }
    const params = normalizeCollectorTaskParams(activeCollector, collectorParamValues, collectorBuilderReference.params);
    const missingRequiredParam = collectorBuilderReference.params.find(([name, , required]) => {
      return required === "是" && !String(params[name] ?? "").trim();
    });
    if (missingRequiredParam) {
      setCollectorError(`请输入 ${missingRequiredParam[0]} 参数。`);
      return;
    }
    const taskName = collectorTaskName.trim() || defaultCollectorTaskName(activeCollector);
    setCollectorBusyTaskId("collector:create");
    setCollectorError(null);
    setCollectorMessage(null);
    try {
      const task = await createCollectorTask(apiBase, {
        collectorName: collectorName,
        enabled: true,
        fields: collectorBuilderReference.fields.map(([field]) => field),
        formats: collectorStorageFormats,
        name: taskName,
        params,
        execution: collectorExecutionSettings,
        triggerType: "manual"
      });
      await refreshCollectorTasks();
      setCollectorMessage(`${task.name || task.task_id} 已创建`);
      window.setTimeout(() => setCollectorMessage(null), 3000);
    } catch (error) {
      setCollectorError(error instanceof Error ? error.message : "Collector task 创建失败");
    } finally {
      setCollectorBusyTaskId(null);
    }
  }

  async function runCollectorTaskAction(task: CollectorTaskStatus) {
    setCollectorBusyTaskId(task.task_id);
    setCollectorError(null);
    setCollectorMessage(null);
    try {
      const run = await runCollectorTask(apiBase, task.task_id);
      await refreshCollectorTasks();
      setCollectorMessage(`${task.name || task.collector_name} 已提交运行：${run.run_id}`);
      window.setTimeout(() => setCollectorMessage(null), 3000);
    } catch (error) {
      setCollectorError(error instanceof Error ? error.message : "Collector task 提交失败");
    } finally {
      setCollectorBusyTaskId(null);
    }
  }

  async function createCollectorTemplateAction(template: CollectorTaskTemplateStatus) {
    setCollectorBusyTaskId(`template:${template.template_id}`);
    setCollectorError(null);
    setCollectorMessage(null);
    try {
      const task = await createCollectorTaskFromTemplate(apiBase, template.template_id);
      await refreshCollectorTasks();
      setCollectorMessage(`${task.name || task.collector_name} 已从模板创建`);
      window.setTimeout(() => setCollectorMessage(null), 3000);
    } catch (error) {
      setCollectorError(error instanceof Error ? error.message : "模板创建任务失败");
    } finally {
      setCollectorBusyTaskId(null);
    }
  }

  async function backfillCollectorTaskAction(
    task: CollectorTaskStatus,
    request: { start: string; end: string; symbol?: string; limit?: number }
  ) {
    setCollectorBusyTaskId(`backfill:${task.task_id}`);
    setCollectorError(null);
    setCollectorMessage(null);
    try {
      const run = await backfillCollectorTask(apiBase, task.task_id, request);
      await refreshCollectorTasks();
      setCollectorMessage(`${task.name || task.collector_name} 已提交补采：${run.run_id}`);
      window.setTimeout(() => setCollectorMessage(null), 3000);
    } catch (error) {
      setCollectorError(error instanceof Error ? error.message : "Collector task 补采失败");
    } finally {
      setCollectorBusyTaskId(null);
    }
  }

  async function setCollectorTaskEnabledAction(task: CollectorTaskStatus, enabled: boolean) {
    setCollectorBusyTaskId(task.task_id);
    setCollectorError(null);
    setCollectorMessage(null);
    try {
      await setCollectorTaskEnabled(apiBase, task.task_id, enabled);
      await refreshCollectorTasks();
      setCollectorMessage(`${task.name || task.collector_name} 已${enabled ? "启用" : "禁用"}`);
      window.setTimeout(() => setCollectorMessage(null), 3000);
    } catch (error) {
      setCollectorError(error instanceof Error ? error.message : "Collector task 更新失败");
    } finally {
      setCollectorBusyTaskId(null);
    }
  }

  async function setCollectorTaskScheduleAction(task: CollectorTaskStatus, triggerType: "manual" | "daily") {
    setCollectorBusyTaskId(`schedule:${task.task_id}`);
    setCollectorError(null);
    setCollectorMessage(null);
    try {
      await updateCollectorTask(apiBase, task.task_id, triggerType === "daily"
        ? {
            daily_time: task.daily_time || "18:00",
            enabled: false,
            interval_seconds: null,
            trigger_type: "daily"
          }
        : {
            daily_time: null,
            enabled: false,
            interval_seconds: null,
            trigger_type: "manual"
          });
      await refreshCollectorTasks();
      setCollectorMessage(`${task.name || task.collector_name} 已切换为${triggerType === "daily" ? "定时采集" : "手动采集"}`);
      window.setTimeout(() => setCollectorMessage(null), 3000);
    } catch (error) {
      setCollectorError(error instanceof Error ? error.message : "Collector task 调度更新失败");
    } finally {
      setCollectorBusyTaskId(null);
    }
  }

  async function setCollectorTaskDailyTimeAction(task: CollectorTaskStatus, dailyTime: string) {
    setCollectorBusyTaskId(`time:${task.task_id}`);
    setCollectorError(null);
    setCollectorMessage(null);
    try {
      await updateCollectorTask(apiBase, task.task_id, {
        daily_time: dailyTime,
        trigger_type: "daily"
      });
      await refreshCollectorTasks();
      setCollectorMessage(`${task.name || task.collector_name} 定时时间已更新`);
      window.setTimeout(() => setCollectorMessage(null), 3000);
    } catch (error) {
      setCollectorError(error instanceof Error ? error.message : "Collector task 定时时间更新失败");
    } finally {
      setCollectorBusyTaskId(null);
    }
  }

  async function deleteCollectorTaskAction(task: CollectorTaskStatus) {
    const taskName = task.name || task.collector_name || task.task_id;
    if (!window.confirm(`删除任务「${taskName}」？已采集的数据和历史记录不会删除。`)) {
      return;
    }
    setCollectorBusyTaskId(`delete:${task.task_id}`);
    setCollectorError(null);
    setCollectorMessage(null);
    try {
      await deleteCollectorTask(apiBase, task.task_id);
      await refreshCollectorTasks();
      setCollectorMessage(`${taskName} 已删除`);
      window.setTimeout(() => setCollectorMessage(null), 3000);
    } catch (error) {
      setCollectorError(error instanceof Error ? error.message : "Collector task 删除失败");
    } finally {
      setCollectorBusyTaskId(null);
    }
  }

  async function deleteCollectorRunAction(run: CollectorRunStatus) {
    const runId = run.run_id || "";
    if (!runId) {
      return;
    }
    if (!window.confirm(`删除这条采集记录「${runId}」？\n\n只会删除这条运行历史，不会删除任务配置，也不会删除已采集的数据文件。`)) {
      return;
    }
    setCollectorBusyTaskId(`run-delete:${runId}`);
    setCollectorError(null);
    setCollectorMessage(null);
    try {
      await deleteCollectorRun(apiBase, runId);
      await refreshCollectorTasks();
      setCollectorMessage(`采集记录 ${runId} 已删除`);
      window.setTimeout(() => setCollectorMessage(null), 3000);
    } catch (error) {
      setCollectorError(error instanceof Error ? error.message : "采集记录删除失败");
    } finally {
      setCollectorBusyTaskId(null);
    }
  }

  if (!activeCollector) {
    return (
      <section className="doc-section">
        <div className="section-title">
          <Download size={20} />
          <h2>采集任务目录</h2>
        </div>
        <p className="empty-state">
          当前没有可用采集任务{activePluginId ? `：${activePluginId}` : ""}。若采集插件被禁用、卸载或缺失，已有任务和 run history 仍会保留在调度状态里。
        </p>
        {relatedTasks.length > 0 ? (
          <DataTable
            columns={["采集任务", "依赖状态", "最近运行"]}
            rows={relatedTasks.map((task) => [
              task.name || task.task_id,
              formatTaskDependency(task),
              task.last_run_id || "暂无"
            ])}
          />
        ) : null}
      </section>
    );
  }

  return (
    <>
      <section className={`doc-hero single ${collectorBuilderReference ? "collector-detail-hero compact" : ""}`}>
        <div className="doc-heading">
          <div className="endpoint-line">
            <span className="section-eyebrow">采集器</span>
            <span className={`ready-badge ${activeCollector.is_legacy ? "example" : ""}`}>
              <CheckCircle2 size={15} />
              {activeCollector.is_legacy ? "旧式采集任务，待迁移" : "独立采集任务"}
            </span>
          </div>
          <div className="title-row">
            <span className="title-icon">
              <Download size={26} />
            </span>
            <h1>{compactCollectorTaskTitle(activeCollector.display_name_zh, collectorName)}</h1>
          </div>
          <p>{collectorHeroDescription(activeCollector)}</p>
          <dl className="collector-hero-meta">
            {collectorHeroRows(activeCollector, activePluginGroup, relatedTasks, latestRun, health).map((row) => (
              <div key={row[0]}>
                <dt>{row[0]}</dt>
                <dd>{row[1]}</dd>
              </div>
            ))}
          </dl>
        </div>
      </section>

      {collectorBuilderReference ? (
        <>
          <section className="doc-section collector-task-builder">
            <div className="section-title collector-builder-title">
              <CheckCircle2 size={20} />
              <h2>创建采集任务</h2>
            </div>
            <div className="collector-builder-body">
              <section className="collector-basic-section" id="collector-basic-params">
                <div className="section-title">
                  <Settings size={20} />
                  <h2>基本参数</h2>
                </div>
                <div className="collector-basic-grid">
                  <label className="download-field">
                    <span>任务名称</span>
                    <input
                      onChange={(event) => setCollectorTaskName(event.target.value)}
                      type="text"
                      value={collectorTaskName}
                    />
                  </label>
                  <label className="download-field">
                    <span>存储格式</span>
                    <div className="collector-format-checklist">
                      {collectorBuilderFormatOptions.map((format) => {
                        const isDeclared = collectorFormatIsDeclared(format, collectorBuilderSupportedFormats);
                        return (
                          <label
                            key={format}
                            className={`checkbox-chip collector-format-chip ${isDeclared ? "" : "unsupported"}`}
                          >
                            <input
                              checked={isDeclared && collectorStorageFormats.includes(format)}
                              disabled={format === "parquet" || !isDeclared}
                              onChange={(event) => toggleCollectorStorageFormat(format, event.target.checked)}
                              type="checkbox"
                            />
                            <span>
                              {collectorFormatLabel(format)}
                              {!isDeclared ? <em>采集器未声明</em> : null}
                            </span>
                          </label>
                        );
                      })}
                    </div>
                  </label>
                  <div className="collector-storage-preview">
                    <span>预计存储路径</span>
                    <div className="collector-storage-path-list">
                      {collectorStoragePaths.map((item) => (
                        <code key={`${item.format}:${item.path}`}>
                          {item.label}: {item.path}
                        </code>
                      ))}
                    </div>
                  </div>
                  <label className="download-field">
                    <span>运行档位</span>
                    <select
                      onChange={(event) => updateCollectorExecutionSetting("mode", event.target.value)}
                      value={collectorExecutionSettings.mode}
                    >
                      <option value="recommended">推荐</option>
                      <option value="custom">自定义</option>
                    </select>
                  </label>
                  <div className="collector-storage-preview collector-execution-preview">
                    <span>执行配置</span>
                    <code>{collectorExecutionSummary(collectorExecutionSettings)}</code>
                  </div>
                  {collectorExecutionSettings.mode === "custom" ? (
                    <>
                      <label className="download-field">
                        <span>服务器数</span>
                        <input
                          min={collectorExecutionMin(activeCollector, "source_server_count")}
                          max={collectorExecutionMax(activeCollector, "source_server_count")}
                          onChange={(event) => updateCollectorExecutionSetting("sourceServerCount", event.target.value)}
                          type="number"
                          value={collectorExecutionSettings.sourceServerCount}
                        />
                      </label>
                      <label className="download-field">
                        <span>长连接/服务器</span>
                        <input
                          min={collectorExecutionMin(activeCollector, "connections_per_server")}
                          max={collectorExecutionMax(activeCollector, "connections_per_server")}
                          onChange={(event) => updateCollectorExecutionSetting("connectionsPerServer", event.target.value)}
                          type="number"
                          value={collectorExecutionSettings.connectionsPerServer}
                        />
                      </label>
                      <label className="download-field">
                        <span>总并发</span>
                        <input
                          readOnly
                          type="text"
                          value={collectorExecutionSettings.maxConcurrentTasks}
                        />
                      </label>
                    </>
                  ) : null}
                </div>
              </section>

              <details
                className="doc-section collector-reference-details"
                id="collector-input-params"
                onToggle={(event) => setIsCollectorInputOpen(event.currentTarget.open)}
                open={isCollectorInputOpen}
              >
                <summary>
                  <div className="section-title">
                    <Code2 size={20} />
                    <h2>输入参数</h2>
                  </div>
                </summary>
                <div className="collector-reference-body">
                  {collectorBuilderReference.paramsNote ? <p className="guide-note">{collectorBuilderReference.paramsNote}</p> : null}
                  <div className="table-wrap collector-param-table-wrap">
                    <table className="collector-param-table">
                      <thead>
                        <tr>
                          <th>名称</th>
                          <th>类型</th>
                          <th>必填</th>
                          <th>描述</th>
                        </tr>
                      </thead>
                      <tbody>
                        {collectorBuilderReference.params.map(([name, type, required, description]) => (
                          <tr key={name}>
                            <td>
                              <CollectorParamEditor
                                collectorName={collectorName}
                                name={name}
                                onChange={(value) => updateCollectorParamValue(name, value)}
                                value={collectorParamValues[name] ?? ""}
                              />
                            </td>
                            <td>{type}</td>
                            <td>{required}</td>
                            <td>{description}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </details>

              <details
                className="doc-section collector-reference-details"
                id="collector-output-params"
                onToggle={(event) => setIsCollectorOutputOpen(event.currentTarget.open)}
                open={isCollectorOutputOpen}
              >
                <summary>
                  <div className="section-title">
                    <Database size={20} />
                    <h2>输出参数</h2>
                  </div>
                </summary>
                <div className="collector-reference-body">
                  <DataTable columns={["字段", "类型", "说明"]} rows={collectorBuilderReference.fields} />
                </div>
              </details>

              <div className="collector-task-create-panel">
                <button
                  className="primary-action"
                  disabled={isCreatingCollectorTask}
                  onClick={createCollectorBuilderTaskAction}
                  type="button"
                >
                  {isCreatingCollectorTask ? <Loader2 className="spin" size={15} /> : <CheckCircle2 size={15} />}
                  创建任务
                </button>
              </div>
            </div>
          </section>
        </>
      ) : null}

      <CollectorTasksPanel
        busyTaskId={collectorBusyTaskId}
        error={collectorError}
        health={health}
        message={collectorMessage}
        onBackfill={backfillCollectorTaskAction}
        onCreateTemplate={createCollectorTemplateAction}
        onDelete={deleteCollectorTaskAction}
        onDeleteRun={deleteCollectorRunAction}
        onEnableChange={setCollectorTaskEnabledAction}
        onRun={runCollectorTaskAction}
        onDailyTimeChange={setCollectorTaskDailyTimeAction}
        onScheduleChange={setCollectorTaskScheduleAction}
        recentRuns={relatedRuns}
        schedulerStatus={scopedSchedulerStatus}
        showCompactTasks={Boolean(collectorBuilderReference)}
        showOperationSummary={!collectorBuilderReference}
        showTemplates={!collectorBuilderReference}
        tasks={relatedTasks}
        templates={relatedTemplates}
        runtimeCatalogItems={runtimeCatalogItems}
      />

      <section className="doc-section collector-advanced-section">
        <details className="collector-page-advanced">
          <summary>
            <Settings size={17} />
            <span>高级信息</span>
          </summary>
          <div className="collector-page-advanced-body">
            <div className="code-grid">
              <div>
                <div className="section-title">
                  <Database size={20} />
                  <h2>采集器信息</h2>
                </div>
                <DataTable columns={["项目", "值", "说明"]} rows={collectorContractRows(activeCollector)} />
              </div>
              <div>
                <div className="section-title">
                  <FileArchive size={20} />
                  <h2>入库规则</h2>
                </div>
                <DataTable columns={["项目", "值", "说明"]} rows={collectorOutputRows(activeCollector)} />
              </div>
            </div>
            <div>
              <div className="section-title">
                <Shield size={20} />
                <h2>质量规则</h2>
              </div>
              <DataTable columns={["规则", "值", "说明"]} rows={collectorQualityRows(activeCollector)} />
            </div>
            <div>
              <div className="section-title">
                <RefreshCw size={20} />
                <h2>最近一次记录明细</h2>
              </div>
              <DataTable columns={["项目", "值", "说明"]} rows={collectorLatestRunRows(latestRun)} />
            </div>
          </div>
        </details>
      </section>
    </>
  );
}

function collectorCatalogId(collector: CollectorStatus) {
  return collector.collector_id || collector.collector_name || collector.name;
}

function isCollectorTaskBuilderEnabled(collector: CollectorStatus) {
  const collectorName = collectorCatalogId(collector);
  return Boolean(collector.runner_entry) && collectorName.endsWith(".snapshot");
}

function CollectorParamEditor({
  collectorName,
  name,
  onChange,
  value
}: {
  collectorName: string;
  name: string;
  onChange: (value: string) => void;
  value: string;
}) {
  const options = collectorParamOptions(collectorName, name);
  if (options.length > 0) {
    return (
      <select aria-label={`${name} 参数`} onChange={(event) => onChange(event.target.value)} value={value}>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    );
  }
  return (
    <input
      aria-label={`${name} 参数`}
      onChange={(event) => onChange(event.target.value)}
      placeholder={collectorParamPlaceholder(name)}
      type="text"
      value={value}
    />
  );
}

function buildCollectorBuilderReference(collector: CollectorStatus, runtimeCatalogItems: CatalogItem[]): CollectorBuilderReference {
  const sourceCatalogItem = sourceCatalogItemForCollector(collector, runtimeCatalogItems);
  if (sourceCatalogItem) {
    return {
      paramsNote: sourceCatalogItem.paramsNote || collectorParamsNote(collector),
      params: sourceCatalogItem.params,
      fields: catalogOutputFieldRows(sourceCatalogItem)
    };
  }
  return {
    paramsNote: collectorParamsNote(collector),
    params: collectorParamRows(collector),
    fields: collectorOutputFieldRows(collector)
  };
}

function sourceCatalogItemForCollector(collector: CollectorStatus, runtimeCatalogItems: CatalogItem[]): CatalogItem | null {
  const interfaceName = collectorInterfaceName(collector);
  if (!interfaceName) {
    return null;
  }
  return runtimeCatalogItems.find((item) => item.name === interfaceName || item.id === interfaceName) ?? null;
}

function collectorInterfaceName(collector: CollectorStatus) {
  const explicit = stringListValue(collector.interfaces)[0];
  if (explicit) {
    return explicit;
  }
  const rawName = collector.collector_name || collector.name || collector.collector_id || "";
  const parts = rawName.split(".").filter(Boolean);
  if (parts.length >= 3) {
    return parts[1];
  }
  if (rawName.endsWith(".snapshot")) {
    return rawName.slice(0, -".snapshot".length).split(".").pop() || "";
  }
  return "";
}

function catalogOutputFieldRows(item: CatalogItem): TableRow[] {
  const hasChineseNameColumn = item.fieldColumns?.length === 4;
  if (!hasChineseNameColumn) {
    return item.fields.map((row) => [row[0], row[1], row[2] ?? ""]);
  }
  return item.fields.map((row) => {
    const [field, label, type, description] = row;
    return [
      field,
      type,
      label && description ? `${label}；${description}` : description || label || ""
    ];
  });
}

function defaultCollectorTaskName(collector: CollectorStatus) {
  return compactCollectorTaskTitle(collector.display_name_zh, collectorCatalogId(collector));
}

function defaultCollectorParamValues(collector: CollectorStatus) {
  const defaults = collector.default_params ?? {};
  const values: Record<string, string> = {};
  for (const [key, value] of Object.entries(defaults)) {
    values[key] = formatParamValue(value);
  }
  return values;
}

function defaultCollectorExecutionSettings(collector: CollectorStatus | null): CollectorExecutionSettings {
  const config = collectorExecutionConfig(collector);
  const maxConcurrentTasks = collectorExecutionConnectionCount({
    sourceServerCount: config.defaults.sourceServerCount,
    connectionsPerServer: config.defaults.connectionsPerServer
  });
  return {
    mode: "recommended",
    sourceServerCount: config.defaults.sourceServerCount,
    connectionsPerServer: config.defaults.connectionsPerServer,
    maxConcurrentTasks
  };
}

function normalizeCollectorExecutionSettings(
  value: Partial<CollectorExecutionSettings>,
  collector: CollectorStatus
): CollectorExecutionSettings {
  const defaults = defaultCollectorExecutionSettings(collector);
  const mode = value.mode === "custom" ? "custom" : "recommended";
  const sourceServerCount = clampNumber(
    value.sourceServerCount,
    collectorExecutionMin(collector, "source_server_count"),
    collectorExecutionMax(collector, "source_server_count"),
    defaults.sourceServerCount
  );
  const connectionsPerServer = clampNumber(
    value.connectionsPerServer,
    collectorExecutionMin(collector, "connections_per_server"),
    collectorExecutionMax(collector, "connections_per_server"),
    defaults.connectionsPerServer
  );
  return {
    mode,
    sourceServerCount,
    connectionsPerServer,
    maxConcurrentTasks: collectorExecutionConnectionCount({ sourceServerCount, connectionsPerServer })
  };
}

function collectorExecutionConfig(collector: CollectorStatus | null) {
  const collectorConfig = isRecord(collector?.collector_config_schema) ? collector?.collector_config_schema : {};
  const pluginConfig = isRecord(collector?.config_schema) ? collector?.config_schema : {};
  const execution = isRecord(collectorConfig.execution)
    ? collectorConfig.execution
    : isRecord(pluginConfig.execution)
      ? pluginConfig.execution
      : {};
  const defaults = isRecord(execution?.defaults) ? execution.defaults : {};
  const sourceServerCount = Math.max(1, numberOr(defaults.source_server_count, 1));
  const connectionsPerServer = Math.max(1, numberOr(defaults.connections_per_server, 1));
  const maxConcurrentTasks = Math.max(
    1,
    numberOr(defaults.max_concurrent_tasks, sourceServerCount * connectionsPerServer)
  );
  return {
    defaults: {
      sourceServerCount,
      connectionsPerServer,
      maxConcurrentTasks
    }
  };
}

function collectorExecutionLimit(
  collector: CollectorStatus | null,
  key: "source_server_count" | "connections_per_server" | "max_concurrent_tasks",
  side: "min" | "max"
) {
  const collectorConfig = isRecord(collector?.collector_config_schema) ? collector?.collector_config_schema : {};
  const pluginConfig = isRecord(collector?.config_schema) ? collector?.config_schema : {};
  const execution = isRecord(collectorConfig.execution)
    ? collectorConfig.execution
    : isRecord(pluginConfig.execution)
      ? pluginConfig.execution
      : {};
  const limits = isRecord(execution?.limits) ? execution.limits : {};
  const limit = isRecord(limits[key]) ? limits[key] : {};
  const fallbackDefaults = collectorExecutionConfig(collector).defaults;
  const fallback =
    key === "source_server_count"
      ? fallbackDefaults.sourceServerCount
      : key === "connections_per_server"
        ? fallbackDefaults.connectionsPerServer
        : fallbackDefaults.maxConcurrentTasks;
  return side === "min" ? Math.max(1, numberOr(limit.min, 1)) : Math.max(fallback, numberOr(limit.max, fallback));
}

function collectorExecutionMin(
  collector: CollectorStatus | null,
  key: "source_server_count" | "connections_per_server" | "max_concurrent_tasks"
) {
  return collectorExecutionLimit(collector, key, "min");
}

function collectorExecutionMax(
  collector: CollectorStatus | null,
  key: "source_server_count" | "connections_per_server" | "max_concurrent_tasks"
) {
  return collectorExecutionLimit(collector, key, "max");
}

function collectorExecutionSummary(settings: CollectorExecutionSettings) {
  return `${settings.sourceServerCount} 台服务器 × ${settings.connectionsPerServer} 条长连接 / 总并发 ${collectorExecutionConnectionCount(settings)}`;
}

function collectorExecutionConnectionCount(settings: Pick<CollectorExecutionSettings, "sourceServerCount" | "connectionsPerServer">) {
  return Math.max(1, settings.sourceServerCount * settings.connectionsPerServer);
}

function clampNumber(value: unknown, min: number, max: number, fallback: number) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.min(Math.max(Math.round(numeric), min), max);
}

function normalizeCollectorTaskParams(
  collector: CollectorStatus,
  values: Record<string, string>,
  paramRows: TableRow[] = []
) {
  const defaults = collector.default_params ?? {};
  const catalogTypes = new Map(paramRows.map((row) => [row[0], String(row[1] ?? "").toLowerCase()]));
  const params: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(values)) {
    const text = String(value ?? "").trim();
    if (!text) {
      continue;
    }
    const defaultValue = defaults[key];
    const catalogType = catalogTypes.get(key) || "";
    if (typeof defaultValue === "boolean" || catalogType.includes("boolean")) {
      params[key] = text === "true";
    } else if (typeof defaultValue === "number" || catalogType.includes("integer") || catalogType.includes("number")) {
      const numeric = Number(text);
      params[key] = Number.isFinite(numeric) ? numeric : text;
    } else {
      params[key] = text;
    }
  }
  return params;
}

function collectorParamRows(collector: CollectorStatus): TableRow[] {
  const defaults = collector.default_params ?? {};
  return Object.keys(defaults).map((name) => [
    name,
    collectorParamType(defaults[name]),
    collectorParamRequired(collector, name),
    collectorFallbackParamDescription(collector, name)
  ]);
}

function collectorOutputFieldRows(collector: CollectorStatus): TableRow[] {
  const output = collector.output ?? {};
  const fields = stringListValue(output.expected_columns).length > 0
    ? stringListValue(output.expected_columns)
    : stringListValue(output.required_columns);
  return fields.map((field) => [
    field,
    collectorFieldType(field),
    collectorFallbackFieldDescription(collector, field)
  ]);
}

function collectorParamsNote(collector: CollectorStatus) {
  if (collector.default_params && Object.keys(collector.default_params).length > 0) {
    return "不改参数时按当前默认值创建任务；需要扩大范围时再手动调整。";
  }
  return "该采集器没有默认输入参数。";
}

function collectorFallbackParamDescription(collector: CollectorStatus, name: string) {
  const defaultValue = collector.default_params?.[name];
  const defaultText = defaultValue === undefined ? "" : `默认值：${formatUserValue(defaultValue)}。`;
  return `${defaultText}来自 CollectorSpec.default_params；如需更完整口径，应由关联接口 runtime catalog 或采集器 manifest 声明。`;
}

function collectorFallbackFieldDescription(collector: CollectorStatus, field: string) {
  const output = collector.output ?? {};
  const isRequired = stringListValue(output.required_columns).includes(field);
  const source = stringListValue(output.expected_columns).includes(field)
    ? "output.expected_columns"
    : isRequired
      ? "output.required_columns"
      : "CollectorSpec.output";
  return `${isRequired ? "必需字段。" : "输出字段。"}来自 ${source}；字段中文口径应优先由关联接口 runtime catalog 或采集器 manifest 声明。`;
}

function collectorParamOptions(collectorName: string, name: string) {
  if (name === "adjust") {
    return [
      { value: "none", label: "none" },
      { value: "qfq", label: "qfq" },
      { value: "hfq", label: "hfq" },
      { value: "fixed_qfq", label: "fixed_qfq" }
    ];
  }
  if (name === "include_touched" || name === "refresh_stats") {
    return [
      { value: "false", label: "否" },
      { value: "true", label: "是" }
    ];
  }
  if (name === "topic_type") {
    return [{ value: "theme", label: "theme" }];
  }
  return [];
}

function collectorParamPlaceholder(name: string) {
  return COLLECTOR_PARAM_PLACEHOLDERS[name] ?? name;
}

function collectorParamType(value: unknown) {
  if (typeof value === "boolean") {
    return "boolean";
  }
  if (typeof value === "number") {
    return "number";
  }
  if (Array.isArray(value)) {
    return "list";
  }
  return "string";
}

function collectorParamRequired(collector: CollectorStatus, name: string) {
  const collectorName = collector.collector_name || collector.name || collector.collector_id || "";
  if (collectorName.includes("stock_kline_daily_tdx") && name === "code") {
    return "是";
  }
  return "否";
}

function collectorFieldType(field: string) {
  if (field.includes("date") || field.includes("time")) {
    return "date/string";
  }
  if (
    ["open", "high", "low", "close", "pre_close", "last_price", "change", "pct_chg", "vol"].includes(field) ||
    field.includes("price") ||
    field.includes("amount") ||
    field.includes("volume") ||
    field.includes("share") ||
    field.includes("score") ||
    field.includes("count") ||
    field.includes("ratio") ||
    field === "adj_factor"
  ) {
    return "number";
  }
  if (field.startsWith("is_") || field.startsWith("include_")) {
    return "boolean";
  }
  return "string";
}

function formatParamValue(value: unknown) {
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).join(",");
  }
  return value == null ? "" : String(value);
}

function collectorHeroRows(
  collector: CollectorStatus,
  pluginGroup: CollectorPluginGroup | null,
  relatedTasks: CollectorTaskStatus[],
  latestRun: CollectorRunStatus | null,
  health: HealthPayload | null
): TableRow[] {
  const commonRows: TableRow[] = [
    ["采集插件", pluginGroup?.title || collector.collector_plugin_id || collector.provider_id || ""],
    ["插件状态", formatProviderStatus(pluginGroup?.status || collector.collector_plugin_status || collector.plugin_status || "")],
    ["采集任务", formatCollectorTaskSummary(relatedTasks)],
    ["覆盖数据", formatCollectorDatasetCoverage(collector, pluginGroup)]
  ];
  if (isCollectorTaskBuilderEnabled(collector)) {
    return [
      ...commonRows,
      ["默认格式", formatCollectorDefaultFormats(collector)],
      ["最近结果", latestRun ? `${formatCollectorRunStatus(latestRun.status)} / ${formatDuration(latestRun.duration_ms)}` : "暂无记录"]
    ];
  }
  return [
    ...commonRows,
    ["默认目录", defaultCollectorOutputDir(collector, health)],
    ["最近结果", latestRun ? `${formatCollectorRunStatus(latestRun.status)} / ${formatDuration(latestRun.duration_ms)}` : "暂无记录"]
  ];
}

function collectorHeroDescription(collector: CollectorStatus) {
  return collector.description || "采集任务负责把源端或本地数据生产为 AxData 本地资产；接口临时调用不会自动保存。";
}

function formatCollectorDefaultFormats(collector: CollectorStatus) {
  const formats = stringListValue(collector.output?.formats);
  if (formats.length === 0) {
    return "未声明";
  }
  return formats.map((format) => OUTPUT_FORMAT_LABELS[format] ?? format).join(", ");
}

function formatCollectorDatasetCoverage(
  collector: CollectorStatus,
  pluginGroup: CollectorPluginGroup | null
) {
  if (collector.dataset_id) {
    return collector.dataset_id;
  }
  const count = pluginGroup?.datasetIds.length ?? 0;
  return count > 0 ? `${count} 个数据集` : "未声明";
}

function defaultCollectorOutputDir(collector: CollectorStatus, health: HealthPayload | null) {
  const root = health?.data_root ?? "data";
  return joinDisplayPath(root, collectorDefaultOutputPathParts(collector));
}

function collectorDefaultOutputPathParts(collector: CollectorStatus) {
  const output = collector.output ?? {};
  const declaredParts = stringListValue(output.default_output_path_parts);
  if (declaredParts.length > 0) {
    return declaredParts;
  }
  const layer = String(output.output_layer || output.layer || collector.expected_layer || "snapshot");
  const defaultDirName = String(output.default_dir_name || collector.dataset_id || collectorCatalogId(collector));
  return [layer, defaultDirName];
}

function collectorSupportedFormats(collector: CollectorStatus) {
  const output = collector.output ?? {};
  const formats = stringListValue(output.supported_formats).length > 0
    ? stringListValue(output.supported_formats)
    : stringListValue(output.formats);
  const normalized = formats.length > 0 ? formats.map((format) => format.toLowerCase()) : ["parquet"];
  const visible = normalized.filter((format) => ["parquet", "csv", "duckdb"].includes(format));
  return normalizeCollectorStorageFormats(visible, visible);
}

function collectorStorageFormatOptions(supportedFormats: string[]) {
  const optionSet = new Set(["parquet", "duckdb", ...supportedFormats.map((format) => format.toLowerCase())]);
  return COLLECTOR_FORMAT_ORDER.filter((format) => optionSet.has(format) && ["parquet", "csv", "duckdb"].includes(format));
}

function collectorFormatIsDeclared(format: string, supportedFormats: string[]) {
  const normalized = format.toLowerCase();
  return normalized === "parquet" || supportedFormats.map((item) => item.toLowerCase()).includes(normalized);
}

function normalizeCollectorStorageFormats(selected: string[], supported: string[]) {
  const supportedSet = new Set(supported.map((format) => format.toLowerCase()));
  const normalized = Array.from(
    new Set(["parquet", ...selected.map((format) => format.toLowerCase())])
  ).filter((format) => supportedSet.size === 0 || supportedSet.has(format));
  if (!normalized.includes("parquet")) {
    normalized.unshift("parquet");
  }
  return normalized.sort((left, right) => {
    const leftIndex = COLLECTOR_FORMAT_ORDER.indexOf(left);
    const rightIndex = COLLECTOR_FORMAT_ORDER.indexOf(right);
    return (leftIndex >= 0 ? leftIndex : COLLECTOR_FORMAT_ORDER.length) -
      (rightIndex >= 0 ? rightIndex : COLLECTOR_FORMAT_ORDER.length);
  });
}

function collectorFormatLabel(format: string) {
  if (format === "parquet") {
    return "Parquet 标准主格式";
  }
  if (format === "csv") {
    return "CSV 额外导出";
  }
  if (format === "duckdb") {
    return "DuckDB 查询缓存";
  }
  return OUTPUT_FORMAT_LABELS[format] ?? format;
}

function collectorOutputPathPreviews(
  collector: CollectorStatus,
  health: HealthPayload | null,
  formats: string[]
) {
  const basePath = defaultCollectorOutputDir(collector, health);
  return formats.map((format) => ({
    format,
    label: OUTPUT_FORMAT_LABELS[format] ?? format,
    path: joinDisplayPath(basePath, [format]),
  }));
}

function stringListValue(value: unknown) {
  return Array.isArray(value)
    ? value.map((item) => String(item).trim()).filter(Boolean)
    : typeof value === "string" && value.trim()
      ? [value.trim()]
      : [];
}

function joinDisplayPath(root: string, parts: string[]) {
  const cleanRoot = root.replace(/[\\/]+$/, "");
  const cleanParts = parts
    .map((part) => String(part).replace(/^[\\/]+|[\\/]+$/g, ""))
    .filter(Boolean);
  return [cleanRoot, ...cleanParts].filter(Boolean).join("\\");
}

function collectorContractRows(collector: CollectorStatus): TableRow[] {
  return [
    ["collector_id", collectorCatalogId(collector), "采集任务系统 ID，供任务和调度引用。"],
    ["collector_plugin_id", collector.collector_plugin_id || "", "贡献该采集任务的插件 ID。"],
    ["dataset_id", collector.dataset_id || "", "采集结果进入的数据集 ID。"],
    ["runner_entry", collector.runner_entry || "", "新采集器的运行入口；存在时不通过接口/DownloaderProfile 套采集。"],
    ["default_output_dir", defaultCollectorOutputDir(collector, null), "未手动指定输出目录时，采集结果默认写入这里；实际文件还会按格式、分区和运行时间继续分子目录。"],
    ["resource_group", collector.resource_group || "default", "调度器用于排队和限流的资源组。"],
    ["asset_class", collector.asset_class || "", "资产类别。"],
    ["category", collector.category || "", "业务分类。"],
    ["lifecycle_status", collector.lifecycle_status || "", "采集任务生命周期状态。"],
    ["plugin_status", collector.collector_plugin_status || collector.plugin_status || "", "采集器插件当前状态。"],
    ["legacy", collector.is_legacy ? collector.legacy_source || "legacy" : "false", "旧式 Provider manifest 兼容采集器会在这里标记。"],
    ["interfaces", formatList(collector.interfaces), "兼容字段；独立采集器通常为空。"],
    ["required_interfaces", formatList(collector.required_interfaces), "兼容字段；新 runner 路径不依赖接口路由。"],
    ["downloader_profile", collector.downloader_profile || "", "兼容字段；新采集器不把 DownloaderProfile 当身份。"]
  ];
}

function formatCollectorTaskSummary(tasks: CollectorTaskStatus[]) {
  if (tasks.length === 0) {
    return "未创建调度任务";
  }
  const enabledCount = tasks.filter((task) => task.enabled).length;
  return `${enabledCount} / ${tasks.length} 已启用`;
}

function collectorOutputRows(collector: CollectorStatus): TableRow[] {
  const output = collector.output ?? {};
  return [
    ["layer", formatUnknown(output.layer), "目标数据层或快照层。"],
    ["formats", formatUnknown(output.formats), "默认写出格式。"],
    ["default_dir_name", formatUnknown(output.default_dir_name), "默认输出目录名。"],
    ["write_mode", formatUnknown(output.write_mode), "写入策略。"],
    ["primary_key", formatUnknown(output.primary_key), "写入或质量检查主键。"],
    ["date_field", formatUnknown(output.date_field), "日期或时间字段。"],
    ["partition_by", formatUnknown(output.partition_by), "分区字段。"],
    ["file_name_template", formatUnknown(output.file_name_template), "文件名模板。"]
  ];
}

function collectorQualityRows(collector: CollectorStatus): TableRow[] {
  const quality = collector.quality ?? {};
  return [
    ["必填字段", formatUnknown(quality.required_columns), "写入结果必须包含这些字段，缺失会判为质量问题。"],
    ["期望字段", formatUnknown(quality.expected_columns), "用于检查输出字段是否完整，额外字段也会被记录。"],
    ["正数校验", formatUnknown(quality.numeric_positive_columns), "这些数值字段必须大于 0。"],
    ["日期字段", formatUnknown(quality.date_field), "用于日期范围、补采和查询裁剪。"],
    ["交易日历校验", quality.calendar_check ? "开启" : "关闭", "开启后会校验数据日期是否落在本地交易日历内。"],
    ["字段映射", formatUnknown(quality.field_mappings), "源端字段到 AxData 字段的兼容映射。"]
  ];
}

function collectorLatestRunRows(run: CollectorRunStatus | null): TableRow[] {
  if (!run) {
    return [["最近运行", "暂无", "创建任务并运行后，这里会显示 run history。"]];
  }
  const logPath = collectorRunLogPath(run);
  return [
    ["run_id", run.run_id, "最近一次相关 run。"],
    ["task_id", run.task_id, "触发该 run 的任务。"],
    ["状态", formatCollectorRunStatus(run.status), "运行状态。"],
    ["开始", formatNullable(run.started_at), "开始时间。"],
    ["结束", formatNullable(run.finished_at), "结束时间。"],
    ["耗时", formatDuration(run.duration_ms), "总耗时。"],
    ["rows_written", String(run.rows_written ?? run.quality?.row_count_value ?? ""), "写入或质量统计行数。"],
    ["write_mode", run.write_mode || String(run.quality?.write_mode ?? ""), "本次写入策略。"],
    ["quality_status", String(run.quality?.quality_status ?? ""), "质量检查状态。"],
    ["日志路径", logPath || "暂无", "本次 run 的 JSON 日志文件路径。"],
    ["error", run.error_summary || run.error || "", "失败摘要。"]
  ];
}

function collectorRunLogPath(run: CollectorRunStatus | null) {
  const result = run?.result;
  if (!result) {
    return "";
  }
  const directLogPath = result.log_path;
  if (typeof directLogPath === "string") {
    return directLogPath;
  }
  const downloadResult = result.download_result;
  if (downloadResult && typeof downloadResult === "object") {
    const nestedLogPath = (downloadResult as Record<string, unknown>).log_path;
    if (typeof nestedLogPath === "string") {
      return nestedLogPath;
    }
  }
  return "";
}

function scopeCollectorSchedulerStatus(
  status: CollectorSchedulerStatus | null,
  tasks: CollectorTaskStatus[],
  runs: CollectorRunStatus[]
): CollectorSchedulerStatus | null {
  if (!status) {
    return null;
  }
  const taskIds = new Set(tasks.map((task) => task.task_id));
  const activeRuns = (status.active_runs ?? []).filter((run) => taskIds.has(run.task_id));
  const latestRuns: Record<string, CollectorRunStatus> = {};
  for (const task of tasks) {
    const latest = runs.find((run) => run.task_id === task.task_id);
    if (latest) {
      latestRuns[task.task_id] = latest;
    }
  }
  return {
    task_count: tasks.length,
    enabled_task_count: tasks.filter((task) => task.enabled).length,
    run_count: runs.length,
    active_run_count: activeRuns.length,
    active_runs: activeRuns,
    latest_runs: latestRuns
  };
}

function formatUnknown(value: unknown) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).join(", ");
  }
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function formatJsonBlock(value: unknown) {
  return JSON.stringify(value ?? {}, null, 2);
}

function LegacyDownloadToolsPage({
  activeDoc,
  apiBase,
  health
}: {
  activeDoc: CatalogItem;
  apiBase: string;
  health: HealthPayload | null;
}) {
  const ActiveIcon = activeDoc.icon;
  const defaultOutputRoot = health?.data_root ?? "data";
  const paths = useMemo(() => makeDownloadPaths(activeDoc, defaultOutputRoot), [activeDoc, defaultOutputRoot]);
  const [downloadProfile, setDownloadProfile] = useState<DownloaderProfile | null>(null);
  const isCollectable = downloadProfile !== null;
  const collectParamRows = useMemo(
    () => (downloadProfile?.params?.length ? downloadProfile.params : activeDoc.params),
    [activeDoc.params, downloadProfile]
  );
  const defaultCollectParams = useMemo(() => defaultParamsForRows(collectParamRows), [collectParamRows]);
  const baseConcurrencyProfile = useMemo(() => makeConcurrencyProfile(downloadProfile), [downloadProfile]);
  const [tdxQuoteServerStatus, setTdxQuoteServerStatus] = useState<TdxServerStatus | null>(null);
  const concurrencyProfile = useMemo(
    () => withRuntimeSourceServerMax(baseConcurrencyProfile, tdxQuoteServerStatus),
    [baseConcurrencyProfile, tdxQuoteServerStatus]
  );
  const defaultConcurrencySettings = useMemo(
    () => defaultConcurrencyForProfile(concurrencyProfile),
    [concurrencyProfile]
  );
  const [outputDir, setOutputDir] = useState("");
  const [outputDirTouched, setOutputDirTouched] = useState(false);
  const [collectParams, setCollectParams] = useState<Record<string, string>>(defaultCollectParams);
  const [concurrencySettings, setConcurrencySettings] = useState<ConcurrencySettings>(defaultConcurrencySettings);
  const [collectionMode, setCollectionMode] = useState<CollectionMode>("manual");
  const [collectionSchedule, setCollectionSchedule] = useState<CollectionSchedule>(DEFAULT_COLLECTION_SCHEDULE);
  const resolvedConcurrency = useMemo(
    () => normalizeConcurrencySettings(concurrencySettings, concurrencyProfile),
    [concurrencySettings, concurrencyProfile]
  );
  const [selectedFormats, setSelectedFormats] = useState<string[]>(["parquet"]);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);
  const [scheduleMessage, setScheduleMessage] = useState<string | null>(null);
  const [isDownloading, setIsDownloading] = useState(false);
  const [isSavingSchedule, setIsSavingSchedule] = useState(false);
  const [isDisablingSchedule, setIsDisablingSchedule] = useState(false);
  const [scheduleStatus, setScheduleStatus] = useState<DownloadScheduleStatus | null>(null);
  const [downloadResult, setDownloadResult] = useState<DownloadResult | null>(null);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    async function loadDownloaderProfile() {
      try {
        const response = await apiFetch(`${apiBase}/v1/downloaders/${activeDoc.name}`, {
          signal: controller.signal,
          headers: { Accept: "application/json" }
        });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = await response.json();
        setDownloadProfile(payload.data ?? payload);
      } catch {
        if (!controller.signal.aborted) {
          setDownloadProfile(null);
        }
      }
    }

    loadDownloaderProfile();
    return () => controller.abort();
  }, [activeDoc.name, apiBase]);

  useEffect(() => {
    if (!isCollectable || !baseConcurrencyProfile.source_server_count_editable) {
      setTdxQuoteServerStatus(null);
      return;
    }
    let cancelled = false;
    fetchTdxServerStatus(apiBase)
      .then((status) => {
        if (!cancelled) {
          setTdxQuoteServerStatus(status.quote ?? null);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setTdxQuoteServerStatus(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [activeDoc.name, apiBase, baseConcurrencyProfile.source_server_count_editable, isCollectable]);

  useEffect(() => {
    if (!isCollectable) {
      setScheduleStatus(null);
      return;
    }
    const controller = new AbortController();

    fetchDownloadScheduleStatus(apiBase, activeDoc.name, controller.signal)
      .then((status) => {
        setScheduleStatus(status);
        if (status?.frequency || status?.time || status?.weekday) {
          setCollectionSchedule(normalizeCollectionSchedule(status));
        }
      })
      .catch((error) => {
        if (!controller.signal.aborted && error instanceof Error) {
          setScheduleMessage(error.message);
        }
      });
    return () => controller.abort();
  }, [activeDoc.name, apiBase, isCollectable]);

  useEffect(() => {
    const savedConfig = loadSavedDownloadConfig(activeDoc.name);
    if (savedConfig) {
      setOutputDir(savedConfig.outputDir);
      setOutputDirTouched(Boolean(savedConfig.outputDir));
      setCollectParams(filterParamsForRows({ ...defaultCollectParams, ...savedConfig.params }, collectParamRows));
      setConcurrencySettings(normalizeConcurrencySettings(savedConfig.concurrency, concurrencyProfile));
      setSelectedFormats(savedConfig.formats);
      setCollectionMode(savedConfig.collectionMode);
      setCollectionSchedule(normalizeCollectionSchedule(savedConfig.schedule));
      return;
    }
    setCollectParams(defaultCollectParams);
    setConcurrencySettings(defaultConcurrencySettings);
    setCollectionMode("manual");
    setCollectionSchedule(DEFAULT_COLLECTION_SCHEDULE);
    if (!outputDirTouched) {
      setOutputDir("");
    }
  }, [activeDoc.name, concurrencyProfile, defaultCollectParams, defaultConcurrencySettings, outputDirTouched]);

  function saveDownloadConfig(options: { silent?: boolean } = {}) {
    saveSavedDownloadConfig(activeDoc.name, {
      outputDir: outputDir.trim(),
      params: compactParams(filterParamsForRows(collectParams, collectParamRows)),
      concurrency: resolvedConcurrency,
      formats: selectedFormats,
      collectionMode,
      schedule: collectionSchedule
    });
    if (!options.silent) {
      setSaveMessage("配置已保存到当前浏览器");
      window.setTimeout(() => setSaveMessage(null), 2500);
    }
  }

  async function enableScheduleConfig() {
    setIsSavingSchedule(true);
    setScheduleMessage(null);
    try {
      if (collectionSchedule.frequency === "trade_day") {
        const status = await ensureTradeCalendarCache(apiBase);
        if (!status.covers_today) {
          throw new Error("本地交易日历没有覆盖今天，请先刷新交易日历。");
        }
      }
      saveDownloadConfig({ silent: true });
      const response = await apiFetch(`${apiBase}/v1/download/schedules/${activeDoc.name}`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          frequency: collectionSchedule.frequency,
          time: collectionSchedule.time,
          weekday: collectionSchedule.weekday,
          enabled: true,
          params: compactParams(filterParamsForRows(collectParams, collectParamRows)),
          output_dir: outputDir.trim() || undefined,
          formats: selectedFormats,
          connection_mode: "long_connection",
          concurrency_mode: resolvedConcurrency.mode,
          source_server_count: resolvedConcurrency.sourceServerCount,
          connections_per_server: resolvedConcurrency.connectionsPerServer,
          batch_size: resolvedConcurrency.batchSize,
          request_interval_ms: resolvedConcurrency.requestIntervalMs,
          retry_count: resolvedConcurrency.retryCount,
          timeout_ms: resolvedConcurrency.timeoutMs
        })
      });
      const payload = await response.json();
      if (!response.ok || payload?.success === false) {
        const message = payload?.error?.message ?? `HTTP ${response.status}`;
        throw new Error(message);
      }
      setScheduleStatus(payload.data ?? payload);
      setScheduleMessage(`已开启定时采集：${formatCollectionSchedule(collectionSchedule)}`);
      window.setTimeout(() => setScheduleMessage(null), 3500);
    } catch (error) {
      setScheduleMessage(error instanceof Error ? error.message : "定时采集开启失败");
    } finally {
      setIsSavingSchedule(false);
    }
  }

  async function disableScheduleConfig() {
    setIsDisablingSchedule(true);
    setScheduleMessage(null);
    try {
      const response = await apiFetch(`${apiBase}/v1/download/schedules/${activeDoc.name}`, {
        method: "DELETE",
        headers: { Accept: "application/json" }
      });
      const payload = await response.json();
      if (!response.ok || payload?.success === false) {
        const message = payload?.error?.message ?? `HTTP ${response.status}`;
        throw new Error(message);
      }
      setScheduleStatus(payload.data ?? payload);
      setScheduleMessage("已关闭定时采集");
      window.setTimeout(() => setScheduleMessage(null), 2500);
    } catch (error) {
      setScheduleMessage(error instanceof Error ? error.message : "定时采集关闭失败");
    } finally {
      setIsDisablingSchedule(false);
    }
  }

  async function runCollectorWithParams(params: Record<string, string>) {
    setIsDownloading(true);
    setDownloadResult(null);
    setDownloadError(null);
    try {
      const response = await apiFetch(`${apiBase}/v1/download/${activeDoc.name}`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          params,
          async_job: true,
          output_dir: outputDir.trim() || undefined,
          formats: selectedFormats,
          connection_mode: "long_connection",
          concurrency_mode: resolvedConcurrency.mode,
          source_server_count: resolvedConcurrency.sourceServerCount,
          connections_per_server: resolvedConcurrency.connectionsPerServer,
          batch_size: resolvedConcurrency.batchSize,
          request_interval_ms: resolvedConcurrency.requestIntervalMs,
          retry_count: resolvedConcurrency.retryCount,
          timeout_ms: resolvedConcurrency.timeoutMs
        })
      });
      const payload = await response.json();
      if (!response.ok || payload?.success === false) {
        const message = payload?.error?.message ?? `HTTP ${response.status}`;
        throw new Error(message);
      }
      const job = payload.data ?? payload;
      setDownloadResult(job);
      await pollCollectorJob(job.job_id);
    } catch (error) {
      setDownloadError(error instanceof Error ? error.message : "采集失败");
    } finally {
      setIsDownloading(false);
    }
  }

  async function runCollector() {
    await runCollectorWithParams(compactParams(filterParamsForRows(collectParams, collectParamRows)));
  }

  async function pollCollectorJob(jobId: string) {
    if (!jobId) {
      return;
    }
    while (true) {
      await sleep(900);
      const response = await apiFetch(`${apiBase}/v1/download/jobs/${jobId}`, {
        headers: { Accept: "application/json" }
      });
      const payload = await response.json();
      if (!response.ok || payload?.success === false) {
        const message = payload?.error?.message ?? `HTTP ${response.status}`;
        throw new Error(message);
      }
      const job = payload.data ?? payload;
      setDownloadResult(job);
      if (job.status === "success") {
        setDownloadResult(job.result ? { ...job.result, job_id: job.job_id, duration_ms: job.duration_ms } : job);
        return;
      }
      if (job.status === "failed") {
        throw new Error(job.error?.message ?? "采集失败");
      }
      if (job.status === "skipped") {
        return;
      }
    }
  }

  return (
    <>
      <section className="doc-hero single">
        <div className="doc-heading">
          <div className="title-row">
            <span className="title-icon">
              <ActiveIcon size={26} />
            </span>
            <h1>{activeDoc.title}</h1>
          </div>

          <dl className="summary-list">
            {(activeDoc.overview ?? [["接口名称", activeDoc.name]]).map((row) => (
              <div key={row.join(":")}>
                <dt>{row[0]}</dt>
                <dd>{row.slice(1).join(" / ")}</dd>
              </div>
            ))}
            {activeDoc.notice ? <InterfaceNotice notice={activeDoc.notice} /> : null}
          </dl>
        </div>
      </section>

      {isCollectable ? (
        <ExampleCollectConfig
          activeDoc={activeDoc}
          apiBase={apiBase}
          collectionMode={collectionMode}
          collectionSchedule={collectionSchedule}
          concurrencyProfile={concurrencyProfile}
          concurrencySettings={resolvedConcurrency}
          downloadError={downloadError}
          downloadResult={downloadResult}
          isDownloading={isDownloading}
          isDisablingSchedule={isDisablingSchedule}
          onSaveConfig={saveDownloadConfig}
          onDisableScheduleConfig={disableScheduleConfig}
          onSaveScheduleConfig={enableScheduleConfig}
          onRunCollector={runCollector}
          outputDir={outputDir}
          paths={paths}
          saveMessage={saveMessage}
          scheduleMessage={scheduleMessage}
          scheduleStatus={scheduleStatus}
          selectedFormats={selectedFormats}
          isSavingSchedule={isSavingSchedule}
          collectParamRows={collectParamRows}
          collectParams={filterParamsForRows(collectParams, collectParamRows)}
          setConcurrencySetting={(name, value) => {
            setConcurrencySettings((current) =>
              normalizeConcurrencySettings({ ...current, [name]: value }, concurrencyProfile)
            );
          }}
          setOutputDir={(value) => {
            setOutputDirTouched(true);
            setOutputDir(value);
          }}
          setCollectParam={(name, value) => setCollectParams((current) => ({ ...current, [name]: value }))}
          setCollectionMode={setCollectionMode}
          setCollectionSchedule={(value) =>
            setCollectionSchedule((current) => normalizeCollectionSchedule({ ...current, ...value }))
          }
          setSelectedFormats={setSelectedFormats}
        />
      ) : null}
    </>
  );
}

function ExampleCollectConfig({
  activeDoc,
  apiBase,
  collectionMode,
  collectionSchedule,
  concurrencyProfile,
  concurrencySettings,
  downloadError,
  downloadResult,
  isDownloading,
  isDisablingSchedule,
  onDisableScheduleConfig,
  onSaveConfig,
  onSaveScheduleConfig,
  onRunCollector,
  outputDir,
  paths,
  saveMessage,
  scheduleMessage,
  scheduleStatus,
  selectedFormats,
  isSavingSchedule,
  collectParamRows,
  collectParams,
  setConcurrencySetting,
  setCollectParam,
  setCollectionMode,
  setCollectionSchedule,
  setOutputDir,
  setSelectedFormats
}: {
  activeDoc: CatalogItem;
  apiBase: string;
  collectionMode: CollectionMode;
  collectionSchedule: CollectionSchedule;
  concurrencyProfile: DownloaderConcurrencyProfile;
  concurrencySettings: ConcurrencySettings;
  downloadError: string | null;
  downloadResult: DownloadResult | null;
  isDownloading: boolean;
  isDisablingSchedule: boolean;
  isSavingSchedule: boolean;
  onDisableScheduleConfig: () => void;
  onSaveConfig: () => void;
  onSaveScheduleConfig: () => void;
  onRunCollector: () => void;
  outputDir: string;
  paths: DownloadPaths;
  saveMessage: string | null;
  scheduleMessage: string | null;
  scheduleStatus: DownloadScheduleStatus | null;
  selectedFormats: string[];
  collectParamRows: TableRow[];
  collectParams: Record<string, string>;
  setConcurrencySetting: (name: keyof ConcurrencySettings, value: number | string) => void;
  setCollectParam: (name: string, value: string) => void;
  setCollectionMode: (value: CollectionMode) => void;
  setCollectionSchedule: (value: Partial<CollectionSchedule>) => void;
  setOutputDir: (value: string) => void;
  setSelectedFormats: (value: string[]) => void;
}) {
  const formats = [
    { value: "parquet", label: "Parquet" },
    { value: "csv", label: "CSV" },
    { value: "jsonl", label: "JSONL" }
  ];

  function toggleFormat(format: string, checked: boolean) {
    const next = checked
      ? [...selectedFormats, format]
      : selectedFormats.filter((item) => item !== format);
    setSelectedFormats(next.length > 0 ? Array.from(new Set(next)) : ["parquet"]);
  }
  const showAdvancedConcurrency = hasEditableConcurrency(concurrencyProfile);
  const customConcurrency = concurrencySettings.mode === "custom";
  const runtimeSourceServerMax = numberOr(concurrencyProfile.max_source_server_count, 1);
  const sourceServerLimitText = concurrencyProfile.runtime_source_server_count
    ? `当前启用普通行情服务器：${concurrencyProfile.runtime_source_server_count} 台`
    : "";
  const displayCollectParams = collectParams;
  const manualCollectCode = makeManualCollectCode({
    activeDoc,
    apiBase,
    collectParams,
    concurrencySettings,
    outputDir,
    selectedFormats
  });
  const autoCollectCode = makeAutoCollectCode({
    activeDoc,
    apiBase,
    collectParams,
    collectionSchedule,
    concurrencySettings,
    outputDir,
    selectedFormats
  });

  return (
    <>
      <section className="doc-section">
        <div className="section-title">
          <Settings size={20} />
          <h2>采集配置</h2>
        </div>
        <div className="collect-config-stack">
          <div className="collect-config-block">
            <div className="collect-config-block-title">
              <strong>输入参数</strong>
            </div>
            <CollectParamConfigTable
              params={collectParamRows}
              values={collectParams}
              onChange={setCollectParam}
            />
          </div>

          <div className="collect-config-block">
            <div className="collect-config-block-title">
              <strong>存储设置</strong>
            </div>
            <div className="collect-storage-grid">
              <label className="download-field storage-path-field">
                <span>保存目录</span>
                <input
                  placeholder="不填则使用默认分级目录"
                  value={outputDir}
                  onChange={(event) => setOutputDir(event.target.value)}
                />
                <em className="download-hint">默认：{paths.raw}</em>
              </label>
              <div className="download-field">
                <span>保存格式</span>
                <div className="checkbox-row">
                  {formats.map((format) => (
                    <label key={format.value} className="checkbox-chip">
                      <input
                        checked={selectedFormats.includes(format.value)}
                        type="checkbox"
                        onChange={(event) => toggleFormat(format.value, event.target.checked)}
                      />
                      {format.label}
                    </label>
                  ))}
                </div>
              </div>
            </div>
          </div>

          <div className="collect-config-block">
            <div className="collect-config-block-title">
              <strong>并发设置</strong>
            </div>
            {showAdvancedConcurrency ? (
              <>
                <div className="concurrency-mode-row">
                  <ConcurrencyField
                    editable={Boolean(concurrencyProfile.mode_editable)}
                    label="并发模式"
                    onChange={(value) => setConcurrencySetting("mode", value)}
                    options={CONCURRENCY_PRESET_OPTIONS}
                    value={concurrencySettings.mode}
                  />
                  <div className="concurrency-summary-pill">
                    {concurrencySummary(concurrencySettings)}
                  </div>
                </div>
                <div className="concurrency-grid">
                  <ConcurrencyNumberField
                    editable={Boolean(concurrencyProfile.source_server_count_editable) && customConcurrency}
                    hint={sourceServerLimitText}
                    label="源端服务器数"
                    max={runtimeSourceServerMax}
                    min={1}
                    onChange={(value) => setConcurrencySetting("sourceServerCount", value)}
                    value={concurrencySettings.sourceServerCount}
                  />
                  <ConcurrencyNumberField
                    editable={Boolean(concurrencyProfile.connections_per_server_editable) && customConcurrency}
                    label="每服务器长连接数"
                    max={numberOr(concurrencyProfile.max_connections_per_server, 1)}
                    min={1}
                    onChange={(value) => setConcurrencySetting("connectionsPerServer", value)}
                    value={concurrencySettings.connectionsPerServer}
                  />
                  <ConcurrencyReadonlyMetric
                    label="总并发"
                    value={concurrencyConnectionCount(concurrencySettings)}
                  />
                  <ConcurrencyNumberField
                    editable={Boolean(concurrencyProfile.batch_size_editable)}
                    label="批大小"
                    max={numberOr(concurrencyProfile.max_batch_size, 1)}
                    min={1}
                    onChange={(value) => setConcurrencySetting("batchSize", value)}
                    value={concurrencySettings.batchSize}
                  />
                  <ConcurrencyNumberField
                    editable={Boolean(concurrencyProfile.request_interval_ms_editable)}
                    label="请求间隔 ms"
                    max={numberOr(concurrencyProfile.max_request_interval_ms, 0)}
                    min={numberOr(concurrencyProfile.min_request_interval_ms, 0)}
                    onChange={(value) => setConcurrencySetting("requestIntervalMs", value)}
                    value={concurrencySettings.requestIntervalMs}
                  />
                  <ConcurrencyNumberField
                    editable={Boolean(concurrencyProfile.retry_count_editable)}
                    label="重试次数"
                    max={numberOr(concurrencyProfile.max_retry_count, 0)}
                    min={0}
                    onChange={(value) => setConcurrencySetting("retryCount", value)}
                    value={concurrencySettings.retryCount}
                  />
                  <ConcurrencyNumberField
                    editable={Boolean(concurrencyProfile.timeout_ms_editable)}
                    label="超时 ms"
                    max={numberOr(concurrencyProfile.max_timeout_ms, 30000)}
                    min={numberOr(concurrencyProfile.min_timeout_ms, 30000)}
                    onChange={(value) => setConcurrencySetting("timeoutMs", value)}
                    value={concurrencySettings.timeoutMs}
                  />
                </div>
              </>
            ) : (
              <div className="fixed-concurrency-note">
                <strong>{fixedConcurrencyTitle(concurrencySettings, activeDoc.name)}</strong>
                <span>{concurrencyProfile.description || "当前接口不需要并发配置。"}</span>
              </div>
            )}
          </div>
        </div>
        <DataTable
          columns={["配置项", "当前值", "说明"]}
          rows={[
            ["接口", activeDoc.name, "本次要采集的数据接口"],
            ["采集方式", collectionMode === "auto" ? "自动采集" : "手动采集", collectionMode === "auto" ? formatCollectionSchedule(collectionSchedule) : "点击开始采集后执行"],
            ["请求参数", JSON.stringify(compactParams(displayCollectParams)), "只传实际请求参数；不填就按接口默认范围采集"],
            ["保存目录", outputDir.trim() || `默认：${paths.raw}`, "不填就保存到项目默认目录；填写后就保存到你填写的目录"],
            [
              "并发",
              fixedConcurrencyTitle(concurrencySettings, activeDoc.name),
              concurrencyProfile.connection_count_editable
                ? "可按接口上限调整采集并发"
                : concurrencyProfile.description || fixedConcurrencyDescription()
            ],
            ["保存格式", selectedFormats.join(", "), "会分别写入对应格式的文件夹"],
            ["文件时间", collectionFileTimeLabel(activeDoc), collectionFileTimeDescription(activeDoc)],
            ["质量检查", "字段 + 主键 + 行数", "写完后确认字段正常、主键不重复、行数不为空"]
          ]}
        />
        <div className="download-actions">
          <button className="ghost-action" onClick={onSaveConfig} type="button">
            <Save size={17} />
            保存配置
          </button>
          {saveMessage ? <span className="save-message">{saveMessage}</span> : null}
        </div>
      </section>

      <section className="doc-section">
        <div className="section-title">
          <Download size={20} />
          <h2>采集方式</h2>
        </div>
        <div className="collect-mode-panel">
          <div className="segmented-control" role="group" aria-label="采集方式">
            <button
              className={collectionMode === "manual" ? "active" : ""}
              onClick={() => setCollectionMode("manual")}
              type="button"
            >
              手动采集
            </button>
            <button
              className={collectionMode === "auto" ? "active" : ""}
              onClick={() => setCollectionMode("auto")}
              type="button"
            >
              自动采集
            </button>
          </div>
          {collectionMode === "manual" ? (
            <div className="collect-tab-stack">
              <div className="collect-action-row">
                <button className="primary-action collect-run-action" disabled={isDownloading} onClick={onRunCollector} type="button">
                  {isDownloading ? <Loader2 size={17} /> : <Download size={17} />}
                  {isDownloading ? "正在采集" : "开始采集"}
                </button>
              </div>
              <CollectionCodePreview code={manualCollectCode} language="网页实际调用" />
            </div>
          ) : (
            <div className="collect-tab-stack">
              <div className="collect-schedule-panel">
                <div className="schedule-status-strip">
                  <span className={scheduleStatus?.enabled ? "status-dot on" : "status-dot"} />
                  <strong>{scheduleStatus?.enabled ? "定时采集已开启" : "定时采集未开启"}</strong>
                  <span>{formatScheduleStatus(scheduleStatus)}</span>
                </div>
                <div className="collect-schedule-grid">
                  <label className="download-field">
                    <span>频率</span>
                    <select
                      value={collectionSchedule.frequency}
                      onChange={(event) => setCollectionSchedule({ frequency: event.target.value as CollectionSchedule["frequency"] })}
                    >
                      <option value="daily">每天</option>
                      <option value="trade_day">交易日</option>
                      <option value="weekly">每周</option>
                    </select>
                  </label>
                  {collectionSchedule.frequency === "weekly" ? (
                    <label className="download-field">
                      <span>星期</span>
                      <select
                        value={collectionSchedule.weekday}
                        onChange={(event) => setCollectionSchedule({ weekday: event.target.value })}
                      >
                        <option value="1">周一</option>
                        <option value="2">周二</option>
                        <option value="3">周三</option>
                        <option value="4">周四</option>
                        <option value="5">周五</option>
                        <option value="6">周六</option>
                        <option value="7">周日</option>
                      </select>
                    </label>
                  ) : null}
                  <label className="download-field">
                    <span>时间</span>
                    <input
                      type="time"
                      value={collectionSchedule.time}
                      onChange={(event) => setCollectionSchedule({ time: event.target.value })}
                    />
                  </label>
                  <div className="schedule-action-row">
                    <button className="primary-action schedule-save-action" disabled={isSavingSchedule} onClick={onSaveScheduleConfig} type="button">
                      {isSavingSchedule ? <Loader2 size={17} /> : <PlayCircle size={17} />}
                      {isSavingSchedule ? "正在开启" : "开启定时采集"}
                    </button>
                    <button
                      className="ghost-action schedule-disable-action"
                      disabled={isDisablingSchedule || !scheduleStatus?.enabled}
                      onClick={onDisableScheduleConfig}
                      type="button"
                    >
                      {isDisablingSchedule ? <Loader2 size={17} /> : <Power size={17} />}
                      {isDisablingSchedule ? "正在关闭" : "关闭定时采集"}
                    </button>
                  </div>
                </div>
              </div>
              <CollectionCodePreview code={autoCollectCode} language="网页实际调用" />
            </div>
          )}
          {scheduleMessage ? <span className="save-message">{scheduleMessage}</span> : null}
        </div>
        {downloadError ? <p className="form-error">{downloadError}</p> : null}
        {downloadResult && isCollectorJob(downloadResult) ? (
          <CollectorProgress job={downloadResult} />
        ) : null}
        {downloadResult && !isCollectorJob(downloadResult) ? (
          <DataTable
            columns={["项目", "结果"]}
            rows={[
              ["状态", downloadResult.status ?? ""],
              ["任务ID", downloadResult.job_id ?? ""],
              ["行数", String(downloadResult.row_count ?? "")],
              ["采集日期", formatSnapshotDate(downloadResult)],
              ["文件时间", downloadResult.collection_time ?? ""],
              ["并发", formatResultConcurrency(downloadResult, concurrencySettings, activeDoc.name)],
              ["格式", (downloadResult.output_formats ?? selectedFormats).join(", ")],
              ["耗时", `${downloadResult.duration_ms ?? 0} ms`],
              ["耗时拆分", formatDurationBreakdown(downloadResult)],
              ...sourceTimingRows(downloadResult),
              ["保存路径", formatOutputPaths(downloadResult)]
            ]}
          />
        ) : null}
      </section>

      <section className="doc-section">
        <div className="section-title">
          <Database size={20} />
          <h2>保存结果</h2>
        </div>
        <DataTable
          columns={["项目", "默认位置", "说明"]}
          rows={[
            ["保存目录", paths.raw, "不填写保存目录时，采集文件写到这里"],
            ["文件名", collectionFileNameExample(activeDoc), collectionFileNameDescription(activeDoc)],
            ["采集记录", `${paths.raw}logs/`, "保存本次采集的参数、行数、质量检查和输出文件路径"]
          ]}
        />
      </section>
    </>
  );
}

function CollectionCodePreview({ code, language }: { code: string; language: string }) {
  return (
    <details className="collection-code-preview">
      <summary className="collect-code-title">
        <Code2 size={18} />
        <strong>查看实际请求</strong>
      </summary>
      <CodeBlock code={code} language={language} />
    </details>
  );
}

function CollectorProgress({ job }: { job: DownloadResult }) {
  const progress = Math.max(0, Math.min(100, Math.round(job.progress_pct ?? 0)));
  const hasCurrent = typeof job.progress_current === "number";
  const hasTotal = typeof job.progress_total === "number";
  const detailLabel = job.progress_label ?? "已处理";
  const unit = job.progress_unit ?? "";
  const hasResource = Boolean(job.resource_group);
  const resourceUsage = hasResource
    ? [
        job.resource_group,
        typeof job.resource_used === "number" && typeof job.resource_limit === "number"
          ? `${job.resource_used}/${job.resource_limit} 连接`
          : null,
        typeof job.resource_granted === "number" && job.resource_granted > 0
          ? `本任务 ${job.resource_granted} 条`
          : typeof job.resource_requested === "number"
            ? `申请 ${job.resource_requested} 条`
            : null
      ]
        .filter(Boolean)
        .join(" · ")
    : "";
  return (
    <div className="collector-progress">
      <div className="collector-progress-bar">
        <span style={{ width: `${progress}%` }} />
      </div>
      <div className="collector-progress-meta">
        <strong>{progress}%</strong>
        <span>{job.message ?? "采集中"}</span>
        {hasCurrent && hasTotal ? (
          <span>
            {detailLabel} {job.progress_current}/{job.progress_total}
            {unit}
          </span>
        ) : hasCurrent ? (
          <span>
            {detailLabel} {job.progress_current}
            {unit}
          </span>
        ) : null}
        {typeof job.eta_ms === "number" && job.eta_ms > 0 ? (
          <span>预计剩余 {formatDuration(job.eta_ms)}</span>
        ) : null}
        <span>已用 {formatDuration(job.duration_ms)}</span>
      </div>
      {hasResource ? (
        <div className="collector-resource-meta">
          <span>{resourceUsage}</span>
          {job.status === "waiting_resource" && job.resource_wait_reason ? (
            <strong>{job.resource_wait_reason}</strong>
          ) : null}
          {job.status === "waiting_output" && job.output_wait_reason ? (
            <strong>{job.output_wait_reason}</strong>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function isCollectorJob(result: DownloadResult) {
  return typeof result.progress_pct === "number" && result.status !== "success";
}

function CollectParamConfigTable({
  onChange,
  params,
  values
}: {
  onChange: (name: string, value: string) => void;
  params: TableRow[];
  values: Record<string, string>;
}) {
  return (
    <div className="param-config-table">
      <div className="param-config-head">
        <span>名称</span>
        <span>类型</span>
        <span>必填</span>
        <span>配置值</span>
        <span>描述</span>
      </div>
      {params.map((param) => {
        const [name, type, required, description] = param;
        const options = collectParamOptions(name, description);
        return (
          <div className="param-config-row" key={name}>
            <code>{name}</code>
            <span>{type}</span>
            <span>{required}</span>
            {options.length > 0 ? (
              <select value={values[name] ?? ""} onChange={(event) => onChange(name, event.target.value)}>
                {options.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            ) : (
              <input
                placeholder={type.includes("list") ? "多个值用英文逗号分隔；不填则不传" : "不填则不传"}
                value={values[name] ?? ""}
                onChange={(event) => onChange(name, event.target.value)}
              />
            )}
            <span>{description}</span>
          </div>
        );
      })}
    </div>
  );
}

function ConcurrencyField({
  editable,
  label,
  onChange,
  options,
  value
}: {
  editable: boolean;
  label: string;
  onChange: (value: string) => void;
  options: [string, string][];
  value: string;
}) {
  return (
    <label className="download-field compact">
      <span>{label}</span>
      <select
        disabled={!editable}
        value={value}
        onChange={(event) => onChange(event.target.value)}
      >
        {options.map(([optionValue, optionLabel]) => (
          <option key={optionValue} value={optionValue}>
            {optionLabel}
          </option>
        ))}
      </select>
    </label>
  );
}

function ConcurrencyNumberField({
  editable,
  hint,
  label,
  max,
  min,
  onChange,
  value
}: {
  editable: boolean;
  hint?: string;
  label: string;
  max: number;
  min: number;
  onChange: (value: number) => void;
  value: number;
}) {
  return (
    <label className="download-field compact">
      <span>{label}</span>
      <input
        disabled={!editable}
        readOnly={!editable}
        min={min}
        max={max}
        type="number"
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
      {hint ? <em className="download-hint">{hint}</em> : null}
    </label>
  );
}

function ConcurrencyReadonlyMetric({ label, value }: { label: string; value: number }) {
  return (
    <div className="download-field compact">
      <span>{label}</span>
      <div className="readonly-metric">{value}</div>
    </div>
  );
}

type DownloadPaths = {
  defaultRoot: string;
  raw: string;
  logs: string;
};

function defaultParamsForRows(params: TableRow[]): Record<string, string> {
  return Object.fromEntries(
    params.map((param) => {
      const [name, , , description] = param;
      if (name === "scope" && description.includes("默认 all")) {
        return [name, "all"];
      }
      return [name, ""];
    })
  );
}

function collectParamOptions(name: string, description: string) {
  if (name !== "scope") {
    return [];
  }
  const options = [
    ["all", "全部"],
    ["main", "主板"],
    ["star", "科创板"],
    ["chinext", "创业板"],
    ["bse", "北交所"],
    ["cdr", "CDR"]
  ];
  return options
    .filter(([value]) => description.includes(value))
    .map(([value, label]) => ({ value, label }));
}

function compactParams(params: Record<string, string>) {
  return Object.fromEntries(
    Object.entries(params)
      .map(([key, value]) => [key, value.trim()] as const)
      .filter(([, value]) => value)
  );
}

function filterParamsForRows(params: Record<string, string>, rows: TableRow[]) {
  const allowed = new Set(rows.map(([name]) => name));
  return Object.fromEntries(Object.entries(params).filter(([name]) => allowed.has(name)));
}

function makeManualCollectCode({
  activeDoc,
  apiBase,
  collectParams,
  concurrencySettings,
  outputDir,
  selectedFormats
}: {
  activeDoc: CatalogItem;
  apiBase: string;
  collectParams: Record<string, string>;
  concurrencySettings: ConcurrencySettings;
  outputDir: string;
  selectedFormats: string[];
}) {
  const body = makeDownloadRequestBody({
    collectParams,
    concurrencySettings,
    outputDir,
    selectedFormats
  });
  return [
    `const apiBase = ${formatTsValue(apiBase)};`,
    "",
    "const response = await fetch(",
    `  \`${"${apiBase}"}/v1/download/${activeDoc.name}\`,`,
    "  {",
    `    method: "POST",`,
    "    headers: {",
    `      Accept: "application/json",`,
    `      "Content-Type": "application/json"`,
    "    },",
    `    body: JSON.stringify(${formatTsValue(body, 4)})`,
    "  }",
    ");",
    "",
    "const payload = await response.json();",
    "if (!response.ok || payload?.success === false) {",
    "  throw new Error(payload?.error?.message ?? `HTTP ${response.status}`);",
    "}",
    "",
    "const job = payload.data ?? payload;",
    "",
    "while (job.job_id) {",
    "  await new Promise((resolve) => window.setTimeout(resolve, 900));",
    "",
    "  const jobResponse = await fetch(",
    `    \`${"${apiBase}"}/v1/download/jobs/${"${job.job_id}"}\`,`,
    "    { headers: { Accept: \"application/json\" } }",
    "  );",
    "  const jobPayload = await jobResponse.json();",
    "  if (!jobResponse.ok || jobPayload?.success === false) {",
    "    throw new Error(jobPayload?.error?.message ?? `HTTP ${jobResponse.status}`);",
    "  }",
    "",
    "  const currentJob = jobPayload.data ?? jobPayload;",
    "  if (currentJob.status === \"success\") break;",
    "  if (currentJob.status === \"failed\") {",
    "    throw new Error(currentJob.error?.message ?? \"采集失败\");",
    "  }",
    "}"
  ].join("\n");
}

function makeAutoCollectCode({
  activeDoc,
  apiBase,
  collectParams,
  collectionSchedule,
  concurrencySettings,
  outputDir,
  selectedFormats
}: {
  activeDoc: CatalogItem;
  apiBase: string;
  collectParams: Record<string, string>;
  collectionSchedule: CollectionSchedule;
  concurrencySettings: ConcurrencySettings;
  outputDir: string;
  selectedFormats: string[];
}) {
  const body = makeScheduleRequestBody({
    collectParams,
    collectionSchedule,
    concurrencySettings,
    outputDir,
    selectedFormats
  });

  return [
    `const apiBase = ${formatTsValue(apiBase)};`,
    "",
    "// 开启定时采集",
    "const response = await fetch(",
    `  \`${"${apiBase}"}/v1/download/schedules/${activeDoc.name}\`,`,
    "  {",
    `    method: "POST",`,
    "    headers: {",
    `      Accept: "application/json",`,
    `      "Content-Type": "application/json"`,
    "    },",
    `    body: JSON.stringify(${formatTsValue(body, 4)})`,
    "  }",
    ");",
    "",
    "const payload = await response.json();",
    "if (!response.ok || payload?.success === false) {",
    "  throw new Error(payload?.error?.message ?? `HTTP ${response.status}`);",
    "}",
    "",
    "// 关闭定时采集",
    "await fetch(",
    `  \`${"${apiBase}"}/v1/download/schedules/${activeDoc.name}\`,`,
    "  { method: \"DELETE\", headers: { Accept: \"application/json\" } }",
    ");"
  ].join("\n");
}

function makeDownloadRequestBody({
  collectParams,
  concurrencySettings,
  outputDir,
  selectedFormats
}: {
  collectParams: Record<string, string>;
  concurrencySettings: ConcurrencySettings;
  outputDir: string;
  selectedFormats: string[];
}) {
  const body: Record<string, unknown> = {
    params: compactParams(collectParams),
    async_job: true,
    formats: selectedFormats,
    connection_mode: "long_connection",
    concurrency_mode: concurrencySettings.mode,
    source_server_count: concurrencySettings.sourceServerCount,
    connections_per_server: concurrencySettings.connectionsPerServer,
    batch_size: concurrencySettings.batchSize,
    request_interval_ms: concurrencySettings.requestIntervalMs,
    retry_count: concurrencySettings.retryCount,
    timeout_ms: concurrencySettings.timeoutMs
  };
  if (outputDir.trim()) {
    body.output_dir = outputDir.trim();
  }
  return body;
}

function makeScheduleRequestBody({
  collectParams,
  collectionSchedule,
  concurrencySettings,
  outputDir,
  selectedFormats
}: {
  collectParams: Record<string, string>;
  collectionSchedule: CollectionSchedule;
  concurrencySettings: ConcurrencySettings;
  outputDir: string;
  selectedFormats: string[];
}) {
  const body: Record<string, unknown> = {
    frequency: collectionSchedule.frequency,
    time: collectionSchedule.time,
    weekday: collectionSchedule.weekday,
    enabled: true,
    params: compactParams(collectParams),
    formats: selectedFormats,
    connection_mode: "long_connection",
    concurrency_mode: concurrencySettings.mode,
    source_server_count: concurrencySettings.sourceServerCount,
    connections_per_server: concurrencySettings.connectionsPerServer,
    batch_size: concurrencySettings.batchSize,
    request_interval_ms: concurrencySettings.requestIntervalMs,
    retry_count: concurrencySettings.retryCount,
    timeout_ms: concurrencySettings.timeoutMs
  };
  if (outputDir.trim()) {
    body.output_dir = outputDir.trim();
  }
  return body;
}

function formatTsValue(value: unknown, indent = 0): string {
  if (typeof value === "string") {
    return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
  }
  if (typeof value === "number") {
    return String(value);
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (value === null || value === undefined) {
    return "undefined";
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => formatTsValue(item, indent)).join(", ")}]`;
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 0) {
      return "{}";
    }
    const pad = " ".repeat(indent);
    const innerPad = " ".repeat(indent + 4);
    return [
      "{",
      ...entries.map(([key, item]) => `${innerPad}${key}: ${formatTsValue(item, indent + 4)},`),
      `${pad}}`
    ].join("\n");
  }
  return formatTsValue(String(value), indent);
}

function makeConcurrencyProfile(profile: DownloaderProfile | null): DownloaderConcurrencyProfile {
  const fallbackConnectionCount = numberOr(profile?.default_connection_count, 1);
  return {
    mode: "fixed",
    mode_editable: false,
    default_source_server_count: 1,
    source_server_count_editable: false,
    max_source_server_count: 1,
    default_connections_per_server: fallbackConnectionCount,
    connections_per_server_editable: Boolean(profile?.connection_count_editable),
    max_connections_per_server: numberOr(profile?.max_connection_count, fallbackConnectionCount),
    default_max_concurrent_tasks: fallbackConnectionCount,
    max_concurrent_tasks_editable: Boolean(profile?.connection_count_editable),
    max_max_concurrent_tasks: numberOr(profile?.max_connection_count, fallbackConnectionCount),
    default_batch_size: 1,
    batch_size_editable: false,
    max_batch_size: 1,
    default_request_interval_ms: 0,
    request_interval_ms_editable: false,
    min_request_interval_ms: 0,
    max_request_interval_ms: 0,
    default_retry_count: 0,
    retry_count_editable: false,
    max_retry_count: 0,
    default_timeout_ms: 30000,
    timeout_ms_editable: false,
    min_timeout_ms: 30000,
    max_timeout_ms: 30000,
    default_connection_count: fallbackConnectionCount,
    connection_count_editable: Boolean(profile?.connection_count_editable),
    max_connection_count: numberOr(profile?.max_connection_count, fallbackConnectionCount),
    ...(profile?.concurrency ?? {})
  };
}

function withRuntimeSourceServerMax(
  profile: DownloaderConcurrencyProfile,
  quoteStatus: TdxServerStatus | null
): DownloaderConcurrencyProfile {
  if (!profile.source_server_count_editable) {
    return profile;
  }
  const configuredMax = numberOr(profile.max_source_server_count, numberOr(profile.default_source_server_count, 1));
  const enabledCount = numberOr(quoteStatus?.enabled_count, 0);
  if (enabledCount <= 0) {
    return profile;
  }
  const maxSourceServerCount = Math.max(1, Math.min(configuredMax, enabledCount));
  return {
    ...profile,
    runtime_source_server_count: enabledCount,
    max_source_server_count: maxSourceServerCount
  };
}

function defaultConcurrencyForProfile(profile: DownloaderConcurrencyProfile): ConcurrencySettings {
  const mode = normalizeConcurrencyModeValue(profile.mode ?? "fixed");
  const preset = concurrencyPresetForMode(mode, profile);
  const sourceServerCount = preset?.sourceServerCount ?? numberOr(profile.default_source_server_count, 1);
  const connectionsPerServer = preset?.connectionsPerServer ?? numberOr(profile.default_connections_per_server, 1);
  return {
    mode,
    sourceServerCount,
    connectionsPerServer,
    maxConcurrentTasks: preset?.maxConcurrentTasks ?? numberOr(profile.default_max_concurrent_tasks, sourceServerCount * connectionsPerServer),
    batchSize: numberOr(profile.default_batch_size, 1),
    requestIntervalMs: numberOr(profile.default_request_interval_ms, 0),
    retryCount: numberOr(profile.default_retry_count, 0),
    timeoutMs: numberOr(profile.default_timeout_ms, 30000)
  };
}

function normalizeConcurrencySettings(
  value: Partial<ConcurrencySettings> | undefined,
  profile: DownloaderConcurrencyProfile
): ConcurrencySettings {
  const defaults = defaultConcurrencyForProfile(profile);
  const mode = profile.mode_editable ? normalizeConcurrencyModeValue(value?.mode ?? defaults.mode) : defaults.mode;
  const preset = concurrencyPresetForMode(mode, profile);
  const sourceServerCount = normalizeConcurrencyNumber(
    preset?.sourceServerCount ?? value?.sourceServerCount,
    preset?.sourceServerCount ?? defaults.sourceServerCount,
    1,
    numberOr(profile.max_source_server_count, defaults.sourceServerCount),
    Boolean(profile.source_server_count_editable) && mode === "custom"
  );
  const connectionsPerServer = normalizeConcurrencyNumber(
    preset?.connectionsPerServer ?? value?.connectionsPerServer,
    preset?.connectionsPerServer ?? defaults.connectionsPerServer,
    1,
    numberOr(profile.max_connections_per_server, defaults.connectionsPerServer),
    Boolean(profile.connections_per_server_editable) && mode === "custom"
  );
  const maxConcurrentTasks = normalizeConcurrencyNumber(
    preset?.maxConcurrentTasks ?? value?.maxConcurrentTasks,
    sourceServerCount * connectionsPerServer,
    1,
    sourceServerCount * connectionsPerServer,
    Boolean(profile.max_concurrent_tasks_editable) && mode === "custom"
  );
  return {
    mode,
    sourceServerCount,
    connectionsPerServer,
    maxConcurrentTasks,
    batchSize: normalizeConcurrencyNumber(
      value?.batchSize,
      defaults.batchSize,
      1,
      numberOr(profile.max_batch_size, defaults.batchSize),
      Boolean(profile.batch_size_editable)
    ),
    requestIntervalMs: normalizeConcurrencyNumber(
      value?.requestIntervalMs,
      defaults.requestIntervalMs,
      numberOr(profile.min_request_interval_ms, 0),
      numberOr(profile.max_request_interval_ms, defaults.requestIntervalMs),
      Boolean(profile.request_interval_ms_editable)
    ),
    retryCount: normalizeConcurrencyNumber(
      value?.retryCount,
      defaults.retryCount,
      0,
      numberOr(profile.max_retry_count, defaults.retryCount),
      Boolean(profile.retry_count_editable)
    ),
    timeoutMs: normalizeConcurrencyNumber(
      value?.timeoutMs,
      defaults.timeoutMs,
      numberOr(profile.min_timeout_ms, defaults.timeoutMs),
      numberOr(profile.max_timeout_ms, defaults.timeoutMs),
      Boolean(profile.timeout_ms_editable)
    )
  };
}

function normalizeConcurrencyModeValue(value: unknown) {
  const raw = String(value ?? "").trim().toLowerCase();
  const aliases: Record<string, string> = {
    configurable: "custom",
    auto: "medium",
    conservative: "low",
    aggressive: "high"
  };
  const normalized = aliases[raw] ?? raw;
  if (["low", "medium", "high", "custom", "fixed"].includes(normalized)) {
    return normalized;
  }
  return "custom";
}

function concurrencyPresetForMode(
  mode: string,
  profile: DownloaderConcurrencyProfile
): Pick<ConcurrencySettings, "sourceServerCount" | "connectionsPerServer" | "maxConcurrentTasks"> | null {
  if (!["low", "medium", "high"].includes(mode)) {
    return null;
  }
  const maxSourceServerCount = Math.max(1, numberOr(profile.max_source_server_count, 1));
  const maxConnectionsPerServer = Math.max(1, numberOr(profile.max_connections_per_server, 1));
  const maxConcurrentTasks = Math.max(1, numberOr(profile.max_max_concurrent_tasks, 1));
  const presets: Record<string, Pick<ConcurrencySettings, "sourceServerCount" | "connectionsPerServer" | "maxConcurrentTasks">> = {
    low: { sourceServerCount: 1, connectionsPerServer: 2, maxConcurrentTasks: 2 },
    medium: { sourceServerCount: 2, connectionsPerServer: 2, maxConcurrentTasks: 4 },
    high: { sourceServerCount: 4, connectionsPerServer: 2, maxConcurrentTasks: 8 }
  };
  const preset = presets[mode];
  const sourceServerCount = Math.min(maxSourceServerCount, preset.sourceServerCount);
  const connectionsPerServer = Math.min(maxConnectionsPerServer, preset.connectionsPerServer);
  return {
    sourceServerCount,
    connectionsPerServer,
    maxConcurrentTasks: Math.min(maxConcurrentTasks, preset.maxConcurrentTasks, sourceServerCount * connectionsPerServer)
  };
}

function normalizeConcurrencyNumber(
  value: number | undefined,
  defaultValue: number,
  min: number,
  max: number,
  editable: boolean
) {
  if (!editable) {
    return defaultValue;
  }
  const resolved = Number(value);
  return Number.isFinite(resolved) ? Math.min(max, Math.max(min, Math.round(resolved))) : defaultValue;
}

function concurrencyConnectionCount(settings: ConcurrencySettings) {
  return settings.sourceServerCount * settings.connectionsPerServer;
}

function concurrencySummary(settings: ConcurrencySettings) {
  if (
    settings.mode === "fixed" &&
    settings.sourceServerCount === 1 &&
    settings.connectionsPerServer === 1 &&
    settings.maxConcurrentTasks === 1
  ) {
    return "固定单连接";
  }
  return `${formatConcurrencyMode(settings.mode)} / ${settings.sourceServerCount} × ${settings.connectionsPerServer} / 总并发 ${concurrencyConnectionCount(settings)}`;
}

function fixedConcurrencyTitle(settings: ConcurrencySettings, interfaceName?: string) {
  const base = concurrencySummary(settings);
  if (interfaceName && F10_TOPIC_WORKER_INTERFACES.has(interfaceName)) {
    return `普通行情：${base}；题材查询：${DEFAULT_F10_TOPIC_WORKERS} 个 F10 worker；漏了再补 ${DEFAULT_F10_TOPIC_REFILL_WORKERS} 个 worker × ${DEFAULT_F10_TOPIC_REFILL_ROUNDS} 轮`;
  }
  return base;
}

function fixedConcurrencyDescription() {
  return "这个接口一次就能拿完整列表，不需要并发";
}

function formatResultConcurrency(result: DownloadResult, fallback: ConcurrencySettings, activeInterfaceName?: string) {
  const concurrency = result.concurrency;
  const interfaceName = result.interface_name || activeInterfaceName || "";
  const topicWorkerPart = resultF10TopicSummary(result, interfaceName);
  if (concurrency) {
    const sourceServerCount = Number(concurrency.source_server_count ?? fallback.sourceServerCount);
    const connectionsPerServer = Number(concurrency.connections_per_server ?? fallback.connectionsPerServer);
    const connectionCount = Number(concurrency.connection_count ?? fallback.maxConcurrentTasks);
    const base = `${formatConcurrencyMode(String(concurrency.mode ?? fallback.mode))} / ${sourceServerCount} × ${connectionsPerServer} / 总并发 ${connectionCount}`;
    return topicWorkerPart ? `普通行情：${base}${topicWorkerPart}` : base;
  }
  const base = `${result.connection_mode ?? "long_connection"} / ${result.connection_count ?? concurrencyConnectionCount(fallback)}`;
  return topicWorkerPart ? `普通行情：${base}${topicWorkerPart}` : base;
}

function resultF10TopicWorkers(result: DownloadResult, interfaceName: string) {
  const sourceMeta = result.source_meta;
  if (sourceMeta && Object.prototype.hasOwnProperty.call(sourceMeta, "tdx_f10_topic_workers")) {
    const metaWorkers = Number(sourceMeta["tdx_f10_topic_workers"]);
    return Number.isFinite(metaWorkers) && metaWorkers > 0 ? Math.round(metaWorkers) : null;
  }
  if (F10_TOPIC_WORKER_INTERFACES.has(interfaceName)) {
    return DEFAULT_F10_TOPIC_WORKERS;
  }
  return null;
}

function resultF10TopicSummary(result: DownloadResult, interfaceName: string) {
  const workers = resultF10TopicWorkers(result, interfaceName);
  if (!workers) {
    return "";
  }
  const sourceMeta = result.source_meta;
  const fallbackRefillWorkers = F10_TOPIC_WORKER_INTERFACES.has(interfaceName)
    ? DEFAULT_F10_TOPIC_REFILL_WORKERS
    : null;
  const fallbackRefillRounds = F10_TOPIC_WORKER_INTERFACES.has(interfaceName)
    ? DEFAULT_F10_TOPIC_REFILL_ROUNDS
    : null;
  const metaRefillWorkers = Number(sourceMeta?.["tdx_f10_topic_refill_workers"]);
  const metaRefillRounds = Number(
    sourceMeta?.["tdx_f10_topic_refill_configured_rounds"] ?? sourceMeta?.["tdx_f10_topic_refill_rounds"]
  );
  const refillWorkers = Number.isFinite(metaRefillWorkers) && metaRefillWorkers > 0
    ? Math.round(metaRefillWorkers)
    : fallbackRefillWorkers;
  const refillRounds = Number.isFinite(metaRefillRounds) && metaRefillRounds > 0
    ? Math.round(metaRefillRounds)
    : fallbackRefillRounds;
  const refillPart = refillWorkers && refillRounds ? `；漏了再补 ${refillWorkers} 个 worker × ${refillRounds} 轮` : "；默认不补漏";
  return `；题材查询：${workers} 个 F10 worker${refillPart}`;
}

function formatConcurrencyMode(mode: string) {
  const labels: Record<string, string> = {
    fixed: "固定",
    low: "低",
    medium: "中（推荐）",
    high: "高",
    custom: "自定义",
    configurable: "自定义",
    auto: "中（推荐）",
    conservative: "低",
    aggressive: "高"
  };
  return labels[mode] ?? mode;
}

function numberOr(value: unknown, fallback: number) {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : fallback;
}

function formatDurationBreakdown(result: DownloadResult) {
  const breakdown = result.duration_breakdown_ms;
  if (!breakdown) {
    return "";
  }
  const connection = numberOr(breakdown.connection, 0);
  const source = numberOr(breakdown.source_request, 0);
  const transform = numberOr(breakdown.transform, 0);
  const write = numberOr(breakdown.write, 0);
  return `连接 ${connection} ms / 源端 ${source} ms / 整理 ${transform} ms / 写文件 ${write} ms`;
}

function sourceTimingRows(result: DownloadResult): string[][] {
  const timing = sourceTimingBreakdown(result);
  if (!timing) {
    return [];
  }
  return [["源端内部耗时", timing]];
}

function sourceTimingBreakdown(result: DownloadResult) {
  const sourceMeta = result.source_meta;
  if (!sourceMeta) {
    return "";
  }
  const limitLadderTiming = timingRecord(sourceMeta.tdx_limit_ladder_timing_ms);
  if (limitLadderTiming) {
    return [
      ["统计文件", limitLadderTiming.stats],
      ["扫涨幅榜", limitLadderTiming.rank_scan],
      ["补股票名", limitLadderTiming.name_lookup],
      ["过滤整理", limitLadderTiming.normalize_filter],
      ["补题材", limitLadderTiming.theme_lookup],
      ["题材汇总", limitLadderTiming.theme_attach],
      ["排序截取", limitLadderTiming.sort_and_slice],
      ["合计", limitLadderTiming.total]
    ]
      .map(([label, value]) => `${label} ${numberOr(value, 0)} ms`)
      .join(" / ");
  }
  const themeStrengthTiming = timingRecord(sourceMeta.tdx_theme_strength_timing_ms);
  if (themeStrengthTiming) {
    return [
      ["统计文件", themeStrengthTiming.stats],
      ["扫涨幅榜", themeStrengthTiming.rank_scan],
      ["补股票名", themeStrengthTiming.name_lookup],
      ["过滤整理", themeStrengthTiming.normalize_filter],
      ["补题材", themeStrengthTiming.theme_lookup],
      ["排行生成", themeStrengthTiming.rank_build],
      ["合计", themeStrengthTiming.total]
    ]
      .map(([label, value]) => `${label} ${numberOr(value, 0)} ms`)
      .join(" / ");
  }
  return "";
}

function timingRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function hasEditableConcurrency(profile: DownloaderConcurrencyProfile) {
  return Boolean(
    profile.mode_editable ||
      profile.source_server_count_editable ||
      profile.connections_per_server_editable ||
      profile.max_concurrent_tasks_editable ||
      profile.batch_size_editable ||
      profile.request_interval_ms_editable ||
      profile.retry_count_editable ||
      profile.timeout_ms_editable
  );
}

function makeDownloadPaths(activeDoc: CatalogItem, defaultRoot: string): DownloadPaths {
  const groupPath = normalizePathPart(activeDoc.sourcePath ?? activeDoc.group);
  const tablePath = `${groupPath}/${activeDoc.name}`;
  return {
    defaultRoot,
    raw: `${defaultRoot}/${tablePath}/`,
    logs: `${defaultRoot}/${tablePath}/logs/`
  };
}

function formatOutputPaths(result: DownloadResult) {
  if (result.output_paths) {
    return Object.entries(result.output_paths)
      .map(([format, path]) => `${format}: ${path}`)
      .join("\n");
  }
  return result.output_path ?? "";
}

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function formatDuration(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  const totalSeconds = Math.max(0, Math.round(value / 1000));
  if (totalSeconds < 60) {
    return `${totalSeconds}s`;
  }
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}m ${seconds}s`;
}

function formatTimingLabel(key: string) {
  const labels: Record<string, string> = {
    queue_wait_ms: "排队",
    params_resolve_ms: "参数",
    provider_resolve_ms: "采集器",
    download_ms: "下载",
    write_ms: "写入",
    quality_ms: "质量",
    total_ms: "总计"
  };
  return labels[key] || key.replace(/_ms$/, "").replace(/_/g, " ");
}

function formatSnapshotDate(result: DownloadResult) {
  if (!result.snapshot_date) {
    return "";
  }
  if (result.snapshot_date_source === "tdx_stats_date") {
    return `${result.snapshot_date}（盘前统计日期）`;
  }
  if (result.snapshot_date_source === "collected_at") {
    return `${result.snapshot_date}（按采集时间）`;
  }
  return `${result.snapshot_date}（源端返回）`;
}

function collectionFileTimeLabel(activeDoc: CatalogItem) {
  if (activeDoc.name === "stock_daily_share_tdx") {
    return "盘前统计日期";
  }
  if (activeDoc.name === "stock_daily_price_limit_tdx") {
    return "最新交易日";
  }
  return "采集时间到分钟";
}

function collectionFileTimeDescription(activeDoc: CatalogItem) {
  if (activeDoc.name === "stock_daily_share_tdx") {
    return "文件名使用 tdxstat 的统计日期，同一统计日期重复采集会覆盖同名文件";
  }
  if (activeDoc.name === "stock_daily_price_limit_tdx") {
    return "文件名使用本次最新涨跌停价格对应的交易日，同一交易日重复采集会覆盖同名文件";
  }
  return "文件名会使用接口名_YYYYMMDD_HHMM，方便区分同一天多次采集";
}

function collectionFileNameExample(activeDoc: CatalogItem) {
  if (activeDoc.name === "stock_daily_share_tdx" || activeDoc.name === "stock_daily_price_limit_tdx") {
    return `${activeDoc.name}_YYYYMMDD.*`;
  }
  return `${activeDoc.name}_YYYYMMDD_HHMM.*`;
}

function collectionFileNameDescription(activeDoc: CatalogItem) {
  if (activeDoc.name === "stock_daily_share_tdx") {
    return "日期是 tdxstat 里的盘前统计日期，不带分钟";
  }
  if (activeDoc.name === "stock_daily_price_limit_tdx") {
    return "日期来自本次最新涨跌停价格的源端返回日期，不带分钟";
  }
  return "时间是实际采集时间，精确到分钟";
}

function formatTdxServerSource(value: string | undefined) {
  const labels: Record<string, string> = {
    built_in: "内置表",
    user: "用户配置",
    environment: "环境变量",
    latency_probe: "测速缓存"
  };
  return labels[value ?? ""] ?? "未读取";
}

function formatTdxServerCount(status: TdxServerStatus | null, drafts: TdxServerRow[]) {
  const total = status?.server_count ?? drafts.length;
  const enabled = status?.enabled_count ?? drafts.filter((row) => row.enabled).length;
  return `${enabled} 个启用 / 共 ${total} 个`;
}

function formatTdxProbeSummary(rows: TdxServerRow[]) {
  const checked = rows.filter((row) => row.last_checked_at);
  if (checked.length === 0) {
    return "还未测速";
  }
  const ok = checked.filter((row) => !row.last_error && row.latency_ms !== null && row.latency_ms !== undefined);
  const fastest = ok.reduce<number | null>((current, row) => {
    const latency = Number(row.latency_ms);
    if (!Number.isFinite(latency)) {
      return current;
    }
    return current === null ? latency : Math.min(current, latency);
  }, null);
  return fastest === null
    ? `已测速 ${checked.length} 个，暂无可用`
    : `已测速 ${checked.length} 个，最快 ${fastest} ms`;
}

function defaultTdxProbeSchedule(): TdxServerProbeSchedule {
  return {
    enabled: false,
    frequency: "daily",
    time: "08:30",
    weekday: "1",
    timeout: 1.2,
    max_workers: 32,
    kinds: ["quote", "extended"],
    last_run_key: null,
    last_checked_at: null,
    last_result: null,
    updated_at: null
  };
}

function normalizeTdxProbeSchedule(value: Partial<TdxServerProbeSchedule> | undefined): TdxServerProbeSchedule {
  const base = defaultTdxProbeSchedule();
  const frequency = value?.frequency === "weekly" ? "weekly" : "daily";
  const time = typeof value?.time === "string" && /^\d{2}:\d{2}$/.test(value.time) ? value.time : base.time;
  const weekday = typeof value?.weekday === "string" && /^[1-7]$/.test(value.weekday) ? value.weekday : base.weekday;
  const kinds = Array.isArray(value?.kinds)
    ? value.kinds.filter((kind): kind is TdxServerKind => kind === "quote" || kind === "extended")
    : base.kinds;
  return {
    ...base,
    ...value,
    enabled: Boolean(value?.enabled),
    frequency,
    time,
    weekday,
    timeout: Number.isFinite(Number(value?.timeout)) ? Number(value?.timeout) : base.timeout,
    max_workers: Number.isFinite(Number(value?.max_workers)) ? Number(value?.max_workers) : base.max_workers,
    kinds: kinds.length > 0 ? Array.from(new Set(kinds)) : base.kinds
  };
}

function formatTdxProbeSchedule(schedule: TdxServerProbeSchedule) {
  const timeText = schedule.frequency === "weekly"
    ? `每周${weekdayLabel(schedule.weekday)} ${schedule.time}`
    : `每天 ${schedule.time}`;
  const kindsText =
    schedule.kinds.length === 2
      ? "普通和扩展"
      : schedule.kinds[0] === "quote"
        ? "普通行情"
        : "扩展行情";
  const lastText = schedule.last_checked_at ? `；上次 ${schedule.last_checked_at}` : "";
  return `${timeText} 测 ${kindsText}${lastText}`;
}

function tdxServerImportExample(kind: TdxServerKind) {
  const samplePort = kind === "quote" ? 7709 : 7727;
  const sampleName = kind === "quote" ? "普通行情示例" : "扩展行情示例";
  return JSON.stringify(
    {
      servers: [
        { name: `${sampleName}1`, host: "127.0.0.1", port: samplePort, enabled: true },
        { name: `${sampleName}2`, host: "127.0.0.2", port: samplePort, enabled: true }
      ]
    },
    null,
    2
  );
}

function parseTdxServerImport(text: string, kind: TdxServerKind): TdxServerRow[] {
  const parsed = JSON.parse(text);
  const rawRows = Array.isArray(parsed)
    ? parsed
    : Array.isArray(parsed?.servers)
      ? parsed.servers
      : null;
  if (!rawRows) {
    throw new Error("导入文件需要是服务器数组，或包含 servers 数组的 JSON 对象");
  }
  const rows: TdxServerRow[] = rawRows.map((item: unknown, index: number): TdxServerRow => {
    if (!isRecord(item)) {
      throw new Error(`第 ${index + 1} 行不是有效对象`);
    }
    const address = typeof item.address === "string" ? item.address.trim() : "";
    const addressParts = address.includes(":") ? address.split(":") : [];
    const host = typeof item.host === "string" && item.host.trim()
      ? item.host.trim()
      : addressParts.length >= 2
        ? addressParts.slice(0, -1).join(":").trim()
        : "";
    const rawPort =
      typeof item.port === "number" || typeof item.port === "string"
        ? item.port
        : addressParts.length >= 2
          ? addressParts[addressParts.length - 1]
          : kind === "quote"
            ? 7709
            : 7727;
    const port = Number(rawPort);
    if (!host || !Number.isInteger(port) || port <= 0 || port > 65535) {
      throw new Error(`第 ${index + 1} 行缺少有效的 host 或 port`);
    }
    return {
      name: typeof item.name === "string" && item.name.trim()
        ? item.name.trim()
        : kind === "quote"
          ? `普通行情${index + 1}`
          : `扩展行情${index + 1}`,
      host,
      port,
      enabled: typeof item.enabled === "boolean" ? item.enabled : true,
      priority: index + 1,
      latency_ms: null,
      last_checked_at: null,
      last_error: null
    };
  });
  const seen = new Set<string>();
  const deduped = rows.filter((row) => {
    const key = `${row.host}:${row.port}`.toLowerCase();
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
  if (deduped.length === 0) {
    throw new Error("导入文件里没有可用服务器");
  }
  return deduped;
}

function formatTdxLatency(row: TdxServerRow) {
  if (row.last_error) {
    return "不可用";
  }
  if (row.latency_ms === null || row.latency_ms === undefined) {
    return "--";
  }
  return `${row.latency_ms} ms`;
}

function formatTdxServerActionMessage(kind: TdxServerKind, action: "save" | "probe" | "reset" | "import") {
  const target = kind === "quote" ? "普通行情服务器" : "扩展行情服务器";
  const actionText =
    action === "save"
      ? "已保存"
      : action === "probe"
        ? "测速排序已完成"
        : action === "reset"
          ? "已恢复内置表"
          : "已导入";
  return `${target}${actionText}`;
}

function formatCollectionSchedule(schedule: CollectionSchedule) {
  const frequency =
    schedule.frequency === "weekly"
      ? `每周${weekdayLabel(schedule.weekday)}`
      : schedule.frequency === "trade_day"
        ? "交易日"
      : "每天";
  return `${frequency} ${schedule.time}`;
}

function formatScheduleStatus(schedule: DownloadScheduleStatus | null) {
  if (!schedule) {
    return "当前没有定时计划";
  }
  const scheduleText = formatCollectionSchedule(normalizeCollectionSchedule(schedule));
  const stateText = schedule.enabled ? scheduleText : `${scheduleText}，已关闭`;
  const extras = [
    schedule.last_job_id ? `上次任务 ${schedule.last_job_id}` : "",
    schedule.last_run_key ? `上次触发 ${schedule.last_run_key}` : ""
  ].filter(Boolean);
  return extras.length > 0 ? `${stateText}；${extras.join("；")}` : stateText;
}

function weekdayLabel(value: string) {
  const labels: Record<string, string> = {
    "1": "一",
    "2": "二",
    "3": "三",
    "4": "四",
    "5": "五",
    "6": "六",
    "7": "日"
  };
  return labels[value] ?? "一";
}

function normalizeCollectionSchedule(value: Partial<CollectionSchedule> | undefined): CollectionSchedule {
  const frequency =
    value?.frequency === "daily" || value?.frequency === "trade_day" || value?.frequency === "weekly"
      ? value.frequency
      : DEFAULT_COLLECTION_SCHEDULE.frequency;
  const time = typeof value?.time === "string" && /^\d{2}:\d{2}$/.test(value.time)
    ? value.time
    : DEFAULT_COLLECTION_SCHEDULE.time;
  const weekday = typeof value?.weekday === "string" && /^[1-7]$/.test(value.weekday)
    ? value.weekday
    : DEFAULT_COLLECTION_SCHEDULE.weekday;
  return { frequency, time, weekday };
}

async function ensureTradeCalendarCache(apiBase: string): Promise<TradeCalendarCacheStatus> {
  const status = await fetchTradeCalendarCacheStatus(apiBase);
  if (status.covers_today) {
    return status;
  }
  return refreshTradeCalendarCache(apiBase, {});
}

async function fetchTradeCalendarCacheStatus(apiBase: string): Promise<TradeCalendarCacheStatus> {
  const response = await apiFetch(`${apiBase}/v1/trade-calendar/cache`, {
    headers: { Accept: "application/json" }
  });
  return parseTradeCalendarCacheResponse(response);
}

async function refreshTradeCalendarCache(
  apiBase: string,
  body: Record<string, unknown>
): Promise<TradeCalendarCacheStatus> {
  const response = await apiFetch(`${apiBase}/v1/trade-calendar/cache/refresh`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });
  return parseTradeCalendarCacheResponse(response);
}

async function fetchTradeCalendarMaintenanceConfig(apiBase: string): Promise<TradeCalendarMaintenanceConfig> {
  const response = await apiFetch(`${apiBase}/v1/trade-calendar/maintenance`, {
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function saveTradeCalendarMaintenanceConfig(
  apiBase: string,
  config: TradeCalendarMaintenanceConfig
): Promise<TradeCalendarMaintenanceConfig> {
  const response = await apiFetch(`${apiBase}/v1/trade-calendar/maintenance`, {
    method: "PUT",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(config)
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function fetchDownloadScheduleStatus(
  apiBase: string,
  interfaceName: string,
  signal?: AbortSignal
): Promise<DownloadScheduleStatus | null> {
  const response = await apiFetch(`${apiBase}/v1/download/schedules/${interfaceName}`, {
    headers: { Accept: "application/json" },
    signal
  });
  const payload = await response.json();
  if (response.status === 404 || payload?.error?.code === "DOWNLOAD_SCHEDULE_NOT_FOUND") {
    return null;
  }
  if (!response.ok || payload?.success === false) {
    const message = payload?.error?.message ?? `HTTP ${response.status}`;
    throw new Error(message);
  }
  return payload.data ?? payload;
}

async function fetchTdxServerStatus(apiBase: string): Promise<{
  quote: TdxServerStatus;
  extended: TdxServerStatus;
  probe_schedule: TdxServerProbeSchedule;
}> {
  const response = await apiFetch(`${apiBase}/v1/tdx/servers`, {
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function saveTdxServerStatus(apiBase: string, kind: TdxServerKind, rows: TdxServerRow[]): Promise<TdxServerStatus> {
  const response = await apiFetch(`${apiBase}/v1/tdx/servers/${kind}`, {
    method: "PUT",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      servers: rows.map((row, index) => ({
        name: row.name,
        host: row.host,
        port: Number(row.port),
        enabled: row.enabled,
        priority: index + 1
      }))
    })
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function probeTdxServerStatus(apiBase: string, kind: TdxServerKind): Promise<TdxServerStatus> {
  const response = await apiFetch(`${apiBase}/v1/tdx/servers/${kind}/probe`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ timeout: 1.2, save: true })
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function resetTdxServerStatus(apiBase: string, kind: TdxServerKind): Promise<TdxServerStatus> {
  const response = await apiFetch(`${apiBase}/v1/tdx/servers/${kind}/reset`, {
    method: "POST",
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function saveTdxProbeSchedule(apiBase: string, schedule: TdxServerProbeSchedule): Promise<TdxServerProbeSchedule> {
  const response = await apiFetch(`${apiBase}/v1/tdx/servers/probe-schedule`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(schedule)
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function disableTdxProbeSchedule(apiBase: string): Promise<TdxServerProbeSchedule> {
  const response = await apiFetch(`${apiBase}/v1/tdx/servers/probe-schedule`, {
    method: "DELETE",
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function previewAxpArchive(apiBase: string, file: File): Promise<AxpPreview> {
  const formData = new FormData();
  formData.append("file", file);
  const response = await apiFetch(`${apiBase}/v1/plugins/axp/preview`, {
    method: "POST",
    headers: { Accept: "application/json" },
    body: formData
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function installAxpArchive(
  apiBase: string,
  file: File,
  options: { enable?: boolean; replace?: boolean } = {}
): Promise<AxpInstallResult> {
  const formData = new FormData();
  formData.append("file", file);
  if (options.enable) {
    formData.append("enable", "true");
  }
  if (options.replace) {
    formData.append("replace", "true");
  }
  const response = await apiFetch(`${apiBase}/v1/plugins/axp/install`, {
    method: "POST",
    headers: { Accept: "application/json" },
    body: formData
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function exportAxpArchive(apiBase: string, pluginId: string): Promise<string> {
  const response = await apiFetch(`${apiBase}/v1/plugins/axp/export/${encodeURIComponent(pluginId)}`, {
    headers: { Accept: "application/octet-stream" }
  });
  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const payload = await response.json();
      message = payload?.error?.message ?? payload?.detail ?? message;
    } catch {
      // Binary downloads return no JSON on success; keep the HTTP status fallback for failed non-JSON responses.
    }
    if (response.status === 404 && message === "Not Found") {
      message = "后端还没加载 AXP 导出接口，请重启 API 服务后再试。";
    }
    throw new Error(message);
  }
  const blob = await response.blob();
  const fileName = contentDispositionFileName(response.headers.get("Content-Disposition")) ?? `${safeDownloadName(pluginId)}.axp`;
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => window.URL.revokeObjectURL(url), 0);
  return fileName;
}

async function fetchInstalledPlugins(apiBase: string): Promise<InstalledPlugin[]> {
  const response = await apiFetch(`${apiBase}/v1/plugins/installed`, {
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function fetchLocalDiagnostics(apiBase: string, endpoint: "status" | "doctor"): Promise<LocalDiagnosticsPayload> {
  const response = await apiFetch(`${apiBase}/v1/${endpoint}`, {
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function fetchCollectorTasks(apiBase: string): Promise<CollectorTaskStatus[]> {
  const response = await apiFetch(`${apiBase}/v1/collector/tasks`, {
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function fetchCollectorTaskTemplates(apiBase: string): Promise<CollectorTaskTemplateStatus[]> {
  const response = await apiFetch(`${apiBase}/v1/collector/tasks/templates`, {
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function fetchCollectorStatus(apiBase: string): Promise<CollectorSchedulerStatus> {
  const response = await apiFetch(`${apiBase}/v1/collector/status`, {
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function fetchCollectorRuns(apiBase: string, limit = 20): Promise<CollectorRunStatus[]> {
  const response = await apiFetch(`${apiBase}/v1/collector/runs?limit=${encodeURIComponent(String(limit))}`, {
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function runCollectorTask(apiBase: string, taskId: string): Promise<CollectorRunStatus> {
  const response = await apiFetch(`${apiBase}/v1/collector/tasks/${encodeURIComponent(taskId)}/run`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ trigger_type: "manual" })
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function backfillCollectorTask(
  apiBase: string,
  taskId: string,
  request: { start: string; end: string; symbol?: string; limit?: number }
): Promise<CollectorRunStatus> {
  const response = await apiFetch(`${apiBase}/v1/collector/tasks/${encodeURIComponent(taskId)}/backfill`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      start: request.start,
      end: request.end,
      symbol: request.symbol || undefined,
      limit: request.limit
    })
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function createCollectorTaskFromTemplate(
  apiBase: string,
  templateId: string
): Promise<CollectorTaskStatus> {
  const response = await apiFetch(`${apiBase}/v1/collector/tasks/from-template`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ template_id: templateId })
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function createCollectorTask(
  apiBase: string,
  request: {
    collectorName: string;
    enabled: boolean;
    fields?: string[];
    formats?: string[];
    name: string;
    params: Record<string, unknown>;
    execution?: CollectorExecutionSettings;
    triggerType: string;
  }
): Promise<CollectorTaskStatus> {
  const execution = request.execution;
  const response = await apiFetch(`${apiBase}/v1/collector/tasks`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      collector_name: request.collectorName,
      enabled: request.enabled,
      fields: request.fields,
      formats: request.formats,
      name: request.name,
      params: request.params,
      connection_count: execution?.maxConcurrentTasks,
      source_server_count: execution?.sourceServerCount,
      connections_per_server: execution?.connectionsPerServer,
      max_concurrent_tasks: execution?.maxConcurrentTasks,
      trigger_type: request.triggerType
    })
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function setCollectorTaskEnabled(
  apiBase: string,
  taskId: string,
  enabled: boolean
): Promise<CollectorTaskStatus> {
  const action = enabled ? "enable" : "disable";
  const response = await apiFetch(`${apiBase}/v1/collector/tasks/${encodeURIComponent(taskId)}/${action}`, {
    method: "POST",
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function updateCollectorTask(
  apiBase: string,
  taskId: string,
  updates: Record<string, unknown>
): Promise<CollectorTaskStatus> {
  const response = await apiFetch(`${apiBase}/v1/collector/tasks/${encodeURIComponent(taskId)}`, {
    method: "PATCH",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(updates)
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function deleteCollectorTask(
  apiBase: string,
  taskId: string
): Promise<CollectorTaskStatus> {
  const response = await apiFetch(`${apiBase}/v1/collector/tasks/${encodeURIComponent(taskId)}`, {
    method: "DELETE",
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function deleteCollectorRun(apiBase: string, runId: string): Promise<CollectorRunStatus> {
  const response = await apiFetch(`${apiBase}/v1/collector/runs/${encodeURIComponent(runId)}`, {
    method: "DELETE",
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function uninstallInstalledPlugin(apiBase: string, providerId: string, disableFirst = false): Promise<Record<string, unknown>> {
  const query = disableFirst ? "?disable_first=true" : "";
  const response = await apiFetch(`${apiBase}/v1/plugins/installed/${encodeURIComponent(providerId)}${query}`, {
    method: "DELETE",
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function fetchApiTokens(apiBase: string): Promise<ApiTokenRecord[]> {
  const response = await apiFetch(`${apiBase}/v1/auth/tokens`, {
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function createApiToken(
  apiBase: string,
  name: string
): Promise<{ token?: string; record?: ApiTokenRecord }> {
  const response = await apiFetch(`${apiBase}/v1/auth/tokens`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ name })
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function revokeApiToken(apiBase: string, tokenId: string): Promise<ApiTokenRecord> {
  const response = await apiFetch(`${apiBase}/v1/auth/tokens/${encodeURIComponent(tokenId)}`, {
    method: "DELETE",
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function saveRuntimeConfig(
  apiBase: string,
  config: { api_host: string; api_port: number; web_port: number }
): Promise<Record<string, unknown>> {
  const response = await apiFetch(`${apiBase}/v1/config/runtime`, {
    method: "PUT",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(config)
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function restartApiBackend(apiBase: string): Promise<Record<string, unknown>> {
  const response = await apiFetch(`${apiBase}/v1/config/restart-api`, {
    method: "POST",
    headers: { Accept: "application/json" }
  });
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function waitForRuntimeConfig(
  apiBase: string,
  expected: { api_host: string; api_port: number; web_port: number },
  timeoutMs = 15000
): Promise<RuntimeConfig> {
  const startedAt = Date.now();
  let lastError: unknown = null;
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await apiFetch(`${apiBase}/v1/config`, {
        headers: { Accept: "application/json" }
      });
      if (response.ok) {
        const payload = (await response.json()) as { data?: RuntimeConfig } & RuntimeConfig;
        const config = payload.data ?? payload;
        if (
          config.api_host === expected.api_host &&
          Number(config.api_port) === expected.api_port &&
          Number(config.web_port) === expected.web_port
        ) {
          return config;
        }
      }
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => window.setTimeout(resolve, 1000));
  }
  throw new Error(
    lastError instanceof Error
      ? `后端没有在预期时间内恢复：${lastError.message}`
      : "后端没有在预期时间内恢复。请查看后端日志或手动重启。"
  );
}

async function parseTradeCalendarCacheResponse(response: Response): Promise<TradeCalendarCacheStatus> {
  const payload = await parseJsonResponse(response);
  return payload.data ?? payload;
}

async function parseJsonResponse(response: Response): Promise<any> {
  const payload = await response.json();
  if (!response.ok || payload?.success === false) {
    const message = payload?.error?.message ?? payload?.detail ?? `HTTP ${response.status}`;
    throw new Error(message);
  }
  return payload;
}

function contentDispositionFileName(value: string | null): string | null {
  if (!value) {
    return null;
  }
  const encoded = value.match(/filename\*=UTF-8''([^;]+)/i);
  if (encoded?.[1]) {
    try {
      return decodeURIComponent(encoded[1].trim().replace(/^"|"$/g, ""));
    } catch {
      return encoded[1].trim().replace(/^"|"$/g, "");
    }
  }
  const plain = value.match(/filename="?([^";]+)"?/i);
  return plain?.[1]?.trim() || null;
}

function safeDownloadName(value: string): string {
  const name = value.trim().replace(/[^a-zA-Z0-9_.-]+/g, "-").replace(/[._-]+$/g, "");
  return name || "axdata-plugin";
}

function normalizePathPart(value: string) {
  return value
    .split("/")
    .map((part) => part.trim())
    .filter(Boolean)
    .join("/");
}

function isLoopbackApiBase(value: string) {
  try {
    const hostname = new URL(value).hostname.toLowerCase();
    return hostname === "127.0.0.1" || hostname === "localhost" || hostname === "::1";
  } catch {
    return false;
  }
}

function maskTokenValue(value: string) {
  if (value.length <= 14) {
    return "••••••••";
  }
  return `${value.slice(0, 8)}••••••••••••${value.slice(-6)}`;
}

type SavedDownloadConfig = {
  outputDir: string;
  params: Record<string, string>;
  concurrency: Partial<ConcurrencySettings>;
  formats: string[];
  collectionMode: CollectionMode;
  schedule: CollectionSchedule;
};

function downloadConfigStorageKey(interfaceName: string) {
  return `${DOWNLOAD_CONFIG_STORAGE_PREFIX}${interfaceName}`;
}

function loadSavedDownloadConfig(interfaceName: string): SavedDownloadConfig | null {
  if (typeof window === "undefined") {
    return null;
  }
  const raw = window.localStorage.getItem(downloadConfigStorageKey(interfaceName));
  if (!raw) {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as Partial<SavedDownloadConfig>;
    const formats = Array.isArray(parsed.formats)
      ? parsed.formats.filter((item) => ["parquet", "csv", "duckdb", "jsonl"].includes(item))
      : [];
    const legacyConnectionCount = Number((parsed as { connectionCount?: unknown }).connectionCount);
    const parsedConcurrency = isRecord(parsed.concurrency) ? parsed.concurrency : {};
    return {
      outputDir:
        typeof parsed.outputDir === "string"
          ? parsed.outputDir
          : typeof (parsed as { outputRoot?: unknown }).outputRoot === "string"
            ? String((parsed as { outputRoot?: unknown }).outputRoot)
            : "",
      params: isStringRecord(parsed.params)
        ? parsed.params
        : typeof (parsed as { scope?: unknown }).scope === "string"
          ? { scope: String((parsed as { scope?: unknown }).scope) }
          : {},
      concurrency: {
        mode: typeof parsedConcurrency.mode === "string" ? parsedConcurrency.mode : undefined,
        sourceServerCount: numberOr(parsedConcurrency.sourceServerCount, 1),
        connectionsPerServer: numberOr(parsedConcurrency.connectionsPerServer, Number.isFinite(legacyConnectionCount) ? legacyConnectionCount : 1),
        maxConcurrentTasks: numberOr(parsedConcurrency.maxConcurrentTasks, Number.isFinite(legacyConnectionCount) ? legacyConnectionCount : 1),
        batchSize: numberOr(parsedConcurrency.batchSize, 1),
        requestIntervalMs: numberOr(parsedConcurrency.requestIntervalMs, 0),
        retryCount: numberOr(parsedConcurrency.retryCount, 0),
        timeoutMs: numberOr(parsedConcurrency.timeoutMs, 30000)
      },
      formats: formats.length > 0 ? formats : ["parquet"],
      collectionMode: parsed.collectionMode === "auto" ? "auto" : "manual",
      schedule: normalizeCollectionSchedule(isRecord(parsed.schedule) ? parsed.schedule : undefined)
    };
  } catch {
    return null;
  }
}

function saveSavedDownloadConfig(interfaceName: string, config: SavedDownloadConfig) {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(downloadConfigStorageKey(interfaceName), JSON.stringify(config));
}

function isStringRecord(value: unknown): value is Record<string, string> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.values(value).every((item) => typeof item === "string")
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function SettingsPage({
  apiBase,
  health,
  onRuntimeConfigRefresh,
  page,
  runtimeConfig
}: {
  apiBase: string;
  health: HealthPayload | null;
  onRuntimeConfigRefresh: () => void | Promise<void>;
  page: InfoPage;
  runtimeConfig: RuntimeConfig | null;
}) {
  const [tdxQuoteStatus, setTdxQuoteStatus] = useState<TdxServerStatus | null>(null);
  const [tdxExtStatus, setTdxExtStatus] = useState<TdxServerStatus | null>(null);
  const [tdxQuoteDrafts, setTdxQuoteDrafts] = useState<TdxServerRow[]>([]);
  const [tdxExtDrafts, setTdxExtDrafts] = useState<TdxServerRow[]>([]);
  const [tdxServerError, setTdxServerError] = useState<string | null>(null);
  const [tdxServerMessage, setTdxServerMessage] = useState<string | null>(null);
  const [tdxServerBusy, setTdxServerBusy] = useState<string | null>(null);
  const [tdxProbeSchedule, setTdxProbeSchedule] = useState<TdxServerProbeSchedule>(defaultTdxProbeSchedule());
  const [apiTokens, setApiTokens] = useState<ApiTokenRecord[]>([]);
  const [newTokenName, setNewTokenName] = useState("");
  const [tokenMessage, setTokenMessage] = useState<string | null>(null);
  const [tokenError, setTokenError] = useState<string | null>(null);
  const [tokenBusy, setTokenBusy] = useState<string | null>(null);
  const [runtimeApiHost, setRuntimeApiHost] = useState(() => readLocalRuntimeDraft().api_host ?? "127.0.0.1");
  const [runtimeApiPort, setRuntimeApiPort] = useState(() => String(readLocalRuntimeDraft().api_port ?? 8666));
  const [runtimeWebPort, setRuntimeWebPort] = useState(() => String(readLocalRuntimeDraft().web_port ?? 8667));
  const [runtimeConfigMessage, setRuntimeConfigMessage] = useState<string | null>(null);
  const [runtimeConfigError, setRuntimeConfigError] = useState<string | null>(null);
  const [runtimeConfigBusy, setRuntimeConfigBusy] = useState<"restart" | null>(null);
  const [tradeCalendarStatus, setTradeCalendarStatus] = useState<TradeCalendarCacheStatus | null>(null);
  const [tradeCalendarStartDate, setTradeCalendarStartDate] = useState("");
  const [tradeCalendarEndDate, setTradeCalendarEndDate] = useState("");
  const [tradeCalendarMessage, setTradeCalendarMessage] = useState<string | null>(null);
  const [tradeCalendarError, setTradeCalendarError] = useState<string | null>(null);
  const [tradeCalendarBusy, setTradeCalendarBusy] = useState(false);
  const [tradeCalendarMaintenance, setTradeCalendarMaintenance] = useState<TradeCalendarMaintenanceConfig | null>(null);
  const [tradeCalendarMaintenanceEnabled, setTradeCalendarMaintenanceEnabled] = useState(false);
  const [tradeCalendarMaintenanceTime, setTradeCalendarMaintenanceTime] = useState("22:30");
  const [tradeCalendarMaintenancePastDays, setTradeCalendarMaintenancePastDays] = useState("30");
  const [tradeCalendarMaintenanceFutureDays, setTradeCalendarMaintenanceFutureDays] = useState("180");
  const [tradeCalendarMaintenanceBusy, setTradeCalendarMaintenanceBusy] = useState(false);
  const [tradeCalendarMaintenanceMessage, setTradeCalendarMaintenanceMessage] = useState<string | null>(null);
  const [tradeCalendarMaintenanceError, setTradeCalendarMaintenanceError] = useState<string | null>(null);

  useEffect(() => {
    if (!runtimeConfig) {
      return;
    }
    const nextStart = runtimeConfig?.next_start ?? runtimeConfig ?? {};
    setRuntimeApiHost(String(nextStart.api_host ?? "127.0.0.1"));
    setRuntimeApiPort(String(nextStart.api_port ?? 8666));
    setRuntimeWebPort(String(nextStart.web_port ?? 8667));
    saveLocalRuntimeDraft({
      api_host: String(nextStart.api_host ?? "127.0.0.1"),
      api_port: Number(nextStart.api_port ?? 8666),
      web_port: Number(nextStart.web_port ?? 8667)
    });
  }, [runtimeConfig]);

  async function refreshApiTokens() {
    try {
      const rows = await fetchApiTokens(apiBase);
      setApiTokens(rows);
      setTokenError(null);
    } catch (error) {
      setApiTokens([]);
      setTokenError(error instanceof Error ? error.message : "API token 列表读取失败");
    }
  }

  useEffect(() => {
    if (page.id !== "access") {
      return;
    }
    refreshApiTokens();
  }, [apiBase, page.id]);

  async function createApiTokenAction() {
    const name = newTokenName.trim();
    if (!name) {
      setTokenError("请输入设备或脚本名称。");
      return;
    }
    setTokenBusy("create");
    setTokenError(null);
    setTokenMessage(null);
    try {
      const result = await createApiToken(apiBase, name);
      const record = result.record;
      setApiTokens((current) => record ? [record, ...current] : current);
      setNewTokenName("");
      setTokenMessage("token 已创建。可在下方列表查看或删除。");
      window.setTimeout(() => setTokenMessage(null), 5000);
      refreshApiTokens();
    } catch (error) {
      setTokenError(error instanceof Error ? error.message : "API token 创建失败");
    } finally {
      setTokenBusy(null);
    }
  }

  async function revokeApiTokenAction(tokenId: string) {
    setTokenBusy(tokenId);
    setTokenError(null);
    setTokenMessage(null);
    try {
      await revokeApiToken(apiBase, tokenId);
      setApiTokens((current) => current.filter((item) => item.id !== tokenId));
      setTokenMessage("token 已删除。已连接设备需要换用新的 token。");
      window.setTimeout(() => setTokenMessage(null), 4000);
    } catch (error) {
      setTokenError(error instanceof Error ? error.message : "API token 删除失败");
    } finally {
      setTokenBusy(null);
    }
  }

  async function saveAndRestartApiBackendAction() {
    const apiPort = Number(runtimeApiPort);
    const webPort = Number(runtimeWebPort);
    if (!["127.0.0.1", "0.0.0.0"].includes(runtimeApiHost)) {
      setRuntimeConfigError("API 监听只能选择 127.0.0.1 或 0.0.0.0。");
      return;
    }
    if (!Number.isInteger(apiPort) || apiPort < 1 || apiPort > 65535) {
      setRuntimeConfigError("API 端口必须是 1 到 65535 之间的数字。");
      return;
    }
    if (!Number.isInteger(webPort) || webPort < 1 || webPort > 65535) {
      setRuntimeConfigError("Web 端口必须是 1 到 65535 之间的数字。");
      return;
    }
    setRuntimeConfigBusy("restart");
    setRuntimeConfigError(null);
    setRuntimeConfigMessage(null);
    try {
      const saveResult = await saveRuntimeConfig(apiBase, {
        api_host: runtimeApiHost,
        api_port: apiPort,
        web_port: webPort
      });
      saveLocalApiPort(apiPort);
      saveLocalRuntimeDraft({
        api_host: runtimeApiHost,
        api_port: apiPort,
        web_port: webPort
      });
      if (saveResult.restart_supported !== true) {
        setRuntimeConfigError(
          "已保存为下次启动配置，但当前后端不是由 AxData 启动器托管，Web 不能自动重启。请先停止当前后端，再运行 npm run dev:api 启动。"
        );
        await onRuntimeConfigRefresh();
        return;
      }
      const result = await restartApiBackend(apiBase);
      if (result.accepted === false) {
        setRuntimeConfigError(
          typeof result.message === "string"
            ? `已保存为下次启动配置，但还没有重启生效。${result.message}`
            : "已保存为下次启动配置，但当前后端不是由 AxData 启动器托管，Web 不能自动重启。请先停止当前后端，再运行 npm run dev:api 启动。"
        );
        await onRuntimeConfigRefresh();
        return;
      }
      setRuntimeConfigMessage(
        typeof result.message === "string"
          ? result.message
          : "已请求重启后端。几秒后页面会自动重新连接。"
      );
      const apiPortChanged = apiPort !== Number(runtimeConfig?.api_port ?? apiPort);
      if (apiPortChanged) {
        window.setTimeout(() => window.location.reload(), 4500);
      } else {
        setRuntimeConfigMessage("已请求重启后端，正在等待后端真实恢复...");
        await waitForRuntimeConfig(apiBase, {
          api_host: runtimeApiHost,
          api_port: apiPort,
          web_port: webPort
        });
        await onRuntimeConfigRefresh();
        setRuntimeConfigMessage("后端已重启并生效。");
      }
    } catch (error) {
      setRuntimeConfigError(error instanceof Error ? error.message : "保存并重启后端失败");
    } finally {
      window.setTimeout(() => setRuntimeConfigBusy(null), 1200);
    }
  }

  useEffect(() => {
    if (page.id !== "tdx_servers") {
      return;
    }
    let isActive = true;
    fetchTdxServerStatus(apiBase)
      .then((payload) => {
        if (!isActive) {
          return;
        }
        setTdxQuoteStatus(payload.quote);
        setTdxExtStatus(payload.extended);
        setTdxQuoteDrafts(payload.quote.servers);
        setTdxExtDrafts(payload.extended.servers);
        setTdxProbeSchedule(normalizeTdxProbeSchedule(payload.probe_schedule));
        setTdxServerError(null);
      })
      .catch((error) => {
        if (isActive) {
          setTdxServerError(error instanceof Error ? error.message : "服务器列表读取失败");
        }
      });
    return () => {
      isActive = false;
    };
  }, [apiBase, page.id]);

  useEffect(() => {
    if (page.id !== "base_data") {
      return;
    }
    let isActive = true;
    Promise.all([
      fetchTradeCalendarCacheStatus(apiBase),
      fetchTradeCalendarMaintenanceConfig(apiBase)
    ])
      .then(([status, maintenance]) => {
        if (!isActive) {
          return;
        }
        setTradeCalendarStatus(status);
        applyTradeCalendarMaintenanceConfig(maintenance);
        setTradeCalendarError(null);
        setTradeCalendarMaintenanceError(null);
      })
      .catch((error) => {
        if (isActive) {
          setTradeCalendarStatus(null);
          setTradeCalendarError(error instanceof Error ? error.message : "交易日历状态读取失败");
        }
      });
    return () => {
      isActive = false;
    };
  }, [apiBase, page.id]);

  function applyTradeCalendarMaintenanceConfig(config: TradeCalendarMaintenanceConfig) {
    setTradeCalendarMaintenance(config);
    setTradeCalendarMaintenanceEnabled(Boolean(config.enabled));
    setTradeCalendarMaintenanceTime(config.time ?? "22:30");
    setTradeCalendarMaintenancePastDays(String(config.past_days ?? 30));
    setTradeCalendarMaintenanceFutureDays(String(config.future_days ?? 180));
  }

  async function refreshTradeCalendarAction(mode: "default" | "custom") {
    setTradeCalendarBusy(true);
    setTradeCalendarError(null);
    setTradeCalendarMessage(mode === "custom" ? "正在补全指定范围..." : "正在同步交易日历...");
    try {
      const body =
        mode === "custom"
          ? {
              start_date: tradeCalendarStartDate.trim() || undefined,
              end_date: tradeCalendarEndDate.trim() || undefined
            }
          : {};
      if (mode === "custom" && !body.start_date && !body.end_date) {
        throw new Error("请至少填写开始日期或结束日期。");
      }
      const status = await refreshTradeCalendarCache(apiBase, body);
      setTradeCalendarStatus(status);
      setTradeCalendarMessage(formatCalendarRefreshResult(status));
      window.setTimeout(() => setTradeCalendarMessage(null), 4000);
    } catch (error) {
      setTradeCalendarError(error instanceof Error ? error.message : "交易日历同步失败");
      setTradeCalendarMessage(null);
    } finally {
      setTradeCalendarBusy(false);
    }
  }

  async function saveTradeCalendarMaintenanceAction() {
    const pastDays = Number(tradeCalendarMaintenancePastDays);
    const futureDays = Number(tradeCalendarMaintenanceFutureDays);
    const [maintenanceHour, maintenanceMinute] = tradeCalendarMaintenanceTime.split(":").map(Number);
    if (
      !/^\d{2}:\d{2}$/.test(tradeCalendarMaintenanceTime) ||
      !Number.isInteger(maintenanceHour) ||
      !Number.isInteger(maintenanceMinute) ||
      maintenanceHour < 0 ||
      maintenanceHour > 23 ||
      maintenanceMinute < 0 ||
      maintenanceMinute > 59
    ) {
      setTradeCalendarMaintenanceError("维护时间必须是 HH:MM，例如 22:30。");
      return;
    }
    if (![pastDays, futureDays].every((value) => Number.isInteger(value) && value >= 0 && value <= 3650)) {
      setTradeCalendarMaintenanceError("维护天数必须是 0 到 3650 之间的整数。");
      return;
    }
    setTradeCalendarMaintenanceBusy(true);
    setTradeCalendarMaintenanceError(null);
    setTradeCalendarMaintenanceMessage(null);
    try {
      const saved = await saveTradeCalendarMaintenanceConfig(apiBase, {
        enabled: tradeCalendarMaintenanceEnabled,
        time: tradeCalendarMaintenanceTime,
        past_days: pastDays,
        future_days: futureDays,
        recheck_past_days: Math.min(7, pastDays)
      });
      applyTradeCalendarMaintenanceConfig(saved);
      setTradeCalendarMaintenanceMessage(tradeCalendarMaintenanceEnabled ? "每日自动维护已保存。" : "每日自动维护已关闭。");
      window.setTimeout(() => setTradeCalendarMaintenanceMessage(null), 4000);
    } catch (error) {
      setTradeCalendarMaintenanceError(error instanceof Error ? error.message : "每日维护设置保存失败");
    } finally {
      setTradeCalendarMaintenanceBusy(false);
    }
  }

  async function runTdxServerAction(
    kind: TdxServerKind,
    action: "save" | "probe" | "reset"
  ) {
    const busyKey = `${kind}.${action}`;
    setTdxServerBusy(busyKey);
    setTdxServerError(null);
    setTdxServerMessage(null);
    try {
      let next: TdxServerStatus;
      if (action === "save") {
        const drafts = kind === "quote" ? tdxQuoteDrafts : tdxExtDrafts;
        next = await saveTdxServerStatus(apiBase, kind, drafts);
      } else if (action === "probe") {
        next = await probeTdxServerStatus(apiBase, kind);
      } else if (action === "reset") {
        next = await resetTdxServerStatus(apiBase, kind);
      } else {
        throw new Error("未知服务器操作");
      }
      if (kind === "quote") {
        setTdxQuoteStatus(next);
        setTdxQuoteDrafts(next.servers);
      } else {
        setTdxExtStatus(next);
        setTdxExtDrafts(next.servers);
      }
      setTdxServerMessage(formatTdxServerActionMessage(kind, action));
      window.setTimeout(() => setTdxServerMessage(null), 3000);
    } catch (error) {
      setTdxServerError(error instanceof Error ? error.message : "服务器配置操作失败");
    } finally {
      setTdxServerBusy(null);
    }
  }

  async function saveProbeScheduleConfig(nextSchedule?: TdxServerProbeSchedule) {
    const schedule = normalizeTdxProbeSchedule(nextSchedule ?? tdxProbeSchedule);
    setTdxServerBusy("probe_schedule.save");
    setTdxServerError(null);
    setTdxServerMessage(null);
    try {
      const saved = await saveTdxProbeSchedule(apiBase, { ...schedule, enabled: true });
      setTdxProbeSchedule(normalizeTdxProbeSchedule(saved));
      setTdxServerMessage(`已开启定时测速：${formatTdxProbeSchedule(saved)}`);
      window.setTimeout(() => setTdxServerMessage(null), 3000);
    } catch (error) {
      setTdxServerError(error instanceof Error ? error.message : "定时测速保存失败");
    } finally {
      setTdxServerBusy(null);
    }
  }

  async function disableProbeScheduleConfig() {
    setTdxServerBusy("probe_schedule.disable");
    setTdxServerError(null);
    setTdxServerMessage(null);
    try {
      const saved = await disableTdxProbeSchedule(apiBase);
      setTdxProbeSchedule(normalizeTdxProbeSchedule(saved));
      setTdxServerMessage("已关闭定时测速");
      window.setTimeout(() => setTdxServerMessage(null), 2500);
    } catch (error) {
      setTdxServerError(error instanceof Error ? error.message : "定时测速关闭失败");
    } finally {
      setTdxServerBusy(null);
    }
  }

  function importTdxServerRows(kind: TdxServerKind, rows: TdxServerRow[]) {
    if (kind === "quote") {
      setTdxQuoteDrafts(rows);
    } else {
      setTdxExtDrafts(rows);
    }
    setTdxServerError(null);
    setTdxServerMessage("已导入到当前草稿，点击保存后生效");
    window.setTimeout(() => setTdxServerMessage(null), 3000);
  }

  function clearTdxServerRows(kind: TdxServerKind) {
    if (kind === "quote") {
      setTdxQuoteDrafts([]);
    } else {
      setTdxExtDrafts([]);
    }
    setTdxServerError(null);
    setTdxServerMessage("已清空当前草稿；可恢复内置或重新导入");
    window.setTimeout(() => setTdxServerMessage(null), 3000);
  }

  return (
    <InfoPageView page={page} showCode={false} showDetails={false} showReadyBadge={false}>
      {page.id === "access" ? (
        <AccessConnectionPanel
          apiBase={apiBase}
          apiTokens={apiTokens}
          health={health}
          newTokenName={newTokenName}
          onCreateApiToken={createApiTokenAction}
          onNewTokenNameChange={setNewTokenName}
          onRefreshApiTokens={refreshApiTokens}
          onRevokeApiToken={revokeApiTokenAction}
          onSaveAndRestartApiBackend={saveAndRestartApiBackendAction}
          onRuntimeApiHostChange={setRuntimeApiHost}
          onRuntimeApiPortChange={setRuntimeApiPort}
          onRuntimeWebPortChange={setRuntimeWebPort}
          runtimeApiHost={runtimeApiHost}
          runtimeApiPort={runtimeApiPort}
          runtimeConfig={runtimeConfig}
          runtimeConfigBusy={runtimeConfigBusy}
          runtimeConfigError={runtimeConfigError}
          runtimeConfigMessage={runtimeConfigMessage}
          runtimeWebPort={runtimeWebPort}
          tokenBusy={tokenBusy}
          tokenError={tokenError}
          tokenMessage={tokenMessage}
        />
      ) : null}

      {page.id === "base_data" ? (
        <TradeCalendarCachePanel
          endDate={tradeCalendarEndDate}
          error={tradeCalendarError}
          isRefreshing={tradeCalendarBusy}
          maintenance={tradeCalendarMaintenance}
          maintenanceBusy={tradeCalendarMaintenanceBusy}
          maintenanceEnabled={tradeCalendarMaintenanceEnabled}
          maintenanceError={tradeCalendarMaintenanceError}
          maintenanceFutureDays={tradeCalendarMaintenanceFutureDays}
          maintenanceMessage={tradeCalendarMaintenanceMessage}
          maintenancePastDays={tradeCalendarMaintenancePastDays}
          maintenanceTime={tradeCalendarMaintenanceTime}
          onEndDateChange={setTradeCalendarEndDate}
          onMaintenanceEnabledChange={setTradeCalendarMaintenanceEnabled}
          onMaintenanceFutureDaysChange={setTradeCalendarMaintenanceFutureDays}
          onMaintenancePastDaysChange={setTradeCalendarMaintenancePastDays}
          onMaintenanceSave={saveTradeCalendarMaintenanceAction}
          onMaintenanceTimeChange={setTradeCalendarMaintenanceTime}
          onRefresh={refreshTradeCalendarAction}
          onStartDateChange={setTradeCalendarStartDate}
          refreshMessage={tradeCalendarMessage}
          startDate={tradeCalendarStartDate}
          status={tradeCalendarStatus}
        />
      ) : null}

      {page.id === "tdx_servers" ? (
        <TdxServersPanel
          busyKey={tdxServerBusy}
          error={tdxServerError}
          extDrafts={tdxExtDrafts}
          extStatus={tdxExtStatus}
          message={tdxServerMessage}
          onAction={runTdxServerAction}
          onProbeScheduleChange={setTdxProbeSchedule}
          onProbeScheduleDisable={disableProbeScheduleConfig}
          onProbeScheduleSave={() => saveProbeScheduleConfig()}
          onExtDraftsChange={setTdxExtDrafts}
          onImportRows={importTdxServerRows}
          onQuoteDraftsChange={setTdxQuoteDrafts}
          onRowsClear={clearTdxServerRows}
          probeSchedule={tdxProbeSchedule}
          quoteDrafts={tdxQuoteDrafts}
          quoteStatus={tdxQuoteStatus}
        />
      ) : null}
    </InfoPageView>
  );
}

export function DiagnosticsPage({
  apiBase,
  health,
  runtimeConfig
}: {
  apiBase: string;
  health: HealthPayload | null;
  runtimeConfig: RuntimeConfig | null;
}) {
  const [localDiagnostics, setLocalDiagnostics] = useState<LocalDiagnosticsState>({
    status: null,
    doctor: null,
    error: null
  });
  const [clearingRunId, setClearingRunId] = useState<string | null>(null);

  async function refreshLocalDiagnostics() {
    try {
      const [status, doctor] = await Promise.all([
        fetchLocalDiagnostics(apiBase, "status"),
        fetchLocalDiagnostics(apiBase, "doctor")
      ]);
      setLocalDiagnostics({ status, doctor, error: null });
    } catch (error) {
      setLocalDiagnostics({
        status: null,
        doctor: null,
        error: error instanceof Error ? error.message : "本地诊断读取失败"
      });
    }
  }

  async function clearFailedRunRecord(run: Record<string, unknown>) {
    const runId = typeof run.run_id === "string" ? run.run_id : "";
    if (!runId) {
      return;
    }
    const taskName = formatFailedRunTitle(run);
    if (!window.confirm(`清除「${taskName}」这条失败记录？\n\n只会删除这条历史运行记录，不会删除任务配置，也不会删除已采集的数据文件。`)) {
      return;
    }
    setClearingRunId(runId);
    try {
      await deleteCollectorRun(apiBase, runId);
      await refreshLocalDiagnostics();
    } catch (error) {
      setLocalDiagnostics((current) => ({
        ...current,
        error: error instanceof Error ? error.message : "清除采集运行记录失败"
      }));
    } finally {
      setClearingRunId(null);
    }
  }

  useEffect(() => {
    let isActive = true;
    async function refreshOnce() {
      try {
        const [status, doctor] = await Promise.all([
          fetchLocalDiagnostics(apiBase, "status"),
          fetchLocalDiagnostics(apiBase, "doctor")
        ]);
        if (isActive) {
          setLocalDiagnostics({ status, doctor, error: null });
        }
      } catch (error) {
        if (isActive) {
          setLocalDiagnostics({
            status: null,
            doctor: null,
            error: error instanceof Error ? error.message : "本地诊断读取失败"
          });
        }
      }
    }
    refreshOnce();
    const intervalId = window.setInterval(refreshOnce, 30000);
    return () => {
      isActive = false;
      window.clearInterval(intervalId);
    };
  }, [apiBase]);

  return (
    <>
      <DiagnosticsHero diagnostics={localDiagnostics} />
      <LocalDiagnosticsPanel
        apiBase={apiBase}
        clearingRunId={clearingRunId}
        diagnostics={localDiagnostics}
        health={health}
        onClearRunRecord={clearFailedRunRecord}
        onRefresh={refreshLocalDiagnostics}
        runtimeConfig={runtimeConfig}
      />
    </>
  );
}

type CollectorPluginSummary = {
  pluginId: string;
  collectors: CollectorStatus[];
  collectorCount: number;
  collectorIds: string[];
  datasetIds: string[];
  runnerEntryCount: number;
  lifecycleCounts: Record<string, number>;
  legacyCount: number;
  statuses: string[];
  firstCollectorId?: string;
  displayName?: string;
  sourceCode?: string;
  sourceNameZh?: string;
  description?: string;
};

export function PluginManagementPage({
  apiBase,
  collectorCatalog,
  onOpenCollector,
  onRuntimeCatalogRefresh,
  providerStatuses
}: {
  apiBase: string;
  collectorCatalog: CollectorStatus[];
  onOpenCollector: (collectorId?: string) => void;
  onRuntimeCatalogRefresh: () => Promise<void>;
  providerStatuses: ProviderStatus[];
}) {
  const [pluginAbilityFilter, setPluginAbilityFilter] = useState<PluginAbilityFilter>("all");
  const [providerBusyId, setProviderBusyId] = useState<string | null>(null);
  const [providerOverrideBusyKey, setProviderOverrideBusyKey] = useState<string | null>(null);
  const [providerMessage, setProviderMessage] = useState<string | null>(null);
  const [providerError, setProviderError] = useState<string | null>(null);
  const [axpFile, setAxpFile] = useState<File | null>(null);
  const [axpPreview, setAxpPreview] = useState<AxpPreview | null>(null);
  const [axpInstallResult, setAxpInstallResult] = useState<AxpInstallResult | null>(null);
  const [axpBusy, setAxpBusy] = useState<"preview" | "install" | null>(null);
  const [axpError, setAxpError] = useState<string | null>(null);
  const [axpMessage, setAxpMessage] = useState<string | null>(null);
  const [axpKeepEnabled, setAxpKeepEnabled] = useState(false);
  const [installedPlugins, setInstalledPlugins] = useState<InstalledPlugin[]>([]);
  const [installedPluginBusyId, setInstalledPluginBusyId] = useState<string | null>(null);
  const [installedPluginError, setInstalledPluginError] = useState<string | null>(null);
  const [installedPluginMessage, setInstalledPluginMessage] = useState<string | null>(null);
  const [pluginExportBusyId, setPluginExportBusyId] = useState<string | null>(null);
  const [pluginExportBusyLabel, setPluginExportBusyLabel] = useState<string | null>(null);
  const [pluginExportError, setPluginExportError] = useState<string | null>(null);
  const [pluginExportMessage, setPluginExportMessage] = useState<string | null>(null);
  const collectorPluginSummaries = useMemo(
    () => summarizeCollectorPlugins(collectorCatalog, providerStatuses),
    [collectorCatalog, providerStatuses]
  );
  const providerIds = useMemo(
    () => new Set(providerStatuses.map((provider) => provider.provider_id)),
    [providerStatuses]
  );
  const axpInstalledPlugin = useMemo(
    () => (
      axpPreview
        ? installedPlugins.find((plugin) => plugin.provider_id === axpPreview.provider_id) ?? null
        : null
    ),
    [axpPreview, installedPlugins]
  );
  const showSourcePlugins = pluginAbilityFilter === "all" || pluginAbilityFilter === "source";
  const showCollectorPlugins = pluginAbilityFilter === "all" || pluginAbilityFilter === "collector";

  async function refreshInstalledPlugins() {
    const plugins = await fetchInstalledPlugins(apiBase);
    setInstalledPlugins(plugins);
  }

  useEffect(() => {
    let isActive = true;
    fetchInstalledPlugins(apiBase)
      .then((plugins) => {
        if (isActive) {
          setInstalledPlugins(plugins);
          setInstalledPluginError(null);
        }
      })
      .catch((error) => {
        if (isActive) {
          setInstalledPluginError(error instanceof Error ? error.message : "已安装插件读取失败");
        }
      });
    return () => {
      isActive = false;
    };
  }, [apiBase]);

  async function runProviderAction(provider: ProviderStatus, action: "enable" | "disable") {
    setProviderBusyId(provider.provider_id);
    setProviderError(null);
    setProviderMessage(null);
    try {
      const response = await apiFetch(
        `${apiBase}/v1/plugins/providers/${encodeURIComponent(provider.provider_id)}/${action}`,
        {
          method: "POST",
          headers: { Accept: "application/json" }
        }
      );
      const payload = await response.json();
      if (!response.ok || payload?.success === false) {
        const message = payload?.error?.message ?? payload?.detail ?? `HTTP ${response.status}`;
        throw new Error(message);
      }
      await onRuntimeCatalogRefresh();
      setProviderMessage(`${provider.source_name_zh || provider.provider_id} 已${action === "enable" ? "启用" : "禁用"}`);
      window.setTimeout(() => setProviderMessage(null), 3000);
    } catch (error) {
      setProviderError(error instanceof Error ? error.message : "Provider 操作失败");
    } finally {
      setProviderBusyId(null);
    }
  }

  async function runProviderOverrideAction(
    interfaceName: string,
    provider: ProviderStatus,
    action: "set" | "clear"
  ) {
    const busyKey = `${interfaceName}:${provider.provider_id}`;
    setProviderOverrideBusyKey(busyKey);
    setProviderError(null);
    setProviderMessage(null);
    try {
      const response = await apiFetch(`${apiBase}/v1/plugins/overrides/${encodeURIComponent(interfaceName)}`, {
        method: action === "set" ? "POST" : "DELETE",
        headers: {
          Accept: "application/json",
          ...(action === "set" ? { "Content-Type": "application/json" } : {})
        },
        body: action === "set" ? JSON.stringify({ provider_id: provider.provider_id }) : undefined
      });
      const payload = await response.json();
      if (!response.ok || payload?.success === false) {
        const message = payload?.error?.message ?? payload?.detail ?? `HTTP ${response.status}`;
        throw new Error(message);
      }
      await onRuntimeCatalogRefresh();
      setProviderMessage(
        action === "set"
          ? `${interfaceName} 已指定到 ${provider.provider_id}`
          : `${interfaceName} 已清除指定来源`
      );
      window.setTimeout(() => setProviderMessage(null), 3000);
    } catch (error) {
      setProviderError(error instanceof Error ? error.message : "Provider 指定失败");
    } finally {
      setProviderOverrideBusyKey(null);
    }
  }

  function selectAxpFile(file: File | null) {
    setAxpFile(file);
    setAxpPreview(null);
    setAxpInstallResult(null);
    setAxpKeepEnabled(false);
    setAxpError(null);
    setAxpMessage(null);
  }

  async function runAxpPreview() {
    if (!axpFile) {
      setAxpError("请选择 .axp 文件");
      return;
    }
    setAxpBusy("preview");
    setAxpError(null);
    setAxpMessage(null);
    try {
      const [preview, plugins] = await Promise.all([
        previewAxpArchive(apiBase, axpFile),
        fetchInstalledPlugins(apiBase)
      ]);
      const installedPlugin = plugins.find((plugin) => plugin.provider_id === preview.provider_id);
      setInstalledPlugins(plugins);
      setAxpPreview(preview);
      setAxpInstallResult(null);
      setAxpKeepEnabled(Boolean(installedPlugin?.enabled));
      setAxpMessage(
        installedPlugin
          ? `${preview.source_name_zh || preview.provider_id} 已安装，可选择更新`
          : `${preview.source_name_zh || preview.provider_id} 预览完成`
      );
      window.setTimeout(() => setAxpMessage(null), 3000);
    } catch (error) {
      setAxpError(error instanceof Error ? error.message : "AXP 预览失败");
    } finally {
      setAxpBusy(null);
    }
  }

  async function runAxpInstall() {
    if (!axpFile) {
      setAxpError("请选择 .axp 文件");
      return;
    }
    setAxpBusy("install");
    setAxpError(null);
    setAxpMessage(null);
    try {
      const installedPlugin = axpPreview
        ? installedPlugins.find((plugin) => plugin.provider_id === axpPreview.provider_id)
        : null;
      const result = await installAxpArchive(apiBase, axpFile, {
        enable: Boolean(installedPlugin && axpKeepEnabled),
        replace: Boolean(installedPlugin)
      });
      setAxpInstallResult(result);
      setAxpPreview(result.preview);
      await onRuntimeCatalogRefresh();
      await refreshInstalledPlugins();
      const label = result.preview.source_name_zh || result.provider_id;
      setAxpMessage(
        result.replaced
          ? `${label} 已更新，${result.enabled ? "保持启用" : "当前禁用"}`
          : `${label} 已安装，默认未启用`
      );
    } catch (error) {
      setAxpError(error instanceof Error ? error.message : "AXP 安装失败");
    } finally {
      setAxpBusy(null);
    }
  }

  async function runPluginExport(pluginId: string, displayName?: string) {
    const label = displayName || pluginId;
    setPluginExportBusyId(pluginId);
    setPluginExportBusyLabel(label);
    setPluginExportError(null);
    setPluginExportMessage(null);
    setProviderError(null);
    setProviderMessage(null);
    try {
      const fileName = await exportAxpArchive(apiBase, pluginId);
      setPluginExportMessage(`${label} 已导出 AXP：${fileName}`);
      window.setTimeout(() => setPluginExportMessage(null), 3000);
    } catch (error) {
      setPluginExportError(error instanceof Error ? error.message : "AXP 导出失败");
    } finally {
      setPluginExportBusyId(null);
      setPluginExportBusyLabel(null);
    }
  }

  async function runInstalledPluginUninstall(plugin: InstalledPlugin) {
    if (plugin.can_uninstall === false) {
      setInstalledPluginError(plugin.uninstall_block_reason || "该插件不能在 AxData 内卸载。");
      return;
    }
    const confirmed = window.confirm(
      `${plugin.name || plugin.provider_id} 将从 AxData AXP 管理目录卸载。已采集的数据、metadata、日志不会被删除；相关接口和采集任务会变为不可用或依赖缺失。`
    );
    if (!confirmed) {
      return;
    }
    setInstalledPluginBusyId(plugin.provider_id);
    setInstalledPluginError(null);
    setInstalledPluginMessage(null);
    try {
      await uninstallInstalledPlugin(apiBase, plugin.provider_id, plugin.enabled);
      await onRuntimeCatalogRefresh();
      await refreshInstalledPlugins();
      setInstalledPluginMessage(`${plugin.name || plugin.provider_id} 已卸载`);
      window.setTimeout(() => setInstalledPluginMessage(null), 3000);
    } catch (error) {
      setInstalledPluginError(error instanceof Error ? error.message : "插件卸载失败");
    } finally {
      setInstalledPluginBusyId(null);
    }
  }

  async function runProviderUninstall(provider: ProviderStatus) {
    if (provider.can_uninstall === false) {
      setProviderError(provider.uninstall_block_reason || "该插件不能在 AxData 内卸载。");
      return;
    }
    const sourceLabel = formatInstallSource(provider.install_source, provider.built_in);
    const confirmed = window.confirm(
      `${provider.source_name_zh || provider.provider_id} 将从当前 AxData 管理状态卸载。${sourceLabel === "预装插件" ? "预装插件会隐藏能力并标记为已卸载，不物理删除随包代码。" : "AXP 管理插件会移除安装文件。"} 已采集数据、metadata 和 run history 不会被删除；相关任务会保留并显示依赖缺失。`
    );
    if (!confirmed) {
      return;
    }
    setProviderBusyId(provider.provider_id);
    setProviderError(null);
    setProviderMessage(null);
    try {
      await uninstallInstalledPlugin(apiBase, provider.provider_id, provider.enabled);
      await onRuntimeCatalogRefresh();
      await refreshInstalledPlugins();
      setProviderMessage(`${provider.source_name_zh || provider.provider_id} 已卸载`);
      window.setTimeout(() => setProviderMessage(null), 3000);
    } catch (error) {
      setProviderError(error instanceof Error ? error.message : "插件卸载失败");
    } finally {
      setProviderBusyId(null);
    }
  }

  return (
    <>
      <PluginManagementHero
        collectorPlugins={collectorPluginSummaries}
        providers={providerStatuses}
        installedPlugins={installedPlugins}
      />
      <AxpInstallPanel
        busy={axpBusy}
        error={axpError}
        file={axpFile}
        installedPlugin={axpInstalledPlugin}
        installResult={axpInstallResult}
        keepEnabled={axpKeepEnabled}
        message={axpMessage}
        onFileChange={selectAxpFile}
        onKeepEnabledChange={setAxpKeepEnabled}
        onInstall={runAxpInstall}
        onPreview={runAxpPreview}
        preview={axpPreview}
      />
      <InstalledPluginsPanel
        busyProviderId={installedPluginBusyId}
        error={installedPluginError}
        message={installedPluginMessage}
        onUninstall={runInstalledPluginUninstall}
        plugins={installedPlugins}
      />
      <PluginAbilityFilterPanel
        collectorCount={collectorPluginSummaries.length}
        onChange={setPluginAbilityFilter}
        sourceCount={providerStatuses.length}
        value={pluginAbilityFilter}
      />
      {pluginExportBusyLabel ? (
        <div className="calendar-cache-refresh-state">
          <Loader2 className="spin" size={16} />
          <span>{pluginExportBusyLabel} 正在打包 AXP，后端正在准备 manifest、wheel 和 checksum；完成后会自动弹出保存文件。</span>
        </div>
      ) : null}
      {pluginExportMessage ? (
        <div className="calendar-cache-refresh-state done">
          <CheckCircle2 size={16} />
          <span>{pluginExportMessage}</span>
        </div>
      ) : null}
      {pluginExportError ? <p className="form-error">{pluginExportError}</p> : null}
      {showSourcePlugins ? (
        <ProviderStatusPanel
          busyExportId={pluginExportBusyId}
          busyOverrideKey={providerOverrideBusyKey}
          busyProviderId={providerBusyId}
          error={providerError}
          message={providerMessage}
          onAction={runProviderAction}
          onExport={runPluginExport}
          onUninstall={runProviderUninstall}
          onOverrideAction={runProviderOverrideAction}
          providers={providerStatuses}
        />
      ) : null}
      {showCollectorPlugins ? (
        <CollectorPluginStatusPanel
          busyExportId={pluginExportBusyId}
          onExport={runPluginExport}
          onOpenCollector={onOpenCollector}
          providerIds={providerIds}
          summaries={collectorPluginSummaries}
        />
      ) : null}
    </>
  );
}

function AccessConnectionPanel({
  apiBase,
  apiTokens,
  health,
  newTokenName,
  onCreateApiToken,
  onNewTokenNameChange,
  onRefreshApiTokens,
  onRevokeApiToken,
  onRuntimeApiHostChange,
  onRuntimeApiPortChange,
  onRuntimeWebPortChange,
  onSaveAndRestartApiBackend,
  runtimeApiHost,
  runtimeApiPort,
  runtimeConfig,
  runtimeConfigBusy,
  runtimeConfigError,
  runtimeConfigMessage,
  runtimeWebPort,
  tokenBusy,
  tokenError,
  tokenMessage
}: {
  apiBase: string;
  apiTokens: ApiTokenRecord[];
  health: HealthPayload | null;
  newTokenName: string;
  onCreateApiToken: () => void;
  onNewTokenNameChange: (value: string) => void;
  onRefreshApiTokens: () => void;
  onRevokeApiToken: (tokenId: string) => void;
  onRuntimeApiHostChange: (value: string) => void;
  onRuntimeApiPortChange: (value: string) => void;
  onRuntimeWebPortChange: (value: string) => void;
  onSaveAndRestartApiBackend: () => void;
  runtimeApiHost: string;
  runtimeApiPort: string;
  runtimeConfig: RuntimeConfig | null;
  runtimeConfigBusy: "restart" | null;
  runtimeConfigError: string | null;
  runtimeConfigMessage: string | null;
  runtimeWebPort: string;
  tokenBusy: string | null;
  tokenError: string | null;
  tokenMessage: string | null;
}) {
  const activeTokens = apiTokens.filter((token) => token.active !== false);
  const [visibleTokenIds, setVisibleTokenIds] = useState<Record<string, boolean>>({});
  function selectAccessMode(mode: "local" | "remote") {
    onRuntimeApiHostChange(mode === "remote" ? "0.0.0.0" : "127.0.0.1");
  }
  const isLocalOpen = runtimeConfig?.auth_enabled === false && isLoopbackApiBase(apiBase);
  const isRemoteMode = runtimeApiHost === "0.0.0.0";
  const currentApiHost = runtimeConfig?.api_host ?? "127.0.0.1";
  const currentApiPort = runtimeConfig?.api_port ?? 8666;
  const currentWebPort = runtimeConfig?.web_port ?? 8667;
  const nextApiHost = runtimeConfig?.next_start?.api_host ?? currentApiHost;
  const nextApiPort = runtimeConfig?.next_start?.api_port ?? currentApiPort;
  const nextWebPort = runtimeConfig?.next_start?.web_port ?? currentWebPort;
  const pendingRestart = Boolean(runtimeConfig?.pending_restart);
  const restartSupported = runtimeConfig?.restart_supported === true;
  const restartUnavailableLabel = formatRestartUnavailableReason(runtimeConfig?.restart_unavailable_reason);
  const currentModeLabel = currentApiHost === "0.0.0.0" ? "远程 SDK/API 已开放" : "本机模式";
  const nextModeLabel = runtimeApiHost === "0.0.0.0" ? "远程 SDK/API" : "本机模式";
  const remoteApiUrl = `http://这台机器的IP:${nextApiPort}`;
  const currentListenAddress = currentApiHost === "0.0.0.0" ? `0.0.0.0:${currentApiPort}` : `127.0.0.1:${currentApiPort}`;
  const nextListenAddress = runtimeApiHost === "0.0.0.0" ? `0.0.0.0:${nextApiPort}` : `127.0.0.1:${nextApiPort}`;
  const startupRows: TableRow[] = [
    ["模式", currentModeLabel, nextModeLabel],
    ["API 监听", currentListenAddress, nextListenAddress],
    ["本机 API", `127.0.0.1:${currentApiPort}`, `127.0.0.1:${nextApiPort}`],
    ["Web 地址", `127.0.0.1:${currentWebPort}`, `127.0.0.1:${nextWebPort}`],
    [
      "状态",
      pendingRestart ? "等待重启" : "已生效",
      restartSupported ? "可保存并重启后端" : restartUnavailableLabel
    ]
  ];
  return (
    <>
      <section className="doc-section access-overview-section" id="token-settings">
        <div className="access-overview-main">
          <div className="section-title">
            <Shield size={20} />
            <h2>访问方式</h2>
          </div>
          <div className="segmented-control access-mode-control" role="tablist" aria-label="访问模式">
            <button
              className={!isRemoteMode ? "active" : ""}
              onClick={() => selectAccessMode("local")}
              type="button"
            >
              本机模式
            </button>
            <button
              className={isRemoteMode ? "active" : ""}
              onClick={() => selectAccessMode("remote")}
              type="button"
            >
              远程模式
            </button>
          </div>
          <div className="access-mode-copy">
            {!isRemoteMode ? (
              <>
                <strong>只在这台电脑上使用 AxData。</strong>
                <p>Web 管理台和 API 都只对本机开放；本机 Python SDK 可以直接读本地库，不需要 token，也不一定要启动后端。</p>
              </>
            ) : (
              <>
                <strong>把这台 AxData 开给其他设备的 SDK / Notebook / 脚本。</strong>
                <p>只开放 API，不开放 Web。其他设备用 API 地址和 token 访问；Web 管理台仍只在本机打开。</p>
                <p>远程 Python SDK 传 <code>token="axd_..."</code>；远程 HTTP / curl 传 <code>Authorization: Bearer axd_...</code>。本机 SDK 不需要 token。</p>
              </>
            )}
          </div>
        </div>

        <div className="access-status-panel">
          <Metric icon={ServerCog} label="当前" value={currentModeLabel} />
          <Metric icon={Shield} label="鉴权" value={isLocalOpen ? "本机无需 token" : formatAuthState(runtimeConfig)} />
          <Metric icon={KeyIcon} label="Token" value={`${activeTokens.length} 个`} />
        </div>
      </section>

      <section className="doc-section access-config-section" id="runtime-startup">
        <div className="access-config-main">
          <div className="section-title">
            <Power size={20} />
            <h2>启动与端口</h2>
          </div>
          <p className="guide-note">改的是下次启动配置。点击保存并重启后端后，API 端口和远程模式才会生效；Web 端口保存后需要重启 Web 服务。Web host 固定为本机。</p>
          <div className="startup-settings-form">
            <label>
              <span>API 端口</span>
              <input
                inputMode="numeric"
                min={1}
                max={65535}
                onChange={(event) => onRuntimeApiPortChange(event.target.value)}
                type="number"
                value={runtimeApiPort}
              />
            </label>
            <label>
              <span>Web 端口</span>
              <input
                inputMode="numeric"
                min={1}
                max={65535}
                onChange={(event) => onRuntimeWebPortChange(event.target.value)}
                type="number"
                value={runtimeWebPort}
              />
            </label>
            <div className="startup-settings-actions">
              <button
                className="primary-action"
                disabled={runtimeConfigBusy !== null}
                onClick={onSaveAndRestartApiBackend}
                type="button"
              >
                {runtimeConfigBusy ? <Loader2 size={17} /> : <RefreshCw size={17} />}
                保存并重启后端
              </button>
            </div>
            {!restartSupported ? (
              <p className="form-warning">{restartUnavailableLabel}。点击后只能保存下次启动配置，不能自动重启。请用 npm run dev:api 启动后端后再使用这里的一键重启。</p>
            ) : null}
            {runtimeConfigMessage ? <p className="form-success">{runtimeConfigMessage}</p> : null}
            {runtimeConfigError ? <p className="form-error">{runtimeConfigError}</p> : null}
          </div>
        </div>

        <div className="runtime-summary-card">
          <DataTable columns={["项目", "当前运行", "下次启动"]} rows={startupRows} />
        </div>
      </section>

      {isRemoteMode ? (
      <section className="doc-section remote-sdk-section">
        <div className="section-title">
          <ServerCog size={20} />
          <h2>远程 SDK/API</h2>
        </div>
        <div className="remote-sdk-grid">
          <div className="access-mode-copy">
            <strong>{remoteApiUrl}</strong>
            <p>把“这台机器的 IP”换成当前电脑的局域网 IP 或服务器地址。远程设备只访问 API，不打开 Web。</p>
          </div>
          <div className="remote-sdk-facts">
            <Metric icon={ServerCog} label="监听" value={currentApiHost === "0.0.0.0" ? "已开放" : "重启后开放"} />
            <Metric icon={KeyIcon} label="Token" value={`${activeTokens.length} 个`} />
          </div>
        </div>
        <details className="advanced-connection-details sdk-example-details">
          <summary>
            <Code2 size={18} />
            <span>SDK 示例</span>
          </summary>
          <CodeBlock
            code={`# PowerShell：只开放 API，不开放 Web\n$env:AXDATA_API_HOST = "0.0.0.0"\n$env:AXDATA_API_PORT = "${nextApiPort}"\nnpm run dev:api\n\n# 其他设备的 Python / Notebook\nclient = ax.AxDataClient(api_base="${remoteApiUrl}", token="axd_...")`}
            language="powershell / py"
          />
        </details>
      </section>
      ) : null}

      {isRemoteMode ? (
      <section className="doc-section">
        <div className="section-title token-section-title">
          <KeyIcon size={20} />
          <h2>远程 SDK/API token</h2>
          <button className="ghost-action compact" type="button" onClick={onRefreshApiTokens}>
            <RefreshCw size={15} />
            刷新
          </button>
        </div>
        <p className="guide-note">给每台远程研究电脑、服务器任务、脚本或 Notebook 单独创建 token；丢失或停用时只删除对应 token。它不是 Web 登录账号。</p>
        <form
          className="api-token-create-form"
          onSubmit={(event) => {
            event.preventDefault();
            onCreateApiToken();
          }}
        >
          <label htmlFor="new-api-token-name">设备名称</label>
          <div className="input-row">
            <input
              id="new-api-token-name"
              value={newTokenName}
              onChange={(event) => onNewTokenNameChange(event.target.value)}
              placeholder="例如 notebook-laptop、home-pc、cloud-job"
            />
            <button className="primary-action" disabled={tokenBusy === "create"} type="submit">
              {tokenBusy === "create" ? <Loader2 size={17} /> : <KeyIcon size={17} />}
              创建
            </button>
          </div>
        </form>
        {tokenMessage ? <p className="form-success">{tokenMessage}</p> : null}
        {tokenError ? <p className="form-error">{tokenError}</p> : null}
        <div className="table-wrap token-table-wrap">
          <table>
            <thead>
              <tr>
                <th>名称</th>
                <th>Token</th>
                <th>创建时间</th>
                <th>最近使用</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {activeTokens.length > 0 ? activeTokens.map((token) => (
                <tr key={token.id}>
                  <td>{token.name}</td>
                  <td>
                    <TokenValueCell
                      isVisible={Boolean(visibleTokenIds[token.id])}
                      onToggle={() => setVisibleTokenIds((current) => ({
                        ...current,
                        [token.id]: !current[token.id]
                      }))}
                      value={token.token}
                    />
                  </td>
                  <td>{formatNullableDateTime(token.created_at)}</td>
                  <td>{formatNullableDateTime(token.last_used_at)}</td>
                  <td>
                    <button
                      className="ghost-action danger compact"
                      disabled={tokenBusy === token.id}
                      onClick={() => onRevokeApiToken(token.id)}
                      type="button"
                    >
                      {tokenBusy === token.id ? <Loader2 size={14} /> : null}
                      删除
                    </button>
                  </td>
                </tr>
              )) : (
                <tr>
                  <td colSpan={5}>当前还没有远程 SDK/API token。创建 token 后，其他设备可通过 SDK/API 访问这台 AxData。</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
      ) : null}

      <details className="doc-section advanced-connection-details">
        <summary>
          <Code2 size={18} />
          <span>运行明细</span>
        </summary>
        <DataTable
          columns={["项目", "当前值", "说明"]}
          rows={[
            ["API host", runtimeConfig?.api_host ?? "127.0.0.1", "AXDATA_API_HOST"],
            ["API port", String(runtimeConfig?.api_port ?? 8666), "AXDATA_API_PORT，可自定义远程 SDK/API 端口"],
            ["本机 API", runtimeConfig?.local_api_base ?? runtimeConfig?.api_base ?? apiBase, "Web 管理台固定通过本机地址连接 API"],
            ["API 监听", runtimeConfig?.listen_api_base ?? `http://${runtimeConfig?.api_host ?? "127.0.0.1"}:${runtimeConfig?.api_port ?? 8666}`, "远程模式下监听 0.0.0.0，但浏览器不使用 0.0.0.0 作为目标地址"],
            ["Web host", runtimeConfig?.web_host ?? "127.0.0.1", "固定为本机 127.0.0.1，不提供远程 Web 入口"],
            ["Web port", String(runtimeConfig?.web_port ?? 8667), "AXDATA_WEB_PORT，可自定义本机 Web 端口"],
            ["数据目录", health?.data_root ?? runtimeConfig?.data_root ?? "./data", "AXDATA_DATA_DIR，本地数据文件保存位置"],
            ["浏览器访问来源", formatRuntimeCors(runtimeConfig), "允许本机 Web 管理台访问 API 的地址"],
            ["API 调试文档", `${apiBase}/docs`, "开发调试用的接口文档入口"],
            ["请求头", "Authorization: Bearer <token>", "远程 token 的 HTTP 写法"]
          ]}
        />
      </details>
    </>
  );
}

function TokenValueCell({
  isVisible,
  onToggle,
  value
}: {
  isVisible: boolean;
  onToggle: () => void;
  value?: string | null;
}) {
  if (!value) {
    return <span className="muted-inline">旧 token 不可显示，请删除后重建</span>;
  }
  return (
    <div className="token-value-cell">
      <code>{isVisible ? value : maskTokenValue(value)}</code>
      <button className="ghost-action compact" onClick={onToggle} type="button">
        {isVisible ? <EyeOff size={14} /> : <Eye size={14} />}
        {isVisible ? "隐藏" : "显示"}
      </button>
    </div>
  );
}

function DiagnosticsHero({ diagnostics }: { diagnostics: LocalDiagnosticsState }) {
  const payload = diagnostics.status ?? diagnostics.doctor;
  const registry = payload?.registry ?? {};
  const collector = payload?.collector ?? {};
  const statusCounts = registry.status_counts ?? {};
  const failedProviderCount =
    numberValue(statusCounts.failed) +
    numberValue(statusCounts.incompatible) +
    numberValue(statusCounts.conflict);
  return (
    <section className="doc-hero single">
      <div className="doc-heading">
        <div className="endpoint-line">
          <span className="section-eyebrow">诊断</span>
          <span className={`ready-badge ${diagnostics.error ? "example" : ""}`}>
            {diagnostics.error ? <AlertCircle size={15} /> : <CheckCircle2 size={15} />}
            {diagnostics.error ? "读取失败" : "本机状态"}
          </span>
        </div>
        <div className="title-row">
          <span className="title-icon">
            <ServerCog size={26} />
          </span>
          <h1>诊断</h1>
        </div>
        <p>看本机哪里有问题。运行检查、路径、依赖、端口、Registry 摘要、真实源 smoke 和最近失败 run 都放在这里。</p>
        <dl className="summary-list">
          <div>
            <dt>诊断状态</dt>
            <dd>{formatDiagnosticSummary(payload ?? null, diagnostics.error)}</dd>
          </div>
          <div>
            <dt>Provider</dt>
            <dd>{`${numberValue(registry.enabled_provider_count)} / ${numberValue(registry.provider_count)} 启用`}</dd>
          </div>
          <div>
            <dt>异常 Provider</dt>
            <dd>{String(failedProviderCount)}</dd>
          </div>
          <div>
            <dt>采集任务</dt>
            <dd>{String(numberValue(registry.collector_count))}</dd>
          </div>
          <div>
            <dt>active run</dt>
            <dd>{String(numberValue(collector.active_run_count))}</dd>
          </div>
        </dl>
      </div>
    </section>
  );
}

function PluginManagementHero({
  collectorPlugins,
  installedPlugins,
  providers
}: {
  collectorPlugins: CollectorPluginSummary[];
  installedPlugins: InstalledPlugin[];
  providers: ProviderStatus[];
}) {
  const issueCount = providers.filter((provider) =>
    ["failed", "incompatible", "conflict", "missing", "uninstalled"].includes(provider.status)
  ).length;
  const managedCount = installedPlugins.length;
  const collectorCount = collectorPlugins.reduce((total, plugin) => total + plugin.collectorCount, 0);
  const collectorPluginIssueCount = collectorPlugins.filter((plugin) =>
    plugin.statuses.some((status) => ["failed", "incompatible", "conflict", "missing", "uninstalled"].includes(status))
  ).length;
  return (
    <section className="doc-hero single">
      <div className="doc-heading">
        <div className="endpoint-line">
          <span className="section-eyebrow">插件</span>
          <span className="ready-badge">
            <CheckCircle2 size={15} />
            本地插件
          </span>
        </div>
        <div className="title-row">
          <span className="title-icon">
            <FileArchive size={26} />
          </span>
          <h1>插件</h1>
        </div>
        <p>管理本地插件能力和安装状态。数据源插件进入 ProviderRegistry，采集插件进入 CollectorRegistry；采集运行和历史仍在采集页。</p>
        <dl className="summary-list">
          <div>
            <dt>数据源插件</dt>
            <dd>{String(providers.length)}</dd>
          </div>
          <div>
            <dt>采集插件</dt>
            <dd>{String(collectorPlugins.length)}</dd>
          </div>
          <div>
            <dt>采集任务</dt>
            <dd>{String(collectorCount)}</dd>
          </div>
          <div>
            <dt>需处理</dt>
            <dd>{String(issueCount + collectorPluginIssueCount)}</dd>
          </div>
          <div>
            <dt>AXP 管理</dt>
            <dd>{String(managedCount)}</dd>
          </div>
        </dl>
      </div>
    </section>
  );
}

function PluginAbilityFilterPanel({
  collectorCount,
  onChange,
  sourceCount,
  value
}: {
  collectorCount: number;
  onChange: (value: PluginAbilityFilter) => void;
  sourceCount: number;
  value: PluginAbilityFilter;
}) {
  const options: Array<{ value: PluginAbilityFilter; label: string; count: number }> = [
    { value: "all", label: "全部", count: sourceCount + collectorCount },
    { value: "source", label: "数据源", count: sourceCount },
    { value: "collector", label: "采集器", count: collectorCount }
  ];
  return (
    <section className="doc-section plugin-ability-section">
      <div className="section-title">
        <ServerCog size={20} />
        <h2>插件能力</h2>
      </div>
      <div className="plugin-ability-toolbar">
        <div className="segmented-control" role="tablist" aria-label="插件能力筛选">
          {options.map((option) => (
            <button
              className={value === option.value ? "active" : ""}
              key={option.value}
              onClick={() => onChange(option.value)}
              type="button"
            >
              {option.label} {option.count}
            </button>
          ))}
        </div>
        <p className="guide-note">能力类型和安装记录是两条轴；这里看能力，AXP 安装记录只放包管理信息。</p>
      </div>
    </section>
  );
}

function LocalDiagnosticsPanel({
  apiBase,
  clearingRunId,
  diagnostics,
  health,
  onClearRunRecord,
  onRefresh,
  runtimeConfig
}: {
  apiBase: string;
  clearingRunId: string | null;
  diagnostics: LocalDiagnosticsState;
  health: HealthPayload | null;
  onClearRunRecord: (run: Record<string, unknown>) => Promise<void>;
  onRefresh: () => Promise<void>;
  runtimeConfig: RuntimeConfig | null;
}) {
  const payload = diagnostics.status ?? diagnostics.doctor;
  const checks = payload?.checks ?? [];
  const config: NonNullable<LocalDiagnosticsPayload["config"]> = payload?.config ?? {};
  const registry: NonNullable<LocalDiagnosticsPayload["registry"]> = payload?.registry ?? {};
  const plugins: NonNullable<LocalDiagnosticsPayload["plugins"]> = payload?.plugins ?? {};
  const statusCounts = registry.status_counts ?? {};
  const collector: Record<string, unknown> = payload?.collector ?? {};
  const recentFailedRuns = recordArray(collector.recent_failed_runs);
  const smoke: Record<string, unknown> = payload?.real_source_smoke ?? {};
  const failedProviderCount =
    numberValue(statusCounts.failed) +
    numberValue(statusCounts.incompatible) +
    numberValue(statusCounts.conflict);

  return (
    <section className="doc-section diagnostics-section">
      <div className="section-title">
        <ServerCog size={20} />
        <h2>本地状态与诊断</h2>
      </div>
      <div className="diagnostic-summary-grid">
        <Metric icon={Shield} label="API" value={formatApiDiagnosticState(health, diagnostics.error)} />
        <Metric icon={Database} label="诊断" value={formatDiagnosticSummary(payload, diagnostics.error)} />
        <Metric icon={Download} label="插件" value={`${numberValue(registry.enabled_provider_count)} / ${numberValue(registry.provider_count)} 启用`} />
        <Metric icon={RefreshCw} label="Collector" value={`${numberValue(collector.enabled_task_count)} / ${numberValue(collector.task_count)} task`} />
      </div>

      <div className="diagnostic-endpoints">
        <span className={diagnostics.status ? "status-dot on" : "status-dot"} />
        <code>{apiBase}/v1/status</code>
        <span className={diagnostics.doctor ? "status-dot on" : "status-dot"} />
        <code>{apiBase}/v1/doctor</code>
        <button className="ghost-action compact" type="button" onClick={() => onRefresh()}>
          <RefreshCw size={15} />
          刷新
        </button>
      </div>

      {diagnostics.error ? (
        <div className="diagnostic-error">
          <AlertCircle size={17} />
          <span>{diagnostics.error}</span>
        </div>
      ) : null}

      {payload ? (
        <>
          <div className="diagnostic-path-grid">
            {[
              ["数据目录", config.data_root],
              ["metadata", config.metadata_root],
              ["插件目录", config.plugin_dir],
              ["插件依赖目录", config.plugin_site_packages],
              ["采集记录文件", config.collector_store_path],
              ["API 地址", runtimeConfig?.api_base ?? config.api_base]
            ].map(([label, value]) => (
              <div className="diagnostic-path-item" key={label}>
                <span>{label}</span>
                <code>{String(value ?? "")}</code>
              </div>
            ))}
          </div>

          <div className="diagnostic-counts">
            <span>AXP 安装记录 {numberValue(plugins.installed_count)}</span>
            <span>已启用 {numberValue(registry.enabled_provider_count)}</span>
            <span>禁用 {numberValue(statusCounts.disabled)}</span>
            <span>失败/冲突 {failedProviderCount}</span>
            <span>active run {numberValue(collector.active_run_count)}</span>
            <span>最近失败 {recentFailedRuns.length}</span>
          </div>

          <TdxDiagnosticNotice tdx={payload.tdx ?? {}} />
          <DiagnosticChecksList checks={checks} />
          <DiagnosticNextActions actions={payload.next_actions ?? []} />
          <RecentFailedRunsSummary clearingRunId={clearingRunId} onClearRunRecord={onClearRunRecord} runs={recentFailedRuns} />
          <RealSourceSmokePanel smoke={smoke} />
        </>
      ) : null}
    </section>
  );
}

function TdxDiagnosticNotice({ tdx }: { tdx: Record<string, unknown> }) {
  const status = String(tdx.status ?? "unknown");
  if (status === "enabled") {
    return null;
  }
  const message =
    typeof tdx.message === "string"
      ? tdx.message
      : "TDX 插件未安装、未启用或不可用；需要 TDX 接口时请安装并启用 TDX 插件。";
  const providers = recordArray(tdx.providers);
  const providerId = providers
    .map((provider) => provider.provider_id)
    .find((value): value is string => typeof value === "string" && value.length > 0);
  return (
    <div className="guidance-notice diagnostic-tdx-notice">
      <span>{message}</span>
      {providerId ? <ActionCommand command={`axdata plugin enable ${providerId}`} /> : null}
    </div>
  );
}

function DiagnosticChecksList({ checks }: { checks: LocalDiagnosticCheck[] }) {
  const sortedChecks = [...checks].sort((left, right) => {
    const rankDiff = diagnosticVisibilityRank(left.status) - diagnosticVisibilityRank(right.status);
    if (rankDiff !== 0) {
      return rankDiff;
    }
    return `${left.category}:${left.name}`.localeCompare(`${right.category}:${right.name}`);
  });
  const visibleChecks = sortedChecks.slice(0, 18);
  return (
    <div className="diagnostic-check-list">
      {visibleChecks.map((check) => (
        <div className={`diagnostic-check-row ${diagnosticStatusClass(check.status)}`} key={`${check.category}:${check.name}:${check.path ?? ""}`}>
          <span className={`diagnostic-status-pill ${diagnosticStatusClass(check.status)}`}>
            {formatDiagnosticCheckStatus(check)}
          </span>
          <strong className="diagnostic-check-title">
            <span>{formatDiagnosticCategory(check.category)} / {formatDiagnosticCheckName(check)}</span>
            <code>{check.category} / {check.name}</code>
          </strong>
          <span>{formatDiagnosticCheckMessage(check)}</span>
          {check.path ? <code>{check.path}</code> : null}
        </div>
      ))}
      {sortedChecks.length > visibleChecks.length ? (
        <div className="diagnostic-check-more">还有 {sortedChecks.length - visibleChecks.length} 项检查未展开。</div>
      ) : null}
    </div>
  );
}

function DiagnosticNextActions({ actions }: { actions: string[] }) {
  if (actions.length === 0) {
    return null;
  }
  return (
    <div className="diagnostic-action-list">
      {actions.map((action) => (
        <div className="guidance-notice" key={action}>
          <span>{formatDiagnosticAction(action)}</span>
          {extractBacktickCommand(action) ? <ActionCommand command={extractBacktickCommand(action)} /> : null}
        </div>
      ))}
    </div>
  );
}

function RecentFailedRunsSummary({
  clearingRunId,
  onClearRunRecord,
  runs
}: {
  clearingRunId: string | null;
  onClearRunRecord: (run: Record<string, unknown>) => Promise<void>;
  runs: Array<Record<string, unknown>>;
}) {
  if (runs.length === 0) {
    return null;
  }
  return (
    <div className="diagnostic-failed-runs">
      {runs.slice(0, 3).map((run) => {
        const runId = typeof run.run_id === "string" ? run.run_id : "";
        const isClearing = runId.length > 0 && clearingRunId === runId;
        return (
          <div className="collector-run-mini" key={String(run.run_id ?? run.task_id ?? "")}>
            <div className="collector-run-mini-content">
              <strong>{formatFailedRunTitle(run)}</strong>
              <span>{formatFailedRunMessage(run)}</span>
              {typeof run.next_action === "string" ? <span>{formatFailedRunNextAction(run.next_action)}</span> : null}
              {typeof run.action_command === "string" ? <ActionCommand command={run.action_command} /> : null}
            </div>
            <button
              className="ghost-action danger compact"
              disabled={!runId || clearingRunId !== null}
              onClick={() => onClearRunRecord(run)}
              type="button"
            >
              {isClearing ? <Loader2 className="spin" size={14} /> : <Trash2 size={14} />}
              清除记录
            </button>
          </div>
        );
      })}
    </div>
  );
}

function formatFailedRunTitle(run: Record<string, unknown>) {
  const taskId = String(run.task_id ?? "");
  const collectorName = String(run.collector_name ?? "");
  return taskId || collectorName || "最近失败的采集运行";
}

function formatFailedRunMessage(run: Record<string, unknown>) {
  const error = String(run.error ?? run.error_summary ?? run.status_message ?? "");
  const normalized = error.toLowerCase();
  if (normalized.includes("scheduler process stopped before this run finished")) {
    return "该次采集尚未完成时 API/调度器进程被停止或重启；通常是手动重启服务留下的历史失败记录。";
  }
  if (normalized.includes("dependency_missing")) {
    return "采集依赖缺失，通常需要先同步基础数据或启用对应插件。";
  }
  if (normalized.includes("plugin_disabled") || normalized.includes("provider_missing")) {
    return "采集依赖的插件不可用，需要检查插件是否启用。";
  }
  return error ? stripBackticks(error) : "最近一次采集运行失败。";
}

function formatFailedRunNextAction(action: string) {
  const normalized = action.toLowerCase();
  if (normalized.includes("api") && normalized.includes("重启")) {
    return "确认当前服务稳定后，重新点击手动采集即可。";
  }
  return stripBackticks(action);
}

function RealSourceSmokePanel({ smoke }: { smoke: Record<string, unknown> }) {
  const requiresExplicitRun = smoke.requires_explicit_run !== false;
  const canRunTdx = Boolean(smoke.can_run_tdx_smoke);
  return (
    <div className="real-smoke-panel">
      <div className="provider-status-head">
        <div>
          <strong>真实源 smoke</strong>
          <code>{String(smoke.script ?? "scripts/smoke_real_sources.py")}</code>
        </div>
        <div className="provider-status-badges">
          <span className="provider-status-badge installed">{String(smoke.default_mode ?? "dry-skip/offline")}</span>
          <span className={`provider-status-badge ${canRunTdx ? "enabled" : "disabled"}`}>
            {canRunTdx ? "TDX 可纳入" : "TDX 需先启用插件"}
          </span>
        </div>
      </div>
      <p className="provider-description">
        默认测试不联网；真实源 smoke 需要在命令行显式运行，Web 这里只展示状态和命令。
      </p>
      <ActionCommand command=".venv\\Scripts\\python scripts\\smoke_real_sources.py --run --interfaces stock_basic_exchange trade_cal daily" />
      {typeof smoke.example === "string" ? <ActionCommand command={smoke.example} /> : null}
      {requiresExplicitRun ? (
        <p className="provider-description">{String(smoke.tdx_requirement ?? "daily 需要先安装并启用 TDX 插件。")}</p>
      ) : null}
    </div>
  );
}

function ProviderStatusPanel({
  busyExportId,
  busyOverrideKey,
  busyProviderId,
  error,
  message,
  onAction,
  onExport,
  onUninstall,
  onOverrideAction,
  providers
}: {
  busyExportId: string | null;
  busyOverrideKey: string | null;
  busyProviderId: string | null;
  error: string | null;
  message: string | null;
  onAction: (provider: ProviderStatus, action: "enable" | "disable") => void;
  onExport: (pluginId: string, displayName?: string) => void;
  onUninstall: (provider: ProviderStatus) => void;
  onOverrideAction: (interfaceName: string, provider: ProviderStatus, action: "set" | "clear") => void;
  providers: ProviderStatus[];
}) {
  const sortedProviders = [...providers].sort((left, right) => {
    const statusDiff = providerStatusRank(left.status) - providerStatusRank(right.status);
    if (statusDiff !== 0) {
      return statusDiff;
    }
    return left.provider_id.localeCompare(right.provider_id);
  });

  return (
    <section className="doc-section provider-status-section">
      <div className="section-title">
        <ServerCog size={20} />
        <h2>数据源插件</h2>
      </div>
      <p className="guide-note">
        数据源插件进入 ProviderRegistry，只负责接口目录和源端临时查询。采集能力看下方采集插件或采集页。
      </p>
      <div className="provider-status-grid">
        {sortedProviders.length > 0 ? (
          sortedProviders.map((provider) => {
            const interfacePreview = provider.interfaces?.slice(0, 3) ?? [];
            const remainingInterfaceCount = Math.max(0, (provider.interfaces?.length ?? 0) - interfacePreview.length);
            return (
              <div className={`provider-status-card ${provider.status}`} key={provider.provider_id}>
                <div className="provider-status-head">
                  <div>
                    <strong>{provider.source_name_zh || provider.source_code}</strong>
                    <code>{provider.provider_id}</code>
                  </div>
                  <div className="provider-status-badges">
                    <span className="plugin-ability-badge source">数据源</span>
                    <span className={`provider-status-badge ${provider.status}`}>
                      {formatProviderStatus(provider.status)}
                    </span>
                    <span className={`provider-trust-badge ${provider.effective_trust_level || "unknown"}`}>
                      {formatTrustLevel(provider.effective_trust_level)}
                    </span>
                  </div>
                </div>
                <p className="provider-risk-note">{providerRiskNote(provider)}</p>
                <dl className="provider-status-meta">
                  <div>
                    <dt>声明信任</dt>
                    <dd>{formatTrustLevel(provider.declared_trust_level)}</dd>
                  </div>
                  <div>
                    <dt>实际信任</dt>
                    <dd>{formatTrustLevel(provider.effective_trust_level)}</dd>
                  </div>
                  <div>
                    <dt>接口</dt>
                    <dd>{provider.interface_count}</dd>
                  </div>
                  <div>
                    <dt>版本</dt>
                    <dd>{provider.version || "unknown"}</dd>
                  </div>
                  <div>
                    <dt>来源</dt>
                    <dd>{formatInstallSource(provider.install_source, provider.built_in)}</dd>
                  </div>
                  <div>
                    <dt>许可</dt>
                    <dd>{provider.license || "未声明"}</dd>
                  </div>
                  <div>
                    <dt>状态</dt>
                    <dd>{provider.enabled ? "参与目录和路由" : "不参与目录和路由"}</dd>
                  </div>
                </dl>
                {provider.description ? <p className="provider-description">{provider.description}</p> : null}
                {interfacePreview.length > 0 ? (
                  <div className="provider-interface-preview">
                    {interfacePreview.map((interfaceName) => (
                      <code key={interfaceName}>{interfaceName}</code>
                    ))}
                    {remainingInterfaceCount > 0 ? <span>+{remainingInterfaceCount}</span> : null}
                  </div>
                ) : null}
                <ProviderConfigHints provider={provider} />
                <GuidanceNotice item={provider} />
                {provider.uninstall_block_reason ? (
                  <p className="provider-description">{provider.uninstall_block_reason}</p>
                ) : null}
                <div className="provider-status-action-row">
                  <div className="provider-status-actions provider-status-actions-left">
                    <button
                      className="ghost-action compact"
                      disabled={busyProviderId !== null || busyExportId !== null}
                      onClick={() => onExport(provider.provider_id, provider.source_name_zh || provider.provider_id)}
                      type="button"
                    >
                      {busyExportId === provider.provider_id ? <Loader2 className="spin" size={15} /> : <Download size={15} />}
                      {busyExportId === provider.provider_id ? "打包中" : "导出 AXP"}
                    </button>
                  </div>
                  <div className="provider-status-actions">
                    {provider.homepage ? (
                      <a className="ghost-action compact" href={provider.homepage} rel="noreferrer" target="_blank">
                        <ExternalLink size={15} />
                        主页
                      </a>
                    ) : null}
                    {provider.enabled ? (
                      <button
                        className="ghost-action compact"
                        disabled={busyProviderId !== null || provider.can_disable === false}
                        onClick={() => onAction(provider, "disable")}
                        type="button"
                      >
                        {busyProviderId === provider.provider_id ? <Loader2 size={15} /> : <Power size={15} />}
                        禁用
                      </button>
                    ) : (
                      <button
                        className="primary-action compact"
                        disabled={busyProviderId !== null || !providerCanEnable(provider)}
                        onClick={() => onAction(provider, "enable")}
                        type="button"
                      >
                        {busyProviderId === provider.provider_id ? <Loader2 size={15} /> : <CheckCircle2 size={15} />}
                        启用
                      </button>
                    )}
                    {provider.can_uninstall ? (
                      <button
                        className="ghost-action compact danger"
                        disabled={busyProviderId !== null}
                        onClick={() => onUninstall(provider)}
                        type="button"
                      >
                        {busyProviderId === provider.provider_id ? <Loader2 size={15} /> : <Power size={15} />}
                        卸载
                      </button>
                    ) : null}
                  </div>
                </div>
                <ProviderOverrideControls
                  busyOverrideKey={busyOverrideKey}
                  busyProviderId={busyProviderId}
                  onOverrideAction={onOverrideAction}
                  provider={provider}
                />
                {provider.error ? <p className="provider-status-error">{provider.error}</p> : null}
              </div>
            );
          })
        ) : (
          <div className="provider-status-empty">后端未返回 Provider 状态。</div>
        )}
      </div>
      {message ? (
        <div className="calendar-cache-refresh-state done">
          <CheckCircle2 size={16} />
          <span>{message}</span>
        </div>
      ) : null}
      {error ? <p className="form-error">{error}</p> : null}
    </section>
  );
}

function CollectorPluginStatusPanel({
  busyExportId,
  onExport,
  onOpenCollector,
  providerIds,
  summaries
}: {
  busyExportId: string | null;
  onExport: (pluginId: string, displayName?: string) => void;
  onOpenCollector: (pluginId?: string, collectorId?: string) => void;
  providerIds: Set<string>;
  summaries: CollectorPluginSummary[];
}) {
  return (
    <section className="doc-section provider-status-section collector-plugin-section">
      <div className="section-title">
        <Download size={20} />
        <h2>采集插件</h2>
      </div>
      <p className="guide-note">
        采集插件进入 CollectorRegistry，只在这里展示能力和状态；采集任务运行、补采和 run history 仍在采集页处理。
      </p>
      <div className="provider-status-grid">
        {summaries.length > 0 ? (
          summaries.map((summary) => {
            const collectorPreview = summary.collectorIds.slice(0, 4);
            const remainingCollectorCount = Math.max(0, summary.collectorIds.length - collectorPreview.length);
            const datasetPreview = summary.datasetIds.slice(0, 4);
            const remainingDatasetCount = Math.max(0, summary.datasetIds.length - datasetPreview.length);
            const statusClass = collectorPluginStatusClass(summary.statuses);
            const hasSourceAbility = providerIds.has(summary.pluginId);
            return (
              <div className={`provider-status-card ${statusClass}`} key={summary.pluginId}>
                <div className="provider-status-head">
                  <div>
                    <strong>{summary.displayName || summary.pluginId}</strong>
                    <code>{summary.pluginId}</code>
                  </div>
                  <div className="provider-status-badges">
                    {hasSourceAbility ? <span className="plugin-ability-badge source">数据源</span> : null}
                    <span className="plugin-ability-badge collector">采集器</span>
                    <span className={`provider-status-badge ${statusClass}`}>
                      {formatCollectorPluginStatuses(summary.statuses)}
                    </span>
                  </div>
                </div>
                <dl className="provider-status-meta">
                  <div>
                    <dt>collector_plugin_id</dt>
                    <dd>{summary.pluginId}</dd>
                  </div>
                  <div>
                    <dt>采集任务</dt>
                    <dd>{summary.collectorCount}</dd>
                  </div>
                  <div>
                    <dt>runner_entry</dt>
                    <dd>{formatRunnerEntrySummary(summary.runnerEntryCount, summary.collectorCount)}</dd>
                  </div>
                  <div>
                    <dt>lifecycle_status</dt>
                    <dd>{formatCountMap(summary.lifecycleCounts)}</dd>
                  </div>
                  <div>
                    <dt>legacy</dt>
                    <dd>{summary.legacyCount}</dd>
                  </div>
                  <div>
                    <dt>来源</dt>
                    <dd>{summary.sourceCode || "collector"}</dd>
                  </div>
                </dl>
                {summary.description ? <p className="provider-description">{summary.description}</p> : null}
                <div className="collector-plugin-preview">
                  <span>采集任务</span>
                  <div className="provider-interface-preview">
                    {collectorPreview.map((collectorId) => (
                      <code key={collectorId}>{collectorId}</code>
                    ))}
                    {remainingCollectorCount > 0 ? <span>+{remainingCollectorCount}</span> : null}
                  </div>
                </div>
                <div className="collector-plugin-preview">
                  <span>覆盖数据集</span>
                  <div className="provider-interface-preview">
                    {datasetPreview.length > 0 ? (
                      datasetPreview.map((datasetId) => <code key={datasetId}>{datasetId}</code>)
                    ) : (
                      <span>未声明</span>
                    )}
                    {remainingDatasetCount > 0 ? <span>+{remainingDatasetCount}</span> : null}
                  </div>
                </div>
                <div className="provider-status-action-row">
                  <div className="provider-status-actions provider-status-actions-left">
                    <button
                      className="ghost-action compact"
                      disabled={busyExportId !== null}
                      onClick={() => onExport(summary.pluginId, summary.displayName || summary.pluginId)}
                      type="button"
                    >
                      {busyExportId === summary.pluginId ? <Loader2 className="spin" size={15} /> : <Download size={15} />}
                      {busyExportId === summary.pluginId ? "打包中" : "导出 AXP"}
                    </button>
                  </div>
                  <div className="provider-status-actions">
                    <button
                      className="ghost-action compact"
                      onClick={() => onOpenCollector(summary.pluginId, summary.firstCollectorId)}
                      type="button"
                    >
                      <ExternalLink size={15} />
                      去采集页
                    </button>
                  </div>
                </div>
              </div>
            );
          })
        ) : (
          <div className="provider-status-empty">后端未返回 CollectorRegistry 采集插件目录。</div>
        )}
      </div>
    </section>
  );
}

function AxpInstallPanel({
  busy,
  error,
  file,
  installedPlugin,
  installResult,
  keepEnabled,
  message,
  onFileChange,
  onKeepEnabledChange,
  onInstall,
  onPreview,
  preview
}: {
  busy: "preview" | "install" | null;
  error: string | null;
  file: File | null;
  installedPlugin: InstalledPlugin | null;
  installResult: AxpInstallResult | null;
  keepEnabled: boolean;
  message: string | null;
  onFileChange: (file: File | null) => void;
  onKeepEnabledChange: (value: boolean) => void;
  onInstall: () => void;
  onPreview: () => void;
  preview: AxpPreview | null;
}) {
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const hasBadWheel = preview?.wheels.some((wheel) => wheel.checksum_status !== "ok") ?? false;
  const isBusy = busy !== null;
  const isUpdate = Boolean(installedPlugin);
  const actionLabel = isUpdate ? "更新插件" : "安装";
  const busyActionLabel = isUpdate ? "更新中" : "安装中";
  const displayedInstallStatus =
    preview && installResult?.preview.provider_id === preview.provider_id
      ? installResult.status_after_install
      : preview?.status_after_install;

  return (
    <section className="doc-section axp-install-section">
      <div className="section-title">
        <FileArchive size={20} />
        <h2>AXP 插件安装</h2>
      </div>
      <div className="axp-install-grid">
        <div className="axp-upload-panel">
          <input
            accept=".axp,.zip,application/zip"
            className="visually-hidden"
            onChange={(event) => onFileChange(event.target.files?.[0] ?? null)}
            ref={fileInputRef}
            type="file"
          />
          <button className="ghost-action" disabled={isBusy} onClick={() => fileInputRef.current?.click()} type="button">
            <Upload size={17} />
            选择文件
          </button>
          <div className="axp-file-summary">
            <strong>{file?.name ?? "未选择文件"}</strong>
            <span>{file ? formatBytes(file.size) : ".axp 安装信封，内部仍是 Python wheel"}</span>
          </div>
          <div className="axp-actions">
            <button className="ghost-action" disabled={!file || isBusy} onClick={onPreview} type="button">
              {busy === "preview" ? <Loader2 size={17} /> : <FileArchive size={17} />}
              预览
            </button>
            <button
              className="primary-action"
              disabled={!file || isBusy || !preview || hasBadWheel}
              onClick={onInstall}
              type="button"
            >
              {busy === "install" ? <Loader2 size={17} /> : <CheckCircle2 size={17} />}
              {busy === "install" ? busyActionLabel : actionLabel}
            </button>
          </div>
        </div>

        {preview ? (
          <div className="axp-preview-panel">
            <div className="provider-status-head">
              <div>
                <strong>{preview.source_name_zh || preview.provider_id}</strong>
                <code>{preview.provider_id}</code>
              </div>
              <div className="provider-status-badges">
                <span className={`provider-trust-badge ${preview.effective_trust_level || "unknown"}`}>
                  {formatTrustLevel(preview.effective_trust_level)}
                </span>
                <span className={`provider-status-badge ${displayedInstallStatus || "disabled"}`}>
                  {formatProviderStatus(displayedInstallStatus || preview.status_after_install)}
                </span>
              </div>
            </div>
            <dl className="provider-status-meta">
              <div>
                <dt>数据源</dt>
                <dd>{preview.source_code}</dd>
              </div>
              <div>
                <dt>版本</dt>
                <dd>{preview.version || "unknown"}</dd>
              </div>
              {installedPlugin ? (
                <>
                  <div>
                    <dt>当前版本</dt>
                    <dd>{installedPlugin.version || "unknown"}</dd>
                  </div>
                  <div>
                    <dt>版本变化</dt>
                    <dd>{formatAxpVersionChange(installedPlugin.version, preview.version)}</dd>
                  </div>
                </>
              ) : null}
              <div>
                <dt>manifest</dt>
                <dd>{preview.manifest_source ?? "unknown"}</dd>
              </div>
            </dl>
            {installedPlugin ? (
              <div className="axp-update-notice">
                <div>
                  <strong>检测到同身份插件已安装</strong>
                  <span>更新只替换 AXP 管理目录里的插件包，不删除 data、metadata、采集任务、运行历史或日志。</span>
                </div>
                <label className="checkbox-chip axp-update-toggle">
                  <input
                    checked={keepEnabled}
                    type="checkbox"
                    onChange={(event) => onKeepEnabledChange(event.target.checked)}
                  />
                  {installedPlugin.enabled ? "更新后保持启用" : "更新后启用插件"}
                </label>
              </div>
            ) : null}
            <div className="axp-wheel-list">
              {preview.wheels.map((wheel) => (
                <div className={`axp-wheel-row ${wheel.checksum_status}`} key={wheel.path}>
                  <code>{wheel.file_name}</code>
                  <span>{formatBytes(wheel.size)}</span>
                  <span>{formatChecksumStatus(wheel.checksum_status)}</span>
                </div>
              ))}
            </div>
            {preview.warnings && preview.warnings.length > 0 ? (
              <ul className="axp-warning-list">
                {preview.warnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            ) : null}
          </div>
        ) : (
          <div className="axp-preview-empty">选择 .axp 后先预览，再安装。安装完成后仍需在 Provider 状态里手动启用。</div>
        )}
      </div>
      {installResult ? (
        <div className="calendar-cache-refresh-state done">
          <CheckCircle2 size={16} />
          <span>
            {installResult.replaced ? "已更新到" : "已安装到"} {installResult.site_packages}，
            当前状态：{formatProviderStatus(installResult.status_after_install)}
          </span>
        </div>
      ) : null}
      {message ? (
        <div className="calendar-cache-refresh-state done">
          <CheckCircle2 size={16} />
          <span>{message}</span>
        </div>
      ) : null}
      {error ? <p className="form-error">{error}</p> : null}
    </section>
  );
}

function InstalledPluginsPanel({
  busyProviderId,
  error,
  message,
  onUninstall,
  plugins
}: {
  busyProviderId: string | null;
  error: string | null;
  message: string | null;
  onUninstall: (plugin: InstalledPlugin) => void;
  plugins: InstalledPlugin[];
}) {
  return (
    <section className="doc-section installed-plugins-section">
      <div className="section-title">
        <Download size={20} />
        <h2>AXP 安装记录</h2>
      </div>
      <p className="guide-note">这里只展示 AxData AXP 管理目录里的包管理记录；插件提供哪些数据源或采集器能力，请看下方能力列表。</p>
      <div className="provider-status-grid">
        {plugins.length > 0 ? (
          plugins.map((plugin) => {
            const isBusy = busyProviderId === plugin.provider_id;
            const wheelPreview = plugin.installed_wheels.slice(0, 3);
            const remainingWheelCount = Math.max(0, plugin.installed_wheels.length - wheelPreview.length);
            return (
              <div className={`provider-status-card ${plugin.status}`} key={plugin.provider_id}>
                <div className="provider-status-head">
                  <div>
                    <strong>{plugin.name || plugin.source_code}</strong>
                    <code>{plugin.provider_id}</code>
                  </div>
                  <div className="provider-status-badges">
                    <span className={`provider-status-badge ${plugin.status}`}>
                      {formatProviderStatus(plugin.status)}
                    </span>
                    <span className={`provider-trust-badge ${plugin.effective_trust_level || "unknown"}`}>
                      {formatTrustLevel(plugin.effective_trust_level)}
                    </span>
                  </div>
                </div>
                <dl className="provider-status-meta">
                  <div>
                    <dt>AXP 记录</dt>
                    <dd>{plugin.provider_id}</dd>
                  </div>
                  <div>
                    <dt>版本</dt>
                    <dd>{plugin.version || "unknown"}</dd>
                  </div>
                  <div>
                    <dt>安装来源</dt>
                    <dd>{formatInstallSource(plugin.install_source, plugin.built_in)}</dd>
                  </div>
                  <div>
                    <dt>wheel</dt>
                    <dd>{plugin.installed_wheels.length}</dd>
                  </div>
                  <div>
                    <dt>安装时间</dt>
                    <dd>{formatNullable(plugin.installed_at)}</dd>
                  </div>
                  <div>
                    <dt>状态</dt>
                    <dd>{formatProviderStatus(plugin.status)}</dd>
                  </div>
                </dl>
                {wheelPreview.length > 0 ? (
                  <div className="provider-interface-preview">
                    {wheelPreview.map((wheel) => (
                      <code key={wheel}>{shortenPathTail(wheel)}</code>
                    ))}
                    {remainingWheelCount > 0 ? <span>+{remainingWheelCount}</span> : null}
                  </div>
                ) : null}
                <div className="plugin-install-paths">
                  {[
                    ["安装路径", plugin.installed_path],
                    ["site-packages", plugin.site_packages],
                    ["安装根目录", plugin.install_root]
                  ].map(([label, value]) => (
                    <div key={label}>
                      <span>{label}</span>
                      <code>{value || "未记录"}</code>
                    </div>
                  ))}
                </div>
                <GuidanceNotice item={plugin} />
                <div className="provider-status-actions">
                  <button
                    className="ghost-action compact danger"
                    disabled={Boolean(busyProviderId) || plugin.can_uninstall === false}
                    onClick={() => onUninstall(plugin)}
                    type="button"
                  >
                    {isBusy ? <Loader2 size={15} /> : <Power size={15} />}
                    卸载
                  </button>
                </div>
                {plugin.enabled ? <p className="provider-status-error">卸载前会先禁用该 Provider；不会删除已采集数据。</p> : null}
                {plugin.uninstall_block_reason ? <p className="provider-status-error">{plugin.uninstall_block_reason}</p> : null}
                {plugin.error ? <p className="provider-status-error">{plugin.error}</p> : null}
              </div>
            );
          })
        ) : (
          <div className="provider-status-empty">当前没有通过 AXP 安装到 AxData 管理目录的插件记录。</div>
        )}
      </div>
      {message ? (
        <div className="calendar-cache-refresh-state done">
          <CheckCircle2 size={16} />
          <span>{message}</span>
        </div>
      ) : null}
      {error ? <p className="form-error">{error}</p> : null}
    </section>
  );
}

function ProviderConfigHints({ provider }: { provider: ProviderStatus }) {
  const configs = provider.required_config ?? [];
  if (configs.length === 0) {
    return null;
  }
  return (
    <div className="provider-config-hints">
      <strong>配置提示</strong>
      <div>
        {configs.map((config) => (
          <span key={`${config.kind}:${config.name}`}>
            <code>{config.name}</code>
            {config.required ? "必填" : "可选"}
          </span>
        ))}
      </div>
    </div>
  );
}

function GuidanceNotice({ item }: { item: GuidanceFields }) {
  if (!item.status_message && !item.next_action && !item.action_command) {
    return null;
  }
  return (
    <div className="guidance-notice">
      {item.status_message ? <span>{formatGuidanceText(item.status_message)}</span> : null}
      {item.next_action ? <strong>{formatGuidanceText(item.next_action)}</strong> : null}
      {item.action_command ? <ActionCommand command={item.action_command} /> : null}
    </div>
  );
}

function formatGuidanceText(value: string) {
  return stripBackticks(value)
    .replace(/Collector run\s*/g, "采集运行")
    .replace(/resource_group=([a-zA-Z0-9_.-]+)/g, (_, group: string) => `资源通道=${formatResourceGroupLabel(group)}`);
}

function ActionCommand({ command }: { command: string | null | undefined }) {
  if (!command) {
    return null;
  }
  return <code className="action-command">{command}</code>;
}

function summarizeCollectorPlugins(
  collectors: CollectorStatus[],
  providers: ProviderStatus[]
): CollectorPluginSummary[] {
  const providerById = new Map(providers.map((provider) => [provider.provider_id, provider]));
  const grouped = new Map<string, CollectorStatus[]>();
  collectors.forEach((collector) => {
    const pluginId = collector.collector_plugin_id || collector.provider_id || "unknown";
    grouped.set(pluginId, [...(grouped.get(pluginId) ?? []), collector]);
  });

  return [...grouped.entries()]
    .map(([pluginId, pluginCollectors]) => {
      const sortedCollectors = [...pluginCollectors].sort((left, right) =>
        collectorCatalogId(left).localeCompare(collectorCatalogId(right))
      );
      const provider = providerById.get(pluginId);
      const collectorIds = uniqueStrings(sortedCollectors.map((collector) => collectorCatalogId(collector)));
      const datasetIds = uniqueStrings(sortedCollectors.map((collector) => collector.dataset_id || ""));
      const statuses = uniqueStrings(
        sortedCollectors.map((collector) => collector.collector_plugin_status || collector.plugin_status || "enabled")
      );
      const lifecycleCounts = countStrings(
        sortedCollectors.map((collector) => collector.lifecycle_status || (collector.is_legacy ? "legacy" : "unspecified"))
      );
      return {
        pluginId,
        collectors: sortedCollectors,
        collectorCount: sortedCollectors.length,
        collectorIds,
        datasetIds,
        runnerEntryCount: sortedCollectors.filter((collector) => Boolean(collector.runner_entry)).length,
        lifecycleCounts,
        legacyCount: sortedCollectors.filter((collector) => collector.is_legacy).length,
        statuses,
        firstCollectorId: collectorIds[0],
        displayName: provider?.source_name_zh || firstDefined(sortedCollectors.map((collector) => collector.source_name_zh)),
        sourceCode: provider?.source_code || firstDefined(sortedCollectors.map((collector) => collector.source_code)),
        sourceNameZh: provider?.source_name_zh || firstDefined(sortedCollectors.map((collector) => collector.source_name_zh)),
        description: provider?.description || firstDefined(sortedCollectors.map((collector) => collector.description))
      };
    })
    .sort((left, right) => {
      const rankDiff = collectorPluginRank(left) - collectorPluginRank(right);
      if (rankDiff !== 0) {
        return rankDiff;
      }
      return left.pluginId.localeCompare(right.pluginId);
    });
}

function uniqueStrings(values: Array<string | null | undefined>) {
  return [...new Set(values.map((value) => String(value || "").trim()).filter(Boolean))];
}

function countStrings(values: Array<string | null | undefined>) {
  return values.reduce<Record<string, number>>((counts, value) => {
    const key = String(value || "unspecified").trim() || "unspecified";
    counts[key] = (counts[key] ?? 0) + 1;
    return counts;
  }, {});
}

function firstDefined(values: Array<string | null | undefined>) {
  return values.find((value) => String(value || "").trim()) || undefined;
}

function collectorPluginRank(summary: CollectorPluginSummary) {
  if (summary.statuses.some((status) => ["failed", "incompatible", "conflict"].includes(status))) {
    return 0;
  }
  if (summary.statuses.some((status) => ["missing", "uninstalled"].includes(status))) {
    return 1;
  }
  if (summary.statuses.some((status) => ["disabled", "installed"].includes(status))) {
    return 2;
  }
  return 3;
}

function collectorPluginStatusClass(statuses: string[]) {
  if (statuses.some((status) => ["failed", "incompatible", "conflict"].includes(status))) {
    return statuses.find((status) => ["failed", "incompatible", "conflict"].includes(status)) || "failed";
  }
  if (statuses.some((status) => status === "disabled")) {
    return "disabled";
  }
  if (statuses.some((status) => status === "installed")) {
    return "installed";
  }
  if (statuses.some((status) => status === "missing" || status === "uninstalled")) {
    return statuses.find((status) => status === "missing" || status === "uninstalled") || "missing";
  }
  return "enabled";
}

function formatCollectorPluginStatuses(statuses: string[]) {
  if (statuses.length === 0) {
    return "未知";
  }
  if (statuses.length === 1) {
    return formatProviderStatus(statuses[0]);
  }
  return statuses.map(formatProviderStatus).join(" / ");
}

function formatRunnerEntrySummary(runnerEntryCount: number, collectorCount: number) {
  if (collectorCount === 0) {
    return "无采集任务";
  }
  if (runnerEntryCount === collectorCount) {
    return "全部存在";
  }
  if (runnerEntryCount === 0) {
    return "均未声明";
  }
  return `${runnerEntryCount} / ${collectorCount}`;
}

function formatCountMap(counts: Record<string, number>) {
  const entries = Object.entries(counts);
  if (entries.length === 0) {
    return "未声明";
  }
  return entries.map(([key, count]) => `${key} ${count}`).join(" / ");
}

function ProviderOverrideControls({
  busyOverrideKey,
  busyProviderId,
  onOverrideAction,
  provider
}: {
  busyOverrideKey: string | null;
  busyProviderId: string | null;
  onOverrideAction: (interfaceName: string, provider: ProviderStatus, action: "set" | "clear") => void;
  provider: ProviderStatus;
}) {
  const interfaces = provider.interfaces ?? [];
  const overridden = new Set(provider.overridden_interfaces ?? []);
  const shouldShow = provider.status === "conflict" || overridden.size > 0;
  if (!shouldShow || interfaces.length === 0) {
    return null;
  }

  return (
    <div className="provider-override-list">
      {interfaces.map((interfaceName) => {
        const isOverridden = overridden.has(interfaceName);
        const busyKey = `${interfaceName}:${provider.provider_id}`;
        const isBusy = busyOverrideKey === busyKey;
        return (
          <div className="provider-override-row" key={interfaceName}>
            <code>{interfaceName}</code>
            {isOverridden ? (
              <button
                className="ghost-action compact"
                disabled={busyProviderId !== null || busyOverrideKey !== null}
                onClick={() => onOverrideAction(interfaceName, provider, "clear")}
                type="button"
              >
                {isBusy ? <Loader2 size={15} /> : <RotateCcw size={15} />}
                清除指定
              </button>
            ) : (
              <button
                className="primary-action compact"
                disabled={busyProviderId !== null || busyOverrideKey !== null || !provider.enabled}
                onClick={() => onOverrideAction(interfaceName, provider, "set")}
                type="button"
              >
                {isBusy ? <Loader2 size={15} /> : <CheckCircle2 size={15} />}
                指定到这里
              </button>
            )}
          </div>
        );
      })}
    </div>
  );
}

function providerStatusRank(status: string) {
  if (status === "failed" || status === "incompatible" || status === "conflict") {
    return 0;
  }
  if (status === "missing" || status === "uninstalled") {
    return 1;
  }
  if (status === "disabled" || status === "installed") {
    return 2;
  }
  return 3;
}

function formatProviderStatus(status: string) {
  const labels: Record<string, string> = {
    enabled: "已启用",
    disabled: "已禁用",
    installed: "已安装",
    uninstalled: "已卸载",
    missing: "未安装",
    failed: "失败",
    incompatible: "不兼容",
    conflict: "冲突"
  };
  return labels[status] ?? (status || "未知");
}

function formatAxpVersionChange(currentVersion: string | null | undefined, nextVersion: string | null | undefined) {
  const current = currentVersion || "unknown";
  const next = nextVersion || "unknown";
  const order = compareSimpleVersions(current, next);
  if (order === null) {
    return `${current} -> ${next}`;
  }
  if (order < 0) {
    return `${current} -> ${next}（升级）`;
  }
  if (order > 0) {
    return `${current} -> ${next}（降级）`;
  }
  return `${current} -> ${next}（同版本覆盖）`;
}

function compareSimpleVersions(left: string, right: string): number | null {
  const leftParts = parseSimpleVersion(left);
  const rightParts = parseSimpleVersion(right);
  if (!leftParts || !rightParts) {
    return null;
  }
  const maxLength = Math.max(leftParts.length, rightParts.length);
  for (let index = 0; index < maxLength; index += 1) {
    const leftValue = leftParts[index] ?? 0;
    const rightValue = rightParts[index] ?? 0;
    if (leftValue !== rightValue) {
      return leftValue - rightValue;
    }
  }
  return 0;
}

function parseSimpleVersion(value: string): number[] | null {
  const core = value.trim().split(/[+-]/, 1)[0];
  if (!core || !/^\d+(?:\.\d+)*$/.test(core)) {
    return null;
  }
  return core.split(".").map((part) => Number(part));
}

function formatTrustLevel(trustLevel: string | null | undefined) {
  const labels: Record<string, string> = {
    official: "预装",
    community: "社区",
    unknown: "未知"
  };
  return labels[trustLevel || "unknown"] ?? (trustLevel || "未知");
}

function formatInstallSource(source: string | null | undefined, builtIn?: boolean) {
  const labels: Record<string, string> = {
    preinstalled: "预装插件",
    builtin: "预装插件",
    axp_managed: "AXP 管理安装",
    python_environment: "Python 环境发现",
    "editable/development": "开发路径发现",
    missing: "未安装/未发现",
    unknown: "未知来源"
  };
  return labels[source || "unknown"] ?? (source || "未知来源");
}

function providerRiskNote(provider: ProviderStatus) {
  const declared = provider.declared_trust_level || "unknown";
  const effective = provider.effective_trust_level || "unknown";
  if (declared !== effective) {
    return `声明为${formatTrustLevel(declared)}，但未通过当前信任规则验证，实际按${formatTrustLevel(effective)}处理。`;
  }
  if (effective === "official") {
    return provider.install_source === "preinstalled" || provider.built_in
      ? "预装插件随 AxData 提供；可启用、禁用或从当前 AxData 管理状态移除。"
      : "已按当前规则标记为预装信任级别。";
  }
  return "社区 Provider 默认不启用；启用后允许其代码在当前环境运行。";
}

function providerCanEnable(provider: ProviderStatus) {
  if (typeof provider.can_enable === "boolean") {
    return provider.can_enable;
  }
  return !["failed", "incompatible", "conflict"].includes(provider.status);
}

function formatChecksumStatus(status: string) {
  const labels: Record<string, string> = {
    ok: "校验通过",
    missing: "缺少校验",
    mismatch: "校验不一致"
  };
  return labels[status] ?? status;
}

function formatApiDiagnosticState(health: HealthPayload | null, error: string | null) {
  if (error) {
    return "诊断不可用";
  }
  return health?.status ?? "未连接";
}

function formatRuntimeCors(runtimeConfig: RuntimeConfig | null) {
  const origins = runtimeConfig?.cors_origins ?? [];
  if (origins.length === 0) {
    return "未配置";
  }
  return origins.join(", ");
}

function formatRestartUnavailableReason(reason: string | null | undefined) {
  const labels: Record<string, string> = {
    not_managed_by_launcher: "当前后端不是由 AxData 启动器托管",
    launcher_file_missing: "没有找到启动器状态文件",
    launcher_file_invalid: "启动器状态文件不可读",
    launcher_path_mismatch: "启动器状态文件不匹配",
    launcher_id_mismatch: "当前后端和启动器状态不匹配",
    launcher_heartbeat_stale: "启动器心跳已过期",
    launcher_process_not_alive: "启动器进程已退出"
  };
  return labels[reason || ""] ?? "当前后端不支持 Web 自动重启";
}

function formatDiagnosticSummary(payload: LocalDiagnosticsPayload | null, error: string | null) {
  if (error) {
    return "读取失败";
  }
  const summary = payload?.summary;
  if (!summary) {
    return "检查中";
  }
  return `${formatDiagnosticCheckStatus(summary.status)} ${numberValue(summary.ok)}/${numberValue(summary.warning)}/${numberValue(summary.error)}`;
}

function diagnosticStatusClass(status: string | null | undefined) {
  if (status === "ok") {
    return "ok";
  }
  if (status === "warning" || status === "skip") {
    return "warning";
  }
  if (status === "error" || status === "failed") {
    return "error";
  }
  return "unknown";
}

function diagnosticVisibilityRank(status: string | null | undefined) {
  if (status === "error" || status === "failed") {
    return 0;
  }
  if (status === "warning" || status === "skip") {
    return 1;
  }
  if (status === "ok") {
    return 2;
  }
  return 3;
}

function formatDiagnosticCheckStatus(checkOrStatus: LocalDiagnosticCheck | string | null | undefined) {
  if (typeof checkOrStatus === "object" && checkOrStatus !== null) {
    if (
      checkOrStatus.category === "paths" &&
      checkOrStatus.status === "warning" &&
      (checkOrStatus.name === "plugin_dir" || checkOrStatus.name === "plugin_site_packages")
    ) {
      return "可选";
    }
    if (checkOrStatus.category === "ports" && checkOrStatus.occupied) {
      return "运行中";
    }
    return formatDiagnosticCheckStatus(checkOrStatus.status);
  }
  const status = checkOrStatus;
  const labels: Record<string, string> = {
    ok: "正常",
    warning: "提醒",
    error: "异常",
    skip: "跳过",
    failed: "异常"
  };
  return labels[status || ""] ?? (status || "UNKNOWN");
}

function formatDiagnosticCheckMessage(check: LocalDiagnosticCheck) {
  const message = check.message ?? "";
  const plainMessage = stripBackticks(message);
  const normalized = plainMessage.toLowerCase();
  if (check.category === "dependencies") {
    if (check.available || check.status === "ok") {
      const labels: Record<string, string> = {
        duckdb: "DuckDB 已可用，用于本地查询 Parquet 数据。",
        pyarrow: "PyArrow 已可用，用于读写 Parquet 文件。",
        pandas: "Pandas 已可用，用于表格数据处理。",
        fastapi: "FastAPI 已可用，用于后端 HTTP API。",
        uvicorn: "Uvicorn 已可用，用于启动本地 API 服务。"
      };
      return labels[check.name] ?? "依赖已可用。";
    }
    return `${formatDiagnosticCheckName(check)} 不可用；请检查 Python 依赖安装。${message ? `原始信息：${stripBackticks(message)}` : ""}`;
  }
  if (normalized.includes("exists and is writable")) {
    return "目录已存在且可写。";
  }
  if (normalized.includes("exists but is not writable")) {
    return "目录存在但不可写，需要检查文件权限。";
  }
  if (normalized.includes("missing; run axdata init")) {
    if (check.name === "plugin_dir") {
      return "本地插件目录还没创建；不影响预装插件，安装 AXP 插件前可执行 axdata init 创建。";
    }
    if (check.name === "plugin_site_packages") {
      return "插件隔离依赖目录还没创建；不影响当前预装插件，安装 AXP 插件前可执行 axdata init 创建。";
    }
    return "目录还没创建；可执行 axdata init 初始化。";
  }
  if (check.category === "ports") {
    const endpoint = `${check.host ?? "127.0.0.1"}:${check.port ?? ""}`;
    if (normalized.includes("appears occupied") || check.occupied) {
      if (check.name === "api" || check.name === "web") {
        return `${endpoint} 正在被服务使用；如果页面和接口正常，这是正常状态。`;
      }
      return `${endpoint} 已被占用；如果启动失败，再换端口。`;
    }
    return `${endpoint} 当前空闲。`;
  }
  if (check.category === "registry" && check.name === "provider_registry") {
    const matched = message.match(/loaded\s+(\d+)\s+providers,\s+(\d+)\s+interfaces,\s+(\d+)\s+collectors/i);
    if (matched) {
      return `插件注册表已加载：${matched[1]} 个 Provider、${matched[2]} 个接口、${matched[3]} 个采集器。`;
    }
    if (normalized.includes("failed to load")) {
      return `插件注册表加载失败：${stripBackticks(message)}`;
    }
  }
  if (check.category === "collector" && check.name === "collector_store") {
    const matched = message.match(/(\d+)\s+recent collector run\(s\) failed/i);
    if (matched) {
      return `最近有 ${matched[1]} 次采集运行失败；可以在下方失败记录或采集页查看详情。`;
    }
    if (normalized.includes("metadata is readable")) {
      return "采集任务记录文件可读取。";
    }
    if (normalized.includes("metadata cannot be read")) {
      return `采集任务记录文件不可读取：${stripBackticks(message)}`;
    }
  }
  if (check.category === "smoke" && check.name === "real_source_smoke") {
    if (normalized.includes("can include tdx")) {
      return "真实源 smoke 可包含 TDX 接口；默认不会自动联网，需要命令行手动运行。";
    }
    if (normalized.includes("tdx items require")) {
      return "真实源 smoke 是可选检查；daily 需要先安装并启用 TDX 插件。";
    }
  }
  if (message) {
    return plainMessage;
  }
  if (check.exists && check.writable) {
    return "已存在，可写";
  }
  if (check.available) {
    return "可用";
  }
  return "";
}

function formatDiagnosticCategory(category: string) {
  const labels: Record<string, string> = {
    paths: "目录",
    dependencies: "依赖",
    registry: "插件注册表",
    ports: "端口",
    collector: "采集记录",
    smoke: "真实源测试"
  };
  return labels[category] ?? category;
}

function formatDiagnosticCheckName(check: LocalDiagnosticCheck) {
  const labels: Record<string, Record<string, string>> = {
    paths: {
      data_root: "数据根目录",
      raw_dir: "raw 原始层目录",
      staging_dir: "staging 清洗层目录",
      core_dir: "core 用户表目录",
      factor_dir: "factor 因子目录",
      metadata_root: "元数据目录",
      collector_metadata_dir: "采集元数据目录",
      plugin_dir: "本地插件目录",
      plugin_site_packages: "插件依赖目录",
      cache_root: "缓存目录",
      logs_root: "日志目录"
    },
    dependencies: {
      duckdb: "DuckDB 查询引擎",
      pyarrow: "Parquet 读写库",
      pandas: "DataFrame 处理库",
      fastapi: "HTTP API 框架",
      uvicorn: "API 启动服务"
    },
    registry: {
      provider_registry: "Provider/接口目录"
    },
    ports: {
      api: "API 端口",
      web: "Web 端口"
    },
    collector: {
      collector_store: "采集任务与运行记录"
    },
    smoke: {
      real_source_smoke: "真实源 smoke 检查"
    }
  };
  return labels[check.category]?.[check.name] ?? check.name;
}

function formatDiagnosticAction(action: string) {
  const normalized = action.toLowerCase();
  if (normalized.includes("axdata init")) {
    return "有本地目录还没创建。需要安装本地 AXP 插件或补齐目录时，运行 axdata init 初始化。";
  }
  if (normalized.includes("pip install")) {
    return "有 Python 依赖缺失。请安装项目依赖，例如 pip install -e .[dev]。";
  }
  if (normalized.includes("install and enable the tdx ext")) {
    return "需要扩展行情接口时，请安装并启用 TDX Ext 插件。";
  }
  if (normalized.includes("install and enable the tdx plugin")) {
    return "需要普通 TDX 行情接口时，请安装并启用 TDX 插件。";
  }
  if (normalized.includes("enable tdx with")) {
    return stripBackticks(action).replace("Enable TDX with", "需要 TDX 接口时启用 TDX：");
  }
  if (normalized.includes("collector run list --status failed")) {
    return "最近有采集失败记录；可以查看失败 run 列表定位原因。";
  }
  if (normalized.includes("start the api")) {
    return "启动本地 API 和 Web 服务后再刷新诊断页。";
  }
  return stripBackticks(action);
}

function numberValue(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function shortenPathTail(value: string) {
  const parts = value.split(/[\\/]/).filter(Boolean);
  return parts.length > 0 ? parts[parts.length - 1] : value;
}

function recordArray(value: unknown): Array<Record<string, unknown>> {
  return Array.isArray(value)
    ? value.filter((item): item is Record<string, unknown> => isRecord(item))
    : [];
}

function stripBackticks(value: string) {
  return value.replace(/`([^`]+)`/g, "$1");
}

function extractBacktickCommand(value: string) {
  const match = value.match(/`([^`]+)`/);
  return match?.[1] ?? null;
}

function formatUserValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  if (typeof value === "boolean") {
    return value ? "是" : "否";
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value.toLocaleString("zh-CN") : "";
  }
  if (Array.isArray(value)) {
    return value.length > 0 ? value.map((item) => formatUserValue(item)).filter(Boolean).join("、") : "";
  }
  if (isRecord(value)) {
    const min = formatUserValue(value.min);
    const max = formatUserValue(value.max);
    if (min || max) {
      return min && max ? `${min} 至 ${max}` : min || max;
    }
    return Object.entries(value)
      .map(([key, item]) => {
        const formatted = formatUserValue(item);
        return formatted ? `${formatQualityLabel(key)} ${formatted}` : "";
      })
      .filter(Boolean)
      .join("、");
  }
  const text = String(value).trim();
  const mapped = qualityStatusLabel(text);
  return mapped || formatKnownQualityText(text);
}

function qualityValueClass(value: unknown) {
  const text = String(value).toLowerCase();
  if (text.includes("pass") || text.includes("ok")) {
    return "ok";
  }
  if (text.includes("fail") || text.includes("error")) {
    return "error";
  }
  if (text.includes("warn")) {
    return "warning";
  }
  return "unknown";
}

function qualityEntryClass(key: string, value: unknown) {
  if (key === "required_columns_present") {
    return value ? "ok" : "error";
  }
  if (key === "quality_errors") {
    return "error";
  }
  if (key === "quality_warnings") {
    return "warning";
  }
  if (typeof value === "number" && value > 0) {
    if (key.includes("negative") || key.includes("duplicate")) {
      return "error";
    }
    if (key.includes("gap") || key.includes("anomaly")) {
      return "warning";
    }
  }
  return qualityValueClass(value);
}

function qualityStatusLabel(value: string) {
  const text = value.trim().toLowerCase();
  const labels: Record<string, string> = {
    ok: "正常",
    pass: "通过",
    success: "成功",
    warn: "提醒",
    warning: "提醒",
    error: "异常",
    fail: "失败",
    failed: "失败",
    missing: "缺失",
    missing_value: "有空值",
    duplicate: "有重复",
    negative: "有负数",
    not_available: "暂无数据",
    partial: "部分覆盖",
    true: "是",
    false: "否",
    append: "追加写入",
    snapshot: "快照写入",
    upsert: "按主键更新",
    upsert_by_key: "按主键更新"
  };
  return labels[text] || "";
}

function formatKnownQualityText(value: string) {
  const text = value.trim();
  if (/^Calendar alignment skipped because no date field is present\.?$/i.test(text)) {
    return "没有日期字段，已跳过交易日历检查。";
  }
  if (/^Calendar alignment skipped because no valid trade dates were parsed\.?$/i.test(text)) {
    return "未解析到有效交易日期，已跳过交易日历检查。";
  }
  if (/^Trading calendar is not available; date gaps are not classified as errors\.?$/i.test(text)) {
    return "本地没有交易日历，日期缺口暂不判定为错误。";
  }
  if (/^Trading date gap\(s\) found/i.test(text)) {
    return "发现交易日缺口，可能来自停牌、上市退市区间、源端缺口或只采集了部分范围。";
  }
  if (/^Trading calendar does not fully cover/i.test(text)) {
    return "本地交易日历覆盖不足，缺口统计可能不完整。";
  }
  if (/^Data contains non-trading date\(s\):\s*/i.test(text)) {
    return text.replace(/^Data contains non-trading date\(s\):\s*/i, "数据里包含非交易日：");
  }
  if (/^Missing primary key field\(s\):\s*/i.test(text)) {
    return text.replace(/^Missing primary key field\(s\):\s*/i, "缺少主键字段：");
  }
  if (/^Primary key contains null value\(s\)\.?$/i.test(text)) {
    return "主键字段存在空值。";
  }
  const duplicateMatch = text.match(/^Found\s+(\d+)\s+duplicate primary key row\(s\)\.?$/i);
  if (duplicateMatch) {
    return `发现 ${duplicateMatch[1]} 行重复主键。`;
  }
  if (/^Quality check failed\.?$/i.test(text)) {
    return "质量检查失败。";
  }
  return text;
}

function formatQualityLabel(key: string) {
  return QUALITY_FIELD_LABELS[key] || key.replace(/_/g, " ");
}

function formatQualityDisplayValue(key: string, value: unknown) {
  if (key === "required_columns_present") {
    return value ? "已齐全" : "有缺失";
  }
  if (key === "calendar_check_applied") {
    return value ? "已启用" : "未启用";
  }
  return formatUserValue(value);
}

function qualityEntriesForDisplay(quality?: Record<string, unknown>) {
  const source = quality ?? {};
  const entries = Object.entries(source)
    .filter(([key, value]) => shouldShowQualityEntry(key, value, source))
    .sort(([left], [right]) => qualitySortIndex(left) - qualitySortIndex(right));
  return entries;
}

function qualitySortIndex(key: string) {
  const index = QUALITY_DISPLAY_ORDER.indexOf(key);
  return index >= 0 ? index : QUALITY_DISPLAY_ORDER.length + key.localeCompare("");
}

function shouldShowQualityEntry(key: string, value: unknown, quality: Record<string, unknown>) {
  if (key.startsWith("_")) {
    return false;
  }
  if (!QUALITY_FIELD_LABELS[key]) {
    return false;
  }
  if (
    [
      "schema_columns",
      "expected_columns",
      "null_counts",
      "field_mappings",
      "numeric_positive_checks",
      "per_symbol_date_coverage",
      "unexplained_missing_dates",
      "suspension_explained_missing_dates",
      "missing_date_samples",
      "price_ohlc_anomaly_samples",
      "negative_volume_samples",
      "negative_amount_samples",
      "date_gap_explanation",
      "min_date",
      "max_date",
      "calendar_date_range"
    ].includes(key)
  ) {
    return false;
  }
  if (value === null || value === undefined || value === "") {
    return false;
  }
  const hasDateField = Boolean(quality.date_field);
  if (
    !hasDateField &&
    ["actual_date_count", "actual_trading_day_count", "expected_trading_day_count", "date_gap_count", "calendar_check_applied"].includes(key) &&
    (value === 0 || value === false)
  ) {
    return false;
  }
  if (Array.isArray(value)) {
    return value.length > 0;
  }
  if (isRecord(value)) {
    return Object.values(value).some((item) => item !== null && item !== undefined && item !== "");
  }
  return true;
}

function CollectorRunsPanel({
  busyKey,
  onDeleteRun,
  runs,
  runtimeCatalogItems
}: {
  busyKey: string | null;
  onDeleteRun: (run: CollectorRunStatus) => void;
  runs: CollectorRunStatus[];
  runtimeCatalogItems: CatalogItem[];
}) {
  const visibleRuns = runs.slice(0, COLLECTOR_RECENT_RUN_LIMIT);
  return (
    <div className="collector-runs-panel">
      <div className="section-subtitle-row">
        <strong>最近采集记录</strong>
        <span>{visibleRuns.length > 0 ? `最近 ${visibleRuns.length} 条` : "暂无历史"}</span>
      </div>
      {visibleRuns.length > 0 ? (
        <div className="collector-run-list">
          {visibleRuns.map((run) => (
            <div className={`collector-run-row ${collectorStatusClass(run.status)}`} key={run.run_id}>
              <div className="collector-run-main">
                <div>
                  <strong>{formatCollectorRunTitle(run, runtimeCatalogItems)}</strong>
                  <code>记录编号：{run.run_id}</code>
                </div>
                <div className="collector-run-head-actions">
                  <div className="provider-status-badges">
                    <span className={`provider-status-badge ${collectorStatusClass(run.status)}`}>
                      {formatCollectorRunStatus(run.status)}
                    </span>
                    <span className="provider-status-badge installed">{formatResourceGroupLabel(run.resource_group)}</span>
                  </div>
                  <button
                    aria-label={`删除采集记录 ${run.run_id}`}
                    className="collector-task-delete"
                    disabled={busyKey !== null || run.status === "running" || run.status === "queued" || run.status === "pending"}
                    onClick={() => onDeleteRun(run)}
                    title="删除这条采集记录"
                    type="button"
                  >
                    {busyKey === `run-delete:${run.run_id}` ? <Loader2 className="spin" size={14} /> : <Trash2 size={14} />}
                  </button>
                </div>
              </div>
              <dl className="provider-status-meta collector-run-meta">
                <div>
                  <dt>开始</dt>
                  <dd>{formatRunDateTime(run.started_at)}</dd>
                </div>
                <div>
                  <dt>结束</dt>
                  <dd>{formatRunDateTime(run.finished_at)}</dd>
                </div>
                <div>
                  <dt>耗时</dt>
                  <dd>{formatDuration(run.duration_ms)}</dd>
                </div>
                <div>
                  <dt>触发方式</dt>
                  <dd>{formatRunTrigger(run.trigger_type)}</dd>
                </div>
                <div>
                  <dt>队列</dt>
                  <dd>{formatCollectorQueueStatus(run.queue_status || run.status)}</dd>
                </div>
                <div>
                  <dt>阻塞</dt>
                  <dd>{formatBlockedReason(run.blocked_reason)}</dd>
                </div>
              </dl>
              <RunErrorSummary run={run} />
              {run.error ? <p className="provider-status-error">{run.error}</p> : null}
              {run.skip_reason ? <p className="provider-description">跳过原因：{run.skip_reason}</p> : null}
              <RunWriteSummary run={run} />
              <StageTimings timings={run.stage_timings} />
              {run.params_override && Object.keys(run.params_override).length > 0 ? (
                <code className="collector-param-preview">
                  {JSON.stringify(run.params_override)}
                </code>
              ) : null}
              <GuidanceNotice item={run} />
              <QualitySummary quality={run.quality} />
              <OutputPaths paths={run.output_paths} id={run.run_id} />
              <RunEventTimeline events={run.events} />
            </div>
          ))}
        </div>
      ) : (
        <div className="provider-status-empty">当前没有采集记录。</div>
      )}
    </div>
  );
}

function formatRunTrigger(value: string | null | undefined) {
  if (value === "manual") {
    return "手动";
  }
  if (value === "backfill") {
    return "补采";
  }
  if (value === "schedule" || value === "daily" || value === "interval") {
    return "自动";
  }
  return value || "";
}

function formatCollectorRunTitle(run: CollectorRunStatus, runtimeCatalogItems: CatalogItem[]) {
  const interfaceName = collectorRunInterfaceName(run);
  const rawText = `${run.collector_name || ""} ${run.task_id || ""}`;
  const fromCatalog = runtimeCatalogItems.find((item) =>
    item.name === interfaceName ||
    item.id === interfaceName ||
    item.name === run.collector_name ||
    item.id === run.collector_name ||
    rawText.includes(item.name) ||
    rawText.includes(item.id)
  );
  const collectorTitle = fromCatalog?.title || compactCollectorTaskTitle(run.collector_name, run.task_id);
  return collectorTitle || run.task_id || run.run_id;
}

function collectorRunInterfaceName(run: CollectorRunStatus) {
  const rawName = run.collector_name || "";
  const parts = rawName.split(".").filter(Boolean);
  if (parts.length >= 3) {
    return parts[1];
  }
  const taskId = run.task_id || "";
  const snapshotIndex = taskId.indexOf("_snapshot");
  if (snapshotIndex > 0) {
    return taskId.slice(0, snapshotIndex).replace(/^tdx_/, "");
  }
  return rawName;
}

function formatResourceGroupLabel(value: string | null | undefined) {
  const key = value || "default";
  const label = COLLECTOR_RESOURCE_GROUP_LABELS[key];
  return label ? `${label}` : key;
}

function formatCollectorQueueStatus(value: string | null | undefined) {
  const labels: Record<string, string> = {
    pending: "等待提交",
    queued: "排队中",
    running: "运行中",
    success: "已完成",
    failed: "失败",
    skipped: "已跳过",
    cancelled: "已取消"
  };
  return labels[value || ""] || value || "";
}

function formatBlockedReason(value: string | null | undefined) {
  if (!value) {
    return "无";
  }
  const normalized = value.toLowerCase();
  if (normalized.includes("resource")) {
    return "等待采集通道空闲";
  }
  if (normalized.includes("duplicate")) {
    return "已有同类任务在运行";
  }
  if (normalized.includes("backoff")) {
    return "失败后冷却中";
  }
  return stripBackticks(value);
}

function QualitySummary({ quality }: { quality?: Record<string, unknown> }) {
  const entries = qualityEntriesForDisplay(quality);
  if (entries.length === 0) {
    return null;
  }
  return (
    <div className="quality-summary">
      {entries.slice(0, 8).map(([key, value]) => (
        <span className={qualityEntryClass(key, value)} key={key}>
          <strong>{formatQualityLabel(key)}</strong>
          {formatQualityDisplayValue(key, value)}
        </span>
      ))}
      {entries.length > 8 ? <span>另有 {entries.length - 8} 项</span> : null}
    </div>
  );
}

function RunWriteSummary({ run }: { run: CollectorRunStatus }) {
  const entries: Array<[string, string]> = [];
  if (run.records_read !== null && run.records_read !== undefined) {
    entries.push(["读取", formatUserValue(run.records_read)]);
  }
  if (run.rows_written !== null && run.rows_written !== undefined) {
    entries.push(["写入", formatUserValue(run.rows_written)]);
  }
  if (run.write_mode) {
    entries.push(["写入方式", formatUserValue(run.write_mode)]);
  }
  if (run.rows_after !== null && run.rows_after !== undefined) {
    entries.push(["写后总行数", formatUserValue(run.rows_after)]);
  }
  if (run.duplicate_rows_dropped !== null && run.duplicate_rows_dropped !== undefined) {
    entries.push(["去重行数", formatUserValue(run.duplicate_rows_dropped)]);
  }
  if (run.partitions_touched && run.partitions_touched.length > 0) {
    entries.push(["影响文件", run.partitions_touched.slice(0, 3).join("、")]);
  }
  if (entries.length === 0) {
    return null;
  }
  return (
    <div className="quality-summary">
      {entries.map(([key, value]) => (
        <span key={key}>
          <strong>{key}</strong>
          {value}
        </span>
      ))}
    </div>
  );
}

function RunErrorSummary({ run }: { run: CollectorRunStatus }) {
  if (!run.error_category && !run.error_summary) {
    return null;
  }
  return (
    <div className="run-error-summary">
      {run.error_category ? <span>{formatRunErrorCategory(run.error_category)}</span> : null}
      {run.error_summary ? <p>{run.error_summary}</p> : null}
    </div>
  );
}

function StageTimings({ timings }: { timings?: Record<string, number | null> }) {
  const entries = Object.entries(timings ?? {}).filter(([, value]) => value !== null && value !== undefined);
  if (entries.length === 0) {
    return null;
  }
  return (
    <div className="stage-timing-list">
      {entries.map(([key, value]) => (
        <span key={key}>
          <strong>{formatTimingLabel(key)}</strong>
          {formatDuration(typeof value === "number" ? value : null)}
        </span>
      ))}
    </div>
  );
}

function RunEventTimeline({ events }: { events?: CollectorRunEvent[] }) {
  const rows = (events ?? []).slice(-8);
  if (rows.length === 0) {
    return null;
  }
  return (
    <div className="run-event-timeline">
      {rows.map((event, index) => (
        <div className={`run-event-row ${event.level || "info"}`} key={event.event_id || `${event.stage}:${index}`}>
          <span>{formatRunEventStage(event.stage)}</span>
          <p>{formatRunEventMessage(event)}</p>
          <time>{formatRunDateTime(event.timestamp)}</time>
        </div>
      ))}
    </div>
  );
}

function formatRunEventStage(stage: string | null | undefined) {
  const key = stage || "";
  return RUN_STAGE_LABELS[key] || "采集步骤";
}

function formatRunEventMessage(event: CollectorRunEvent) {
  const stage = event.stage || "";
  const details = event.details ?? {};
  if (stage === "downloaded" && details.row_count !== null && details.row_count !== undefined) {
    return `已读取 ${formatUserValue(details.row_count)} 条记录。`;
  }
  if (stage === "written" && Array.isArray(details.formats) && details.formats.length > 0) {
    const formats = details.formats.map((item) => formatOutputFileLabel(String(item))).join("、");
    return `已写入 ${formats}。`;
  }
  if (stage === "quality_checked") {
    const status = formatUserValue(details.quality_status || "");
    const warningCount = numberValue(details.warning_count);
    const errorCount = numberValue(details.error_count);
    if (errorCount > 0) {
      return `质量检查发现 ${errorCount} 个错误。`;
    }
    if (warningCount > 0) {
      return `质量检查通过，但有 ${warningCount} 个提醒。`;
    }
    return status ? `质量检查完成：${status}。` : "质量检查已完成。";
  }
  if (stage === "provider_resolved") {
    return details.runner_entry ? "已找到独立采集器入口。" : "已找到采集插件和下载配置。";
  }

  const raw = stripBackticks(event.message || "").trim();
  if (!raw) {
    return `${formatRunEventStage(stage)}已完成。`;
  }
  const exact = RUN_EVENT_MESSAGE_LABELS[raw];
  if (exact) {
    return exact;
  }
  const qualityMatch = raw.match(/^Quality check completed with status\s+([a-z_]+)\.?$/i);
  if (qualityMatch) {
    return `质量检查完成：${formatUserValue(qualityMatch[1])}。`;
  }
  const retryMatch = raw.match(/^Collector run attempt\s+(\d+)\s+failed; retrying\.?$/i);
  if (retryMatch) {
    return `第 ${retryMatch[1]} 次采集失败，准备重试。`;
  }
  if (raw.includes("Collector run")) {
    return formatGuidanceText(raw);
  }
  return raw;
}

function OutputPaths({ id, paths }: { id: string; paths?: Record<string, string> }) {
  const entries = Object.entries(paths ?? {});
  if (entries.length === 0) {
    return null;
  }
  return (
    <div className="output-path-list">
      {entries.map(([format, path]) => (
        <div className="output-path-item" key={`${id}:${format}`}>
          <strong>{formatOutputFileLabel(format)}</strong>
          <code>{path}</code>
        </div>
      ))}
    </div>
  );
}

function formatOutputFileLabel(format: string) {
  const labels: Record<string, string> = {
    csv: "CSV 文件",
    duckdb: "DuckDB 查询缓存",
    jsonl: "JSONL 文件",
    parquet: "Parquet 文件"
  };
  return labels[format] || `${format} 文件`;
}

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) {
    return "0 B";
  }
  const units = ["B", "KB", "MB", "GB"];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  const digits = unitIndex === 0 ? 0 : 1;
  return `${size.toFixed(digits)} ${units[unitIndex]}`;
}

function CollectorTasksPanel({
  busyTaskId,
  error,
  health,
  message,
  onBackfill,
  onCreateTemplate,
  onDelete,
  onDeleteRun,
  onDailyTimeChange,
  onEnableChange,
  onRun,
  onScheduleChange,
  recentRuns,
  schedulerStatus,
  showCompactTasks = false,
  showOperationSummary = true,
  showTemplates = true,
  tasks,
  templates,
  runtimeCatalogItems
}: {
  busyTaskId: string | null;
  error: string | null;
  health: HealthPayload | null;
  message: string | null;
  onBackfill: (
    task: CollectorTaskStatus,
    request: { start: string; end: string; symbol?: string; limit?: number }
  ) => void;
  onCreateTemplate: (template: CollectorTaskTemplateStatus) => void;
  onDelete: (task: CollectorTaskStatus) => void;
  onDeleteRun: (run: CollectorRunStatus) => void;
  onDailyTimeChange: (task: CollectorTaskStatus, dailyTime: string) => void;
  onEnableChange: (task: CollectorTaskStatus, enabled: boolean) => void;
  onRun: (task: CollectorTaskStatus) => void;
  onScheduleChange: (task: CollectorTaskStatus, triggerType: "manual" | "daily") => void;
  recentRuns: CollectorRunStatus[];
  schedulerStatus: CollectorSchedulerStatus | null;
  showCompactTasks?: boolean;
  showOperationSummary?: boolean;
  showTemplates?: boolean;
  tasks: CollectorTaskStatus[];
  templates: CollectorTaskTemplateStatus[];
  runtimeCatalogItems: CatalogItem[];
}) {
  const sortedTasks = [...tasks].sort((left, right) => {
    if (left.enabled !== right.enabled) {
      return left.enabled ? -1 : 1;
    }
    return left.task_id.localeCompare(right.task_id);
  });
  const activeRunByTask = new Map<string, CollectorRunStatus>();
  const activeRuns = schedulerStatus?.active_runs ?? [];
  for (const run of activeRuns) {
    activeRunByTask.set(run.task_id, run);
  }

  return (
    <section className={`doc-section collector-tasks-section ${showCompactTasks ? "compact" : ""}`}>
      {showOperationSummary ? (
        <>
          <div className="section-title">
            <RefreshCw size={20} />
            <h2>采集操作</h2>
          </div>
          <div className="collector-summary-strip">
            <Metric icon={Database} label="采集任务" value={`${schedulerStatus?.task_count ?? tasks.length}`} />
            <Metric icon={CheckCircle2} label="已启用" value={`${schedulerStatus?.enabled_task_count ?? tasks.filter((task) => task.enabled).length}`} />
            <Metric icon={RefreshCw} label="运行中" value={`${schedulerStatus?.active_run_count ?? 0}`} />
            <Metric icon={Code2} label="采集记录" value={`${schedulerStatus?.run_count ?? 0}`} />
          </div>
        </>
      ) : null}
      <div className={`section-subtitle-row ${showOperationSummary ? "" : "collector-task-list-title"}`}>
        <strong>任务列表</strong>
        <span>{sortedTasks.length > 0 ? `${sortedTasks.filter((task) => task.enabled).length} / ${sortedTasks.length} 已启用` : "暂无任务"}</span>
      </div>
      {showCompactTasks ? (
        <CollectorCompactTaskList
          activeRunByTask={activeRunByTask}
          busyTaskId={busyTaskId}
          latestRuns={schedulerStatus?.latest_runs ?? {}}
          onBackfill={onBackfill}
          onDelete={onDelete}
          onDailyTimeChange={onDailyTimeChange}
          onEnableChange={onEnableChange}
          onRun={onRun}
          onScheduleChange={onScheduleChange}
          tasks={sortedTasks}
        />
      ) : (
        <div className="collector-task-grid">
          {sortedTasks.length > 0 ? (
          sortedTasks.map((task) => {
            const activeRun = activeRunByTask.get(task.task_id);
            const latestRun = activeRun ?? schedulerStatus?.latest_runs?.[task.task_id] ?? null;
            const status = activeRun?.status ?? task.last_status ?? "pending";
            const isBusy = busyTaskId === task.task_id;
            return (
              <div className={`collector-task-card ${task.enabled ? "enabled" : "disabled"} ${status}`} key={task.task_id}>
                <div className="provider-status-head">
                  <div>
                    <strong>{task.name || task.collector_name}</strong>
                    <span className="collector-task-subtitle">{formatTaskOutputDir(task, health)}</span>
                  </div>
                  <div className="provider-status-badges">
                    <span className={`provider-status-badge ${task.enabled ? "enabled" : "disabled"}`}>
                      {task.enabled ? "已启用" : "已禁用"}
                    </span>
                    <span className={`provider-status-badge ${collectorStatusClass(status)}`}>
                      {formatCollectorRunStatus(status)}
                    </span>
                  </div>
                </div>
                <dl className="provider-status-meta collector-task-meta collector-task-meta-primary">
                  <div>
                    <dt>自动计划</dt>
                    <dd>{formatCollectorTrigger(task)}</dd>
                  </div>
                  <div>
                    <dt>前置数据</dt>
                    <dd>{formatRequiredDatasets(task.required_datasets)}</dd>
                  </div>
                  <div>
                    <dt>运行状态</dt>
                    <dd>{task.can_run_now === false ? task.blocked_reason || "否" : "是"}</dd>
                  </div>
                  <div>
                    <dt>写入方式</dt>
                    <dd>{formatTaskWritePolicy(task)}</dd>
                  </div>
                  <div>
                    <dt>默认目录</dt>
                    <dd>{formatTaskOutputDir(task, health)}</dd>
                  </div>
                  <div>
                    <dt>最近结果</dt>
                    <dd>{latestRun ? `${formatCollectorRunStatus(latestRun.status)} / ${formatDuration(latestRun.duration_ms)}` : task.last_run_id || "暂无"}</dd>
                  </div>
                </dl>
                <details className="collector-task-advanced">
                  <summary>
                    <Settings size={15} />
                    <span>更多任务信息</span>
                  </summary>
                  <dl className="provider-status-meta collector-task-meta">
                    <div>
                      <dt>任务来源</dt>
                      <dd>{formatTaskSource(task)}</dd>
                    </div>
                    <div>
                      <dt>任务 ID</dt>
                      <dd>{task.task_id}</dd>
                    </div>
                    <div>
                      <dt>采集任务</dt>
                      <dd>{task.collector_name}</dd>
                    </div>
                    <div>
                      <dt>接口</dt>
                      <dd>{task.interface_name || ""}</dd>
                    </div>
                    <div>
                      <dt>调度建议</dt>
                      <dd>{task.schedule_hint || ""}</dd>
                    </div>
                    <div>
                      <dt>资源组</dt>
                      <dd>{task.resource_group || "default"}</dd>
                    </div>
                    <div>
                      <dt>Profile</dt>
                      <dd>{task.downloader_profile || ""}</dd>
                    </div>
                    <div>
                      <dt>依赖</dt>
                      <dd>{formatTaskDependency(task)}</dd>
                    </div>
                    <div>
                      <dt>主键</dt>
                      <dd>{formatList(task.primary_key)}</dd>
                    </div>
                    <div>
                      <dt>分区</dt>
                      <dd>{formatList(task.partition_by)}</dd>
                    </div>
                    <div>
                      <dt>最近成功</dt>
                      <dd>{formatNullable(task.last_success_at)}</dd>
                    </div>
                    <div>
                      <dt>最近失败</dt>
                      <dd>{formatNullable(task.last_failure_at)}</dd>
                    </div>
                    <div>
                      <dt>下次运行</dt>
                      <dd>{formatNullable(task.next_run_at)}</dd>
                    </div>
                    <div>
                      <dt>退避到</dt>
                      <dd>{formatNullable(task.backoff_until)}</dd>
                    </div>
                    <div>
                      <dt>重试</dt>
                      <dd>{formatRetryPolicy(task)}</dd>
                    </div>
                    <div>
                      <dt>队列</dt>
                      <dd>{task.queue_status || "ready"}</dd>
                    </div>
                    <div>
                      <dt>最近重试</dt>
                      <dd>{latestRun?.retry_count ?? 0}</dd>
                    </div>
                  </dl>
                </details>
                {activeRun ? (
                  <p className="provider-description">
                    当前运行：{activeRun.run_id} / {formatCollectorRunStatus(activeRun.status)}
                  </p>
                ) : task.last_run_id ? (
                  <p className="provider-description">最近运行：{task.last_run_id}</p>
                ) : null}
                {task.last_error || latestRun?.error ? (
                  <p className="provider-status-error">{latestRun?.error || task.last_error}</p>
                ) : task.dependency_message ? (
                  <p className="provider-status-error">{task.dependency_message}</p>
                ) : task.last_error_summary ? (
                  <p className="provider-status-error">{task.last_error_summary}</p>
                ) : latestRun?.skip_reason ? (
                  <p className="provider-description">跳过原因：{latestRun.skip_reason}</p>
                ) : null}
                <GuidanceNotice item={task} />
                {latestRun && latestRun.status_message !== task.status_message ? (
                  <GuidanceNotice item={latestRun} />
                ) : null}
                {latestRun?.output_paths && Object.keys(latestRun.output_paths).length > 0 ? (
                  <div className="provider-interface-preview">
                    {Object.entries(latestRun.output_paths).map(([format, path]) => (
                      <code key={`${task.task_id}:${format}`}>{format}: {path}</code>
                    ))}
                  </div>
                ) : null}
                <div className="provider-status-actions">
                  <button
                    className="primary-action compact"
                    disabled={busyTaskId !== null || Boolean(activeRun) || !canRunTaskManually(task)}
                    onClick={() => onRun(task)}
                    type="button"
                  >
                    {isBusy ? <Loader2 size={15} /> : <PlayCircle size={15} />}
                    手动采集
                  </button>
                  {task.enabled ? (
                    <button
                      className="ghost-action compact"
                      disabled={busyTaskId !== null}
                      onClick={() => onEnableChange(task, false)}
                      type="button"
                    >
                      {isBusy ? <Loader2 size={15} /> : <Power size={15} />}
                      暂停任务
                    </button>
                  ) : (
                    <button
                      className="primary-action compact"
                      disabled={busyTaskId !== null}
                      onClick={() => onEnableChange(task, true)}
                      type="button"
                    >
                      {isBusy ? <Loader2 size={15} /> : <CheckCircle2 size={15} />}
                      启用任务
                    </button>
                  )}
                </div>
                <CollectorBackfillForm
                  busy={busyTaskId === `backfill:${task.task_id}`}
                  disabled={busyTaskId !== null || Boolean(activeRun) || !canRunTaskManually(task)}
                  onBackfill={(request) => onBackfill(task, request)}
                />
              </div>
            );
          })
          ) : (
            <div className="provider-status-empty">当前还没有采集任务。</div>
          )}
        </div>
      )}
      {showCompactTasks ? (
        <CollectorRunProgressPanel
          activeRuns={activeRuns}
          busyTaskId={busyTaskId}
          message={message}
          recentRuns={recentRuns}
          tasks={sortedTasks}
        />
      ) : null}
      {showTemplates ? (
        <CollectorTemplatesPanel
          busyTaskId={busyTaskId}
          health={health}
          onCreateTemplate={onCreateTemplate}
          tasks={tasks}
          templates={templates}
        />
      ) : null}
      <CollectorRunsPanel
        busyKey={busyTaskId}
        onDeleteRun={onDeleteRun}
        runs={recentRuns}
        runtimeCatalogItems={runtimeCatalogItems}
      />
      {message && !showCompactTasks ? (
        <div className="calendar-cache-refresh-state done">
          <CheckCircle2 size={16} />
          <span>{message}</span>
        </div>
      ) : null}
      {error ? <p className="form-error">{error}</p> : null}
    </section>
  );
}

function CollectorCompactTaskList({
  activeRunByTask,
  busyTaskId,
  latestRuns,
  onBackfill,
  onDelete,
  onDailyTimeChange,
  onEnableChange,
  onRun,
  onScheduleChange,
  tasks
}: {
  activeRunByTask: Map<string, CollectorRunStatus>;
  busyTaskId: string | null;
  latestRuns: Record<string, CollectorRunStatus>;
  onBackfill: (task: CollectorTaskStatus, request: { start: string; end: string; symbol?: string; limit?: number }) => void;
  onDelete: (task: CollectorTaskStatus) => void;
  onDailyTimeChange: (task: CollectorTaskStatus, dailyTime: string) => void;
  onEnableChange: (task: CollectorTaskStatus, enabled: boolean) => void;
  onRun: (task: CollectorTaskStatus) => void;
  onScheduleChange: (task: CollectorTaskStatus, triggerType: "manual" | "daily") => void;
  tasks: CollectorTaskStatus[];
}) {
  if (tasks.length === 0) {
    return <div className="provider-status-empty">当前还没有采集任务。</div>;
  }

  return (
    <div className="collector-task-table">
      <div className="collector-task-table-head">
        <span>任务</span>
        <span>方式</span>
        <span>时间</span>
        <span>定时</span>
        <span>最近状态</span>
        <span>操作</span>
        <span></span>
      </div>
      {tasks.map((task) => {
        const activeRun = activeRunByTask.get(task.task_id);
        const latestRun = activeRun ?? latestRuns[task.task_id] ?? null;
        const status = activeRun?.status ?? latestRun?.status ?? task.last_status ?? "pending";
        const isTimed = task.trigger_type === "daily" || task.trigger_type === "interval";
        const scheduleValue = task.trigger_type === "daily" ? "daily" : "manual";
        const isBusy = busyTaskId === task.task_id || busyTaskId === `schedule:${task.task_id}` || busyTaskId === `time:${task.task_id}`;
        const isDeleting = busyTaskId === `delete:${task.task_id}`;
        const canRun = !activeRun && canRunTaskManually(task);
        const taskInlineMessage = task.dependency_message || task.last_error_summary || task.status_message;
        const taskInlineMessageTone = task.dependency_message || task.last_error_summary ? "error" : "info";
        return (
          <div className="collector-task-table-row" key={task.task_id}>
            <div className="collector-task-name-cell">
              <strong>{task.name || compactCollectorTaskTitle(task.collector_name)}</strong>
              {taskInlineMessage ? (
                <span className={taskInlineMessageTone}>{taskInlineMessage}</span>
              ) : null}
            </div>
            <select
              aria-label="采集方式"
              disabled={busyTaskId !== null}
              onChange={(event) => onScheduleChange(task, event.target.value === "daily" ? "daily" : "manual")}
              value={scheduleValue}
            >
              <option value="manual">手动</option>
              <option value="daily">交易日每天</option>
            </select>
            {scheduleValue === "daily" ? (
              <input
                aria-label="定时时间"
                className="collector-task-time-input"
                disabled={busyTaskId !== null}
                key={task.daily_time || "18:00"}
                onBlur={(event) => {
                  if (event.target.value && event.target.value !== (task.daily_time || "18:00")) {
                    onDailyTimeChange(task, event.target.value);
                  }
                }}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && event.currentTarget.value) {
                    event.currentTarget.blur();
                  }
                }}
                type="time"
                defaultValue={task.daily_time || "18:00"}
              />
            ) : (
              <span className="collector-task-time-empty">未设置</span>
            )}
            <label className={`collector-task-switch ${task.enabled ? "on" : ""}`}>
              <input
                checked={task.enabled}
                disabled={busyTaskId !== null}
                onChange={(event) => onEnableChange(task, event.target.checked)}
                type="checkbox"
              />
              <span>{task.enabled ? "开" : "关"}</span>
            </label>
            <span className={`provider-status-badge ${collectorStatusClass(status)}`}>
              {formatCollectorRunStatus(status)}
            </span>
            <button
              className="primary-action compact"
              disabled={busyTaskId !== null || !canRun}
              onClick={() => onRun(task)}
              type="button"
            >
              {isBusy ? <Loader2 className="spin" size={15} /> : <PlayCircle size={15} />}
              手动采集
            </button>
            <button
              aria-label={`删除任务 ${task.name || task.task_id}`}
              className="collector-task-delete"
              disabled={busyTaskId !== null}
              onClick={() => onDelete(task)}
              title="删除任务"
              type="button"
            >
              {isDeleting ? <Loader2 className="spin" size={14} /> : <Trash2 size={14} />}
            </button>
            <CollectorBackfillForm
              busy={busyTaskId === `backfill:${task.task_id}`}
              disabled={busyTaskId !== null || Boolean(activeRun) || !canRunTaskManually(task)}
              onBackfill={(request) => onBackfill(task, request)}
            />
          </div>
        );
      })}
    </div>
  );
}

function CollectorRunProgressPanel({
  activeRuns,
  busyTaskId,
  message,
  recentRuns,
  tasks
}: {
  activeRuns: CollectorRunStatus[];
  busyTaskId: string | null;
  message: string | null;
  recentRuns: CollectorRunStatus[];
  tasks: CollectorTaskStatus[];
}) {
  const isSubmittingRun = tasks.some((task) => task.task_id === busyTaskId);
  const activeRun = activeRuns[0] ?? recentRuns.find((run) => isActiveCollectorRunStatus(run.status)) ?? null;
  const submittedRun = message?.includes("已提交运行") ? recentRuns[0] ?? null : null;
  const run = activeRun ?? submittedRun;
  if (!run && !isSubmittingRun) {
    return null;
  }
  const state = run?.status ?? "pending";
  const percent = collectorRunProgressPercent(run);
  const progressClass = `${collectorRunProgressClass(state)} ${percent == null ? "indeterminate" : ""}`.trim();
  const statusText = run ? [formatCollectorRunStatus(run.status), percent != null ? `${percent}%` : ""].filter(Boolean).join(" · ") : "提交中";
  const progressMessage = collectorRunProgressMessage(run);
  const meta = run
    ? [
        progressMessage,
        run.run_id,
        run.rows_written != null ? `写入 ${run.rows_written} 行` : "",
        run.duration_ms != null ? formatDuration(run.duration_ms) : "",
        run.error_summary || run.error || run.skip_reason || ""
      ].filter(Boolean)
    : ["正在提交采集请求"];
  return (
    <div className={`collector-run-progress ${progressClass}`}>
      <div className="collector-run-progress-head">
        <strong>采集进度</strong>
        <span>{statusText}</span>
      </div>
      <div className="collector-run-progress-track" aria-hidden="true">
        <span className="collector-run-progress-bar" style={percent != null ? { width: `${percent}%` } : undefined} />
      </div>
      <div className="collector-run-progress-meta">
        {meta.map((item) => (
          <span key={item}>{item}</span>
        ))}
      </div>
    </div>
  );
}

function canRunTaskManually(task: CollectorTaskStatus) {
  if (task.can_run_now !== false) {
    return true;
  }
  return task.blocked_reason === "task_disabled";
}

function isActiveCollectorRunStatus(status: string | null | undefined) {
  return status === "pending" || status === "queued" || status === "running";
}

function collectorRunProgressClass(status: string | null | undefined) {
  if (status === "success") {
    return "success";
  }
  if (status === "failed" || status === "cancelled") {
    return "failed";
  }
  if (status === "skipped") {
    return "skipped";
  }
  return "active";
}

function collectorRunProgressPercent(run: CollectorRunStatus | null) {
  const progress = run?.metadata?.progress;
  if (!progress || typeof progress !== "object") {
    return null;
  }
  const value = Number((progress as Record<string, unknown>).percent);
  if (!Number.isFinite(value)) {
    return null;
  }
  return Math.min(100, Math.max(0, Math.round(value)));
}

function collectorRunProgressMessage(run: CollectorRunStatus | null) {
  const progress = run?.metadata?.progress;
  if (progress && typeof progress === "object") {
    const message = (progress as Record<string, unknown>).message;
    if (typeof message === "string" && message.trim()) {
      return message.trim();
    }
  }
  const latestEvent = run?.events?.[run.events.length - 1];
  return latestEvent?.message || "";
}

function formatCollectorTrigger(task: CollectorTaskStatus) {
  if (task.trigger_type === "interval") {
    return task.interval_seconds ? `每 ${task.interval_seconds} 秒` : "循环";
  }
  if (task.trigger_type === "daily") {
    return task.daily_time ? `交易日每天 ${task.daily_time}` : "交易日每天";
  }
  if (task.trigger_type === "startup") {
    return "启动时";
  }
  return "仅手动";
}

function formatTaskSource(task: CollectorTaskStatus) {
  if (task.created_by === "system") {
    return task.template_id ? `系统默认 / ${task.template_id}` : "系统默认";
  }
  return task.template_id ? `从模板创建 / ${task.template_id}` : "手动创建";
}

function formatTaskDependency(task: CollectorTaskStatus) {
  const status = task.dependency_status || (task.required_plugin ? "unknown" : "ok");
  if (status === "available" || status === "ok") {
    if (task.required_plugin) {
      return `${task.required_plugin} / 可用`;
    }
    const datasets = formatRequiredDatasets(task.required_datasets);
    return datasets === "无" ? "无外部依赖" : `基础数据已满足：${datasets}`;
  }
  return task.dependency_message || `${task.required_plugin || "dependency"} / ${status}`;
}

function formatRequiredDatasets(values?: string[] | null) {
  if (!values || values.length === 0) {
    return "无";
  }
  return values.map(formatDatasetName).join(", ");
}

function formatDatasetName(value: string) {
  const labels: Record<string, string> = {
    trade_cal: "交易日历"
  };
  return labels[value] ?? value;
}

function formatTaskWritePolicy(task: CollectorTaskStatus) {
  return formatWritePolicyText(task.write_mode || "snapshot", task.date_field || "");
}

function formatWritePolicyText(mode: string, dateField?: string) {
  const labels: Record<string, string> = {
    append: "追加写入",
    overwrite_partition: "覆盖分区",
    replace_range: "覆盖日期范围",
    snapshot: "快照保存",
    upsert_by_key: "按主键更新"
  };
  const label = labels[mode] ?? mode;
  return dateField ? `${label} / ${dateField}` : label;
}

function formatTaskOutputDir(task: CollectorTaskStatus, health: HealthPayload | null) {
  const root = health?.data_root ?? "data";
  const layer = task.expected_layer || "core";
  const dataset = task.collector_name || task.task_id;
  return `${root}\\${layer}\\${dataset}`;
}

function formatTemplateTrigger(template: CollectorTaskTemplateStatus) {
  if (template.trigger_type === "interval") {
    return template.interval_seconds ? `每 ${template.interval_seconds} 秒` : "循环";
  }
  if (template.trigger_type === "daily") {
    return template.daily_time ? `交易日每天 ${template.daily_time}` : "交易日每天";
  }
  if (template.trigger_type === "startup") {
    return "启动时";
  }
  return "仅手动";
}

function formatRunErrorCategory(category: string) {
  const labels: Record<string, string> = {
    dependency_missing: "依赖缺失",
    invalid_params: "参数错误",
    network_error: "网络错误",
    provider_missing: "采集器缺失",
    quality_failed: "质量检查失败",
    scheduler_interrupted: "调度器中断",
    schema_mismatch: "字段不匹配",
    storage_missing: "存储缺失",
    storage_permission: "存储权限",
    upstream_empty: "上游空数据",
    upstream_error: "上游错误",
    write_failed: "写入失败",
    unknown: "未知错误"
  };
  return labels[category] ?? category;
}

function formatTemplateWritePolicy(template: CollectorTaskTemplateStatus) {
  return formatTaskWritePolicy({
    collector_name: template.collector_name,
    date_field: template.date_field,
    enabled: false,
    task_id: template.task_id || template.template_id,
    trigger_type: template.trigger_type,
    write_mode: template.write_mode
  });
}

function formatTemplateOutputDir(template: CollectorTaskTemplateStatus, health: HealthPayload | null) {
  const root = health?.data_root ?? "data";
  const layer = template.expected_layer || "core";
  const dataset = template.collector_name || template.task_id || template.template_id;
  return `${root}\\${layer}\\${dataset}`;
}

function formatList(values?: string[] | null) {
  return values && values.length > 0 ? values.join(", ") : "";
}

function formatRetryPolicy(task: CollectorTaskStatus) {
  const retries = Math.max(0, task.max_retries ?? 0);
  if (retries <= 0) {
    return "0 次";
  }
  const delay = Math.max(0, task.backoff_seconds ?? 0);
  return delay > 0 ? `最多 ${retries} 次 / 间隔 ${delay}s` : `最多 ${retries} 次`;
}

function CollectorTemplatesPanel({
  busyTaskId,
  health,
  onCreateTemplate,
  tasks,
  templates
}: {
  busyTaskId: string | null;
  health: HealthPayload | null;
  onCreateTemplate: (template: CollectorTaskTemplateStatus) => void;
  tasks: CollectorTaskStatus[];
  templates: CollectorTaskTemplateStatus[];
}) {
  const taskIds = new Set(tasks.map((task) => task.task_id));
  if (templates.length === 0) {
    return null;
  }
  return (
    <div className="collector-templates-panel">
      <div className="section-subtitle-row">
        <strong>新建采集任务</strong>
        <span>{templates.length} 个可用模板</span>
      </div>
      <div className="collector-template-grid">
        {templates.map((template) => {
          const exists =
            template.task_id ? taskIds.has(template.task_id) : false;
          const isBusy = busyTaskId === `template:${template.template_id}`;
          return (
            <div className={`collector-template-card ${template.available ? "enabled" : "disabled"}`} key={template.template_id}>
              <div className="provider-status-head">
                <div>
                  <strong>{template.title}</strong>
                  <span className="collector-task-subtitle">{formatTemplateOutputDir(template, health)}</span>
                </div>
                <span className={`provider-status-badge ${template.available ? "enabled" : "disabled"}`}>
                  {template.available ? "可创建" : "不可用"}
                </span>
                {template.system_default ? (
                  <span className="provider-status-badge installed">默认</span>
                ) : null}
              </div>
              {template.description ? <p className="provider-description">{template.description}</p> : null}
              <dl className="provider-status-meta collector-task-meta">
                <div>
                  <dt>采集内容</dt>
                  <dd>{template.collector_name}</dd>
                </div>
                <div>
                  <dt>默认计划</dt>
                  <dd>{formatTemplateTrigger(template)}</dd>
                </div>
                <div>
                  <dt>前置数据</dt>
                  <dd>{formatRequiredDatasets(template.required_datasets)}</dd>
                </div>
                <div>
                  <dt>入库规则</dt>
                  <dd>{formatTemplateWritePolicy(template)}</dd>
                </div>
                <div>
                  <dt>默认目录</dt>
                  <dd>{formatTemplateOutputDir(template, health)}</dd>
                </div>
              </dl>
              <details className="collector-task-advanced collector-template-advanced">
                <summary>
                  <Settings size={15} />
                  <span>更多模板信息</span>
                </summary>
                <dl className="provider-status-meta collector-task-meta">
                  <div>
                    <dt>模板 ID</dt>
                    <dd>{template.template_id}</dd>
                  </div>
                  <div>
                    <dt>任务 ID</dt>
                    <dd>{template.task_id || ""}</dd>
                  </div>
                  <div>
                    <dt>资源组</dt>
                    <dd>{template.resource_group || "default"}</dd>
                  </div>
                  <div>
                    <dt>层级</dt>
                    <dd>{template.expected_layer || ""}</dd>
                  </div>
                  <div>
                    <dt>接口</dt>
                    <dd>{template.interface_name || ""}</dd>
                  </div>
                  <div>
                    <dt>主键</dt>
                    <dd>{formatList(template.primary_key)}</dd>
                  </div>
                  <div>
                    <dt>分区</dt>
                    <dd>{formatList(template.partition_by)}</dd>
                  </div>
                  <div>
                    <dt>调度建议</dt>
                    <dd>{template.schedule_hint || ""}</dd>
                  </div>
                </dl>
                <code className="collector-param-preview">
                  {JSON.stringify(template.default_params ?? {})}
                </code>
              </details>
              {template.unavailable_reason ? (
                <p className="provider-description">{template.unavailable_reason}</p>
              ) : null}
              <GuidanceNotice item={template} />
              <div className="provider-status-actions">
                <button
                  className="primary-action compact"
                  disabled={busyTaskId !== null || !template.available || exists}
                  onClick={() => onCreateTemplate(template)}
                  type="button"
                >
                  {isBusy ? <Loader2 size={15} /> : <CheckCircle2 size={15} />}
                  {exists ? "已创建" : "创建任务"}
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function CollectorBackfillForm({
  busy,
  disabled,
  onBackfill
}: {
  busy: boolean;
  disabled: boolean;
  onBackfill: (request: { start: string; end: string; symbol?: string; limit?: number }) => void;
}) {
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [symbol, setSymbol] = useState("");
  const [limit, setLimit] = useState("");
  const limitNumber = limit.trim() ? Number(limit.trim()) : undefined;
  const canSubmit =
    !disabled &&
    start.trim().length > 0 &&
    end.trim().length > 0 &&
    (limitNumber === undefined || (Number.isFinite(limitNumber) && limitNumber >= 0));
  return (
    <details className="collector-backfill-panel">
      <summary>
        <RotateCcw size={15} />
        <span>补采历史</span>
      </summary>
      <form
        className="collector-backfill-form"
        onSubmit={(event) => {
          event.preventDefault();
          if (!canSubmit) {
            return;
          }
          onBackfill({
            start: start.trim(),
            end: end.trim(),
            symbol: symbol.trim() || undefined,
            limit: limitNumber
          });
        }}
      >
        <label>
          <span>开始日期</span>
          <input
            disabled={disabled || busy}
            onChange={(event) => setStart(event.target.value)}
            placeholder="YYYYMMDD"
            value={start}
          />
        </label>
        <label>
          <span>结束日期</span>
          <input
            disabled={disabled || busy}
            onChange={(event) => setEnd(event.target.value)}
            placeholder="YYYYMMDD"
            value={end}
          />
        </label>
        <label>
          <span>代码</span>
          <input
            disabled={disabled || busy}
            onChange={(event) => setSymbol(event.target.value)}
            placeholder="可选"
            value={symbol}
          />
        </label>
        <label>
          <span>条数</span>
          <input
            disabled={disabled || busy}
            inputMode="numeric"
            onChange={(event) => setLimit(event.target.value)}
            placeholder="可选"
            value={limit}
          />
        </label>
        <button className="ghost-action compact" disabled={!canSubmit || busy} type="submit">
          {busy ? <Loader2 size={15} /> : <RotateCcw size={15} />}
          开始补采
        </button>
      </form>
    </details>
  );
}

function formatCollectorRunStatus(status: string | null | undefined) {
  const labels: Record<string, string> = {
    pending: "待运行",
    queued: "排队中",
    running: "运行中",
    success: "成功",
    failed: "失败",
    skipped: "已跳过",
    cancelled: "已取消"
  };
  return labels[status || ""] ?? (status || "未知");
}

function collectorStatusClass(status: string | null | undefined) {
  if (status === "success") {
    return "enabled";
  }
  if (status === "failed" || status === "cancelled") {
    return "failed";
  }
  if (status === "skipped") {
    return "disabled";
  }
  return "installed";
}

function TradeCalendarCachePanel({
  endDate,
  error,
  isRefreshing,
  maintenance,
  maintenanceBusy,
  maintenanceEnabled,
  maintenanceError,
  maintenanceFutureDays,
  maintenanceMessage,
  maintenancePastDays,
  maintenanceTime,
  onEndDateChange,
  onMaintenanceEnabledChange,
  onMaintenanceFutureDaysChange,
  onMaintenancePastDaysChange,
  onMaintenanceSave,
  onMaintenanceTimeChange,
  onRefresh,
  onStartDateChange,
  refreshMessage,
  startDate,
  status
}: {
  endDate: string;
  error: string | null;
  isRefreshing: boolean;
  maintenance: TradeCalendarMaintenanceConfig | null;
  maintenanceBusy: boolean;
  maintenanceEnabled: boolean;
  maintenanceError: string | null;
  maintenanceFutureDays: string;
  maintenanceMessage: string | null;
  maintenancePastDays: string;
  maintenanceTime: string;
  onEndDateChange: (value: string) => void;
  onMaintenanceEnabledChange: (value: boolean) => void;
  onMaintenanceFutureDaysChange: (value: string) => void;
  onMaintenancePastDaysChange: (value: string) => void;
  onMaintenanceSave: () => void;
  onMaintenanceTimeChange: (value: string) => void;
  onRefresh: (mode: "default" | "custom") => void;
  onStartDateChange: (value: string) => void;
  refreshMessage: string | null;
  startDate: string;
  status: TradeCalendarCacheStatus | null;
}) {
  const state = calendarBaseDataState(status);
  return (
    <section className="doc-section">
      <div className="section-title">
        <Database size={20} />
        <h2>基础数据 / 交易日历</h2>
      </div>

      <div className="calendar-settings-layout">
        <div className="calendar-status-card">
          <DataTable
            columns={["项目", "当前值", "说明"]}
            rows={[
              ["当前状态", state, "采集任务运行前会检查交易日历是否满足"],
              ["覆盖范围", formatCalendarCoverage(status), "默认同步当日前后 180 天；可补全指定范围"],
              ["今天", formatCalendarToday(status), "用于判断今天是否交易日"],
              ["记录数", String(status?.row_count ?? 0), "自然日记录数"],
              ["上次更新", formatNullable(status?.updated_at), "本地基础数据更新时间"],
              ["每日维护", maintenanceEnabled ? `已开启，${maintenanceTime}` : "已关闭", "用户显式开启后才会定时补全"]
            ]}
          />
        </div>

        <div className="calendar-settings-grid">
          <div className="calendar-cache-actions">
            <div className="section-subtitle-row">
              <strong>手动同步</strong>
              <span>立即补全本地交易日历</span>
            </div>
            <div className="calendar-cache-action-row">
              <button className="primary-action" disabled={isRefreshing} onClick={() => onRefresh("default")} type="button">
                {isRefreshing ? <Loader2 size={17} /> : <RefreshCw size={17} />}
                {isRefreshing ? "正在同步" : "一键同步"}
              </button>
              <button className="ghost-action" disabled={isRefreshing} onClick={() => onRefresh("custom")} type="button">
                补全指定范围
              </button>
            </div>
            <div className="calendar-cache-date-row">
              <label className="download-field compact">
                <span>开始日期</span>
                <input
                  placeholder="YYYYMMDD"
                  value={startDate}
                  onChange={(event) => onStartDateChange(event.target.value)}
                />
              </label>
              <label className="download-field compact">
                <span>结束日期</span>
                <input
                  placeholder="YYYYMMDD"
                  value={endDate}
                  onChange={(event) => onEndDateChange(event.target.value)}
                />
              </label>
            </div>
          </div>

          <div className="calendar-maintenance-card">
            <div className="section-subtitle-row">
              <strong>每日自动维护</strong>
              <span>{maintenance?.path ? "本机设置" : "默认关闭"}</span>
            </div>
            <label className="checkbox-chip calendar-maintenance-toggle">
              <input
                checked={maintenanceEnabled}
                onChange={(event) => onMaintenanceEnabledChange(event.target.checked)}
                type="checkbox"
              />
              每天自动补全交易日历
            </label>
            <div className="calendar-maintenance-grid">
              <label className="download-field compact">
                <span>运行时间</span>
                <input
                  type="time"
                  value={maintenanceTime}
                  onChange={(event) => onMaintenanceTimeChange(event.target.value)}
                />
              </label>
              <label className="download-field compact">
                <span>历史覆盖天数</span>
                <input
                  inputMode="numeric"
                  type="number"
                  min={0}
                  max={3650}
                  value={maintenancePastDays}
                  onChange={(event) => onMaintenancePastDaysChange(event.target.value)}
                />
              </label>
              <label className="download-field compact">
                <span>未来覆盖天数</span>
                <input
                  inputMode="numeric"
                  type="number"
                  min={0}
                  max={3650}
                  value={maintenanceFutureDays}
                  onChange={(event) => onMaintenanceFutureDaysChange(event.target.value)}
                />
              </label>
            </div>
            <p className="guide-note compact">每天按这里设置的历史覆盖天数和未来覆盖天数维护交易日历；写入时按市场和日期去重，保留最新记录。</p>
            <button className="primary-action compact" disabled={maintenanceBusy} onClick={onMaintenanceSave} type="button">
              {maintenanceBusy ? <Loader2 size={15} /> : <Save size={15} />}
              保存维护设置
            </button>
            {maintenanceMessage ? <p className="form-success">{maintenanceMessage}</p> : null}
            {maintenanceError ? <p className="form-error">{maintenanceError}</p> : null}
          </div>
        </div>
      </div>
      {isRefreshing ? (
        <div className="calendar-cache-refresh-state">
          <Loader2 size={16} />
          <span>{refreshMessage ?? "正在同步交易日历..."}</span>
        </div>
      ) : refreshMessage ? (
        <div className="calendar-cache-refresh-state done">
          <CheckCircle2 size={16} />
          <span>{refreshMessage}</span>
        </div>
      ) : null}
      {error ? <p className="form-error">{error}</p> : null}
    </section>
  );
}

function TdxServersPanel({
  busyKey,
  error,
  extDrafts,
  extStatus,
  message,
  onAction,
  onExtDraftsChange,
  onImportRows,
  onProbeScheduleChange,
  onProbeScheduleDisable,
  onProbeScheduleSave,
  onQuoteDraftsChange,
  onRowsClear,
  probeSchedule,
  quoteDrafts,
  quoteStatus
}: {
  busyKey: string | null;
  error: string | null;
  extDrafts: TdxServerRow[];
  extStatus: TdxServerStatus | null;
  message: string | null;
  onAction: (kind: TdxServerKind, action: "save" | "probe" | "reset") => void;
  onExtDraftsChange: (value: TdxServerRow[]) => void;
  onImportRows: (kind: TdxServerKind, rows: TdxServerRow[]) => void;
  onProbeScheduleChange: (value: TdxServerProbeSchedule) => void;
  onProbeScheduleDisable: () => void;
  onProbeScheduleSave: () => void;
  onQuoteDraftsChange: (value: TdxServerRow[]) => void;
  onRowsClear: (kind: TdxServerKind) => void;
  probeSchedule: TdxServerProbeSchedule;
  quoteDrafts: TdxServerRow[];
  quoteStatus: TdxServerStatus | null;
}) {
  return (
    <section className="doc-section">
      <TdxProbeSchedulePanel
        busyKey={busyKey}
        onChange={onProbeScheduleChange}
        onDisable={onProbeScheduleDisable}
        onSave={onProbeScheduleSave}
        schedule={probeSchedule}
      />
      <div className="tdx-server-grid">
        <TdxServerCard
          busyKey={busyKey}
          drafts={quoteDrafts}
          kind="quote"
          onAction={onAction}
          onDraftsChange={onQuoteDraftsChange}
          onImportRows={onImportRows}
          onRowsClear={onRowsClear}
          status={quoteStatus}
          title="普通行情服务器"
        />
        <TdxServerCard
          busyKey={busyKey}
          drafts={extDrafts}
          kind="extended"
          onAction={onAction}
          onDraftsChange={onExtDraftsChange}
          onImportRows={onImportRows}
          onRowsClear={onRowsClear}
          status={extStatus}
          title="扩展行情服务器"
        />
      </div>
      {busyKey ? (
        <div className="calendar-cache-refresh-state">
          <Loader2 size={16} />
          <span>正在处理服务器配置...</span>
        </div>
      ) : message ? (
        <div className="calendar-cache-refresh-state done">
          <CheckCircle2 size={16} />
          <span>{message}</span>
        </div>
      ) : null}
      {error ? <p className="form-error">{error}</p> : null}
    </section>
  );
}

function TdxProbeSchedulePanel({
  busyKey,
  onChange,
  onDisable,
  onSave,
  schedule
}: {
  busyKey: string | null;
  onChange: (value: TdxServerProbeSchedule) => void;
  onDisable: () => void;
  onSave: () => void;
  schedule: TdxServerProbeSchedule;
}) {
  const isSaving = busyKey === "probe_schedule.save";
  const isDisabling = busyKey === "probe_schedule.disable";
  function update(patch: Partial<TdxServerProbeSchedule>) {
    onChange(normalizeTdxProbeSchedule({ ...schedule, ...patch }));
  }
  return (
    <div className="tdx-probe-schedule">
      <div className="schedule-status-strip">
        <span className={schedule.enabled ? "status-dot on" : "status-dot"} />
        <strong>{schedule.enabled ? "定时测速已开启" : "定时测速未开启"}</strong>
        <span>{formatTdxProbeSchedule(schedule)}</span>
      </div>
      <div className="collect-schedule-grid">
        <label className="download-field">
          <span>频率</span>
          <select
            value={schedule.frequency}
            onChange={(event) => update({ frequency: event.target.value as TdxServerProbeSchedule["frequency"] })}
          >
            <option value="daily">每天</option>
            <option value="weekly">每周</option>
          </select>
        </label>
        {schedule.frequency === "weekly" ? (
          <label className="download-field">
            <span>星期</span>
            <select value={schedule.weekday} onChange={(event) => update({ weekday: event.target.value })}>
              <option value="1">周一</option>
              <option value="2">周二</option>
              <option value="3">周三</option>
              <option value="4">周四</option>
              <option value="5">周五</option>
              <option value="6">周六</option>
              <option value="7">周日</option>
            </select>
          </label>
        ) : null}
        <label className="download-field">
          <span>时间</span>
          <input type="time" value={schedule.time} onChange={(event) => update({ time: event.target.value })} />
        </label>
        <label className="download-field">
          <span>测速对象</span>
          <select
            value={schedule.kinds.join(",")}
            onChange={(event) => update({ kinds: event.target.value.split(",").filter(Boolean) as TdxServerKind[] })}
          >
            <option value="quote,extended">普通 + 扩展</option>
            <option value="quote">只测普通行情</option>
            <option value="extended">只测扩展行情</option>
          </select>
        </label>
        <div className="schedule-action-row">
          <button className="primary-action schedule-save-action" disabled={Boolean(busyKey)} onClick={onSave} type="button">
            {isSaving ? <Loader2 size={17} /> : <PlayCircle size={17} />}
            {isSaving ? "正在开启" : "开启定时测速"}
          </button>
          <button
            className="ghost-action schedule-disable-action"
            disabled={Boolean(busyKey) || !schedule.enabled}
            onClick={onDisable}
            type="button"
          >
            {isDisabling ? <Loader2 size={17} /> : <Power size={17} />}
            {isDisabling ? "正在关闭" : "关闭"}
          </button>
        </div>
      </div>
    </div>
  );
}

function TdxServerCard({
  busyKey,
  drafts,
  kind,
  onAction,
  onDraftsChange,
  onImportRows,
  onRowsClear,
  status,
  title
}: {
  busyKey: string | null;
  drafts: TdxServerRow[];
  kind: TdxServerKind;
  onAction: (kind: TdxServerKind, action: "save" | "probe" | "reset") => void;
  onDraftsChange: (value: TdxServerRow[]) => void;
  onImportRows: (kind: TdxServerKind, rows: TdxServerRow[]) => void;
  onRowsClear: (kind: TdxServerKind) => void;
  status: TdxServerStatus | null;
  title: string;
}) {
  const [showAll, setShowAll] = useState(false);
  const [showImportHelp, setShowImportHelp] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const visibleRows = showAll ? drafts : drafts.slice(0, 12);
  const isBusy = (action: string) => busyKey === `${kind}.${action}`;

  function updateRow(index: number, patch: Partial<TdxServerRow>) {
    onDraftsChange(drafts.map((row, rowIndex) => (rowIndex === index ? { ...row, ...patch } : row)));
  }

  function addRow() {
    onDraftsChange([
      ...drafts,
      {
        name: kind === "quote" ? "普通行情" : "扩展行情",
        host: "",
        port: kind === "quote" ? 7709 : 7727,
        enabled: true,
        priority: drafts.length + 1
      }
    ]);
    setShowAll(true);
  }

  function removeRow(index: number) {
    onDraftsChange(drafts.filter((_, rowIndex) => rowIndex !== index));
  }

  async function importRowsFromFile(file: File | undefined) {
    if (!file) {
      return;
    }
    try {
      const text = await file.text();
      const rows = parseTdxServerImport(text, kind);
      onImportRows(kind, rows);
      setShowAll(true);
    } catch (error) {
      window.alert(error instanceof Error ? error.message : "服务器文件导入失败");
    } finally {
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  }

  return (
    <div className="tdx-server-card">
      <div className="tdx-server-card-head">
        <div>
          <strong>{title}</strong>
          <span>{formatTdxServerSource(status?.source)}</span>
        </div>
        <div className="tdx-server-status-strip">
          <span>{formatTdxServerCount(status, drafts)}</span>
          <span>{formatTdxProbeSummary(drafts)}</span>
        </div>
      </div>
      <div className="tdx-server-actions">
        <button className="primary-action" disabled={Boolean(busyKey) || drafts.length === 0} onClick={() => onAction(kind, "probe")} type="button">
          {isBusy("probe") ? <Loader2 size={17} /> : <RefreshCw size={17} />}
          测速排序
        </button>
        <button className="ghost-action" disabled={Boolean(busyKey) || drafts.length === 0} onClick={() => onAction(kind, "save")} type="button">
          <Save size={17} />
          保存
        </button>
        <button className="ghost-action" disabled={Boolean(busyKey)} onClick={() => onAction(kind, "reset")} type="button">
          <RotateCcw size={17} />
          恢复内置
        </button>
        <button className="ghost-action" disabled={Boolean(busyKey)} onClick={() => fileInputRef.current?.click()} type="button">
          批量导入
        </button>
        <button className="ghost-action" onClick={() => setShowImportHelp((value) => !value)} type="button">
          格式示例
        </button>
        <button className="ghost-action danger" disabled={Boolean(busyKey) || drafts.length === 0} onClick={() => onRowsClear(kind)} type="button">
          全部删除
        </button>
        <input
          ref={fileInputRef}
          accept=".json,application/json"
          className="hidden-file-input"
          type="file"
          onChange={(event) => importRowsFromFile(event.target.files?.[0])}
        />
      </div>
      {showImportHelp ? (
        <div className="tdx-server-import-help">
          <CodeBlock code={tdxServerImportExample(kind)} language="json" />
        </div>
      ) : null}
      <div className="tdx-server-table">
        <div className="tdx-server-table-head">
          <span>启用</span>
          <span>名称</span>
          <span>地址</span>
          <span>端口</span>
          <span>延迟</span>
          <span>操作</span>
        </div>
        {visibleRows.map((row, index) => (
          <div className="tdx-server-row" key={`${row.host}:${row.port}:${index}`}>
            <label className="tdx-server-check">
              <input
                checked={row.enabled}
                type="checkbox"
                onChange={(event) => updateRow(index, { enabled: event.target.checked })}
              />
            </label>
            <input value={row.name} onChange={(event) => updateRow(index, { name: event.target.value })} />
            <input value={row.host} onChange={(event) => updateRow(index, { host: event.target.value })} />
            <input value={row.port} onChange={(event) => updateRow(index, { port: event.target.value })} />
            <span className={row.last_error ? "tdx-latency error" : "tdx-latency"}>
              {formatTdxLatency(row)}
            </span>
            <button className="ghost-action compact" onClick={() => removeRow(index)} type="button">
              删除
            </button>
          </div>
        ))}
        {visibleRows.length === 0 ? (
          <div className="tdx-server-empty">当前列表为空。可以批量导入、添加服务器，或点击恢复内置。</div>
        ) : null}
      </div>
      <div className="tdx-server-footer">
        <button className="ghost-action" onClick={addRow} type="button">
          添加服务器
        </button>
        {drafts.length > 12 ? (
          <button className="ghost-action" onClick={() => setShowAll((value) => !value)} type="button">
            {showAll ? "收起" : `显示全部 ${drafts.length} 个`}
          </button>
        ) : null}
      </div>
    </div>
  );
}

function formatCalendarCoverage(status: TradeCalendarCacheStatus | null) {
  if (!status?.start_date || !status?.end_date) {
    return "未缓存";
  }
  return `${status.start_date} - ${status.end_date}`;
}

function calendarBaseDataState(status: TradeCalendarCacheStatus | null) {
  if (!status?.exists || !status.start_date || !status.end_date || (status.row_count ?? 0) <= 0) {
    return "未同步";
  }
  if (status.covers_today === false) {
    return "覆盖不足";
  }
  return "已同步";
}

function formatCalendarToday(status: TradeCalendarCacheStatus | null) {
  if (!status?.today) {
    return "";
  }
  if (!status.covers_today) {
    return `${status.today} 未覆盖`;
  }
  return status.today_is_open ? `${status.today} 交易日` : `${status.today} 非交易日`;
}

function formatNullable(value: string | null | undefined) {
  return value || "";
}

function formatRunDateTime(value: string | null | undefined) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).format(date).replace(/\//g, "-");
}

function formatNullableDateTime(value: string | null | undefined) {
  if (!value) {
    return "未记录";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  }).format(date);
}

function formatCalendarRefreshResult(status: TradeCalendarCacheStatus) {
  const ranges = status.fetched_ranges ?? [];
  if (ranges.length === 0) {
    return "已检查，本地缓存没有缺口";
  }
  const first = ranges[0];
  const last = ranges[ranges.length - 1];
  const rangeText = ranges.length === 1
    ? `${first.start_date} - ${first.end_date}`
    : `${first.start_date} - ${last.end_date}`;
  return `已补全 ${rangeText}，新增 ${status.fetched_row_count ?? 0} 条`;
}
