"""东方财富涨停池:自动下载 + 本地沉淀 + 可视化报表。

每次运行自动完成:
1. 调用 eastmoney_limit_up_pool 拉取指定交易日数据
2. 落盘到 output/limit_up/daily/limit_up_YYYYMMDD.csv
3. 合并进 output/limit_up/history.parquet 历史库(按 日期+代码 去重)
4. 生成单文件 HTML 报表:统计卡片、连板梯队、行业分布、可排序明细表,
   历史库超过 1 天时附带近 20 个交易日趋势图

用法:
    python scripts/viz_limit_up.py                  # 今日(下载+落盘+报表)
    python scripts/viz_limit_up.py --date 20260825  # 指定日期
    python scripts/viz_limit_up.py --backfill 30    # 回补最近 30 个自然日
    python scripts/viz_limit_up.py --start 20260801 --end 20260826
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "packages" / "axdata-sdk" / "src"))

import axdata as ax  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "output"
DATA_DIR = OUT_DIR / "limit_up"
DAILY_DIR = DATA_DIR / "daily"
HISTORY_PATH = DATA_DIR / "history.parquet"
TREND_DAYS = 20


def fmt_yi(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value / 1e8:.2f}亿"


def fmt_wan(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value / 1e4:.0f}万"


def fmt_time(value: str | None) -> str:
    if not value or len(value) != 6:
        return value or "-"
    return f"{value[:2]}:{value[2:4]}:{value[4:]}"


# ---------------------------------------------------------------- 下载与落盘


def fetch_day(client: ax.AxDataClient, day: str) -> list[dict]:
    df = client.call("eastmoney_limit_up_pool", trade_date=day)
    if df is None or df.empty:
        return []
    return json.loads(df.to_json(orient="records", force_ascii=False))


def save_daily_csv(rows: list[dict], day: str) -> Path:
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    path = DAILY_DIR / f"limit_up_{day}.csv"
    import pandas as pd

    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    return path


def upsert_history(rows: list[dict]) -> int:
    import pandas as pd

    new_df = pd.DataFrame(rows)
    if HISTORY_PATH.exists():
        old = pd.read_parquet(HISTORY_PATH)
        combined = pd.concat([old, new_df], ignore_index=True)
    else:
        combined = new_df
    combined = (
        combined.drop_duplicates(subset=["trade_date", "instrument_id"], keep="last")
        .sort_values(["trade_date", "instrument_id"])
        .reset_index(drop=True)
    )
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(HISTORY_PATH, index=False)
    return len(combined)


def load_history_summary() -> list[dict]:
    """按交易日汇总:家数、最高连板、主力净流入合计。"""
    import pandas as pd

    if not HISTORY_PATH.exists():
        return []
    df = pd.read_parquet(HISTORY_PATH)
    if df.empty:
        return []
    g = df.groupby("trade_date").agg(
        count=("instrument_id", "size"),
        max_ladder=("continuous_count", "max"),
        inflow=("main_inflow", "sum"),
    ).reset_index()
    return json.loads(g.to_json(orient="records", force_ascii=False))


# ---------------------------------------------------------------- HTML 模板

CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: "Microsoft YaHei", "PingFang SC", sans-serif; background: #f5f6fa; color: #2d3436; padding: 24px; }
.wrap { max-width: 1200px; margin: 0 auto; }
h1 { font-size: 22px; margin-bottom: 4px; }
.sub { color: #7f8c8d; font-size: 13px; margin-bottom: 20px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; margin-bottom: 20px; }
.card { background: #fff; border-radius: 10px; padding: 16px 18px; box-shadow: 0 1px 4px rgba(0,0,0,.06); }
.card .label { font-size: 12px; color: #95a5a6; margin-bottom: 6px; }
.card .value { font-size: 26px; font-weight: 700; }
.card .value.red { color: #e74c3c; }
.card .value.blue { color: #2980b9; }
.panel { background: #fff; border-radius: 10px; padding: 18px 20px; box-shadow: 0 1px 4px rgba(0,0,0,.06); margin-bottom: 20px; }
.panel h2 { font-size: 15px; margin-bottom: 14px; color: #34495e; }
.bars { display: flex; flex-direction: column; gap: 7px; }
.bar-row { display: grid; grid-template-columns: 110px 1fr 52px; align-items: center; gap: 10px; font-size: 13px; }
.bar-track { background: #ecf0f1; border-radius: 4px; height: 18px; overflow: hidden; }
.bar-fill { height: 100%; border-radius: 4px; background: linear-gradient(90deg,#e74c3c,#c0392b); }
.bar-fill.blue { background: linear-gradient(90deg,#3498db,#2980b9); }
.bar-row .num { text-align: right; color: #555; font-variant-numeric: tabular-nums; }
.trend { display: flex; align-items: flex-end; gap: 6px; height: 150px; padding-top: 18px; }
.trend .col { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: flex-end; height: 100%; gap: 3px; }
.trend .col .bar { width: 70%; background: linear-gradient(180deg,#e74c3c,#c0392b); border-radius: 3px 3px 0 0; min-height: 3px; }
.trend .col.blue .bar { background: linear-gradient(180deg,#3498db,#2980b9); }
.trend .col .v { font-size: 11px; color: #e74c3c; font-weight: 700; }
.trend .col.blue .v { color: #2980b9; }
.trend .col .d { font-size: 10px; color: #95a5a6; white-space: nowrap; }
.trend .col.today .d { color: #2d3436; font-weight: 700; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { background: #f8f9fa; text-align: right; padding: 9px 10px; cursor: pointer; user-select: none; white-space: nowrap; color: #566; position: sticky; top: 0; }
th:first-child, td:first-child { text-align: left; }
th.asc::after { content: " ▲"; color: #e74c3c; }
th.desc::after { content: " ▼"; color: #2980b9; }
td { padding: 8px 10px; text-align: right; border-top: 1px solid #eef0f2; white-space: nowrap; font-variant-numeric: tabular-nums; }
td.name { text-align: left; font-weight: 600; }
tr:hover td { background: #fdf3f2; }
.up { color: #e74c3c; font-weight: 700; }
.lb { display: inline-block; min-width: 22px; text-align: center; background: #e74c3c; color: #fff; border-radius: 4px; padding: 1px 5px; font-size: 12px; font-weight: 700; }
.lb.hi { background: #9b59b6; }
.lb.low { background: #95a5a6; }
.table-scroll { max-height: 640px; overflow: auto; }
.footer { text-align: center; color: #b2bec3; font-size: 12px; margin-top: 8px; line-height: 1.8; }
"""

JS = """
function sortTable(n) {
  const table = document.getElementById('pool');
  const tbody = table.querySelector('tbody');
  const rows = Array.from(tbody.rows);
  const asc = !table.dataset['sort' + n] || table.dataset['sort' + n] === 'desc';
  table.dataset['sort' + n] = asc ? 'asc' : 'desc';
  document.querySelectorAll('th').forEach(th => th.classList.remove('asc', 'desc'));
  table.querySelectorAll('th')[n].classList.add(asc ? 'asc' : 'desc');
  rows.sort((a, b) => {
    const va = a.cells[n].dataset.v ?? a.cells[n].innerText;
    const vb = b.cells[n].dataset.v ?? b.cells[n].innerText;
    const na = parseFloat(va), nb = parseFloat(vb);
    const cmp = (!isNaN(na) && !isNaN(nb)) ? na - nb : String(va).localeCompare(String(vb), 'zh');
    return asc ? cmp : -cmp;
  });
  rows.forEach(r => tbody.appendChild(r));
}
"""


def render_bar_rows(counter: Counter, blue: bool = False) -> str:
    if not counter:
        return ""
    top = counter.most_common(12)
    peak = top[0][1]
    color = "bar-fill blue" if blue else "bar-fill"
    rows = []
    for name, count in top:
        width = max(4, round(count / peak * 100))
        rows.append(
            f'<div class="bar-row"><span>{name}</span>'
            f'<div class="bar-track"><div class="{color}" style="width:{width}%"></div></div>'
            f'<span class="num">{count}</span></div>'
        )
    return "\n".join(rows)


def render_trend(summary: list[dict], key: str, blue: bool = False, title: str = "") -> str:
    """近 N 个交易日纵向柱状图。key: count / max_ladder"""
    recent = summary[-TREND_DAYS:]
    if len(recent) < 2:
        return ""
    peak = max(r[key] for r in recent) or 1
    cols = []
    for r in recent:
        d = str(r.get("trade_date", ""))
        label = f"{d[4:6]}-{d[6:]}" if len(d) == 8 else d
        height = max(4, round(r[key] / peak * 100))
        today_cls = " today" if r is recent[-1] else ""
        blue_cls = " blue" if blue else ""
        cols.append(
            f'<div class="col{blue_cls}{today_cls}">'
            f'<span class="v">{r[key]}</span>'
            f'<div class="bar" style="height:{height}%"></div>'
            f'<span class="d">{label}</span></div>'
        )
    return (
        f'<div class="panel"><h2>{title}(近 {len(recent)} 个交易日)</h2>'
        f'<div class="trend">{"".join(cols)}</div></div>'
    )


def lb_badge(cc) -> str:
    cc = int(cc or 1)
    cls = "lb hi" if cc >= 5 else ("lb low" if cc <= 1 else "lb")
    return f'<span class="{cls}">{cc}</span>'


def build_html(rows: list[dict], summary: list[dict]) -> str:
    trade_date = str(rows[0].get("trade_date", ""))

    ladder: Counter = Counter()
    sector: Counter = Counter()
    for r in rows:
        cc = r.get("continuous_count")
        ladder[f"{int(cc)}连板" if cc and cc > 1 else "首板"] += 1
        sector[r.get("sector") or "其他"] += 1

    max_cc = max((int(r.get("continuous_count") or 1) for r in rows))
    total_inflow = sum(r.get("main_inflow") or 0 for r in rows)
    zero_open = sum(1 for r in rows if not r.get("open_times"))
    hist_days = len(summary)

    trs = []
    for r in rows:
        pct = r.get("change_pct")
        pct_cell = (
            f'<td data-v="{pct or 0}" class="up">{pct:.2f}%</td>'
            if pct is not None
            else '<td data-v="0">-</td>'
        )
        trs.append(
            "<tr>"
            f'<td>{r.get("instrument_id") or ""}</td>'
            f'<td class="name">{r.get("name") or ""}</td>'
            f'<td data-v="{r.get("continuous_count") or 0}">{lb_badge(r.get("continuous_count"))}</td>'
            f'<td data-v="{r.get("open_times") or 0}">{r.get("open_times") if r.get("open_times") is not None else "-"}次</td>'
            f'<td data-v="{r.get("last_price") or 0}" class="up">{r.get("last_price") if r.get("last_price") is not None else "-"}</td>'
            + pct_cell
            + f'<td data-v="{r.get("amount") or 0}">{fmt_wan(r.get("amount"))}</td>'
            f'<td data-v="{r.get("float_market_value") or 0}">{fmt_yi(r.get("float_market_value"))}</td>'
            f'<td data-v="{r.get("turnover_rate") or 0}">{(r.get("turnover_rate") or 0):.2f}%</td>'
            f'<td>{fmt_time(r.get("first_limit_time"))}</td>'
            f'<td>{fmt_time(r.get("last_limit_time"))}</td>'
            f'<td data-v="{r.get("main_inflow") or 0}">{fmt_yi(r.get("main_inflow"))}</td>'
            f'<td>{r.get("sector") or "-"}</td>'
            "</tr>"
        )

    trend_html = (
        render_trend(summary, "count", title="每日涨停家数走势")
        + render_trend(summary, "max_ladder", blue=True, title="每日最高连板走势")
    )

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>涨停池 {trade_date}</title>
<style>{CSS}</style>
</head>
<body>
<div class="wrap">
  <h1>📊 涨停池可视化报表</h1>
  <div class="sub">交易日 {trade_date} · 数据源 eastmoney_limit_up_pool · 共 {len(rows)} 只 · 历史库 {hist_days} 个交易日</div>

  <div class="cards">
    <div class="card"><div class="label">涨停家数</div><div class="value red">{len(rows)}</div></div>
    <div class="card"><div class="label">最高连板</div><div class="value red">{max_cc} 板</div></div>
    <div class="card"><div class="label">一字/未开板</div><div class="value">{zero_open} 只</div></div>
    <div class="card"><div class="label">主力净流入合计</div><div class="value blue">{fmt_yi(total_inflow)}</div></div>
  </div>

  {trend_html}
  <div class="panel"><h2>连板梯队</h2><div class="bars">{render_bar_rows(ladder)}</div></div>
  <div class="panel"><h2>行业分布 Top</h2><div class="bars">{render_bar_rows(sector, blue=True)}</div></div>

  <div class="panel">
    <h2>涨停明细(点击表头排序)</h2>
    <div class="table-scroll">
    <table id="pool">
      <thead><tr>
        <th onclick="sortTable(0)">代码</th><th onclick="sortTable(1)">名称</th>
        <th onclick="sortTable(2)">连板</th><th onclick="sortTable(3)">开板</th>
        <th onclick="sortTable(4)">现价</th><th onclick="sortTable(5)">涨幅</th>
        <th onclick="sortTable(6)">成交额</th><th onclick="sortTable(7)">流通市值</th>
        <th onclick="sortTable(8)">换手率</th><th onclick="sortTable(9)">首次封板</th>
        <th onclick="sortTable(10)">最后封板</th><th onclick="sortTable(11)">主力净流入</th>
        <th onclick="sortTable(12)">行业</th>
      </tr></thead>
      <tbody>{''.join(trs)}</tbody>
    </table>
    </div>
  </div>
  <div class="footer">
    AxData 本地生成 · {trade_date}<br>
    数据自动落盘:output/limit_up/daily/limit_up_{trade_date}.csv 与 output/limit_up/history.parquet
  </div>
</div>
<script>{JS}</script>
</body>
</html>"""


# ---------------------------------------------------------------- 主流程


def daterange(start: str, end: str) -> list[str]:
    d0 = datetime.strptime(start, "%Y%m%d").date()
    d1 = datetime.strptime(end, "%Y%m%d").date()
    return [(d0 + timedelta(days=i)).strftime("%Y%m%d") for i in range((d1 - d0).days + 1)]


def main() -> None:
    parser = argparse.ArgumentParser(description="东方财富涨停池:自动下载 + 可视化")
    parser.add_argument("--date", default=None, help="单个交易日 YYYYMMDD,默认今日")
    parser.add_argument("--start", default=None, help="回补起始日 YYYYMMDD")
    parser.add_argument("--end", default=None, help="回补结束日 YYYYMMDD,默认今日")
    parser.add_argument("--backfill", type=int, default=None, help="回补最近 N 个自然日")
    parser.add_argument("--no-html", action="store_true", help="只下载落盘,不生成报表")
    args = parser.parse_args()

    today = date.today().strftime("%Y%m%d")
    if args.backfill:
        days = daterange((date.today() - timedelta(days=args.backfill - 1)).strftime("%Y%m%d"), today)
    elif args.start:
        days = daterange(args.start, args.end or today)
    elif args.date:
        days = [args.date]
    else:
        days = [today]

    client = ax.AxDataClient()
    last_rows: list[dict] = []
    for day in days:
        try:
            rows = fetch_day(client, day)
        except Exception as exc:
            print(f"[{day}] 拉取失败: {exc}")
            continue
        if not rows:
            print(f"[{day}] 无数据(非交易日或源端为空),跳过")
            continue
        csv_path = save_daily_csv(rows, day)
        total = upsert_history(rows)
        print(f"[{day}] 下载 {len(rows)} 只 -> {csv_path.name} | 历史库累计 {total} 条")
        last_rows = rows

    if args.no_html or not last_rows:
        return

    summary = load_history_summary()
    html = build_html(last_rows, summary)
    trade_date = str(last_rows[0].get("trade_date", ""))
    OUT_DIR.mkdir(exist_ok=True)
    out = OUT_DIR / f"limit_up_{trade_date}.html"
    out.write_text(html, encoding="utf-8")
    print(f"已生成报表: {out}")


if __name__ == "__main__":
    main()
