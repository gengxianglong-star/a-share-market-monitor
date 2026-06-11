"""
【阶段二】统一调度官 — 每日自动化流水线。

顺序：拉取截面 → 更新滚动 K 线池 → 大盘宽度 → RS 选股 → 输出 Watchlist JSON
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src import config
from src.breadth_engine import update_daily_breadth
from src.data_fetcher import (
    fetch_akshare_market_snapshot,
    fetch_sector_mapping,
    filter_snapshot_to_universe,
    get_latest_trade_date,
    load_stock_name_map,
)
from src.kline_processor import add_technical_indicators, trim_to_rolling_window
from src.rs_scanner import run_daily_scan
from src.utils import ensure_data_dir, load_parquet, save_parquet

# 清除可能影响云端/本地的代理，避免 AkShare 请求失败
for _proxy_key in (
    "http_proxy", "https_proxy", "all_proxy",
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
):
    os.environ.pop(_proxy_key, None)
os.environ["NO_PROXY"] = "*"


def _clear_proxy_env() -> None:
    """运行时再次清理代理环境变量。"""
    for key in list(os.environ):
        if "proxy" in key.lower():
            os.environ.pop(key, None)
    os.environ["NO_PROXY"] = "*"


def _update_rolling_pool(rolling_df: pd.DataFrame, snapshot: pd.DataFrame) -> pd.DataFrame:
    """合并今日截面，重算指标，裁剪至 150 日滑动窗口。"""
    base_cols = [c for c in rolling_df.columns if c not in ("ma20", "ma50", "vol_ma20")]
    rolling_base = rolling_df[base_cols].copy()
    merged = pd.concat([rolling_base, snapshot], ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    merged = merged.sort_values(["code", "date"]).drop_duplicates(subset=["code", "date"], keep="last")
    merged = trim_to_rolling_window(merged)
    return add_technical_indicators(merged)


def main() -> None:
    _clear_proxy_env()
    ensure_data_dir()

    print("=" * 72)
    print(f"A-Share Market Monitor 自动化流水线 | {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 72)

    if not config.ROLLING_KLINES_FILE.exists():
        print(f"❌ 未找到 {config.ROLLING_KLINES_FILE}，请先运行 init_database.py 导入通达信数据。")
        sys.exit(1)

    trade_date = get_latest_trade_date()
    print(f"📅 目标交易日: {trade_date}")

    # --- 1. 加载滚动 K 线池 ---
    print("🗄️  加载滚动 K 线池...")
    rolling_df = load_parquet(config.ROLLING_KLINES_FILE)
    rolling_df["date"] = pd.to_datetime(rolling_df["date"], errors="coerce")
    existing_dates = set(rolling_df["date"].dt.strftime("%Y-%m-%d"))
    data_updated = False

    # --- 2. 拉取今日截面并更新滑动窗口 ---
    if trade_date in existing_dates:
        print(f"⏭️  热数据池已含 {trade_date}，跳过截面拉取，直接运行选股与 JSON 输出。")
        enriched = rolling_df
    else:
        print("📡 拉取 AkShare 全市场截面...")
        try:
            snapshot = fetch_akshare_market_snapshot(trade_date)
            name_map = load_stock_name_map()
            valid_codes = set(rolling_df["code"].unique())
            snapshot = filter_snapshot_to_universe(snapshot, valid_codes, name_map)
            snapshot["date"] = pd.to_datetime(snapshot["date"], errors="coerce")
            print(f"✅ 截面 {len(snapshot)} 只（与热数据池交集）")
        except Exception as exc:
            print(f"❌ 拉取今日快照失败: {exc}")
            sys.exit(1)

        print("🔄 更新 150 日滑动窗口...")
        enriched = _update_rolling_pool(rolling_df, snapshot)
        save_parquet(enriched, config.ROLLING_KLINES_FILE)
        data_updated = True
        print(f"💾 热数据池已更新 | {enriched['date'].nunique()} 个交易日 | {len(enriched):,} 行")

    # 今日截面（用于宽度计算）：取 enriched 中最新一天
    today_df = enriched[enriched["date"] == pd.Timestamp(trade_date)].copy()
    if today_df.empty and data_updated is False:
        today_df = enriched.groupby("code", as_index=False).tail(1)

    # --- 3. 大盘宽度引擎 ---
    print("🌡️  计算大盘市场宽度...")
    try:
        breadth_row = update_daily_breadth(today_df=today_df, rolling_df=enriched, trade_date=trade_date)
        print(
            f"📊 宽度已写入 CSV | 涨>5%: {int(breadth_row['above_5pct_count'].iloc[0])} 家 | "
            f"PT20: {float(breadth_row['pt20_ratio'].iloc[0]):.1%}"
        )
    except Exception as exc:
        print(f"⚠️  宽度计算异常: {exc}")

    # --- 4. RS 选股漏斗 ---
    print("🎯 运行 RS 动量漏斗 + 申万二级行业交叉...")
    try:
        try:
            sector_mapping = fetch_sector_mapping(force_refresh=False)
        except Exception as exc:
            if config.SECTOR_MAPPING_FILE.exists():
                print(f"⚠️  板块拉取失败 ({exc})，使用本地缓存 sector_mapping.parquet")
                sector_mapping = pd.read_parquet(config.SECTOR_MAPPING_FILE, engine="pyarrow")
            else:
                raise RuntimeError(
                    "无板块映射缓存。请运行:\n"
                    "  python build_sector_mapping.py --source sw --force\n"
                    "再将 data/sector_mapping.parquet 提交到 GitHub。"
                ) from exc

        if sector_mapping is None or sector_mapping.empty:
            raise RuntimeError("板块映射表为空，请运行 python build_sector_mapping.py --source sw --force")

        watchlist_result = run_daily_scan(
            rolling_df=enriched,
            sector_mapping_df=sector_mapping,
            trade_date=trade_date,
        )
        config.DAILY_WATCHLIST_FILE.write_text(
            json.dumps(watchlist_result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        intersection = watchlist_result.get("intersection", [])
        print(f"🎉 终极 Watchlist 已保存: {config.DAILY_WATCHLIST_FILE}")
        print(f"📈 今日交集入选: {len(intersection)} 只 | Top 申万二级: {len(watchlist_result.get('top_sectors', []))} 个")
    except Exception as exc:
        print(f"❌ 选股漏斗失败: {exc}")
        sys.exit(1)

    print("=" * 72)
    print("✅ 流水线执行完毕")
    print("=" * 72)


if __name__ == "__main__":
    main()
