"""
【阶段二】云端每日增量更新（GitHub Actions 15:30 执行）。

数据源：AkShare 全市场截面（一次请求），追加到 rolling_klines.parquet，
更新 market_breadth_history.csv。历史冷数据由通达信本地导入后上传 GitHub。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src import config
from src.data_fetcher import (
    fetch_akshare_market_snapshot,
    filter_snapshot_to_universe,
    get_latest_trade_date,
    load_stock_name_map,
)
from src.kline_processor import (
    add_technical_indicators,
    compute_market_breadth,
    trim_to_rolling_window,
)
from src.utils import ensure_data_dir, load_parquet, save_parquet


def _load_breadth_history() -> pd.DataFrame:
    if not config.MACRO_BREADTH_FILE.exists():
        return pd.DataFrame(
            columns=["date", "above_5pct_count", "below_5pct_count", "pt20_ratio", "pt50_ratio"]
        )
    df = pd.read_csv(config.MACRO_BREADTH_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def _append_breadth_history(existing: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    if new_rows.empty:
        return existing
    merged = pd.concat([existing, new_rows], ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    merged = merged.drop_duplicates(subset=["date"], keep="last")
    return merged.sort_values("date").reset_index(drop=True)


def main() -> None:
    ensure_data_dir()

    if not config.ROLLING_KLINES_FILE.exists():
        raise FileNotFoundError(
            "未找到 rolling_klines.parquet。\n"
            "请先在本地运行 init_database.py 导入通达信数据并 push 到 GitHub。"
        )

    trade_date = get_latest_trade_date()
    print("=" * 72)
    print("A股 Market Monitor — 每日增量更新 (AkShare)")
    print(f"目标交易日: {trade_date}")
    print("=" * 72)

    rolling_df = load_parquet(config.ROLLING_KLINES_FILE)
    rolling_df["date"] = pd.to_datetime(rolling_df["date"], errors="coerce")
    existing_dates = set(rolling_df["date"].dt.strftime("%Y-%m-%d"))

    if trade_date in existing_dates:
        print(f"[跳过] 热数据池已包含 {trade_date}，无需重复更新。")
        return

    print("[步骤 1/3] 拉取 AkShare 全市场截面...")
    snapshot = fetch_akshare_market_snapshot(trade_date)
    name_map = load_stock_name_map()
    valid_codes = set(rolling_df["code"].unique())
    snapshot = filter_snapshot_to_universe(snapshot, valid_codes, name_map)
    snapshot["date"] = pd.to_datetime(snapshot["date"], errors="coerce")
    print(f"[完成] 截面覆盖 {len(snapshot)} 只（与热数据池交集）")

    print("[步骤 2/3] 合并热数据池并重算指标...")
    base_cols = [c for c in rolling_df.columns if c not in ("ma20", "ma50", "vol_ma20")]
    rolling_base = rolling_df[base_cols].copy()
    merged = pd.concat([rolling_base, snapshot], ignore_index=True)
    merged = merged.sort_values(["code", "date"]).drop_duplicates(subset=["code", "date"], keep="last")
    merged = trim_to_rolling_window(merged)
    enriched = add_technical_indicators(merged)
    save_parquet(enriched, config.ROLLING_KLINES_FILE)
    print(f"[完成] 热数据池 {len(enriched):,} 行，最新日 {trade_date}")

    print("[步骤 3/3] 更新宏观广度 CSV...")
    today_breadth = compute_market_breadth(enriched)
    today_breadth = today_breadth[today_breadth["date"] == trade_date]
    history = _load_breadth_history()
    updated = _append_breadth_history(history, today_breadth)
    updated.to_csv(config.MACRO_BREADTH_FILE, index=False, encoding="utf-8-sig")
    print(f"[完成] 广度历史共 {len(updated)} 个交易日")

    print("=" * 72)
    print("每日更新完成！")
    print("=" * 72)


if __name__ == "__main__":
    main()
