"""
仅重算宏观广度历史（完整宏观情绪列，写入 DuckDB）。

适用：已有通达信本地日线，想补全历史趋势图，不必重跑整个 init_database。
完成后 git push data/market_monitor.duckdb 与 data/market_breadth_history.json。
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src import config
from src.breadth_engine import compute_full_market_breadth_history
from src.data_fetcher import read_local_tdx_market
from src.db_client import replace_breadth_history
from src.kline_processor import add_technical_indicators
from src.utils import ensure_data_dir


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从通达信本地日线重算宏观广度 DuckDB")
    parser.add_argument(
        "--tdxdir",
        type=str,
        default=str(config.TDX_DIR),
        help=f"通达信安装目录（默认 {config.TDX_DIR}）",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=config.FULL_HISTORY_START_DATE,
        help=f"起始日期（默认 {config.FULL_HISTORY_START_DATE}）",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=config.TDX_READ_WORKERS,
        help=f"读盘并发数（默认 {config.TDX_READ_WORKERS}）",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    start_time = time.time()
    ensure_data_dir()

    print("=" * 72)
    print("重算宏观广度历史 (DuckDB)")
    print(f"通达信目录: {args.tdxdir}")
    print(f"起始日期:   {args.start}")
    print("=" * 72)

    print("[1/3] 读取通达信本地日线...")
    raw_df = read_local_tdx_market(
        tdxdir=Path(args.tdxdir),
        workers=args.workers,
        start_date=args.start,
    )

    print("[2/3] 计算技术指标...")
    enriched_df = add_technical_indicators(raw_df)

    print("[3/3] 向量化计算完整广度历史...")
    breadth_df = compute_full_market_breadth_history(enriched_df)
    replace_breadth_history(breadth_df)

    elapsed = (time.time() - start_time) / 60.0
    sample = breadth_df.dropna(subset=["limit_up_count"]).tail(1)
    print("=" * 72)
    print(f"已写入: {config.DUCKDB_FILE} + {config.BREADTH_EXPORT_JSON}")
    print(f"交易日: {len(breadth_df)} 天 | 耗时: {elapsed:.1f} 分钟")
    if not sample.empty:
        row = sample.iloc[0]
        print(
            f"最新样本 {row['date']}: 涨停 {int(row['limit_up_count'])} / "
            f"跌停 {int(row['limit_down_count'])} | 60日新高 {int(row['new_high_60d'])}"
        )
    print()
    print("  git add data/market_monitor.duckdb data/market_breadth_history.json")
    print('  git commit -m "chore: backfill macro breadth history"')
    print("  git push")
    print("=" * 72)


if __name__ == "__main__":
    main()
