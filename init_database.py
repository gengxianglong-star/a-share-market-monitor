"""
【阶段一】通达信本地导入 → 生成冷热数据基座 → 上传 GitHub。

流程：
1. 在通达信里执行「盘后数据下载」（建议覆盖 2019 年至今）
2. 本地运行本脚本，读取 vipdoc 生成 CSV + Parquet
3. git push 将 data/ 目录推上 GitHub
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
from src.data_fetcher import read_local_tdx_market
from src.kline_processor import (
    add_technical_indicators,
    compute_market_breadth,
    extract_rolling_hot_data,
)
from src.utils import ensure_data_dir, save_parquet


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="阶段一：从通达信本地 vipdoc 导入历史数据",
    )
    parser.add_argument(
        "--tdxdir",
        type=str,
        default=str(config.TDX_DIR),
        help=f"通达信安装目录（默认 {config.TDX_DIR}，可用环境变量 TDX_DIR 覆盖）",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=config.FULL_HISTORY_START_DATE,
        help=f"只保留此日期之后的数据（默认 {config.FULL_HISTORY_START_DATE}）",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=config.TDX_READ_WORKERS,
        help=f"本地读盘并发数（默认 {config.TDX_READ_WORKERS}）",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    start_time = time.time()
    ensure_data_dir()

    print("=" * 72)
    print("A股 Market Monitor — 阶段一：通达信本地导入")
    print(f"通达信目录: {args.tdxdir}")
    print(f"起始日期:   {args.start}")
    print("=" * 72)

    print("[步骤 1/4] 读取通达信本地日线...")
    raw_df = read_local_tdx_market(
        tdxdir=Path(args.tdxdir),
        workers=args.workers,
        start_date=args.start,
    )

    print("[步骤 2/4] 计算技术指标...")
    enriched_df = add_technical_indicators(raw_df)

    print("[步骤 3/4] 计算宏观 Market Breadth...")
    breadth_df = compute_market_breadth(enriched_df)

    print("[步骤 4/4] 截取微观热数据池...")
    hot_df = extract_rolling_hot_data(enriched_df)

    breadth_df.to_csv(config.MACRO_BREADTH_FILE, index=False, encoding="utf-8-sig")
    save_parquet(hot_df, config.ROLLING_KLINES_FILE)

    elapsed = (time.time() - start_time) / 60.0
    print("=" * 72)
    print("阶段一完成！请将 data/ 目录推上 GitHub：")
    print(f"  宏观冷数据: {config.MACRO_BREADTH_FILE}  ({len(breadth_df)} 个交易日)")
    print(f"  微观热数据: {config.ROLLING_KLINES_FILE}  ({len(hot_df):,} 行)")
    print(f"  耗时: {elapsed:.1f} 分钟")
    print()
    print("  git add data/")
    print('  git commit -m "chore: init market data from Tongdaxin"')
    print("  git push")
    print("=" * 72)


if __name__ == "__main__":
    main()
