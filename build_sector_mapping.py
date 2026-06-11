"""
一次性构建板块映射表，建议本地运行后 commit 到 GitHub。

用法:
  python build_sector_mapping.py
  python build_sector_mapping.py --force
  python build_sector_mapping.py --source sw --force
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.data_fetcher import (
    diagnose_network,
    fetch_em_sector_mapping,
    fetch_sw_sector_mapping,
    fetch_ths_sector_mapping,
    import_sector_mapping_from_csv,
)
from src.network_utils import disable_all_proxies

disable_all_proxies()


def main() -> None:
    parser = argparse.ArgumentParser(description="构建板块成分股映射表")
    parser.add_argument("--force", action="store_true", help="忽略缓存强制重建")
    parser.add_argument(
        "--source",
        choices=["auto", "sw", "ths", "em"],
        default="sw",
        help="sw=申万二级(默认) | ths=同花顺概念 | em=东财概念 | auto=按 config",
    )
    parser.add_argument(
        "--import-csv",
        type=str,
        default=None,
        help="从 CSV 手动导入 (列: code, sector, name)",
    )
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help="仅诊断网络/代理，不拉取数据",
    )
    args = parser.parse_args()

    if args.diagnose:
        diagnose_network()
        return

    if args.import_csv:
        import_sector_mapping_from_csv(Path(args.import_csv))
        return

    print("提示: 申万二级约 130 个行业，首次拉取约 1~2 分钟。")

    if args.source == "sw":
        df = fetch_sw_sector_mapping(force_refresh=args.force)
        label = "申万二级行业"
    elif args.source == "em":
        df = fetch_em_sector_mapping(force_refresh=args.force)
        label = "东财概念"
    elif args.source == "ths":
        diagnose_network()
        df = fetch_ths_sector_mapping(force_refresh=args.force)
        label = "同花顺概念"
    else:
        from src import config

        if config.SECTOR_MAPPING_SOURCE == "sw":
            df = fetch_sw_sector_mapping(force_refresh=args.force)
            label = "申万二级行业"
        else:
            try:
                df = fetch_ths_sector_mapping(force_refresh=args.force)
                label = "同花顺概念"
            except Exception:
                print("同花顺拉取失败，切换东财...")
                df = fetch_em_sector_mapping(force_refresh=True)
                label = "东财概念"
    print(f"完成: {len(df):,} 条映射，{df['sector'].nunique()} 个{label}")


if __name__ == "__main__":
    main()
