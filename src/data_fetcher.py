"""
数据管道：
- 阶段一：读取通达信本地 vipdoc 日线（离线，极速）
- 阶段二：AkShare 全市场截面增量更新（云端每日一次请求）
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import akshare as ak
import pandas as pd
from mootdx.reader import Reader
from tqdm import tqdm

from src import config
from src.kline_processor import (
    is_mainboard_a_share_code,
    normalize_akshare_spot,
    normalize_tdx_daily,
    symbol_to_code,
)
from src.utils import clean_stock_name

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2


def _is_a_share_tdx_stem(stem: str) -> bool:
    """过滤通达信 lday 文件名，排除指数与非 A 股。"""
    stem = stem.lower()
    if stem.startswith(("sh600", "sh601", "sh603", "sh605", "sh688")):
        return True
    if stem.startswith(("sz000", "sz001", "sz002", "sz003", "sz300")):
        return True
    return False


def discover_tdx_symbols(tdxdir: Path) -> List[str]:
    """扫描 vipdoc/sh/lday 与 vipdoc/sz/lday 下的 .day 文件。"""
    symbols: List[str] = []
    for market in ("sh", "sz"):
        lday_dir = tdxdir / "vipdoc" / market / "lday"
        if not lday_dir.exists():
            continue
        for day_file in lday_dir.glob("*.day"):
            stem = day_file.stem.lower()
            if _is_a_share_tdx_stem(stem):
                symbols.append(stem)
    symbols.sort()
    return symbols


def load_stock_name_map() -> Dict[str, str]:
    """
    从 AkShare 拉取代码-名称映射，用于过滤 ST/退市股。
    本地无网络时返回空 dict，仅按代码规则过滤。
    """
    for attempt in range(MAX_RETRIES):
        try:
            name_df = ak.stock_info_a_code_name()
            mapping: Dict[str, str] = {}
            for _, row in name_df.iterrows():
                code = symbol_to_code(str(row["code"]))
                mapping[code] = str(row["name"])
            return mapping
        except Exception:
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    print("[警告] 无法获取股票名称列表，将跳过 ST 名称过滤。")
    return {}


def _read_one_tdx_symbol(tdxdir: Path, stem: str) -> Optional[pd.DataFrame]:
    """读取单只股票的通达信本地日线。"""
    try:
        reader = Reader.factory(market="std", tdxdir=str(tdxdir))
        raw = reader.daily(symbol=stem)
        if raw is None or raw.empty:
            return None
        return normalize_tdx_daily(raw, stem)
    except Exception:
        return None


def read_local_tdx_market(
    tdxdir: Optional[Path] = None,
    workers: Optional[int] = None,
    start_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    从通达信本地 vipdoc 读取全市场 A 股日线。

    参数
    ----
    tdxdir : Path | None
        通达信安装目录，默认 config.TDX_DIR。
    workers : int | None
        并发读盘线程数。
    start_date : str | None
        可选，过滤掉此日期之前的数据（YYYY-MM-DD）。
    """
    root = Path(tdxdir or config.TDX_DIR)
    if not root.exists():
        raise FileNotFoundError(
            f"通达信目录不存在: {root}\n"
            "请先在通达信执行「盘后数据下载」，或通过 --tdxdir / 环境变量 TDX_DIR 指定路径。"
        )

    stems = discover_tdx_symbols(root)
    if not stems:
        raise RuntimeError(f"未在 {root} 下找到任何 A 股日线文件，请先下载盘后数据。")

    name_map = load_stock_name_map()
    thread_count = workers or config.TDX_READ_WORKERS
    frames: List[pd.DataFrame] = []
    failed: List[str] = []

    print(f"[通达信] 目录: {root} | 待读取 {len(stems)} 只 | 并发 {thread_count}")

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = {
            executor.submit(_read_one_tdx_symbol, root, stem): stem
            for stem in stems
        }
        with tqdm(total=len(stems), desc="读取通达信本地日线", unit="stock") as progress:
            for future in as_completed(futures):
                stem = futures[future]
                try:
                    stock_df = future.result()
                except Exception:
                    stock_df = None

                if stock_df is None or stock_df.empty:
                    failed.append(stem)
                    progress.update(1)
                    continue

                code = stock_df["code"].iloc[0]
                if not is_mainboard_a_share_code(code):
                    progress.update(1)
                    continue

                name = name_map.get(code)
                if name is not None and not clean_stock_name(name):
                    progress.update(1)
                    continue

                frames.append(stock_df)
                progress.update(1)

    if not frames:
        raise RuntimeError("通达信本地读取失败：未获得任何有效数据")

    merged = pd.concat(frames, ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    merged = merged.dropna(subset=["date", "close"])
    merged = merged.sort_values(["code", "date"]).reset_index(drop=True)

    if start_date:
        merged = merged[merged["date"] >= pd.Timestamp(start_date)]

    if failed:
        print(f"[警告] {len(failed)} 只股票读取失败，已跳过。")

    print(
        f"[完成] 通达信本地合并 {len(merged):,} 行 | "
        f"{merged['code'].nunique()} 只股票 | "
        f"区间 {merged['date'].min().date()} ~ {merged['date'].max().date()}"
    )
    return merged


def fetch_akshare_market_snapshot(trade_date: Optional[str] = None) -> pd.DataFrame:
    """
    通过 AkShare 一次请求获取全市场 A 股当日截面行情。
    用于 daily_job 云端增量更新（比逐只拉 K 线快得多）。
    """
    if trade_date is None:
        trade_date = get_latest_trade_date()

    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            spot_df = ak.stock_zh_a_spot_em()
            if spot_df is None or spot_df.empty:
                raise RuntimeError("AkShare 返回空截面数据")
            normalized = normalize_akshare_spot(spot_df, trade_date)
            return normalized
        except Exception as exc:
            last_error = exc
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))

    raise RuntimeError(f"AkShare 截面行情获取失败: {last_error}") from last_error


def fetch_ths_sectors() -> pd.DataFrame:
    """获取同花顺概念板块列表（AkShare）。"""
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            sector_df = ak.stock_board_concept_name_ths()
            if sector_df is None or sector_df.empty:
                raise RuntimeError("AkShare 返回空的同花顺概念板块列表")
            return sector_df.copy()
        except Exception as exc:
            last_error = exc
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))

    raise RuntimeError(f"获取同花顺概念板块失败: {last_error}") from last_error


def get_latest_trade_date() -> str:
    """获取不晚于今天的最近一个 A 股交易日。"""
    today = datetime.today().strftime("%Y-%m-%d")
    trade_df = ak.tool_trade_date_hist_sina()
    trade_dates = pd.to_datetime(trade_df["trade_date"], errors="coerce").dropna()
    trade_dates = trade_dates[trade_dates <= pd.Timestamp(today)]
    if trade_dates.empty:
        raise RuntimeError("交易日历为空")
    return trade_dates.max().strftime("%Y-%m-%d")


def filter_snapshot_to_universe(
    snapshot: pd.DataFrame,
    valid_codes: Set[str],
    name_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """将截面数据限制在已有热数据池的股票 universe 内，并过滤 ST。"""
    work = snapshot[snapshot["code"].isin(valid_codes)].copy()
    if name_map:
        keep_mask = work["code"].map(
            lambda code: clean_stock_name(name_map[code]) if code in name_map else True
        )
        work = work[keep_mask]
    return work.reset_index(drop=True)
