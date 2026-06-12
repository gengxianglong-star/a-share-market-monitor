"""
工具箱：A 股交易日历判定、股票名称清洗、Parquet 读写辅助。
"""

from __future__ import annotations

import time
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Union

import akshare as ak
import pandas as pd

from src import config
from src.db_client import localize_datetime_series, shanghai_today

__all__ = ["localize_datetime_series", "shanghai_today"]

DateLike = Union[str, date, datetime, pd.Timestamp]


def _normalize_date_str(value: DateLike) -> str:
    """将多种日期输入统一为 YYYY-MM-DD 字符串。"""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if len(text) >= 10:
        return text[:10]
    raise ValueError(f"无法解析日期: {value!r}")


@lru_cache(maxsize=1)
def _load_trade_dates() -> frozenset[str]:
    """
    从 AkShare 加载 A 股历史交易日集合，并缓存到内存。
    缓存只构建一次，避免重复网络请求。
    """
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            trade_df = ak.tool_trade_date_hist_sina()
            if trade_df is None or trade_df.empty:
                raise RuntimeError("AkShare 返回空的交易日历")
            dates = pd.to_datetime(trade_df["trade_date"], errors="coerce")
            valid_dates = dates.dropna().dt.strftime("%Y-%m-%d")
            return frozenset(valid_dates.tolist())
        except Exception as exc:
            last_error = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"加载 A 股交易日历失败: {last_error}") from last_error


def is_trade_day(value: DateLike) -> bool:
    """
    判断指定日期是否为 A 股交易日。

    参数
    ----
    value : str | date | datetime | Timestamp
        待判断日期，支持 YYYY-MM-DD 字符串或 datetime 对象。

    返回
    ----
    bool
        True 表示是交易日，False 表示休市。
    """
    date_str = _normalize_date_str(value)
    return date_str in _load_trade_dates()


def clean_stock_name(name: str) -> bool:
    """
    判断股票名称是否合法，用于过滤 ST、退市等异常标的。

    参数
    ----
    name : str
        股票简称，例如 "贵州茅台" 或 "*ST某某"。

    返回
    ----
    bool
        True 表示名称合法、可以保留；False 表示应剔除。
    """
    if name is None:
        return False
    normalized = str(name).strip().upper()
    if not normalized:
        return False
    for keyword in config.EXCLUDE_NAME_KEYWORDS:
        if keyword.upper() in normalized:
            return False
    return True


def ensure_data_dir() -> None:
    """确保 data 目录存在，并初始化 DuckDB 表结构。"""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    from src.db_client import init_duckdb

    init_duckdb()


def save_parquet(df: pd.DataFrame, file_path: Union[str, Path]) -> None:
    """
    将 DataFrame 保存为 Parquet 文件。

    参数
    ----
    df : pd.DataFrame
        待保存的数据。
    file_path : str | Path
        目标文件路径。
    """
    ensure_data_dir()
    path = Path(file_path)
    df.to_parquet(path, index=False, engine="pyarrow")


def load_parquet(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    从 Parquet 文件读取 DataFrame。

    参数
    ----
    file_path : str | Path
        Parquet 文件路径。

    返回
    ----
    pd.DataFrame
        读取到的数据。
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet 文件不存在: {path}")
    return pd.read_parquet(path, engine="pyarrow")
