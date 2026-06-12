"""
K 线数据处理：标准化、技术指标、宏观广度、滚动窗口截取。
init_database（通达信本地）与 daily_job（AkShare 增量）共用。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src import config

STANDARD_COLUMNS = [
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "pct_change",
]


def tdx_stem_to_code(stem: str) -> str:
    """sh600519 -> sh.600519"""
    stem = stem.lower()
    if stem.startswith("sh") and len(stem) > 2:
        return f"sh.{stem[2:]}"
    if stem.startswith("sz") and len(stem) > 2:
        return f"sz.{stem[2:]}"
    return stem


def symbol_to_code(symbol: str) -> str:
    """600519 / sh600519 -> sh.600519"""
    text = str(symbol).strip().lower()
    if text.startswith("sh."):
        return text
    if text.startswith("sz."):
        return text
    if text.startswith("sh") and len(text) > 2:
        return f"sh.{text[2:]}"
    if text.startswith("sz") and len(text) > 2:
        return f"sz.{text[2:]}"
    digits = text.zfill(6)
    if digits.startswith(("6", "9")):
        return f"sh.{digits}"
    return f"sz.{digits}"


def is_mainboard_a_share_code(code: str) -> bool:
    """判断是否为主板/创业板/科创板 A 股，排除指数与北交所。"""
    code = code.lower()
    if code.startswith("bj."):
        return False
    if code.startswith("sh."):
        num = code.split(".", 1)[1]
        return num.startswith(("600", "601", "603", "605", "688"))
    if code.startswith("sz."):
        num = code.split(".", 1)[1]
        return num.startswith(("000", "001", "002", "003", "300"))
    return False


def normalize_tdx_daily(df: pd.DataFrame, tdx_stem: str) -> pd.DataFrame:
    """将 mootdx 读出的单股日线标准化为统一 schema。"""
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_COLUMNS)

    work = df.copy()
    work = work.reset_index(drop=False)

    if "datetime" in work.columns and "date" not in work.columns:
        work = work.rename(columns={"datetime": "date"})
    if "date" not in work.columns and work.index.name in ("date", "datetime"):
        work = work.reset_index()

    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work["code"] = tdx_stem_to_code(tdx_stem)

    if "vol" in work.columns and "volume" not in work.columns:
        work["volume"] = work["vol"]
    if "volume" not in work.columns:
        work["volume"] = np.nan

    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    if "amount" not in work.columns or work["amount"].isna().all():
        # 通达信 amount 缺失时，用 收盘价 × 成交量(手) × 100 估算成交额
        work["amount"] = work["close"] * work["volume"] * 100.0

    work = work.sort_values("date")
    work["pct_change"] = work["close"].pct_change() * 100.0

    work = work.dropna(subset=["date", "close"])
    return work[STANDARD_COLUMNS].reset_index(drop=True)


def normalize_akshare_spot(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    """
    将 AkShare 全市场截面行情（stock_zh_a_spot_em）转为统一 schema。
    一次请求覆盖全市场，适合 daily_job 每日增量。
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_COLUMNS)

    work = df.copy()
    rename_map = {
        "代码": "symbol",
        "名称": "name",
        "最新价": "close",
        "今开": "open",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "涨跌幅": "pct_change",
    }
    work = work.rename(columns=rename_map)

    required = {"symbol", "close"}
    if not required.issubset(work.columns):
        raise ValueError(f"AkShare 截面数据缺少必要列，当前列: {list(work.columns)}")

    work["code"] = work["symbol"].astype(str).str.zfill(6).map(symbol_to_code)
    work["date"] = pd.to_datetime(trade_date)

    for col in ["open", "high", "low", "close", "volume", "amount", "pct_change"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    if "open" not in work.columns or work["open"].isna().all():
        work["open"] = work["close"]
    if "high" not in work.columns or work["high"].isna().all():
        work["high"] = work["close"]
    if "low" not in work.columns or work["low"].isna().all():
        work["low"] = work["close"]
    if "volume" not in work.columns:
        work["volume"] = np.nan
    if "amount" not in work.columns or work["amount"].isna().all():
        work["amount"] = work["close"] * work["volume"] * 100.0

    work = work.dropna(subset=["close"])
    return work[STANDARD_COLUMNS].reset_index(drop=True)


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """向量化计算涨跌幅、MA20/MA50、20 日均成交额。"""
    work_df = df.copy()
    work_df = work_df.sort_values(["code", "date"]).reset_index(drop=True)
    grouped = work_df.groupby("code", group_keys=False)

    if "pct_change" not in work_df.columns or work_df["pct_change"].isna().all():
        work_df["pct_change"] = grouped["close"].pct_change() * 100.0

    work_df["ma20"] = grouped["close"].transform(
        lambda s: s.rolling(config.MA_SHORT_PERIOD, min_periods=config.MA_SHORT_PERIOD).mean()
    )
    work_df["ma50"] = grouped["close"].transform(
        lambda s: s.rolling(config.MA_LONG_PERIOD, min_periods=config.MA_LONG_PERIOD).mean()
    )
    work_df["vol_ma20"] = grouped["amount"].transform(
        lambda s: s.rolling(config.MA_SHORT_PERIOD, min_periods=config.MA_SHORT_PERIOD).mean()
    )
    work_df["daily_range_pct"] = (work_df["high"] - work_df["low"]) / work_df["close"] * 100.0
    work_df["adr_5d"] = grouped["daily_range_pct"].transform(
        lambda s: s.rolling(5, min_periods=5).mean()
    )
    work_df["vol_ma50"] = grouped["volume"].transform(
        lambda s: s.rolling(50, min_periods=50).mean()
    )
    work_df["rvol"] = work_df["volume"] / work_df["vol_ma50"]
    return work_df


def compute_market_breadth(df: pd.DataFrame) -> pd.DataFrame:
    """按交易日聚合宏观 Market Breadth 指标。"""
    threshold = config.BREADTH_EXTREME_UP_DOWN_PCT
    work_df = df.copy()

    work_df["above_extreme_up"] = work_df["pct_change"] > threshold
    work_df["below_extreme_down"] = work_df["pct_change"] < -threshold

    valid_ma20 = work_df["ma20"].notna()
    valid_ma50 = work_df["ma50"].notna()

    work_df["above_ma20_flag"] = np.where(
        valid_ma20,
        work_df["close"] > work_df["ma20"],
        np.nan,
    )
    work_df["above_ma50_flag"] = np.where(
        valid_ma50,
        work_df["close"] > work_df["ma50"],
        np.nan,
    )

    breadth_df = work_df.groupby("date", as_index=False).agg(
        above_5pct_count=("above_extreme_up", "sum"),
        below_5pct_count=("below_extreme_down", "sum"),
        pt20_ratio=("above_ma20_flag", "mean"),
        pt50_ratio=("above_ma50_flag", "mean"),
    )
    breadth_df["date"] = pd.to_datetime(breadth_df["date"]).dt.strftime("%Y-%m-%d")
    return breadth_df.sort_values("date").reset_index(drop=True)


def extract_rolling_hot_data(df: pd.DataFrame) -> pd.DataFrame:
    """截取最近 ROLLING_WINDOW_DAYS 个交易日的微观热数据。"""
    unique_dates = sorted(df["date"].dropna().unique())
    if len(unique_dates) <= config.ROLLING_WINDOW_DAYS:
        selected_dates = unique_dates
    else:
        selected_dates = unique_dates[-config.ROLLING_WINDOW_DAYS :]

    hot_df = df[df["date"].isin(selected_dates)].copy()
    return hot_df.sort_values(["code", "date"]).reset_index(drop=True)


def trim_to_rolling_window(df: pd.DataFrame) -> pd.DataFrame:
    """将 DataFrame 裁剪到最近 ROLLING_WINDOW_DAYS 个交易日。"""
    return extract_rolling_hot_data(df)
