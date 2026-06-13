"""
市场宽度引擎 (Stockbee Market Breadth Monitor)。

基于当日截面 + 150 日滚动 K 线，计算宏观广度指标并追加至历史 CSV。
"""

from __future__ import annotations

from typing import Optional, Union

import numpy as np
import pandas as pd

from src import config


# 向后兼容：列定义已迁至 config.BREADTH_COLUMNS
BREADTH_COLUMNS = config.BREADTH_COLUMNS


def _normalize_date(value) -> pd.Timestamp:
    """统一日期为 Timestamp。"""
    return pd.to_datetime(value)


def _prepare_combined_close(rolling_df: pd.DataFrame, today_df: pd.DataFrame) -> pd.DataFrame:
    """
    合并滚动历史与今日截面，去重后按日期排序。
    用于向量化计算 MA 与中长线累计涨跌幅。
    """
    cols = ["date", "code", "close", "pct_change"]
    rolling_part = rolling_df[cols].copy()
    today_part = today_df[cols].copy()
    rolling_part["date"] = pd.to_datetime(rolling_part["date"])
    today_part["date"] = pd.to_datetime(today_part["date"])

    combined = pd.concat([rolling_part, today_part], ignore_index=True)
    combined = combined.drop_duplicates(subset=["code", "date"], keep="last")
    return combined.sort_values(["code", "date"]).reset_index(drop=True)


def _limit_pct_threshold(code: str) -> float:
    """按板块返回涨跌停幅度阈值（%）。"""
    text = str(code).lower()
    num = text.split(".", 1)[1] if "." in text else text
    if num.startswith(("300", "688")):
        return config.BREADTH_LIMIT_UP_PCT_GROWTH
    return config.BREADTH_LIMIT_UP_PCT_MAIN


def _compute_limit_counts(work_today: pd.DataFrame) -> tuple[int, int, float]:
    """统计涨停/跌停家数及比值（跌停为 0 时返回 nan）。"""
    work = work_today.dropna(subset=["pct_change"])
    if work.empty:
        return 0, 0, np.nan

    thresholds = work["code"].astype(str).map(_limit_pct_threshold)
    limit_up = int((work["pct_change"] >= thresholds).sum())
    limit_down = int((work["pct_change"] <= -thresholds).sum())
    if limit_down > 0:
        ratio = round(limit_up / limit_down, 2)
    elif limit_up > 0:
        ratio = np.nan
    else:
        ratio = np.nan
    return limit_up, limit_down, ratio


def _compute_new_high_low(combined: pd.DataFrame) -> tuple[int | float, int | float]:
    """统计 N 个交易日窗口内创新高/新低的股票家数（N = BREADTH_NEW_HIGH_LOW_DAYS）。"""
    lookback = config.BREADTH_NEW_HIGH_LOW_DAYS
    pivot = combined.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    pivot = pivot.sort_index()
    if len(pivot) < lookback:
        return np.nan, np.nan

    window = pivot.tail(lookback)
    today_close = window.iloc[-1]
    rolling_max = window.max(axis=0)
    rolling_min = window.min(axis=0)
    valid = today_close.notna() & rolling_max.notna() & rolling_min.notna()

    new_high = int((today_close[valid] >= rolling_max[valid]).sum())
    new_low = int((today_close[valid] <= rolling_min[valid]).sum())
    return new_high, new_low


def _count_new_high_low_rows(pivot: pd.DataFrame, lookback: int) -> pd.DataFrame:
    """向量化：每个交易日统计创 lookback 日新高/新低的家数；窗口不足时为 NaN。"""
    rolling_max = pivot.rolling(lookback, min_periods=lookback).max()
    rolling_min = pivot.rolling(lookback, min_periods=lookback).min()

    new_high = (pivot >= rolling_max).sum(axis=1).astype(float)
    new_low = (pivot <= rolling_min).sum(axis=1).astype(float)

    # 前 lookback-1 个交易日没有完整窗口，不能显示为 0
    if len(pivot) >= lookback:
        valid_mask = pd.Series(False, index=pivot.index)
        valid_mask.iloc[lookback - 1 :] = True
        new_high = new_high.where(valid_mask, np.nan)
        new_low = new_low.where(valid_mask, np.nan)
    else:
        new_high[:] = np.nan
        new_low[:] = np.nan

    return pd.DataFrame(
        {"new_high_120d": new_high, "new_low_120d": new_low},
        index=pivot.index,
    )


def _compute_pt_ratios(combined: pd.DataFrame) -> tuple[float, float]:
    """
    向量化计算今日 PT20 / PT50（收盘价在均线上方的家数占比，0~1）。
    使用含今日在内的最近 20 / 50 个交易日收盘价滚动均值。
    """
    pivot = combined.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    pivot = pivot.sort_index()

    if pivot.empty:
        return np.nan, np.nan

    ma20 = pivot.tail(config.BREADTH_PT_SHORT).mean()
    ma50 = pivot.tail(config.BREADTH_PT_LONG).mean()
    today_close = pivot.iloc[-1]

    valid20 = ma20.notna() & today_close.notna()
    valid50 = ma50.notna() & today_close.notna()

    pt20 = (today_close[valid20] > ma20[valid20]).mean() if valid20.any() else np.nan
    pt50 = (today_close[valid50] > ma50[valid50]).mean() if valid50.any() else np.nan
    return pt20, pt50


def _compute_month_qtr_extremes(combined: pd.DataFrame) -> tuple[int, int, int, int]:
    """
    Stockbee 中长线极限：统计 20 / 60 个交易日内累计涨跌幅超过 ±25% 的家数。
    """
    pivot = combined.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    pivot = pivot.sort_index()
    n_dates = len(pivot)

    up_month = down_month = up_qtr = down_qtr = 0
    threshold = config.BREADTH_MONTH_QTR_PCT / 100.0

    if n_dates > config.BREADTH_MONTH_DAYS:
        ret_month = pivot.iloc[-1] / pivot.iloc[-1 - config.BREADTH_MONTH_DAYS] - 1.0
        valid = ret_month.replace([np.inf, -np.inf], np.nan).dropna()
        up_month = int((valid > threshold).sum())
        down_month = int((valid < -threshold).sum())

    if n_dates > config.BREADTH_QTR_DAYS:
        ret_qtr = pivot.iloc[-1] / pivot.iloc[-1 - config.BREADTH_QTR_DAYS] - 1.0
        valid = ret_qtr.replace([np.inf, -np.inf], np.nan).dropna()
        up_qtr = int((valid > threshold).sum())
        down_qtr = int((valid < -threshold).sum())

    return up_month, down_month, up_qtr, down_qtr


def _attach_hs300_close(daily: pd.DataFrame) -> pd.DataFrame:
    """按日期合并沪深300收盘价，供看板右轴叠加。"""
    from src.data_fetcher import fetch_hs300_close_series

    if daily.empty:
        daily["hs300_close"] = pd.Series(dtype=float)
        return daily

    work = daily.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    start = work["date"].min()
    hs300 = fetch_hs300_close_series(start_date=start)
    merged = work.merge(hs300, on="date", how="left")
    return merged


def _lookup_hs300_close(trade_date_str: str) -> float:
    from src.data_fetcher import fetch_hs300_close_series
    from src.db_client import load_breadth_history

    history = load_breadth_history()
    if not history.empty and "hs300_close" in history.columns:
        history["date"] = pd.to_datetime(history["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        match = history[history["date"] == trade_date_str]
        if not match.empty and pd.notna(match.iloc[0]["hs300_close"]):
            return float(match.iloc[0]["hs300_close"])

    series = fetch_hs300_close_series(start_date=trade_date_str)
    match = series[series["date"] == trade_date_str]
    if match.empty:
        return np.nan
    return float(match["hs300_close"].iloc[0])


def refresh_breadth_derived_metrics(rolling_df: pd.DataFrame) -> pd.DataFrame:
    """
    用滚动 K 线池补全广度历史中可计算的 120 日新高/新低，并合并沪深300收盘价。
    """
    from src.data_fetcher import fetch_hs300_close_series
    from src.db_client import load_breadth_history, replace_breadth_history

    history = load_breadth_history()
    if history.empty:
        return history

    work = history.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")

    if rolling_df is not None and not rolling_df.empty:
        pool = rolling_df.copy()
        pool["date"] = pd.to_datetime(pool["date"], errors="coerce")
        pivot = pool.pivot_table(index="date", columns="code", values="close", aggfunc="last").sort_index()
        metrics = _count_new_high_low_rows(pivot, config.BREADTH_NEW_HIGH_LOW_DAYS).reset_index()
        metrics["date"] = pd.to_datetime(metrics["date"], errors="coerce")

        work = work.merge(metrics, on="date", how="left", suffixes=("", "_roll"))
        for col in ("new_high_120d", "new_low_120d"):
            roll_col = f"{col}_roll"
            if roll_col in work.columns:
                has_roll = work[roll_col].notna()
                work.loc[has_roll, col] = work.loc[has_roll, roll_col]
                work.drop(columns=[roll_col], inplace=True)

    date_str = work["date"].dt.strftime("%Y-%m-%d")
    cached_hs = pd.DataFrame({"date": date_str, "hs300_close": work["hs300_close"]}).dropna(subset=["hs300_close"])
    missing_mask = work["hs300_close"].isna()
    if missing_mask.any():
        start = date_str.loc[missing_mask].min()
        fetched = fetch_hs300_close_series(start_date=start)
        if not fetched.empty:
            merged_hs = pd.concat([cached_hs, fetched], ignore_index=True).drop_duplicates("date", keep="last")
            work = work.assign(date=date_str).drop(columns=["hs300_close"]).merge(merged_hs, on="date", how="left")

    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return replace_breadth_history(work[config.BREADTH_COLUMNS])


def compute_full_market_breadth_history(enriched_df: pd.DataFrame) -> pd.DataFrame:
    """
    从全量 K 线（含 ma20 / ma50 / pct_change）向量化计算完整广度历史。
    用于 init_database / backfill_breadth_history 一次性生成 CSV。
    """
    if enriched_df is None or enriched_df.empty:
        return pd.DataFrame(columns=BREADTH_COLUMNS)

    work = enriched_df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    threshold = config.BREADTH_EXTREME_UP_DOWN_PCT

    work["above_extreme_up"] = work["pct_change"] > threshold
    work["below_extreme_down"] = work["pct_change"] < -threshold
    work["limit_thr"] = work["code"].astype(str).map(_limit_pct_threshold)
    work["limit_up_flag"] = work["pct_change"] >= work["limit_thr"]
    work["limit_down_flag"] = work["pct_change"] <= -work["limit_thr"]

    valid_ma20 = work["ma20"].notna()
    valid_ma50 = work["ma50"].notna()
    work["above_ma20"] = np.where(valid_ma20, work["close"] > work["ma20"], np.nan)
    work["above_ma50"] = np.where(valid_ma50, work["close"] > work["ma50"], np.nan)

    daily = work.groupby("date", as_index=False).agg(
        above_5pct_count=("above_extreme_up", "sum"),
        below_5pct_count=("below_extreme_down", "sum"),
        pt20_ratio=("above_ma20", "mean"),
        pt50_ratio=("above_ma50", "mean"),
        limit_up_count=("limit_up_flag", "sum"),
        limit_down_count=("limit_down_flag", "sum"),
        universe_size=("code", "count"),
    )

    daily["limit_up_down_ratio"] = np.where(
        daily["limit_down_count"] > 0,
        (daily["limit_up_count"] / daily["limit_down_count"]).round(2),
        np.nan,
    )

    pivot = work.pivot_table(index="date", columns="code", values="close", aggfunc="last")
    pivot = pivot.sort_index()

    pct_thr = config.BREADTH_MONTH_QTR_PCT / 100.0
    ret_month = pivot / pivot.shift(config.BREADTH_MONTH_DAYS) - 1.0
    ret_qtr = pivot / pivot.shift(config.BREADTH_QTR_DAYS) - 1.0

    lookback = config.BREADTH_NEW_HIGH_LOW_DAYS
    high_low_metrics = _count_new_high_low_rows(pivot, lookback)

    pivot_metrics = pd.DataFrame(
        {
            "up_25pct_month": (ret_month > pct_thr).sum(axis=1),
            "down_25pct_month": (ret_month < -pct_thr).sum(axis=1),
            "up_25pct_qtr": (ret_qtr > pct_thr).sum(axis=1),
            "down_25pct_qtr": (ret_qtr < -pct_thr).sum(axis=1),
        },
        index=pivot.index,
    ).join(high_low_metrics, how="left")

    daily = daily.set_index("date").join(pivot_metrics, how="left").reset_index()

    daily = daily.drop(columns=["universe_size"])
    daily["date"] = daily["date"].dt.strftime("%Y-%m-%d")
    daily = _attach_hs300_close(daily)
    return daily[BREADTH_COLUMNS].sort_values("date").reset_index(drop=True)


def _load_breadth_history() -> pd.DataFrame:
    """读取宏观广度历史（DuckDB）。"""
    from src.db_client import load_breadth_history

    return load_breadth_history()


def _append_breadth_row(new_row: pd.DataFrame) -> pd.DataFrame:
    """安全追加一行到 DuckDB（同日期覆盖，否则 append）。"""
    from src.db_client import upsert_breadth_row

    return upsert_breadth_row(new_row[BREADTH_COLUMNS])


def update_daily_breadth(
    today_df: pd.DataFrame,
    rolling_df: pd.DataFrame,
    trade_date: Optional[Union[str, pd.Timestamp]] = None,
) -> pd.DataFrame:
    """
    计算今日市场宽度指标，并追加写入 DuckDB market_breadth_history 表。

    参数
    ----
    today_df : pd.DataFrame
        今日全市场截面快照（含 date, code, close, pct_change 等）。
    rolling_df : pd.DataFrame
        过去 150 个交易日的 DuckDB 热数据池。
    trade_date : str | Timestamp | None
        交易日期；默认取 today_df 中最新日期。

    返回
    ----
    pd.DataFrame
        今日新增/更新的一行广度指标。
    """
    if today_df is None or today_df.empty:
        raise ValueError("today_df 不能为空")

    work_today = today_df.copy()
    work_today["date"] = pd.to_datetime(work_today["date"], errors="coerce")
    if trade_date is None:
        trade_date = work_today["date"].max()
    else:
        trade_date = _normalize_date(trade_date)
    trade_date_str = trade_date.strftime("%Y-%m-%d")

    threshold = config.BREADTH_EXTREME_UP_DOWN_PCT

    # --- 当日极端涨跌家数 ---
    above_5pct = int((work_today["pct_change"] > threshold).sum())
    below_5pct = int((work_today["pct_change"] < -threshold).sum())
    limit_up, limit_down, limit_ratio = _compute_limit_counts(work_today)

    # --- PT20 / PT50 + 月/季极限 + 120日新高新低（需结合 rolling） ---
    combined = _prepare_combined_close(rolling_df, work_today)
    pt20, pt50 = _compute_pt_ratios(combined)
    up_month, down_month, up_qtr, down_qtr = _compute_month_qtr_extremes(combined)
    new_high_120d, new_low_120d = _compute_new_high_low(combined)
    hs300_close = _lookup_hs300_close(trade_date_str)

    today_row = pd.DataFrame(
        [
            {
                "date": trade_date_str,
                "above_5pct_count": above_5pct,
                "below_5pct_count": below_5pct,
                "pt20_ratio": pt20,
                "pt50_ratio": pt50,
                "limit_up_count": limit_up,
                "limit_down_count": limit_down,
                "limit_up_down_ratio": limit_ratio,
                "new_high_120d": new_high_120d,
                "new_low_120d": new_low_120d,
                "hs300_close": hs300_close,
                "up_25pct_month": up_month,
                "down_25pct_month": down_month,
                "up_25pct_qtr": up_qtr,
                "down_25pct_qtr": down_qtr,
            }
        ]
    )

    _append_breadth_row(today_row)
    return today_row
