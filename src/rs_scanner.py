"""
相对强度扫描引擎 (Qullamaggie Blended RS + 板块级联递补)。

基于 150 日滚动 K 线池与申万二级行业映射，输出 Top 行业与终极 Watchlist 交集。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from src import config


def _resolve_sector_columns(sector_mapping_df: pd.DataFrame) -> tuple[str, str, Optional[str]]:
    """解析板块映射表的列名（兼容多种命名）。"""
    sector_candidates = ["sector", "sector_name", "板块名称", "概念名称", "name_y"]
    code_candidates = ["code", "股票代码", "symbol"]
    name_candidates = ["name", "股票名称", "名称", "name_x"]

    sector_col = next((c for c in sector_candidates if c in sector_mapping_df.columns), None)
    code_col = next((c for c in code_candidates if c in sector_mapping_df.columns), None)
    name_col = next((c for c in name_candidates if c in sector_mapping_df.columns), None)

    if sector_col is None or code_col is None:
        raise ValueError(
            f"sector_mapping_df 需包含 code 与 sector 列，当前列: {list(sector_mapping_df.columns)}"
        )
    return code_col, sector_col, name_col


def _build_latest_snapshot(rolling_df: pd.DataFrame) -> pd.DataFrame:
    """
    向量化计算每只股票的 ROC、混合 RS、百分位 RS_Score，
    并附加最新一日的 OHLCV 与均线指标。
    """
    work = rolling_df.sort_values(["code", "date"]).copy()

    # --- 识别涨停基因 (主板约9.5%，创业板/科创板约19.5%) ---
    is_growth = work["code"].astype(str).str.contains(r"\.(?:300|688)")
    limit_thr = np.where(is_growth, 19.5, 9.5)
    work["is_limit_up"] = work["pct_change"] >= limit_thr

    grouped = work.groupby("code", group_keys=False)

    # --- 各周期 ROC (Rate of Change, %) ---
    for period in config.RS_WEIGHTS:
        work[f"roc_{period}"] = grouped["close"].pct_change(periods=period) * 100.0

    # --- 加权混合动量 ---
    work["rs_blended"] = 0.0
    for period, weight in config.RS_WEIGHTS.items():
        work["rs_blended"] += work[f"roc_{period}"].fillna(0.0) * weight

    # --- MA50 斜率：20 天前的 MA50 ---
    work["ma50_lag"] = grouped["ma50"].shift(config.MA_LONG_SLOPE_LOOKBACK)

    # --- 每只股票的有效上市交易日数 ---
    work["listing_days"] = grouped.cumcount() + 1

    # --- 过去 60 天的累计涨停次数 ---
    work["limit_up_60d"] = grouped["is_limit_up"].transform(
        lambda s: s.rolling(60, min_periods=1).sum()
    )

    # 取每只股票最新一行作为截面快照
    latest = work.groupby("code", as_index=False).tail(1).copy()

    # --- 全市场横向百分位排名 → RS_Score (0~100) ---
    latest["RS_Score"] = latest["rs_blended"].rank(pct=True, method="average") * 100.0

    return latest


def _apply_stock_funnel(snapshot: pd.DataFrame) -> pd.Series:
    """
    Qullamaggie 个股过滤漏斗（向量化布尔掩码）：
    - Price > MA20 > MA50
    - MA50 向上发散
    - 当日成交额 >= 5 亿
    - 20 日均成交额 >= 5 亿
    - 上市满 60 个交易日
    - 近 60 日至少 1 次涨停（涨停基因）
    """
    mask = (
        (snapshot["close"] > snapshot["ma20"])
        & (snapshot["ma20"] > snapshot["ma50"])
        & (snapshot["ma50"] > snapshot["ma50_lag"])
        & (snapshot["amount"] >= config.MIN_DAILY_AMOUNT)
        & (snapshot["vol_ma20"] >= config.MIN_VOLUME_MA20)
        & (snapshot["listing_days"] >= config.MIN_LISTING_DAYS)
        & (snapshot["limit_up_60d"] >= config.MIN_LIMIT_UP_COUNT_60D)
        & snapshot["ma20"].notna()
        & snapshot["ma50"].notna()
        & snapshot["ma50_lag"].notna()
        & snapshot["amount"].notna()
        & snapshot["vol_ma20"].notna()
    )
    return mask


def _compute_sector_rs_table(
    merged: pd.DataFrame,
    funnel_codes: set[str],
    rs_top_codes: set[str],
    sector_col: str,
    top_n: int = config.TARGET_SECTOR_COUNT,
) -> List[Dict[str, Any]]:
    """
    申万二级行业 RS 排名（行业内全部成分股 RS 中位数，先排名、后漏斗、级联递补）。

    从 RS 第 1 名往下扫：入选为 0 的行业跳过，由后面行业递补，直到凑满 top_n 个
    「有入选股」的行业（若全市场不足 top_n 个则取实际数量）。

    返回字段：
    - rank: 原始 RS 排名（全市场）
    - stock_count: 行业成分股总数
    - funnel_count: 行业内符合 Qullamaggie 标准的股票数
    - watchlist_count: 行业内 ∩ RS Top10% 的最终入选数
    """
    sector_rs = (
        merged.groupby(sector_col)["RS_Score"]
        .median()
        .sort_values(ascending=False)
    )

    selected: List[Dict[str, Any]] = []
    for orig_rank, (sector_name, sector_score) in enumerate(sector_rs.items(), start=1):
        industry = merged[merged[sector_col] == sector_name]
        funnel_pass = industry[industry["code"].isin(funnel_codes)]
        watchlist = funnel_pass[funnel_pass["code"].isin(rs_top_codes)]
        watchlist_count = int(len(watchlist))
        if watchlist_count == 0:
            continue

        selected.append(
            {
                "rank": orig_rank,
                "sector": str(sector_name),
                "sector_rs": round(float(sector_score), 2),
                "stock_count": int(len(industry)),
                "funnel_count": int(len(funnel_pass)),
                "watchlist_count": watchlist_count,
            }
        )
        if len(selected) >= top_n:
            break
    return selected


def _extract_recent_klines(
    rolling_df: pd.DataFrame,
    code: str,
    days: int = config.KLINE_CHART_DAYS,
) -> List[Dict[str, Any]]:
    """从滚动 K 线池提取最近 N 个交易日 OHLC，供看板绘制。"""
    stock_df = rolling_df[rolling_df["code"] == code].sort_values("date")
    if stock_df.empty:
        return []

    tail = stock_df.tail(days)
    klines: List[Dict[str, Any]] = []
    for _, row in tail.iterrows():
        klines.append(
            {
                "date": pd.to_datetime(row["date"]).strftime("%Y-%m-%d"),
                "open": round(float(row["open"]), 4),
                "high": round(float(row["high"]), 4),
                "low": round(float(row["low"]), 4),
                "close": round(float(row["close"]), 4),
                "volume": float(row["volume"]) if pd.notna(row.get("volume")) else 0.0,
            }
        )
    return klines


def _attach_recent_klines(
    records: List[Dict[str, Any]],
    rolling_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """为 Watchlist 记录附加最近 K 线序列。"""
    for record in records:
        record["klines"] = _extract_recent_klines(rolling_df, str(record["code"]))
    return records


def _attach_all_sectors(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """附加行业标签（申万二级每只股票通常仅一个）。"""
    for record in records:
        sector = record.get("sector")
        record["sectors"] = [str(sector)] if sector else []
    return records


def _is_breakout_candidate(row: pd.Series) -> bool:
    adr = row.get("adr_20d")
    rvol = row.get("rvol")
    if pd.isna(adr) or pd.isna(rvol):
        return False
    return float(adr) < config.VCP_ADR_THRESHOLD_PCT and float(rvol) > config.ORB_RVOL_THRESHOLD


def _build_watchlist_records(
    intersection_df: pd.DataFrame,
    sector_col: str,
    name_col: Optional[str],
) -> List[Dict[str, Any]]:
    """将交集结果转为可 JSON 序列化的 Watchlist 列表。"""
    records: List[Dict[str, Any]] = []
    for _, row in intersection_df.iterrows():
        adr_val = float(row["adr_20d"]) if pd.notna(row.get("adr_20d")) else None
        rvol_val = float(row["rvol"]) if pd.notna(row.get("rvol")) else None
        record: Dict[str, Any] = {
            "code": row["code"],
            "name": row[name_col] if name_col and name_col in row.index and pd.notna(row[name_col]) else "",
            "sector": str(row[sector_col]),
            "rs_score": round(float(row["RS_Score"]), 2),
            "ma20": round(float(row["ma20"]), 4) if pd.notna(row["ma20"]) else None,
            "ma50": round(float(row["ma50"]), 4) if pd.notna(row["ma50"]) else None,
            "close": round(float(row["close"]), 4) if pd.notna(row["close"]) else None,
            "adr_20d": round(adr_val, 4) if adr_val is not None else None,
            "rvol": round(rvol_val, 4) if rvol_val is not None else None,
            "is_breakout_candidate": _is_breakout_candidate(row),
        }
        records.append(record)
    return records


def run_daily_scan(
    rolling_df: pd.DataFrame,
    sector_mapping_df: pd.DataFrame,
    trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    执行每日 RS 扫描：
    1. 计算个股 RS_Score
    2. 申万二级行业 RS 排名（行业内全部成分股 RS 中位数）→ Top 10（入选为 0 则级联递补）
    3. 在 Top 10 行业内筛选符合 Qullamaggie 标准的股票
    4. 与全市场 RS Top 10% 取交集 → 终极 Watchlist

    参数
    ----
    rolling_df : pd.DataFrame
        150 日滚动 K 线热数据池（含 ma20, ma50, vol_ma20）。
    sector_mapping_df : pd.DataFrame
        申万二级行业成分映射，至少含 code + sector 列。
    trade_date : str | None
        扫描日期，默认取 rolling_df 最新交易日。

    返回
    ----
    dict
        {
            "date": str,
            "top_sectors": [...],
            "rs_top_stocks": [...],   # 全市场 RS >= 90 分位
            "intersection": [...],    # Top10 板块幸存股 ∩ RS Top10%
        }
    """
    if rolling_df is None or rolling_df.empty:
        raise ValueError("rolling_df 不能为空")
    if sector_mapping_df is None or sector_mapping_df.empty:
        raise ValueError("sector_mapping_df 不能为空")

    code_col, sector_col, name_col = _resolve_sector_columns(sector_mapping_df)

    # --- 1. 构建最新截面 + RS_Score ---
    snapshot = _build_latest_snapshot(rolling_df)

    if trade_date is None:
        trade_date = pd.to_datetime(snapshot["date"].max()).strftime("%Y-%m-%d")
    else:
        trade_date = pd.to_datetime(trade_date).strftime("%Y-%m-%d")

    # --- 2. 个股过滤漏斗 ---
    funnel_mask = _apply_stock_funnel(snapshot)

    # --- 3. 合并行业映射（申万二级：一股一行业） ---
    mapping = sector_mapping_df[[code_col, sector_col] + ([name_col] if name_col else [])].copy()
    mapping = mapping.rename(columns={code_col: "code"})
    if name_col and name_col != "name":
        mapping = mapping.rename(columns={name_col: "name"})
        name_col = "name"
    mapping["code"] = mapping["code"].astype(str)

    merged = mapping.merge(snapshot, on="code", how="inner")
    if merged.empty:
        return {
            "date": trade_date,
            "top_sectors": [],
            "rs_top_stocks": [],
            "intersection": [],
        }

    # 重新对齐 funnel / RS Top 到 merged
    funnel_codes = set(snapshot.loc[funnel_mask, "code"])
    rs_top = snapshot[snapshot["RS_Score"] >= config.RS_PERCENTILE_THRESHOLD].copy()
    rs_top_codes = set(rs_top["code"])

    # --- 4. 申万二级 RS Top 10（先行业排名，再看行业内漏斗与交集） ---
    top_sectors = _compute_sector_rs_table(
        merged, funnel_codes, rs_top_codes, sector_col
    )
    selected_sector_names = {s["sector"] for s in top_sectors}

    # --- 5. 全市场 RS Top 10%（供参考） ---
    rs_top_records = _build_watchlist_records(
        rs_top.merge(mapping.drop_duplicates("code"), on="code", how="left"),
        sector_col,
        name_col,
    )

    # --- 6. 交集：Top10 行业 ∩ 漏斗合格 ∩ RS Top10% ---
    in_top_sectors = merged[merged[sector_col].isin(selected_sector_names)]
    sector_funnel_pass = in_top_sectors[in_top_sectors["code"].isin(funnel_codes)]
    intersection_df = sector_funnel_pass[sector_funnel_pass["code"].isin(rs_top_codes)].copy()

    if not intersection_df.empty:
        intersection_df = (
            intersection_df.sort_values("RS_Score", ascending=False)
            .drop_duplicates(subset=["code"], keep="first")
        )

    intersection_records = _build_watchlist_records(intersection_df, sector_col, name_col)
    intersection_records = _attach_all_sectors(intersection_records)
    intersection_records = _attach_recent_klines(intersection_records, rolling_df)

    return {
        "date": trade_date,
        "kline_days": config.KLINE_CHART_DAYS,
        "sector_type": "sw_l2",
        "top_sectors": top_sectors,
        "rs_top_stocks": rs_top_records,
        "intersection": intersection_records,
    }
