"""
DuckDB 存储内核：滚动 K 线池 + 宏观广度历史。
所有写入 DuckDB 的日期列在入库前强制 Asia/Shanghai 时区。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Union

import duckdb
import pandas as pd

from src import config

SH_TZ = "Asia/Shanghai"

ROLLING_KLINES_TABLE = "rolling_klines"
BREADTH_TABLE = "market_breadth_history"


def localize_datetime_series(series: pd.Series) -> pd.Series:
    """入库前：所有 datetime 强制为 Asia/Shanghai  aware。"""
    dt = pd.to_datetime(series, errors="coerce")
    if getattr(dt.dt, "tz", None) is not None:
        return dt.dt.tz_convert(SH_TZ)
    return dt.dt.tz_localize(SH_TZ, nonexistent="shift_forward", ambiguous="NaT")


def shanghai_today() -> str:
    """当前上海时区日期 YYYY-MM-DD。"""
    return pd.Timestamp.now(tz=SH_TZ).strftime("%Y-%m-%d")


def prepare_df_for_db(df: pd.DataFrame, date_columns: tuple[str, ...] = ("date",)) -> pd.DataFrame:
    """对 DataFrame 的日期列做上海时区规范化后再写入 DuckDB。"""
    work = df.copy()
    for col in date_columns:
        if col in work.columns:
            work[col] = localize_datetime_series(work[col])
    return work


def ensure_data_dir() -> None:
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    ensure_data_dir()
    return duckdb.connect(str(config.DUCKDB_FILE), read_only=read_only)


def init_duckdb() -> None:
    """初始化 DuckDB 文件与空表结构（若尚不存在）。"""
    ensure_data_dir()
    conn = get_connection()
    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {BREADTH_TABLE} (
                date VARCHAR PRIMARY KEY,
                above_5pct_count BIGINT,
                below_5pct_count BIGINT,
                pt20_ratio DOUBLE,
                pt50_ratio DOUBLE,
                limit_up_count BIGINT,
                limit_down_count BIGINT,
                limit_up_down_ratio DOUBLE,
                new_high_120d BIGINT,
                new_low_120d BIGINT,
                hs300_close DOUBLE,
                up_25pct_month BIGINT,
                down_25pct_month BIGINT,
                up_25pct_qtr BIGINT,
                down_25pct_qtr BIGINT
            )
            """
        )
    finally:
        conn.close()


def _read_df_from_table(table: str) -> pd.DataFrame:
    if not config.DUCKDB_FILE.exists():
        return pd.DataFrame()
    conn = get_connection(read_only=True)
    try:
        try:
            return conn.execute(f"SELECT * FROM {table}").df()
        except duckdb.CatalogException:
            return pd.DataFrame()
    finally:
        conn.close()


def load_rolling_klines() -> pd.DataFrame:
    """从 DuckDB 读取滚动 K 线热数据池。"""
    df = _read_df_from_table(ROLLING_KLINES_TABLE)
    if df.empty:
        return df
    if "date" in df.columns:
        df["date"] = localize_datetime_series(df["date"]).dt.tz_localize(None)
    return df


def save_rolling_klines(df: pd.DataFrame) -> None:
    """全量替换滚动 K 线表。"""
    if df is None or df.empty:
        raise ValueError("rolling klines 不能为空")
    prepared = prepare_df_for_db(df)
    for col in prepared.select_dtypes(include=["datetimetz"]).columns:
        prepared[col] = prepared[col].dt.tz_localize(None)

    conn = get_connection()
    try:
        conn.register("_rolling_write", prepared)
        conn.execute(f"CREATE OR REPLACE TABLE {ROLLING_KLINES_TABLE} AS SELECT * FROM _rolling_write")
        conn.unregister("_rolling_write")
    finally:
        conn.close()


def load_breadth_history() -> pd.DataFrame:
    """读取宏观广度历史。"""
    df = _read_df_from_table(BREADTH_TABLE)
    if df.empty:
        return pd.DataFrame(columns=config.BREADTH_COLUMNS)
    for col in config.BREADTH_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[config.BREADTH_COLUMNS].sort_values("date").reset_index(drop=True)


def replace_breadth_history(df: pd.DataFrame) -> pd.DataFrame:
    """全量替换广度历史表。"""
    if df is None or df.empty:
        conn = get_connection()
        try:
            conn.execute(f"DELETE FROM {BREADTH_TABLE}")
        finally:
            conn.close()
        export_breadth_json()
        return pd.DataFrame(columns=config.BREADTH_COLUMNS)

    work = df[config.BREADTH_COLUMNS].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    conn = get_connection()
    try:
        conn.register("_breadth_write", work)
        conn.execute(f"CREATE OR REPLACE TABLE {BREADTH_TABLE} AS SELECT * FROM _breadth_write")
        conn.unregister("_breadth_write")
    finally:
        conn.close()

    export_breadth_json()
    return work


def upsert_breadth_row(row_df: pd.DataFrame) -> pd.DataFrame:
    """按日期 upsert 单行广度记录，返回完整历史。"""
    init_duckdb()
    row = row_df[config.BREADTH_COLUMNS].copy()
    trade_date = pd.to_datetime(row["date"].iloc[0], errors="coerce").strftime("%Y-%m-%d")
    row["date"] = trade_date

    history = load_breadth_history()
    if not history.empty:
        history["date"] = pd.to_datetime(history["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        history = history[history["date"] != trade_date]

    updated = pd.concat([history, row], ignore_index=True)
    updated = updated.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    replace_breadth_history(updated)
    return updated


def _records_to_json_safe(df: pd.DataFrame) -> str:
    """将 DataFrame 转为标准 JSON（NaN/Inf → null）。"""
    # pandas to_json 会把 NaN 写成 null，避免 json.dumps 输出非法 NaN
    return df.to_json(orient="records", force_ascii=False, indent=2)


def export_breadth_json(path: Optional[Union[str, Path]] = None) -> Path:
    """导出广度 JSON 供静态看板 fetch（GitHub Pages 兼容）。"""
    out_path = Path(path or config.BREADTH_EXPORT_JSON)
    df = load_breadth_history()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_records_to_json_safe(df), encoding="utf-8")
    return out_path


def duckdb_is_ready() -> bool:
    """DuckDB 中是否已有滚动 K 线数据。"""
    if not config.DUCKDB_FILE.exists():
        return False
    conn = get_connection(read_only=True)
    try:
        count = conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{ROLLING_KLINES_TABLE}'"
        ).fetchone()[0]
        if not count:
            return False
        rows = conn.execute(f"SELECT COUNT(*) FROM {ROLLING_KLINES_TABLE}").fetchone()[0]
        return rows > 0
    except duckdb.CatalogException:
        return False
    finally:
        conn.close()


def migrate_legacy_storage() -> bool:
    """
    一次性从 parquet / csv 迁移至 DuckDB。
    返回 True 表示发生了迁移。
    """
    if duckdb_is_ready():
        return False

    migrated = False
    init_duckdb()

    if config.ROLLING_KLINES_FILE.exists():
        legacy = pd.read_parquet(config.ROLLING_KLINES_FILE, engine="pyarrow")
        save_rolling_klines(legacy)
        migrated = True
        print(f"[迁移] rolling_klines.parquet → {config.DUCKDB_FILE}")

    if config.MACRO_BREADTH_FILE.exists():
        legacy_breadth = pd.read_csv(config.MACRO_BREADTH_FILE)
        replace_breadth_history(legacy_breadth)
        migrated = True
        print(f"[迁移] market_breadth_history.csv → {config.DUCKDB_FILE}")
    elif migrated:
        export_breadth_json()

    return migrated
