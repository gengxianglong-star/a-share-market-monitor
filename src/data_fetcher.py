"""
数据管道：
- 阶段一：读取通达信本地 vipdoc 日线（离线，极速）
- 阶段二：AkShare 全市场截面增量更新（云端每日一次请求）
"""

from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import akshare as ak
import pandas as pd
import requests
from bs4 import BeautifulSoup
from mootdx.reader import Reader
from tqdm import tqdm

from src import config
from src.kline_processor import (
    is_mainboard_a_share_code,
    normalize_akshare_spot,
    normalize_tdx_daily,
    symbol_to_code,
)
from src.network_utils import disable_all_proxies, fetch_eastmoney_paginated, http_get_json, _clean_session
from src.utils import clean_stock_name

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2
SECTOR_SAVE_EVERY = 30  # 每拉取 N 个概念落盘一次，防止中断全丢
THS_BLOCKRANK_LIMIT = 5000  # blockrank 单次上限，覆盖超大概念
_THS_CLID_CACHE: Dict[str, str] = {}
_THS_CLID_LOCK = threading.Lock()


def _get_ths_request_headers() -> Dict[str, str]:
    """生成同花顺请求头（含动态 Cookie）。"""
    try:
        from akshare.datasets import get_ths_js
        import py_mini_racer

        js_code = py_mini_racer.MiniRacer()
        with open(get_ths_js("ths.js"), encoding="utf-8") as handle:
            js_code.eval(handle.read())
        v_code = js_code.call("v")
    except Exception:
        v_code = ""

    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://q.10jqka.com.cn/",
        "Cookie": f"v={v_code}",
    }


def _parse_cons_table(df: pd.DataFrame, sector_name: str) -> List[Dict[str, str]]:
    """从成分股表格解析 code / name / sector。"""
    if df is None or df.empty:
        return []

    code_col = next((c for c in df.columns if "代码" in str(c)), None)
    name_col = next((c for c in df.columns if "名称" in str(c)), None)
    if code_col is None and len(df.columns) >= 2:
        code_col = df.columns[1]
    if name_col is None and len(df.columns) >= 3:
        name_col = df.columns[2]
    if code_col is None:
        return []

    rows: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        raw_code = str(row[code_col]).strip()
        if not raw_code or raw_code == "nan":
            continue
        code = symbol_to_code(raw_code.split(".")[-1][:6])
        if not is_mainboard_a_share_code(code):
            continue
        rows.append(
            {
                "code": code,
                "sector": sector_name,
                "name": str(row[name_col]).strip() if name_col else "",
            }
        )
    return rows


def _resolve_ths_clid(detail_code: str) -> Optional[str]:
    """
    从同花顺概念详情页解析板块内码（885xxx / 886xxx）。
    AkShare 返回的 code 是 URL 编号，blockrank 接口需用 clid 内码。
    """
    detail_code = str(detail_code).strip()
    if not detail_code:
        return None

    with _THS_CLID_LOCK:
        cached = _THS_CLID_CACHE.get(detail_code)
    if cached:
        return cached

    url = f"https://q.10jqka.com.cn/gn/detail/code/{detail_code}/"
    session = _clean_session()
    try:
        response = session.get(url, headers=_get_ths_request_headers(), timeout=15)
        if response.status_code != 200:
            return None
        soup = BeautifulSoup(response.text, "lxml")
        clid_el = soup.find("input", attrs={"id": "clid"})
        if clid_el is None or not clid_el.get("value"):
            return None
        clid = str(clid_el["value"]).strip()
        with _THS_CLID_LOCK:
            _THS_CLID_CACHE[detail_code] = clid
        return clid
    except Exception:
        return None


def _parse_ths_blockrank_items(items: List[dict], sector_name: str) -> List[Dict[str, str]]:
    """从 blockrank JSON 解析成分股。"""
    rows: List[Dict[str, str]] = []
    for item in items:
        raw_code = str(item.get("5", "")).strip()
        if not raw_code:
            continue
        code = symbol_to_code(raw_code.zfill(6)[-6:])
        if not is_mainboard_a_share_code(code):
            continue
        name = str(item.get("55", "") or item.get("1", "")).strip()
        rows.append({"code": code, "sector": sector_name, "name": name})
    return rows


def _fetch_ths_concept_cons_scrape(sector_name: str, sector_code: str) -> List[Dict[str, str]]:
    """
    通过同花顺 blockrank 接口抓取概念成分股。
    sector_code 为详情页编号（如 308614），需先解析 clid（如 886056）。
    """
    clid = _resolve_ths_clid(sector_code)
    if not clid:
        return []

    url = f"https://d.10jqka.com.cn/v2/blockrank/{clid}/199112/d{THS_BLOCKRANK_LIMIT}.js"
    session = _clean_session()
    try:
        response = session.get(url, headers=_get_ths_request_headers(), timeout=15)
        if response.status_code != 200 or "(" not in response.text:
            return []
        payload = response.text.split("(", 1)[1].rsplit(")", 1)[0]
        data = json.loads(payload)
        items = data.get("items") or []
        if not items:
            return []
        return _parse_ths_blockrank_items(items, sector_name)
    except Exception:
        return []


def _fetch_em_sector_cons(sector_name: str, sector_code: Optional[str] = None) -> List[Dict[str, str]]:
    """东财概念成分股（直连 API，绕过系统代理）。"""
    disable_all_proxies()
    board_code = sector_code

    if not board_code:
        try:
            names_df = _fetch_em_concept_names_direct()
            matched = names_df[names_df["板块名称"] == sector_name]
            if not matched.empty:
                board_code = str(matched.iloc[0]["板块代码"])
        except Exception:
            return []

    if not board_code:
        return []

    url = "https://29.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "100",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": f"b:{board_code} f:!50",
        "fields": "f12,f14",
    }

    for attempt in range(MAX_RETRIES):
        try:
            raw_df = fetch_eastmoney_paginated(url, params)
            if raw_df.empty:
                return []
            cons_df = raw_df.rename(columns={"f12": "代码", "f14": "名称"})
            return _parse_cons_table(cons_df, sector_name)
        except Exception:
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    return []


def _fetch_em_concept_names_direct() -> pd.DataFrame:
    """东财概念板块列表（直连 API）。"""
    url = "https://79.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "100",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f12",
        "fs": "m:90 t:3 f:!50",
        "fields": "f12,f14",
    }
    raw_df = fetch_eastmoney_paginated(url, params)
    if raw_df.empty:
        raise ConnectionError("东财概念列表为空")
    result = raw_df.rename(columns={"f12": "板块代码", "f14": "板块名称"})
    return result[["板块代码", "板块名称"]].drop_duplicates()


def import_sector_mapping_from_csv(csv_path: Path) -> pd.DataFrame:
    """
    从 CSV 导入概念映射（手动备用方案）。
    必需列: code, sector；可选列: name
    """
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if "code" not in df.columns or "sector" not in df.columns:
        raise ValueError("CSV 需包含 code 与 sector 两列")
    if "name" not in df.columns:
        df["name"] = ""
    out = df[["code", "sector", "name"]].drop_duplicates()
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    out.to_parquet(config.SECTOR_MAPPING_FILE, index=False, engine="pyarrow")
    print(f"[板块] 已从 CSV 导入 {len(out):,} 条 → {config.SECTOR_MAPPING_FILE}")
    return out


def diagnose_network() -> None:
    """打印网络/代理诊断信息。"""
    disable_all_proxies()
    proxy_keys = [k for k in os.environ if "proxy" in k.lower()]
    print("--- 网络诊断 ---")
    print(f"代理环境变量: {proxy_keys if proxy_keys else '无'}")
    session = _clean_session()
    headers = _get_ths_request_headers()

    # 东财
    try:
        resp = session.get(
            "https://79.push2.eastmoney.com/api/qt/clist/get",
            params={
                "pn": "1",
                "pz": "2",
                "po": "1",
                "np": "1",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": "2",
                "invt": "2",
                "fid": "f12",
                "fs": "m:90 t:3 f:!50",
                "fields": "f12,f14",
            },
            timeout=10,
        )
        print(f"东财概念: OK ({resp.status_code}, {len(resp.text)} bytes)")
    except Exception as exc:
        print(f"东财概念: 失败 ({type(exc).__name__})")

    # 同花顺：详情页 + blockrank 成分股（与正式拉取相同路径）
    try:
        detail_resp = session.get(
            "https://q.10jqka.com.cn/gn/detail/code/301558/",
            headers=headers,
            timeout=10,
        )
        clid = None
        if detail_resp.status_code == 200:
            soup = BeautifulSoup(detail_resp.text, "lxml")
            clid_el = soup.find("input", attrs={"id": "clid"})
            if clid_el and clid_el.get("value"):
                clid = str(clid_el["value"]).strip()
        if not clid:
            print(f"同花顺成分股: 失败 (无法解析 clid, 详情页 {detail_resp.status_code})")
        else:
            br_resp = session.get(
                f"https://d.10jqka.com.cn/v2/blockrank/{clid}/199112/d100.js",
                headers=headers,
                timeout=10,
            )
            item_count = 0
            if br_resp.status_code == 200 and "(" in br_resp.text:
                payload = br_resp.text.split("(", 1)[1].rsplit(")", 1)[0]
                item_count = len(json.loads(payload).get("items") or [])
            print(
                f"同花顺成分股: OK (clid={clid}, 样本 {item_count} 只)"
                if item_count
                else f"同花顺成分股: 失败 (blockrank 空, status={br_resp.status_code})"
            )
    except Exception as exc:
        print(f"同花顺成分股: 失败 ({type(exc).__name__})")

    print("若东财失败、同花顺 OK → 用 --source ths")
    print("若都失败 → 完全退出 Clash 后重试，或用 --import-csv")
    print("----------------")


def _save_sector_mapping_partial(rows: List[Dict[str, str]]) -> None:
    """增量保存板块映射，避免长时间拉取后中断全丢。"""
    if not rows:
        return
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    partial_df = pd.DataFrame(rows).drop_duplicates(subset=["code", "sector"])
    partial_df.to_parquet(config.SECTOR_MAPPING_FILE, index=False, engine="pyarrow")


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


def _fetch_one_sector_cons(sector_name: str, sector_code: Optional[str] = None) -> List[Dict[str, str]]:
    """
    拉取单个概念板块成分股：优先同花顺抓取，失败则回退东财直连。
    """
    disable_all_proxies()
    rows: List[Dict[str, str]] = []
    if sector_code:
        rows = _fetch_ths_concept_cons_scrape(sector_name, str(sector_code))
    if not rows:
        rows = _fetch_em_sector_cons(sector_name, sector_code)
    return rows


def fetch_ths_sector_mapping(force_refresh: bool = False) -> pd.DataFrame:
    """
    获取概念 ↔ 成分股映射表，带本地 Parquet 缓存。

    返回列: code, sector, name
    """
    cache_path = config.SECTOR_MAPPING_FILE
    if (
        not force_refresh
        and cache_path.exists()
        and (time.time() - cache_path.stat().st_mtime) < config.SECTOR_CACHE_DAYS * 86400
    ):
        print(f"[板块] 使用缓存: {cache_path}")
        cached = pd.read_parquet(cache_path, engine="pyarrow")
        if len(cached) > 0:
            return cached

    print("[板块] 缓存过期或不存在，开始拉取概念成分股（同花顺优先，东财备用）...")
    sectors_df = fetch_ths_sectors()
    name_col = "name" if "name" in sectors_df.columns else sectors_df.columns[0]
    code_col = "code" if "code" in sectors_df.columns else None

    sector_items = []
    for _, row in sectors_df.iterrows():
        sector_items.append(
            (str(row[name_col]), str(row[code_col]) if code_col else None)
        )

    all_rows: List[Dict[str, str]] = []
    success_sectors = 0

    # 同花顺 Cookie 对并发敏感，采用低并发 + 增量落盘
    workers = min(config.AKSHARE_UPDATE_WORKERS, 4)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_fetch_one_sector_cons, name, code): name
            for name, code in sector_items
        }
        with tqdm(total=len(futures), desc="拉取概念成分股", unit="sector") as progress:
            for index, future in enumerate(as_completed(futures), start=1):
                sector_name = futures[future]
                try:
                    sector_rows = future.result()
                    if sector_rows:
                        all_rows.extend(sector_rows)
                        success_sectors += 1
                except Exception:
                    pass
                if index % SECTOR_SAVE_EVERY == 0 and all_rows:
                    _save_sector_mapping_partial(all_rows)
                progress.set_postfix(成功=success_sectors, 记录=len(all_rows))
                progress.update(1)

    if not all_rows:
        if cache_path.exists():
            print("[警告] 本次拉取为空，回退至旧缓存。")
            return pd.read_parquet(cache_path, engine="pyarrow")
        raise RuntimeError(
            "概念板块映射拉取失败且无可用缓存。\n"
            "同花顺成分股接口已从 AkShare 移除；请关闭代理后重试，或本地运行:\n"
            "  python build_sector_mapping.py"
        )

    mapping_df = pd.DataFrame(all_rows).drop_duplicates(subset=["code", "sector"])
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    mapping_df.to_parquet(cache_path, index=False, engine="pyarrow")
    print(
        f"[板块] 映射表已更新: {len(mapping_df):,} 条 | "
        f"{mapping_df['sector'].nunique()} 个概念 | 成功 {success_sectors}/{len(sector_items)}"
    )
    return mapping_df


def fetch_em_sector_mapping(force_refresh: bool = False) -> pd.DataFrame:
    """仅使用东财直连 API 构建概念映射。"""
    disable_all_proxies()
    cache_path = config.SECTOR_MAPPING_FILE
    if (
        not force_refresh
        and cache_path.exists()
        and (time.time() - cache_path.stat().st_mtime) < config.SECTOR_CACHE_DAYS * 86400
    ):
        cached = pd.read_parquet(cache_path, engine="pyarrow")
        if len(cached) > 0:
            return cached

    print("[板块] 使用东财直连 API 拉取概念成分股（已绕过系统代理）...")
    sectors_df = _fetch_em_concept_names_direct()
    sector_names = sectors_df["板块名称"].astype(str).tolist()
    code_map = dict(zip(sectors_df["板块名称"], sectors_df["板块代码"]))

    all_rows: List[Dict[str, str]] = []
    workers = min(config.AKSHARE_UPDATE_WORKERS, 6)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_fetch_em_sector_cons, name, str(code_map.get(name, ""))): name
            for name in sector_names
        }
        with tqdm(total=len(futures), desc="东财概念成分股", unit="sector") as progress:
            for index, future in enumerate(as_completed(futures), start=1):
                try:
                    all_rows.extend(future.result())
                except Exception:
                    pass
                if index % SECTOR_SAVE_EVERY == 0 and all_rows:
                    _save_sector_mapping_partial(all_rows)
                progress.update(1)

    if not all_rows:
        raise RuntimeError(
            "东财概念映射拉取失败。请确认已关闭 Clash，或在 PowerShell 执行:\n"
            "  $env:HTTP_PROXY=''; $env:HTTPS_PROXY=''; python build_sector_mapping.py --source em"
        )

    mapping_df = pd.DataFrame(all_rows).drop_duplicates(subset=["code", "sector"])
    mapping_df.to_parquet(cache_path, index=False, engine="pyarrow")
    return mapping_df


def _normalize_sw_index_code(index_code: str) -> str:
    """801016.SI -> 801016"""
    return str(index_code).replace(".SI", "").strip()


def fetch_sw_sectors() -> pd.DataFrame:
    """获取申万二级行业列表（AkShare）。"""
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            sector_df = ak.sw_index_second_info()
            if sector_df is None or sector_df.empty:
                raise RuntimeError("AkShare 返回空的申万二级行业列表")
            out = sector_df.rename(
                columns={"行业代码": "code", "行业名称": "name"}
            )[["name", "code"]].copy()
            out["code"] = out["code"].map(_normalize_sw_index_code)
            return out
        except Exception as exc:
            last_error = exc
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise RuntimeError(f"获取申万二级行业失败: {last_error}") from last_error


def _fetch_sw_sector_cons(sector_name: str, sector_code: str) -> List[Dict[str, str]]:
    """拉取单个申万二级行业成分股。"""
    disable_all_proxies()
    symbol = _normalize_sw_index_code(sector_code)
    for attempt in range(MAX_RETRIES):
        try:
            cons_df = ak.index_component_sw(symbol=symbol)
            if cons_df is None or cons_df.empty:
                return []
            rows: List[Dict[str, str]] = []
            for _, row in cons_df.iterrows():
                raw_code = str(row.get("证券代码", "")).strip()
                if not raw_code:
                    continue
                code = symbol_to_code(raw_code)
                if not is_mainboard_a_share_code(code):
                    continue
                rows.append(
                    {
                        "code": code,
                        "sector": sector_name,
                        "name": str(row.get("证券名称", "")).strip(),
                    }
                )
            return rows
        except Exception:
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    return []


def fetch_sw_sector_mapping(force_refresh: bool = False) -> pd.DataFrame:
    """
    获取申万二级行业 ↔ 成分股映射表，带本地 Parquet 缓存。
    每只股票通常仅归属一个二级行业。
    """
    cache_path = config.SECTOR_MAPPING_FILE
    if (
        not force_refresh
        and cache_path.exists()
        and (time.time() - cache_path.stat().st_mtime) < config.SECTOR_CACHE_DAYS * 86400
    ):
        cached = pd.read_parquet(cache_path, engine="pyarrow")
        if len(cached) > 0:
            is_sw_cache = (
                "mapping_source" in cached.columns
                and cached["mapping_source"].eq("sw").all()
            )
            if is_sw_cache:
                print(f"[板块] 使用申万二级缓存: {cache_path}")
                return cached
            print("[板块] 检测到旧版概念映射，将重建申万二级映射...")

    print("[板块] 拉取申万二级行业成分股（约 130 个行业）...")
    sectors_df = fetch_sw_sectors()
    sector_items = [
        (str(row["name"]), str(row["code"]))
        for _, row in sectors_df.iterrows()
    ]

    all_rows: List[Dict[str, str]] = []
    success_sectors = 0
    workers = min(config.AKSHARE_UPDATE_WORKERS, 6)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_fetch_sw_sector_cons, name, code): name
            for name, code in sector_items
        }
        with tqdm(total=len(futures), desc="申万二级成分股", unit="sector") as progress:
            for index, future in enumerate(as_completed(futures), start=1):
                sector_name = futures[future]
                try:
                    sector_rows = future.result()
                    if sector_rows:
                        all_rows.extend(sector_rows)
                        success_sectors += 1
                except Exception:
                    pass
                if index % SECTOR_SAVE_EVERY == 0 and all_rows:
                    _save_sector_mapping_partial(all_rows)
                progress.set_postfix(成功=success_sectors, 记录=len(all_rows))
                progress.update(1)

    if not all_rows:
        if cache_path.exists():
            print("[警告] 申万拉取为空，回退至旧缓存。")
            return pd.read_parquet(cache_path, engine="pyarrow")
        raise RuntimeError(
            "申万二级映射拉取失败。请运行:\n"
            "  python build_sector_mapping.py --source sw --force"
        )

    mapping_df = pd.DataFrame(all_rows).drop_duplicates(subset=["code", "sector"])
    mapping_df["mapping_source"] = "sw"
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    mapping_df.to_parquet(cache_path, index=False, engine="pyarrow")
    print(
        f"[板块] 申万映射已更新: {len(mapping_df):,} 条 | "
        f"{mapping_df['sector'].nunique()} 个二级行业 | 成功 {success_sectors}/{len(sector_items)}"
    )
    return mapping_df


def fetch_sector_mapping(force_refresh: bool = False) -> pd.DataFrame:
    """按 config.SECTOR_MAPPING_SOURCE 拉取板块映射。"""
    source = getattr(config, "SECTOR_MAPPING_SOURCE", "sw").lower()
    if source == "sw":
        return fetch_sw_sector_mapping(force_refresh=force_refresh)
    if source == "em":
        return fetch_em_sector_mapping(force_refresh=force_refresh)
    return fetch_ths_sector_mapping(force_refresh=force_refresh)


# 兼容旧引用
fetch_sector_mapping_ths = fetch_ths_sector_mapping


def get_latest_trade_date() -> str:
    """获取不晚于今天的最近一个 A 股交易日（Asia/Shanghai）。"""
    from src.db_client import shanghai_today

    today = shanghai_today()
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
