"""
网络请求工具：强制绕过系统代理（Clash / VPN 常导致东财接口断连）。
"""

from __future__ import annotations

import math
import os
import random
import time
from typing import Any, Dict, Optional

import pandas as pd
import requests


def disable_all_proxies() -> None:
    """清除环境变量代理，并标记不走系统代理。"""
    for key in list(os.environ):
        if "proxy" in key.lower():
            os.environ.pop(key, None)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"


def _clean_session() -> requests.Session:
    """创建不读取系统代理设置的 Session。"""
    session = requests.Session()
    session.trust_env = False
    session.proxies = {"http": None, "https": None}
    return session


def http_get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """带重试的 GET 请求，返回 JSON。"""
    disable_all_proxies()
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            session = _clean_session()
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            time.sleep(1.5 * (attempt + 1) + random.uniform(0.2, 0.8))
    raise ConnectionError(f"请求失败: {url} | {last_error}") from last_error


def fetch_eastmoney_paginated(url: str, base_params: Dict[str, Any]) -> pd.DataFrame:
    """东财分页接口通用拉取（trust_env=False）。"""
    params = base_params.copy()
    data_json = http_get_json(url, params=params)
    diff = data_json.get("data", {}).get("diff", [])
    if not diff:
        return pd.DataFrame()

    per_page = len(diff)
    total = data_json["data"].get("total", per_page)
    total_page = max(1, math.ceil(total / per_page))

    frames = [pd.DataFrame(diff)]
    for page in range(2, total_page + 1):
        params["pn"] = str(page)
        time.sleep(random.uniform(0.3, 0.8))
        page_json = http_get_json(url, params=params)
        page_diff = page_json.get("data", {}).get("diff", [])
        if page_diff:
            frames.append(pd.DataFrame(page_diff))

    return pd.concat(frames, ignore_index=True)
