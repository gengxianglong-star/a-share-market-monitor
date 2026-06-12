# src/config.py
import os
from pathlib import Path
# ==============================================================================
# 📂 1. 基础路径配置 (Path Configurations)
# ==============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
# 三驾马车数据持久化路径
DUCKDB_FILE = DATA_DIR / "market_monitor.duckdb"               # DuckDB 主存储（K线池 + 广度历史）
BREADTH_EXPORT_JSON = DATA_DIR / "market_breadth_history.json" # 静态看板用的广度 JSON 导出
MACRO_BREADTH_FILE = DATA_DIR / "market_breadth_history.csv" # [已废弃] 仅用于 legacy 迁移
ROLLING_KLINES_FILE = DATA_DIR / "rolling_klines.parquet"    # [已废弃] 仅用于 legacy 迁移
DAILY_WATCHLIST_FILE = DATA_DIR / "daily_watchlist.json"     # 每日最终输出观察名单
SECTOR_MAPPING_FILE = DATA_DIR / "sector_mapping.parquet"    # 申万二级 / 概念 成分股映射缓存
SECTOR_MAPPING_SOURCE = "sw"  # sw=申万二级(默认) | ths=同花顺概念 | em=东财概念
# ==============================================================================
# 📊 2. 微观指标：Qullamaggie 相对强度 (Relative Strength) 引擎参数
# ==============================================================================
# 滚动窗口天数：为了计算 120天动量 + 50日均线，至少需要 150 个有效交易日的数据
ROLLING_WINDOW_DAYS = 150
# 混合动量计算权重 (ROC: Rate of Return)
# 游资视角：A股轮动快，降低中长线权重，提升 5天/20天 (一周/一月) 的爆发力权重
RS_WEIGHTS = {
    5: 0.30,   # 1周：超短异动爆发力（极具A股特色）
    20: 0.40,  # 1个月：核心主升浪阶段
    60: 0.20,  # 1个季度：趋势底座
    120: 0.10  # 半年：长线生命线
}
# 相对强度百分位阈值 (90 代表全市场最强的前 10% 股票)
RS_PERCENTILE_THRESHOLD = 90
# 涨停基因防线（过去60个交易日内至少包含的涨停次数）
MIN_LIMIT_UP_COUNT_60D = 1
# VCP 波动收缩 + 相对成交量突破阈值
VCP_ADR_THRESHOLD_PCT = 5.0
ORB_RVOL_THRESHOLD = 2.0
ADR_PERIOD = 20  # ADR：近 20 日 (High-Low)/Close 平均（对齐美股 Qullamaggie）
# 宏观广度历史 DuckDB / JSON 列定义
BREADTH_COLUMNS = [
    "date",
    "above_5pct_count",
    "below_5pct_count",
    "pt20_ratio",
    "pt50_ratio",
    "limit_up_count",
    "limit_down_count",
    "limit_up_down_ratio",
    "new_high_120d",
    "new_low_120d",
    "hs300_close",
    "up_25pct_month",
    "down_25pct_month",
    "up_25pct_qtr",
    "down_25pct_qtr",
]
# ==============================================================================
# 🛡️ 3. 股票过滤漏斗底线 (Quality & Trend Filters)
# ==============================================================================
# 流动性防线：当日 / 20 日均成交额均须 > 5 亿人民币
MIN_DAILY_AMOUNT = 500_000_000
MIN_VOLUME_MA20 = 500_000_000  # vol_ma20 = 20 日滚动均成交额（元）
# 上市时间防线：次新股形态不稳定，剔除上市不足 60 个交易日的股票
MIN_LISTING_DAYS = 60
# 均线多头排列条件：用于布尔值判断
# 逻辑：Price > MA20 > MA50，且 MA50 必须向上发散 (当前MA50 > 20天前的MA50)
MA_SHORT_PERIOD = 20
MA_LONG_PERIOD = 50
MA_LONG_SLOPE_LOOKBACK = 20 # 评估 MA50 趋势斜率的回溯天数
# 强势板块递补目标数量：筛选出至少10个包含符合条件股票的强势申万二级行业
TARGET_SECTOR_COUNT = 10
# ==============================================================================
# 🌡️ 4. 宏观指标：Stockbee 市场宽度监控器 (Market Breadth Monitor) 参数
# ==============================================================================
# 赚钱/亏钱效应极值阈值
BREADTH_EXTREME_UP_DOWN_PCT = 5.0 # 每天涨跌幅 > 5% 或 < -5% 的家数统计阈值
# 均线广度水温水位
BREADTH_PT_SHORT = 20 # PT20: 股价在 20日均线上的比例 (判断短线反弹/回调)
BREADTH_PT_LONG = 50  # PT50: 股价在 50日均线上的比例 (判断中线牛熊)
# 中长线极端情绪衰竭指标 (Stockbee 月度/季度极限)
BREADTH_MONTH_DAYS = 20 # 交易日月度周期
BREADTH_QTR_DAYS = 60   # 交易日季度周期
BREADTH_MONTH_QTR_PCT = 25.0 # 统计 20天/60天内涨跌幅超过 25% 的股票家数
# 涨跌停判定阈值（主板约 10%，创业板/科创板约 20%）
BREADTH_LIMIT_UP_PCT_MAIN = 9.8
BREADTH_LIMIT_UP_PCT_GROWTH = 19.5
BREADTH_NEW_HIGH_LOW_DAYS = 120  # 120 日新高/新低回溯窗口
HS300_INDEX_SYMBOL = "000300"
# ==============================================================================
# 🧹 5. 数据清洗常量 (Data Cleaning Constants)
# ==============================================================================
# 遇到这些名字的股票直接剔除
EXCLUDE_NAME_KEYWORDS = ["ST", "*ST", "退", "PT"]
# ==============================================================================
# 📡 6. 数据管道 (Data Pipeline)
# ==============================================================================
# 通达信安装目录（vipdoc 所在路径），可用环境变量 TDX_DIR 覆盖
TDX_DIR = Path(os.environ.get("TDX_DIR", "C:/new_tdx"))
# 本地读取通达信日线时的并发线程数（读磁盘，可开大一些）
TDX_READ_WORKERS = 16
# 云端 AkShare 增量更新时的并发线程数（备用，截面模式通常不需要）
AKSHARE_UPDATE_WORKERS = 8
# 滚动热数据窗口（与 RS 引擎一致）
# ROLLING_WINDOW_DAYS 已在上方定义
# 全量历史起点（通达信盘后下载建议至少覆盖此日期）
FULL_HISTORY_START_DATE = "2019-01-01"
# 板块映射缓存有效期（天），过期后在 daily_job 中自动刷新
SECTOR_CACHE_DAYS = 7
# 看板 K 线图展示最近 N 个交易日（新浪 GIF 无法指定天数；JSON 接口 datalen 最大约 1023）
KLINE_CHART_DAYS = 60
