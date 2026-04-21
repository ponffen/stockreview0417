#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用 AKShare 拉取日线收盘价，写入 SQLite 表 symbol_daily_close（与 Node 端结构一致）。

依赖: pip install -r requirements-akshare.txt

用法:
  python3 scripts/akshare_fill_symbol_daily_close.py
  python3 scripts/akshare_fill_symbol_daily_close.py --symbol aapl
  DB_PATH=/path/to/app.db python3 scripts/akshare_fill_symbol_daily_close.py

数据口径: AKShare stock_*_hist，adjust=\"\" 为不复权收盘价。
沪深 A 股/ETF：优先腾讯 `stock_zh_a_hist_tx`，东财 `stock_zh_a_hist` 仅作回退（避免 push2his 断连）。

代理: 默认会临时清除 HTTP(S)_PROXY，避免本机代理无法访问 push2his.eastmoney.com。
若必须走代理拉数，请先 export AKSHARE_HONOR_PROXY=1 再运行。
"""

from __future__ import annotations

import os

# AkShare/requests 走坏掉的系统代理时东财全挂；除非用户显式要求保留代理
if os.environ.get("AKSHARE_HONOR_PROXY", "").strip() != "1":
    for _k in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        os.environ.pop(_k, None)

import argparse
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import akshare as ak
    import pandas as pd
    import requests
except ImportError:
    print("请先安装: pip install -r requirements-akshare.txt", file=sys.stderr)
    sys.exit(1)

# 避免 requests 仍走系统代理/环境代理（东财常被代理误伤）
_orig_sess_req = requests.sessions.Session.request


def _session_request_bypass_proxy(self, method, url, **kwargs):
    self.trust_env = False
    kwargs.setdefault("proxies", {"http": None, "https": None})
    return _orig_sess_req(self, method, url, **kwargs)


requests.sessions.Session.request = _session_request_bypass_proxy

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "app.db"
SOURCE_TAG = "akshare"

# 东财美股列表里若与代码不一致，可在此处写死 secid（完整 106.xxx）
US_SECID_OVERRIDES: dict[str, str] = {}


def normalize_symbol(raw: str) -> str:
    """与 src/db.js normalizeSymbol 一致。"""
    value = "".join(str(raw or "").split()).lower()
    if not value:
        return ""
    if (
        value.startswith("sh")
        or value.startswith("sz")
        or value.startswith("hk")
        or value.startswith("rt_hk")
        or value.startswith("gb_")
    ):
        return value
    if len(value) == 6 and value.isdigit():
        if value[0] in ("5", "6", "9"):
            return f"sh{value}"
        return f"sz{value}"
    if len(value) == 5 and value.isdigit():
        return f"hk{value}"
    if value.isdigit() and 1 <= len(value) <= 4:
        return f"hk{value.zfill(5)}"
    return value


def add_calendar_days(date_str: str, delta: int) -> str:
    base = str(date_str or "")[:10]
    d = datetime.strptime(base, "%Y-%m-%d") + timedelta(days=delta)
    return d.strftime("%Y-%m-%d")


def ymd_compact(s: str) -> str:
    return str(s)[:10].replace("-", "")


def strip_gb_prefix_if_any(norm: str) -> str:
    n = norm.strip().lower()
    if n.startswith("gb_"):
        return n[3:]
    return n


def to_us_secid(normalized: str) -> str | None:
    """东财美股 secid，如 106.AAPL；brk.a -> 106.BRK-A。"""
    n = strip_gb_prefix_if_any(normalized).strip().lower()
    if n in US_SECID_OVERRIDES:
        return US_SECID_OVERRIDES[n]
    if not n or not n.replace(".", "").replace("-", "").isalnum():
        return None
    if "." in n and n.count(".") == 1 and not n.startswith("hk"):
        left, right = n.split(".", 1)
        if left and len(right) <= 2:
            return f"106.{left.upper()}-{right.upper()}"
    return f"106.{n.upper().replace('.', '-')}"


def fetch_cn_hist_em(symbol_6: str, start: str, end: str) -> pd.DataFrame:
    """东财沪深日 K（易触发 push2his 断连）。"""
    return ak.stock_zh_a_hist(
        symbol=symbol_6,
        period="daily",
        start_date=ymd_compact(start),
        end_date=ymd_compact(end),
        adjust="",
    )


def fetch_cn_hist_tx(norm_sh_sz: str, start: str, end: str) -> pd.DataFrame:
    """
    腾讯沪深日 K，代码形如 sh510300 / sz159605（与 AkShare 文档一致）。
    与东财不同源，一般更耐代理/TLS 抖动。
    """
    sym = norm_sh_sz.strip().lower()
    return ak.stock_zh_a_hist_tx(
        symbol=sym,
        start_date=ymd_compact(start),
        end_date=ymd_compact(end),
        adjust="",
        timeout=60,
    )


def fetch_cn_hist_with_fallback(norm_sh_sz: str, code_6: str, start: str, end: str) -> tuple[pd.DataFrame, str]:
    """先腾讯，失败后东财。"""
    try:
        df = fetch_cn_hist_tx(norm_sh_sz, start, end)
        if df is not None and not df.empty:
            return df, "stock_zh_a_hist_tx"
    except Exception:
        pass
    time.sleep(0.5)
    df2 = fetch_cn_hist_em(code_6, start, end)
    return df2, "stock_zh_a_hist"


def fetch_hk_hist(code_5: str, start: str, end: str) -> pd.DataFrame:
    return ak.stock_hk_hist(
        symbol=code_5,
        period="daily",
        start_date=ymd_compact(start),
        end_date=ymd_compact(end),
        adjust="",
    )


def fetch_us_hist(secid: str, start: str, end: str) -> pd.DataFrame:
    return ak.stock_us_hist(
        symbol=secid,
        period="daily",
        start_date=ymd_compact(start),
        end_date=ymd_compact(end),
        adjust="",
    )


def df_to_rows(
    df: pd.DataFrame,
    norm_symbol: str,
    range_from: str,
    range_to: str,
) -> list[tuple[str, str, float, str, int]]:
    if df is None or df.empty:
        return []
    date_col = None
    close_col = None
    for c in df.columns:
        cs = str(c).strip()
        if cs in ("日期", "date", "Date"):
            date_col = c
        if cs in ("收盘", "close", "Close"):
            close_col = c
    if date_col is None:
        date_col = df.columns[0]
    if close_col is None:
        raise ValueError(f"找不到收盘列: {df.columns.tolist()}")

    rf = range_from[:10]
    rt = range_to[:10]
    now_ms = int(time.time() * 1000)
    out: list[tuple[str, str, float, str, int]] = []
    for _, row in df.iterrows():
        d_raw = row[date_col]
        if pd.isna(d_raw):
            continue
        if isinstance(d_raw, pd.Timestamp):
            dk = d_raw.strftime("%Y-%m-%d")
        elif hasattr(d_raw, "strftime"):
            dk = d_raw.strftime("%Y-%m-%d")
        else:
            ds = str(d_raw)[:10].replace("/", "-")
            dk = ds if len(ds) >= 10 else ""
        close_v = row[close_col]
        if pd.isna(close_v):
            continue
        close_f = float(close_v)
        if not dk or close_f <= 0:
            continue
        if dk < rf or dk > rt:
            continue
        out.append((norm_symbol, dk, close_f, SOURCE_TAG, now_ms))
    return out


def get_symbols_with_ranges(
    conn: sqlite3.Connection,
) -> list[tuple[str, str, str]]:
    rows = conn.execute("SELECT symbol, trade_date FROM trades").fetchall()
    per: dict[str, tuple[str, str]] = {}
    for sym_raw, trade_date in rows:
        s = normalize_symbol(sym_raw)
        if not s:
            continue
        d = str(trade_date)[:10]
        if s not in per:
            per[s] = (d, d)
        else:
            lo, hi = per[s]
            if d < lo:
                lo = d
            if d > hi:
                hi = d
            per[s] = (lo, hi)
    today = datetime.now().strftime("%Y-%m-%d")
    out: list[tuple[str, str, str]] = []
    for s in sorted(per.keys()):
        lo, hi = per[s]
        rf = add_calendar_days(lo, -1)
        rt = add_calendar_days(hi, 1)
        if rt < today:
            rt = today
        out.append((s, rf, rt))
    return out


def upsert_batch(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT INTO symbol_daily_close (symbol, date, close, source, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(symbol, date) DO UPDATE SET
          close = excluded.close,
          source = excluded.source,
          updated_at = excluded.updated_at
        """,
        rows,
    )
    return len(rows)


def fetch_hist_dataframe(norm: str, rf: str, rt: str) -> tuple[pd.DataFrame, str]:
    """返回 (DataFrame, 说明标签)。"""
    n = norm.lower().strip()
    if n.startswith("sh") and len(n) == 8 and n[2:].isdigit():
        return fetch_cn_hist_with_fallback(n, n[2:], rf, rt)
    if n.startswith("sz") and len(n) == 8 and n[2:].isdigit():
        return fetch_cn_hist_with_fallback(n, n[2:], rf, rt)
    if n.startswith("hk") and len(n) == 7 and n[2:].isdigit():
        return fetch_hk_hist(n[2:], rf, rt), "stock_hk_hist"
    # rt_hk_xxxxx → 按港股处理
    if n.startswith("rt_hk"):
        rest = re.sub(r"^rt_hk_?", "", n, flags=re.I)
        digits = "".join(c for c in rest if c.isdigit()).zfill(5)[-5:]
        if len(digits) != 5:
            return pd.DataFrame(), "rt_hk_parse"
        return fetch_hk_hist(digits, rf, rt), "stock_hk_hist(rt_hk)"
    # gb_ 或裸美股代码
    sec = to_us_secid(n)
    if not sec:
        return pd.DataFrame(), "no_us_secid"
    return fetch_us_hist(sec, rf, rt), sec


def main() -> None:
    ap = argparse.ArgumentParser(description="AKShare 回填 symbol_daily_close")
    ap.add_argument("--symbol", help="仅处理该归一化代码（如 aapl、hk00700、sh510300）")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="只列出将处理的标的与日期区间，不拉数、不写库",
    )
    args = ap.parse_args()

    db_path = Path(os.environ.get("DB_PATH", str(DEFAULT_DB))).resolve()
    if not db_path.is_file():
        print(f"数据库不存在: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    try:
        pairs = get_symbols_with_ranges(conn)
    finally:
        conn.close()

    if not pairs:
        print("无成交记录，退出。")
        return

    if args.symbol:
        filt = normalize_symbol(args.symbol)
        pairs = [(s, a, b) for (s, a, b) in pairs if s == filt]
        if not pairs:
            print(f"未找到标的: {args.symbol} -> {filt}", file=sys.stderr)
            sys.exit(1)

    print(f"[akshare] DB={db_path} 标的数={len(pairs)}")

    if args.dry_run:
        for s, rf, rt in pairs:
            print(f"  {s}\t{rf}\t{rt}")
        return

    conn = sqlite3.connect(str(db_path))
    total_upsert = 0
    try:
        for i, (sym, rf, rt) in enumerate(pairs):
            print(f"  [{i + 1}/{len(pairs)}] {sym} … ", end="", flush=True)
            try:
                df, note = fetch_hist_dataframe(sym, rf, rt)
                sym_lower = sym.lower().strip()
                rows = df_to_rows(df, sym_lower, rf, rt)
                if not rows:
                    print(f"0 行 ({note})")
                else:
                    upsert_batch(conn, rows)
                    conn.commit()
                    total_upsert += len(rows)
                    print(f"{len(rows)} 行 ({note})")
            except Exception as e:
                print(f"失败: {e}")
            time.sleep(0.3)
    finally:
        conn.close()

    print(f"[akshare] 完成，共写入 {total_upsert} 条日线记录（含更新）。")


if __name__ == "__main__":
    main()
