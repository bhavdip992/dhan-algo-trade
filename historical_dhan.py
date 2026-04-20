"""
historical_dhan.py  —  Historical OHLCV data via Dhan API
═══════════════════════════════════════════════════════════════════════
Replaces: yfinance + Kotak historical_candles()

Provides:
  fetch_dhan_history(index, interval_min, days)  → pd.DataFrame
  warmup_engine(engine, index, tf_min, days)     → int (bars loaded)

Dhan v2 intraday endpoint:
  POST /v2/charts/intraday
  {
    "securityId":      "13",         ← NIFTY index security_id
    "exchangeSegment": "IDX_I",
    "instrument":      "INDEX",
    "interval":        "1",          ← "1","5","15","25","60"
    "oi":              false,
    "fromDate":        "2024-09-11 09:15:00",
    "toDate":          "2024-09-15 15:30:00"
  }

Response:
  {
    "open":   [...], "high": [...], "low": [...],
    "close":  [...], "volume": [...], "timestamp": [...]
  }

Dhan allows max 90-day window per call (intraday).
"""

import os, time, logging
from datetime import datetime, date, timedelta
from typing import Optional

import requests
import pandas as pd

from broker_dhan import session, INDEX_SECURITY_IDS

log = logging.getLogger("HistoricalDhan")

# ── Dhan exchange/instrument codes for index history ─────────────────
_INDEX_CONFIG = {
    "NIFTY":      {"segment": "IDX_I", "instrument": "INDEX", "sid": "13"},
    "BANKNIFTY":  {"segment": "IDX_I", "instrument": "INDEX", "sid": "25"},
    "FINNIFTY":   {"segment": "IDX_I", "instrument": "INDEX", "sid": "27"},
    "MIDCPNIFTY": {"segment": "IDX_I", "instrument": "INDEX", "sid": "26"},
    "SENSEX":     {"segment": "IDX_I", "instrument": "INDEX", "sid": "1"},
}

BASE = "https://api.dhan.co/v2"


# ════════════════════════════════════════════════════════════════════
# FETCH INTRADAY HISTORY
# ════════════════════════════════════════════════════════════════════
def fetch_dhan_history(
    index:        str = "NIFTY",
    interval_min: int = 5,
    days:         int = 5,
) -> pd.DataFrame:
    """
    Fetch intraday OHLCV candles from Dhan API.

    Args:
        index:        "NIFTY" | "BANKNIFTY" | "FINNIFTY" | "MIDCPNIFTY"
        interval_min: 1 | 5 | 15 | 25 | 60
        days:         Number of calendar days to look back (≤ 90)

    Returns:
        DataFrame with columns [open, high, low, close, volume]
        indexed by pd.DatetimeIndex (IST timestamps).
        Empty DataFrame on failure.
    """
    idx    = index.upper()
    cfg    = _INDEX_CONFIG.get(idx)
    if not cfg:
        log.error(f"Unknown index: {idx}")
        return pd.DataFrame()

    # Dhan accepts intervals as strings
    interval_str = str(interval_min)
    if interval_str not in ("1", "5", "15", "25", "60"):
        log.warning(f"Unsupported interval {interval_min}m → using 5m")
        interval_str = "5"

    to_dt   = datetime.now()
    from_dt = to_dt - timedelta(days=min(days + 5, 89))   # +5 for weekends

    headers = session.headers
    all_rows = []

    # Chunk into 30-day windows to stay safe
    chunk_days = 30
    cur_end = to_dt
    while cur_end > from_dt:
        cur_start = max(cur_end - timedelta(days=chunk_days), from_dt)
        payload = {
            "securityId":      cfg["sid"],
            "exchangeSegment": cfg["segment"],
            "instrument":      cfg["instrument"],
            "interval":        interval_str,
            "oi":              False,
            "fromDate":        cur_start.strftime("%Y-%m-%d 09:15:00"),
            "toDate":          cur_end.strftime("%Y-%m-%d 15:30:00"),
        }
        try:
            r = requests.post(
                f"{BASE}/charts/intraday",
                headers=headers, json=payload, timeout=20
            )
            if r.status_code == 429:
                log.warning("Dhan rate limit — sleeping 2s")
                time.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()

            opens      = data.get("open",      [])
            highs      = data.get("high",      [])
            lows       = data.get("low",       [])
            closes     = data.get("close",     [])
            volumes    = data.get("volume",    [])
            timestamps = data.get("timestamp", [])

            if not closes:
                log.debug(f"No data for chunk {cur_start.date()}–{cur_end.date()}")
                cur_end = cur_start - timedelta(days=1)
                continue

            n = min(len(closes), len(timestamps))
            for i in range(n):
                try:
                    ts = pd.Timestamp(timestamps[i], unit="s", tz="Asia/Kolkata")
                    ts = ts.tz_localize(None)   # naive local time
                    all_rows.append({
                        "timestamp": ts,
                        "open":      float(opens[i]   if i < len(opens)   else closes[i]),
                        "high":      float(highs[i]   if i < len(highs)   else closes[i]),
                        "low":       float(lows[i]    if i < len(lows)    else closes[i]),
                        "close":     float(closes[i]),
                        "volume":    float(volumes[i] if i < len(volumes) else 0),
                    })
                except Exception:
                    continue

            log.debug(f"  {cur_start.date()}–{cur_end.date()}: "
                      f"{n} candles")

        except requests.HTTPError as e:
            log.warning(f"Dhan history HTTP {e.response.status_code}: "
                        f"{e.response.text[:200]}")
        except Exception as e:
            log.warning(f"Dhan history chunk error: {e}")

        cur_end = cur_start - timedelta(days=1)
        time.sleep(0.3)   # be polite

    if not all_rows:
        log.warning(
            f"Dhan history returned no data for {idx} {interval_str}m. "
            "Falling back to yfinance if installed."
        )
        return _yfinance_fallback(idx, interval_min, days)

    df = pd.DataFrame(all_rows).set_index("timestamp")
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="first")]
    df = df[["open", "high", "low", "close", "volume"]].astype(float)
    df = df.dropna()

    # Keep only last `days` calendar days
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    df = df[df.index >= cutoff]

    # Filter to market hours 09:15–15:30
    df = df.between_time("09:15", "15:30")
    df = df[df.index.dayofweek < 5]   # weekdays only

    log.info(
        f"✅ Dhan history: {len(df)} {interval_str}m candles | "
        f"{df.index[0].date() if not df.empty else '?'} → "
        f"{df.index[-1].date() if not df.empty else '?'}"
    )
    return df


# ════════════════════════════════════════════════════════════════════
# YFINANCE FALLBACK
# ════════════════════════════════════════════════════════════════════
def _yfinance_fallback(
    index:        str,
    interval_min: int,
    days:         int,
) -> pd.DataFrame:
    YF_TICKERS = {
        "NIFTY":      "^NSEI",
        "BANKNIFTY":  "^NSEBANK",
        "FINNIFTY":   "NIFTY_FIN_SERVICE.NS",
        "MIDCPNIFTY": "NIFTY_MIDCAP_100.NS",
    }
    ticker = YF_TICKERS.get(index)
    if not ticker:
        return pd.DataFrame()
    try:
        import yfinance as yf
        tf_map = {1: "1m", 5: "5m", 15: "15m", 25: "15m", 60: "60m"}
        yf_tf  = tf_map.get(interval_min, "5m")
        period = "5d" if days <= 5 else "10d" if days <= 10 else "30d"

        log.info(f"📚 yfinance fallback: {ticker} {yf_tf} {period}")
        df = yf.download(
            ticker, period=period, interval=yf_tf,
            auto_adjust=True, progress=False, multi_level_index=False
        )
        if df is None or df.empty:
            return pd.DataFrame()

        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        df.columns = [c.lower() for c in df.columns]
        if "volume" not in df.columns:
            df["volume"] = 1_000_000

        df = df[["open", "high", "low", "close", "volume"]].astype(float).dropna()
        df = df.between_time("09:15", "15:30")
        df = df[df.index.dayofweek < 5]
        log.info(f"✅ yfinance: {len(df)} candles")
        return df
    except ImportError:
        log.warning("yfinance not installed — pip install yfinance")
        return pd.DataFrame()
    except Exception as e:
        log.warning(f"yfinance fallback error: {e}")
        return pd.DataFrame()


# ════════════════════════════════════════════════════════════════════
# WARMUP HELPER
# ════════════════════════════════════════════════════════════════════
def warmup_engine(engine, index: str, tf_min: int, days: int = 5) -> int:
    """
    Load historical candles into the signal engine.
    Returns number of bars loaded.

    engine: SignalEngine instance from signal_engine.py
    """
    needed = getattr(engine, "_warmup_bars", 55)
    log.info(f"📚 Warming up engine — need {needed} bars for {index}")

    df = fetch_dhan_history(index=index, interval_min=tf_min, days=days)

    if df.empty:
        log.warning(f"⚠️  No history — cold start. "
                    f"Need {needed} live candles before signals fire.")
        return 0

    count = 0
    for ts, row in df.iterrows():
        try:
            engine.update(
                timestamp=ts,
                open_=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
            )
            count += 1
        except Exception:
            continue

    loaded = len(engine.df)
    short  = max(0, needed - loaded)
    if short > 0:
        log.warning(
            f"⚠️  Partial warmup: {loaded}/{needed} bars. "
            f"Need ~{short * tf_min} more minutes of live data."
        )
    else:
        log.info(f"✅ Warmup complete — {loaded} bars in engine")

    return count
