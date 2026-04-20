"""
dhan_backtest.py
════════════════════════════════════════════════════════════════════
Backtest the signal engine on real Dhan historical data.
Works on ANY day — weekends, holidays, off-hours.
No broker login needed beyond DHAN_ACCESS_TOKEN.

Usage:
  python dhan_backtest.py                          # NIFTY 5-min, last 30 days
  python dhan_backtest.py --index BANKNIFTY        # BankNifty
  python dhan_backtest.py --days 60 --tf 5         # 60 days, 5-min
  python dhan_backtest.py --days 90 --tf 15        # 90 days, 15-min
  python dhan_backtest.py --csv saved_data.csv     # use saved CSV (no API)
  python dhan_backtest.py --save-csv               # fetch + save CSV for reuse
  python dhan_backtest.py --optimize               # grid-search best params

Output:
  - Terminal summary
  - dhan_backtest_report.html  (equity curve + monthly P&L + trade log)

Install:
  pip install dhanhq requests pandas numpy python-dotenv
  pip install yfinance   # optional fallback if Dhan returns no data
"""

import os, sys, json, argparse, time, logging, warnings
warnings.filterwarnings("ignore")

from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from typing import List, Optional
from itertools import product

import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from signal_engine import SignalEngine, EngineConfig, SignalResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("dhan_backtest.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("DhanBacktest")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# ══════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════
LOT_SIZES = {
    "NIFTY": 65, "BANKNIFTY": 30, "FINNIFTY": 65, "MIDCPNIFTY": 120
}

# Dhan security IDs for indices
INDEX_SECURITY_IDS = {
    "NIFTY":      "13",
    "BANKNIFTY":  "25",
    "FINNIFTY":   "27",
    "MIDCPNIFTY": "26",
}

# Dhan exchange segments for index historical data
INDEX_SEGMENTS = {
    "NIFTY":      "IDX_I",
    "BANKNIFTY":  "IDX_I",
    "FINNIFTY":   "IDX_I",
    "MIDCPNIFTY": "IDX_I",
}

DHAN_BASE = "https://api.dhan.co/v2"


# ══════════════════════════════════════════════════════════════════
# DHAN HISTORICAL DATA FETCHER
# ══════════════════════════════════════════════════════════════════
class DhanDataFetcher:
    """
    Fetches intraday OHLCV candles from Dhan API v2.

    Endpoint: POST /v2/charts/intraday
    - Supports 1, 5, 15, 25, 60 min intervals
    - Max 90-day window per call
    - No broker session needed — just access token
    """

    def __init__(self):
        self.client_id    = os.getenv("DHAN_CLIENT_ID", "")
        self.access_token = os.getenv("DHAN_ACCESS_TOKEN", "")

    @property
    def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Accept":       "application/json",
            "access-token": self.access_token,
        }

    def fetch(
        self,
        index:        str = "NIFTY",
        interval_min: int = 5,
        days:         int = 30,
    ) -> pd.DataFrame:
        """
        Fetch intraday OHLCV candles for `days` calendar days back.
        Returns clean DataFrame indexed by naive IST timestamp.
        Falls back to yfinance if Dhan returns empty.
        """
        idx = index.upper()
        sid = INDEX_SECURITY_IDS.get(idx)
        seg = INDEX_SEGMENTS.get(idx, "IDX_I")

        if not sid:
            log.error(f"Unknown index: {idx}")
            return pd.DataFrame()

        if not self.access_token:
            log.warning("DHAN_ACCESS_TOKEN not set — skipping Dhan API")
            return self._yfinance_fallback(idx, interval_min, days)

        # Dhan only supports these intervals
        interval_str = str(interval_min)
        if interval_str not in ("1", "5", "15", "25", "60"):
            log.warning(f"Unsupported interval {interval_min}m → using 5m")
            interval_str = "5"

        to_dt   = datetime.now()
        from_dt = to_dt - timedelta(days=min(days + 7, 89))

        all_rows = []
        chunk_days = 25   # conservative to avoid any edge cases
        cur_end = to_dt

        log.info(
            f"📥 Fetching {idx} {interval_str}m candles from Dhan "
            f"({days} days)..."
        )

        while cur_end > from_dt:
            cur_start = max(cur_end - timedelta(days=chunk_days), from_dt)

            payload = {
                "securityId":      sid,
                "exchangeSegment": seg,
                "instrument":      "INDEX",
                "interval":        interval_str,
                "oi":              False,
                "fromDate": cur_start.strftime("%Y-%m-%d 09:15:00"),
                "toDate":   cur_end.strftime("%Y-%m-%d 15:30:00"),
            }

            try:
                r = requests.post(
                    f"{DHAN_BASE}/charts/intraday",
                    headers=self._headers,
                    json=payload,
                    timeout=20,
                )

                if r.status_code == 429:
                    log.warning("Rate limit — waiting 3s...")
                    time.sleep(3)
                    continue

                if r.status_code == 401:
                    log.error(
                        "❌ Dhan API 401 Unauthorized — "
                        "check DHAN_ACCESS_TOKEN in .env"
                    )
                    break

                r.raise_for_status()
                data = r.json()

                opens      = data.get("open",      [])
                highs      = data.get("high",      [])
                lows       = data.get("low",       [])
                closes     = data.get("close",     [])
                volumes    = data.get("volume",    [])
                timestamps = data.get("timestamp", [])

                if not closes:
                    log.debug(
                        f"  No data: "
                        f"{cur_start.date()} → {cur_end.date()}"
                    )
                    cur_end = cur_start - timedelta(days=1)
                    continue

                n = min(len(closes), len(timestamps))
                chunk_rows = 0
                for i in range(n):
                    try:
                        # Dhan timestamps are Unix seconds
                        ts = pd.Timestamp(
                            timestamps[i], unit="s", tz="Asia/Kolkata"
                        ).tz_localize(None)  # naive local

                        all_rows.append({
                            "timestamp": ts,
                            "open":   float(opens[i]   if i < len(opens)   else closes[i]),
                            "high":   float(highs[i]   if i < len(highs)   else closes[i]),
                            "low":    float(lows[i]    if i < len(lows)    else closes[i]),
                            "close":  float(closes[i]),
                            "volume": float(volumes[i] if i < len(volumes) else 0),
                        })
                        chunk_rows += 1
                    except Exception:
                        continue

                log.info(
                    f"  {cur_start.date()} → {cur_end.date()}: "
                    f"{chunk_rows} candles"
                )

            except requests.HTTPError as e:
                log.warning(
                    f"HTTP {e.response.status_code} for chunk "
                    f"{cur_start.date()}–{cur_end.date()}: "
                    f"{e.response.text[:150]}"
                )
            except Exception as e:
                log.warning(f"Chunk error {cur_start.date()}: {e}")

            cur_end = cur_start - timedelta(days=1)
            time.sleep(0.3)

        if not all_rows:
            log.warning("Dhan API returned no data — trying yfinance fallback")
            return self._yfinance_fallback(idx, interval_min, days)

        df = self._clean(all_rows, days)
        log.info(
            f"✅ Total: {len(df)} candles | "
            f"{df.index[0].date()} → {df.index[-1].date()}"
        )
        return df

    def _clean(self, rows: list, days: int) -> pd.DataFrame:
        df = (pd.DataFrame(rows)
                .set_index("timestamp")
                .sort_index()
                .pipe(lambda d: d[~d.index.duplicated(keep="first")])
                [["open", "high", "low", "close", "volume"]]
                .astype(float)
                .dropna())

        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        df = df[df.index >= cutoff]
        df = df.between_time("09:15", "15:30")
        df = df[df.index.dayofweek < 5]
        return df

    # ── yfinance fallback ─────────────────────────────────────────────
    def _yfinance_fallback(
        self,
        index:        str,
        interval_min: int,
        days:         int,
    ) -> pd.DataFrame:
        YF = {
            "NIFTY":      "^NSEI",
            "BANKNIFTY":  "^NSEBANK",
            "FINNIFTY":   "NIFTY_FIN_SERVICE.NS",
            "MIDCPNIFTY": "NIFTY_MIDCAP_100.NS",
        }
        ticker = YF.get(index)
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
                auto_adjust=True, progress=False, multi_level_index=False,
            )
            if df is None or df.empty:
                return pd.DataFrame()

            if hasattr(df.index, "tz") and df.index.tz is not None:
                df.index = df.index.tz_localize(None)

            df.columns = [c.lower() for c in df.columns]
            if "volume" not in df.columns:
                df["volume"] = 1_000_000

            df = (df[["open", "high", "low", "close", "volume"]]
                    .astype(float).dropna()
                    .between_time("09:15", "15:30"))
            df = df[df.index.dayofweek < 5]
            log.info(f"✅ yfinance: {len(df)} candles")
            return df

        except ImportError:
            log.error("yfinance not installed — pip install yfinance")
            return pd.DataFrame()
        except Exception as e:
            log.error(f"yfinance error: {e}")
            return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════
# TRADE RECORD
# ══════════════════════════════════════════════════════════════════
@dataclass
class Trade:
    entry_time:   datetime
    exit_time:    datetime
    signal:       str
    entry_price:  float
    exit_price:   float
    sl:           float
    target:       float
    qty:          int
    lots:         int
    exit_reason:  str
    pnl:          float
    pnl_pct:      float
    rsi_at_entry: float
    adx_at_entry: float
    atr_at_entry: float
    conf_count:   int
    duration_min: int

    @property
    def is_winner(self) -> bool:
        return self.pnl > 0


# ══════════════════════════════════════════════════════════════════
# BACKTESTER
# ══════════════════════════════════════════════════════════════════
class Backtester:
    """
    Simulates the live trading logic on historical data.
    Uses same signal engine, same SL/target/trail logic.
    """

    def __init__(
        self,
        df:               pd.DataFrame,
        cfg:              EngineConfig,
        index:            str   = "NIFTY",
        max_trades_day:   int   = 3,
        initial_capital:  float = 100_000.0,
        max_daily_loss_pct: float = 3.0,
    ):
        self.df              = df
        self.cfg             = cfg
        self.index           = index
        self.max_tpd         = max_trades_day
        self.capital         = initial_capital
        self.max_loss_pct    = max_daily_loss_pct
        self.lot_size        = LOT_SIZES.get(index, 65)
        self.trades: List[Trade] = []
        self.equity_curve: List[tuple] = []

    def run(self) -> List[Trade]:
        log.info(f"🔄 Running backtest on {len(self.df)} bars...")
        engine = SignalEngine(self.cfg)

        cur_trade      = None
        daily_trades   = 0
        daily_pnl      = 0.0
        halted_today   = False
        last_date      = None
        running_cap    = self.capital
        max_loss_abs   = self.capital * self.max_loss_pct / 100

        for ts, row in self.df.iterrows():
            result = engine.update(
                timestamp=ts,
                open_=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
            )

            bar_date = ts.date()

            # ── Daily reset ───────────────────────────────────────────
            if bar_date != last_date:
                daily_trades = 0
                daily_pnl    = 0.0
                halted_today = False
                last_date    = bar_date

            px = float(row["close"])

            # ── Manage open trade ─────────────────────────────────────
            if cur_trade is not None:
                is_ce     = cur_trade["signal"] == "BUY_CE"
                exit_r    = None
                exit_px   = px

                # Target hit
                if is_ce and px >= cur_trade["target"]:
                    exit_r  = "TARGET"
                    exit_px = cur_trade["target"]
                elif not is_ce and px <= cur_trade["target"]:
                    exit_r  = "TARGET"
                    exit_px = cur_trade["target"]

                # SL / Trail SL hit
                elif is_ce and px <= cur_trade["trail_sl"]:
                    above_entry = cur_trade["trail_sl"] > cur_trade["sl"]
                    exit_r  = "TRAIL_SL" if above_entry else "SL"
                    exit_px = cur_trade["trail_sl"]
                elif not is_ce and px >= cur_trade["trail_sl"]:
                    below_entry = cur_trade["trail_sl"] < cur_trade["sl"]
                    exit_r  = "TRAIL_SL" if below_entry else "SL"
                    exit_px = cur_trade["trail_sl"]

                # EOD square-off at 15:10
                elif ts.hour == 15 and ts.minute >= 10:
                    exit_r  = "EOD"
                    exit_px = px

                # Update trailing SL
                else:
                    atr = result.atr if result.atr > 0 else cur_trade["atr"]
                    if is_ce:
                        trig = cur_trade["entry"] + atr * self.cfg.trail_trig
                        if px >= trig:
                            new_sl = px - atr * self.cfg.trail_step
                            cur_trade["trail_sl"] = max(
                                cur_trade["trail_sl"], new_sl
                            )
                    else:
                        trig = cur_trade["entry"] - atr * self.cfg.trail_trig
                        if px <= trig:
                            new_sl = px + atr * self.cfg.trail_step
                            cur_trade["trail_sl"] = min(
                                cur_trade["trail_sl"], new_sl
                            )

                # Close trade
                if exit_r:
                    raw_pnl = (
                        (exit_px - cur_trade["entry"]) * cur_trade["qty"]
                        if is_ce
                        else (cur_trade["entry"] - exit_px) * cur_trade["qty"]
                    )
                    pnl_pct       = raw_pnl / running_cap * 100
                    running_cap  += raw_pnl
                    daily_pnl    += raw_pnl
                    dur_min       = int(
                        (ts - cur_trade["entry_time"]).total_seconds() / 60
                    )

                    self.trades.append(Trade(
                        entry_time=cur_trade["entry_time"],
                        exit_time=ts,
                        signal=cur_trade["signal"],
                        entry_price=cur_trade["entry"],
                        exit_price=exit_px,
                        sl=cur_trade["sl"],
                        target=cur_trade["target"],
                        qty=cur_trade["qty"],
                        lots=cur_trade["lots"],
                        exit_reason=exit_r,
                        pnl=round(raw_pnl, 2),
                        pnl_pct=round(pnl_pct, 3),
                        rsi_at_entry=cur_trade["rsi"],
                        adx_at_entry=cur_trade["adx"],
                        atr_at_entry=cur_trade["atr"],
                        conf_count=cur_trade["conf"],
                        duration_min=dur_min,
                    ))
                    self.equity_curve.append((ts, round(running_cap, 2)))
                    cur_trade = None

                    # Check daily loss halt
                    if daily_pnl <= -max_loss_abs:
                        halted_today = True

            # ── New signal ────────────────────────────────────────────
            can_trade = (
                cur_trade is None
                and result.signal != "NONE"
                and daily_trades < self.max_tpd
                and not halted_today
                and result.atr > 0
                and ts.hour * 60 + ts.minute >= 9 * 60 + 30  # after 9:30
                and ts.hour * 60 + ts.minute < 15 * 60        # before 15:00
            )

            if can_trade:
                qty  = result.qty_lots * self.lot_size
                sl   = result.sl
                tgt  = result.target

                # Ensure valid SL/target direction
                is_ce = result.signal == "BUY_CE"
                if is_ce and sl >= px:
                    sl = px - result.atr * self.cfg.sl_atr_mult
                if not is_ce and sl <= px:
                    sl = px + result.atr * self.cfg.sl_atr_mult

                cur_trade = {
                    "signal":     result.signal,
                    "entry_time": ts,
                    "entry":      px,
                    "sl":         sl,
                    "target":     tgt,
                    "trail_sl":   sl,
                    "qty":        qty,
                    "lots":       result.qty_lots,
                    "atr":        result.atr,
                    "rsi":        result.rsi,
                    "adx":        result.adx,
                    "conf":       result.confirmations,
                }
                daily_trades += 1

        # Close any open trade at end of data
        if cur_trade is not None:
            px    = float(self.df["close"].iloc[-1])
            is_ce = cur_trade["signal"] == "BUY_CE"
            raw_pnl = (
                (px - cur_trade["entry"]) * cur_trade["qty"]
                if is_ce
                else (cur_trade["entry"] - px) * cur_trade["qty"]
            )
            running_cap += raw_pnl
            dur = int(
                (self.df.index[-1] - cur_trade["entry_time"]).total_seconds()
                / 60
            )
            self.trades.append(Trade(
                entry_time=cur_trade["entry_time"],
                exit_time=self.df.index[-1],
                signal=cur_trade["signal"],
                entry_price=cur_trade["entry"],
                exit_price=px,
                sl=cur_trade["sl"],
                target=cur_trade["target"],
                qty=cur_trade["qty"],
                lots=cur_trade["lots"],
                exit_reason="END_OF_DATA",
                pnl=round(raw_pnl, 2),
                pnl_pct=round(raw_pnl / self.capital * 100, 3),
                rsi_at_entry=cur_trade["rsi"],
                adx_at_entry=cur_trade["adx"],
                atr_at_entry=cur_trade["atr"],
                conf_count=cur_trade["conf"],
                duration_min=dur,
            ))

        log.info(f"✅ Backtest complete — {len(self.trades)} trades")
        return self.trades


# ══════════════════════════════════════════════════════════════════
# METRICS
# ══════════════════════════════════════════════════════════════════
def compute_metrics(trades: List[Trade], initial_capital: float) -> dict:
    if not trades:
        print("\n  ⚠️  No trades generated.")
        print("  Possible reasons:")
        print("  1. Not enough warmup bars (need ~55 bars = ~5 days of 5-min data)")
        print("  2. Signal conditions too strict — try --days 60 or more")
        print("  3. Time filter blocks signals (only fires 9:30–15:00)")
        sys.exit(0)

    winners = [t for t in trades if t.is_winner]
    losers  = [t for t in trades if not t.is_winner]
    pnls    = [t.pnl for t in trades]
    total   = sum(pnls)

    avg_win  = sum(t.pnl for t in winners) / max(len(winners), 1)
    avg_loss = sum(t.pnl for t in losers)  / max(len(losers),  1)
    rr       = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    # Drawdown
    equity, peak, max_dd = initial_capital, initial_capital, 0.0
    for t in trades:
        equity += t.pnl
        peak    = max(peak, equity)
        dd      = (peak - equity) / peak * 100
        max_dd  = max(max_dd, dd)

    # Profit factor
    gp = sum(t.pnl for t in winners)
    gl = abs(sum(t.pnl for t in losers))
    pf = gp / max(gl, 1)

    # Sharpe
    daily: dict = {}
    for t in trades:
        d = t.exit_time.date()
        daily[d] = daily.get(d, 0) + t.pnl
    dpnls  = list(daily.values())
    sharpe = (
        np.mean(dpnls) / np.std(dpnls) * np.sqrt(252)
        if len(dpnls) > 1 and np.std(dpnls) > 0
        else 0
    )

    exit_counts: dict = {}
    for t in trades:
        exit_counts[t.exit_reason] = exit_counts.get(t.exit_reason, 0) + 1

    ce_trades = [t for t in trades if t.signal == "BUY_CE"]
    pe_trades = [t for t in trades if t.signal == "BUY_PE"]

    # Weekly P&L
    weekly: dict = {}
    for t in trades:
        wk = t.exit_time.strftime("%Y-W%W")
        weekly[wk] = weekly.get(wk, 0) + t.pnl

    # Monthly P&L
    monthly: dict = {}
    for t in trades:
        mo = t.exit_time.strftime("%Y-%m")
        monthly[mo] = monthly.get(mo, 0) + t.pnl

    return {
        "total_trades":     len(trades),
        "winners":          len(winners),
        "losers":           len(losers),
        "win_rate":         round(len(winners) / len(trades) * 100, 1),
        "total_pnl":        round(total, 2),
        "total_pnl_pct":    round(total / initial_capital * 100, 2),
        "avg_win":          round(avg_win,  2),
        "avg_loss":         round(avg_loss, 2),
        "rr_ratio":         round(rr,   2),
        "profit_factor":    round(pf,   2),
        "max_drawdown_pct": round(max_dd, 2),
        "sharpe_ratio":     round(sharpe, 2),
        "best_trade":       round(max(pnls), 2),
        "worst_trade":      round(min(pnls), 2),
        "avg_duration_min": round(sum(t.duration_min for t in trades) / len(trades), 1),
        "exit_breakdown":   exit_counts,
        "ce_trades":        len(ce_trades),
        "pe_trades":        len(pe_trades),
        "ce_win_rate":      round(len([t for t in ce_trades if t.is_winner]) / max(len(ce_trades), 1) * 100, 1),
        "pe_win_rate":      round(len([t for t in pe_trades if t.is_winner]) / max(len(pe_trades), 1) * 100, 1),
        "final_capital":    round(initial_capital + total, 2),
        "monthly_pnl":      monthly,
        "weekly_pnl":       weekly,
        "trading_days":     len(set(t.exit_time.date() for t in trades)),
        "avg_trades_day":   round(len(trades) / max(len(set(t.exit_time.date() for t in trades)), 1), 2),
    }


# ══════════════════════════════════════════════════════════════════
# TERMINAL REPORT
# ══════════════════════════════════════════════════════════════════
def print_report(metrics: dict, trades: List[Trade]):
    S = "─" * 54
    W = lambda v: "\033[92m" + str(v) + "\033[0m" if v >= 0 else "\033[91m" + str(v) + "\033[0m"

    print(f"\n{'═'*54}")
    print(f"  DHAN BACKTEST RESULTS")
    print(f"{'═'*54}")
    print(f"  Total Trades      : {metrics['total_trades']}")
    print(f"  Winners           : {metrics['winners']}  ({metrics['win_rate']}%)")
    print(f"  Losers            : {metrics['losers']}")
    print(f"  Trading Days      : {metrics['trading_days']}")
    print(f"  Avg Trades/Day    : {metrics['avg_trades_day']}")
    print(S)
    print(f"  Total P&L         : ₹{metrics['total_pnl']:>10,.2f}  ({metrics['total_pnl_pct']}%)")
    print(f"  Final Capital     : ₹{metrics['final_capital']:>10,.2f}")
    print(f"  Max Drawdown      : {metrics['max_drawdown_pct']}%")
    print(f"  Profit Factor     : {metrics['profit_factor']}")
    print(f"  Sharpe Ratio      : {metrics['sharpe_ratio']}")
    print(S)
    print(f"  Avg Win           : ₹{metrics['avg_win']:>8,.2f}")
    print(f"  Avg Loss          : ₹{metrics['avg_loss']:>8,.2f}")
    print(f"  Risk:Reward       : {metrics['rr_ratio']}")
    print(f"  Avg Duration      : {metrics['avg_duration_min']} min")
    print(f"  Best Trade        : ₹{metrics['best_trade']:>8,.2f}")
    print(f"  Worst Trade       : ₹{metrics['worst_trade']:>8,.2f}")
    print(S)
    print(f"  CE trades         : {metrics['ce_trades']}  (Win: {metrics['ce_win_rate']}%)")
    print(f"  PE trades         : {metrics['pe_trades']}  (Win: {metrics['pe_win_rate']}%)")
    print(S)
    print(f"  Exit reasons:")
    for reason, count in sorted(
        metrics["exit_breakdown"].items(), key=lambda x: -x[1]
    ):
        bar = "█" * min(count, 20)
        print(f"    {reason:<16}: {count:>3}  {bar}")

    # Monthly P&L table
    if metrics["monthly_pnl"]:
        print(S)
        print(f"  Monthly P&L:")
        for mo, pnl in sorted(metrics["monthly_pnl"].items()):
            sign  = "+" if pnl >= 0 else ""
            bar   = "█" * min(int(abs(pnl) / 500), 15)
            color = "\033[92m" if pnl >= 0 else "\033[91m"
            rst   = "\033[0m"
            print(f"    {mo}  {color}{sign}₹{pnl:>8,.0f}  {bar}{rst}")

    print(f"{'═'*54}\n")

    # Last 15 trades
    print(f"  Last {min(15, len(trades))} trades:")
    print(f"  {'Date':<12} {'Sig':<7} {'Entry':>7} {'Exit':>7} "
          f"{'P&L':>9} {'Dur':>5} {'Reason'}")
    print(f"  {'-'*12} {'-'*7} {'-'*7} {'-'*7} "
          f"{'-'*9} {'-'*5} {'-'*12}")
    for t in trades[-15:]:
        sign  = "+" if t.pnl >= 0 else ""
        color = "\033[92m" if t.is_winner else "\033[91m"
        rst   = "\033[0m"
        print(
            f"  {str(t.entry_time.date()):<12} {t.signal:<7} "
            f"{t.entry_price:>7.0f} {t.exit_price:>7.0f} "
            f"{color}{sign}₹{t.pnl:>7,.0f}{rst}  "
            f"{t.duration_min:>4}m  {t.exit_reason}"
        )
    print()


# ══════════════════════════════════════════════════════════════════
# HTML REPORT
# ══════════════════════════════════════════════════════════════════
def generate_html_report(
    trades:       List[Trade],
    metrics:      dict,
    equity_curve: list,
    cfg:          EngineConfig,
    index:        str,
    tf_min:       int,
    days:         int,
    output_path:  str = "dhan_backtest_report.html",
):
    # Equity curve — sample to ~200 points
    step = max(1, len(equity_curve) // 200)
    eq_labels = [str(ts.date()) for ts, _ in equity_curve[::step]]
    eq_values = [v for _, v in equity_curve[::step]]

    # Monthly chart
    monthly = metrics["monthly_pnl"]
    mo_labels = sorted(monthly.keys())
    mo_values = [round(monthly[k], 2) for k in mo_labels]
    mo_colors = ["'#1D9E75'" if v >= 0 else "'#E24B4A'" for v in mo_values]

    # Trade rows
    rows_html = ""
    for i, t in enumerate(reversed(trades), 1):
        bg    = "#162a1c" if t.is_winner else "#2a1616"
        sign  = "+" if t.pnl >= 0 else ""
        color = "#4ade80" if t.is_winner else "#f87171"
        sig_c = "ce" if t.signal == "BUY_CE" else "pe"
        rows_html += f"""
        <tr style="background:{bg}">
            <td>{len(trades)-i+1}</td>
            <td>{t.entry_time.strftime('%d-%b %H:%M')}</td>
            <td>{t.exit_time.strftime('%d-%b %H:%M')}</td>
            <td><span class="badge {sig_c}">{t.signal}</span></td>
            <td>{t.entry_price:.0f}</td>
            <td>{t.exit_price:.0f}</td>
            <td>{t.sl:.0f}</td>
            <td>{t.target:.0f}</td>
            <td>{t.lots}</td>
            <td style="color:{color}">{sign}₹{t.pnl:,.0f}</td>
            <td>{t.exit_reason}</td>
            <td>{t.conf_count}/8</td>
            <td>{t.rsi_at_entry:.1f}</td>
            <td>{t.adx_at_entry:.1f}</td>
            <td>{t.duration_min}m</td>
        </tr>"""

    # Determine P&L color
    pnl_color = "#4ade80" if metrics["total_pnl"] >= 0 else "#f87171"
    wr_color  = "#4ade80" if metrics["win_rate"] >= 50 else "#f87171"
    pf_color  = "#4ade80" if metrics["profit_factor"] >= 1.5 else "#fb923c"
    sh_color  = "#4ade80" if metrics["sharpe_ratio"] >= 1 else "#fb923c"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Dhan Backtest — {index} {tf_min}m</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ background:#0d1117; color:#e2e8f0; font-family:'Segoe UI',sans-serif; padding:24px; }}
h1 {{ color:#f97316; font-size:22px; margin-bottom:4px; }}
h2 {{ color:#64748b; font-size:13px; font-weight:400; margin-bottom:20px; }}
h3 {{ color:#94a3b8; font-size:14px; margin:20px 0 10px; }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin-bottom:20px; }}
.card {{ background:#161b22; border:1px solid #21262d; border-radius:10px; padding:14px; }}
.card .lbl {{ font-size:10px; color:#64748b; text-transform:uppercase; letter-spacing:.5px; margin-bottom:5px; }}
.card .val {{ font-size:20px; font-weight:700; }}
.chart-box {{ background:#161b22; border:1px solid #21262d; border-radius:10px; padding:16px; margin-bottom:20px; }}
table {{ width:100%; border-collapse:collapse; font-size:11.5px; }}
th {{ background:#161b22; color:#94a3b8; padding:8px 6px; text-align:left; border-bottom:1px solid #21262d; position:sticky; top:0; }}
td {{ padding:6px 6px; border-bottom:1px solid #0d1117; }}
.badge {{ padding:2px 6px; border-radius:4px; font-size:10px; font-weight:600; }}
.badge.ce {{ background:#052e16; color:#4ade80; }}
.badge.pe {{ background:#450a0a; color:#f87171; }}
.tbl-box {{ background:#161b22; border:1px solid #21262d; border-radius:10px; overflow:auto; max-height:480px; }}
.params {{ background:#161b22; border:1px solid #21262d; border-radius:10px; padding:14px; margin-top:20px; font-size:11px; color:#64748b; line-height:2; }}
</style>
</head>
<body>
<h1>Dhan Backtest — {index} {tf_min}-min | {days} days</h1>
<h2>Generated: {datetime.now().strftime('%d %b %Y %H:%M')} | Capital: ₹{cfg.capital:,.0f} | Data: Dhan API</h2>

<div class="grid">
  <div class="card"><div class="lbl">Total P&L</div>
    <div class="val" style="color:{pnl_color}">₹{metrics['total_pnl']:,.0f}</div></div>
  <div class="card"><div class="lbl">Win Rate</div>
    <div class="val" style="color:{wr_color}">{metrics['win_rate']}%</div></div>
  <div class="card"><div class="lbl">Profit Factor</div>
    <div class="val" style="color:{pf_color}">{metrics['profit_factor']}</div></div>
  <div class="card"><div class="lbl">Max Drawdown</div>
    <div class="val" style="color:#f87171">{metrics['max_drawdown_pct']}%</div></div>
  <div class="card"><div class="lbl">Sharpe Ratio</div>
    <div class="val" style="color:{sh_color}">{metrics['sharpe_ratio']}</div></div>
  <div class="card"><div class="lbl">Risk:Reward</div>
    <div class="val" style="color:#fb923c">{metrics['rr_ratio']}</div></div>
  <div class="card"><div class="lbl">Total Trades</div>
    <div class="val" style="color:#e2e8f0">{metrics['total_trades']}</div></div>
  <div class="card"><div class="lbl">Final Capital</div>
    <div class="val" style="color:#e2e8f0">₹{metrics['final_capital']:,.0f}</div></div>
  <div class="card"><div class="lbl">Avg Duration</div>
    <div class="val" style="color:#e2e8f0">{metrics['avg_duration_min']}m</div></div>
  <div class="card"><div class="lbl">CE Win Rate</div>
    <div class="val" style="color:#4ade80">{metrics['ce_win_rate']}%</div></div>
  <div class="card"><div class="lbl">PE Win Rate</div>
    <div class="val" style="color:#f87171">{metrics['pe_win_rate']}%</div></div>
  <div class="card"><div class="lbl">Avg Trades/Day</div>
    <div class="val" style="color:#e2e8f0">{metrics['avg_trades_day']}</div></div>
</div>

<div class="chart-box">
  <h3>Equity Curve</h3>
  <div style="height:260px"><canvas id="eqChart"></canvas></div>
</div>

<div class="chart-box">
  <h3>Monthly P&L</h3>
  <div style="height:200px"><canvas id="moChart"></canvas></div>
</div>

<h3>All Trades ({metrics['total_trades']})</h3>
<div class="tbl-box">
<table>
  <thead><tr>
    <th>#</th><th>Entry</th><th>Exit</th><th>Signal</th>
    <th>Entry</th><th>Exit</th><th>SL</th><th>Target</th>
    <th>Lots</th><th>P&L</th><th>Exit Reason</th>
    <th>Confs</th><th>RSI</th><th>ADX</th><th>Dur</th>
  </tr></thead>
  <tbody>{rows_html}</tbody>
</table>
</div>

<div class="params">
  <b style="color:#94a3b8">Strategy Parameters:</b><br>
  Index: {index} | TF: {tf_min}m | Days: {days} |
  EMA: {cfg.fast_len}/{cfg.mid_len}/{cfg.slow_len}/{cfg.trend_len} |
  ST: {cfg.st_factor}×{cfg.st_atr_len} |
  RSI: {cfg.rsi_len} ({cfg.rsi_os}–{cfg.rsi_ob}) |
  ADX: {cfg.adx_thresh} |
  Min Confs: {cfg.min_confirmations} |
  SL: ATR {cfg.sl_atr_mult}× |
  TGT: ATR {cfg.tgt_atr_mult}× |
  Trail: trig={cfg.trail_trig}×ATR step={cfg.trail_step}×ATR |
  Capital: ₹{cfg.capital:,.0f} | Risk: {cfg.risk_pct}%
</div>

<script>
const EQ_L = {json.dumps(eq_labels)};
const EQ_V = {json.dumps(eq_values)};
const MO_L = {json.dumps(mo_labels)};
const MO_V = {json.dumps(mo_values)};
const MO_C = [{','.join(mo_colors)}];

const chartDefaults = {{
  responsive: true,
  maintainAspectRatio: false,
  plugins: {{ legend: {{ display: false }} }},
  scales: {{
    x: {{ ticks: {{ color:'#64748b', maxTicksLimit:10 }}, grid: {{ color:'#161b22' }} }},
    y: {{ ticks: {{ color:'#64748b' }}, grid: {{ color:'#161b22' }} }},
  }}
}};

new Chart(document.getElementById('eqChart'), {{
  type: 'line',
  data: {{ labels: EQ_L, datasets: [{{
    data: EQ_V,
    borderColor: '#f97316',
    backgroundColor: 'rgba(249,115,22,0.07)',
    borderWidth: 2, pointRadius: 0, fill: true, tension: 0.3
  }}]}},
  options: chartDefaults,
}});

new Chart(document.getElementById('moChart'), {{
  type: 'bar',
  data: {{ labels: MO_L, datasets: [{{
    data: MO_V,
    backgroundColor: MO_C,
    borderRadius: 4,
  }}]}},
  options: chartDefaults,
}});
</script>
</body></html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    log.info(f"📊 HTML report saved: {output_path}")


# ══════════════════════════════════════════════════════════════════
# OPTIMIZER  (grid search)
# ══════════════════════════════════════════════════════════════════
def run_optimize(df: pd.DataFrame, index: str, capital: float):
    print("\n🔍 Running parameter optimization (grid search)...\n")

    grid = {
        "st_factor":        [2.5, 3.0, 3.5],
        "sl_atr_mult":      [1.0, 1.2, 1.5],
        "tgt_atr_mult":     [2.0, 2.5, 3.0],
        "adx_thresh":       [18, 22, 26],
        "min_confirmations":[4, 5, 6],
    }

    keys   = list(grid.keys())
    combos = list(product(*grid.values()))
    total  = len(combos)
    print(f"  Testing {total} combinations...")

    results = []
    best_score, best_cfg, best_met = -999, None, None

    for i, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        cfg    = EngineConfig(
            capital  = capital,
            lot_size = LOT_SIZES.get(index, 65),
            **params,
        )
        bt   = Backtester(df, cfg, index=index, initial_capital=capital)
        trds = bt.run()
        if len(trds) < 5:
            continue
        met   = compute_metrics(trds, capital)
        score = (met["profit_factor"]
                 * (met["win_rate"] / 100)
                 * (1 - met["max_drawdown_pct"] / 100))

        results.append({
            **params,
            "trades":   len(trds),
            "win_rate": met["win_rate"],
            "pf":       met["profit_factor"],
            "dd":       met["max_drawdown_pct"],
            "pnl":      met["total_pnl"],
            "score":    round(score, 4),
        })

        if score > best_score:
            best_score = score
            best_cfg   = cfg
            best_met   = met

        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{total} | Best score: {best_score:.3f}",
                  end="\r")

    print()
    rdf = pd.DataFrame(results).sort_values("score", ascending=False)
    print("\n  Top 10 parameter combinations:")
    print(rdf.head(10).to_string(index=False))
    print()

    if best_met:
        print_report(best_met, [])
        print(f"\n  Best parameters:")
        for k in keys:
            print(f"    {k} = {getattr(best_cfg, k)}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    ap = argparse.ArgumentParser(
        description="Dhan Backtest — real NSE historical data"
    )
    ap.add_argument("--index",    default="NIFTY",
                    choices=["NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY"])
    ap.add_argument("--tf",       type=int,   default=5,
                    help="Timeframe minutes (1/5/15/25/60)")
    ap.add_argument("--days",     type=int,   default=30,
                    help="Calendar days of history to fetch (max 89)")
    ap.add_argument("--capital",  type=float, default=100_00.0)
    ap.add_argument("--max-tpd",  type=int,   default=3,
                    help="Max trades per day")
    ap.add_argument("--max-loss", type=float, default=3.0,
                    help="Max daily loss pct before halting")
    ap.add_argument("--csv",      default="",
                    help="Use saved CSV instead of fetching from Dhan")
    ap.add_argument("--save-csv", action="store_true",
                    help="Fetch from Dhan and save CSV for reuse")
    ap.add_argument("--optimize", action="store_true",
                    help="Run parameter grid search")
    ap.add_argument("--output",   default="dhan_backtest_report.html")
    args = ap.parse_args()

    print("=" * 58)
    print("  Dhan Backtest Engine")
    print(f"  Index     : {args.index}")
    print(f"  Timeframe : {args.tf} min")
    print(f"  Days      : {args.days}")
    print(f"  Capital   : ₹{args.capital:,.0f}")
    print(f"  Max TPD   : {args.max_tpd}")
    print(f"  Data      : {'CSV — ' + args.csv if args.csv else 'Dhan API'}")
    print("=" * 58)

    # ── Load data ─────────────────────────────────────────────────
    df = pd.DataFrame()

    if args.csv:
        # Load from saved CSV
        log.info(f"📂 Loading CSV: {args.csv}")
        df = pd.read_csv(args.csv)
        for col in ["timestamp", "datetime", "date", "Datetime", "Date"]:
            if col in df.columns:
                df["timestamp"] = pd.to_datetime(df[col])
                break
        else:
            df["timestamp"] = pd.to_datetime(df.iloc[:, 0])
        df.columns = [c.lower().strip() for c in df.columns]
        df = df.rename(columns={
            "o":"open","h":"high","l":"low","c":"close","v":"volume"
        })
        df = df.set_index("timestamp").sort_index()
        for c in ["open","high","low","close"]:
            if c not in df.columns:
                raise ValueError(f"Column '{c}' missing from CSV")
        if "volume" not in df.columns:
            df["volume"] = 1_000_000
        df = df[["open","high","low","close","volume"]].astype(float).dropna()
        log.info(f"✅ Loaded {len(df)} rows from CSV")

    else:
        # Fetch from Dhan API
        fetcher = DhanDataFetcher()
        df = fetcher.fetch(
            index=args.index,
            interval_min=args.tf,
            days=min(args.days, 89),
        )

        if df.empty:
            log.error(
                "❌ No data returned.\n"
                "  Options:\n"
                "  1. Check DHAN_ACCESS_TOKEN in .env\n"
                "  2. pip install yfinance  (for fallback)\n"
                "  3. Provide CSV:  --csv your_file.csv"
            )
            sys.exit(1)

        if args.save_csv:
            fname = f"{args.index}_{args.tf}min_{args.days}d.csv"
            df.to_csv(fname)
            print(f"\n  ✅ CSV saved: {fname}")
            print(f"  Next time run:  python dhan_backtest.py --csv {fname}\n")

    if df.empty:
        log.error("DataFrame is empty — cannot run backtest")
        sys.exit(1)

    print(f"\n  Data ready: {len(df)} candles | "
          f"{df.index[0].date()} → {df.index[-1].date()}\n")

    # ── Config ────────────────────────────────────────────────────
    cfg = EngineConfig(
        capital           = args.capital,
        lot_size          = LOT_SIZES.get(args.index, 65),
        risk_pct          = 2.0,
        max_lots          = 1,
        # Signal
        fast_len          = 9,
        mid_len           = 21,
        slow_len          = 50,
        trend_len         = 200,
        st_atr_len        = 10,
        st_factor         = 2.5,
        rsi_len           = 14,
        rsi_ob            = 65,
        rsi_os            = 32,
        adx_thresh        = 22,
        vol_mult          = 1.2,
        vol_required      = False,
        bb_len            = 20,
        bb_std            = 2.0,
        min_confirmations = 5,
        # SL/Target/Trail
        sl_mode           = "ATR",
        sl_atr_mult       = 1.2,
        sl_pct            = 0.5,
        tgt_mode          = "ATR",
        tgt_atr_mult      = 2.5,
        tgt_pct           = 1.5,
        use_trail         = True,
        trail_trig        = 0.8,
        trail_step        = 0.4,
    )

    # ── Optimize or single run ────────────────────────────────────
    if args.optimize:
        run_optimize(df, args.index, args.capital)
        return

    bt      = Backtester(
        df,
        cfg,
        index          = args.index,
        max_trades_day = args.max_tpd,
        initial_capital= args.capital,
        max_daily_loss_pct = args.max_loss,
    )
    trades  = bt.run()
    metrics = compute_metrics(trades, args.capital)

    print_report(metrics, trades)

    generate_html_report(
        trades, metrics, bt.equity_curve, cfg,
        index=args.index, tf_min=args.tf, days=args.days,
        output_path=args.output,
    )

    print(f"  📊 HTML report: {args.output}")
    print(f"  Open in browser to see equity curve + full trade log\n")


if __name__ == "__main__":
    main()
