"""
╔══════════════════════════════════════════════════════════════════════════╗
║   DHAN — Automated Options Trader  v1.0  (CE + PE)                     ║
║   Migrated from Kotak Neo → Dhan API                                    ║
╠══════════════════════════════════════════════════════════════════════════╣
║  HOW IT WORKS                                                           ║
║  1. Auth via Dhan access token (set in .env)                            ║
║  2. Load 5 days history from Dhan API to warm up indicators             ║
║  3. Dhan WebSocket for live index LTP (primary)                         ║
║  4. REST poll fallback if WS is silent                                  ║
║  5. Build 5-min OHLC candles from ticks                                 ║
║  6. On candle close → run 8-confirmation signal engine                  ║
║  7. BUY CE signal → get security_id → place BUY order                  ║
║  8. BUY PE signal → get security_id → place BUY order                  ║
║  9. Place SL order immediately after entry                              ║
║ 10. TrailMonitor thread every 5s: trail, partial exit, EOD squareoff   ║
╠══════════════════════════════════════════════════════════════════════════╣
║  RUN                                                                    ║
║    python dhan_live.py              ← live mode                         ║
║    python dhan_live.py --paper      ← paper trade (no real orders)     ║
║    python dhan_live.py --debug      ← verbose per-candle output        ║
╚══════════════════════════════════════════════════════════════════════════╝

.env keys:
  DHAN_CLIENT_ID=...
  DHAN_ACCESS_TOKEN=...
  PAPER_TRADE=false          # true = simulate only
  INDEX=NIFTY
  LOT_SIZE=65
  STRIKE_STEP=50
  EXPIRY_WEEKDAY=TUE
  EXPIRY_TYPE=Weekly
  STRIKE_MODE=OTM1
  OPTION_TYPE=AUTO           # AUTO | CE Only | PE Only
  TIMEFRAME=5
  MAX_CAPITAL=10000
  MAX_DAILY_LOSS=3
  MAX_TRADES=3
  MAX_PREMIUM=450
  RISK_PCT=2.0
  MAX_LOTS=1
  MIN_CONFIRMATIONS=5
"""

import os, sys, time, logging, threading, argparse, warnings
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field

warnings.filterwarnings("ignore", category=FutureWarning)
from dotenv import load_dotenv
load_dotenv()

# ── CLI ──────────────────────────────────────────────────────────────
ap = argparse.ArgumentParser(description="Dhan Options Auto-Trader")
ap.add_argument("--paper", action="store_true", help="Paper trade — no real orders")
ap.add_argument("--debug", action="store_true", help="Verbose per-candle output")
args, _ = ap.parse_known_args()

if args.paper:
    os.environ["PAPER_TRADE"] = "true"

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG if args.debug else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("dhan_live.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("DhanLive")

for _lib in ("urllib3", "requests", "websocket", "yfinance", "peewee"):
    logging.getLogger(_lib).setLevel(logging.WARNING)

# ── Env helpers ──────────────────────────────────────────────────────
def _s(k, d=""): return os.getenv(k, str(d)).strip()
def _b(k, d="false"): return _s(k, d).lower() in ("true","1","yes")
def _i(k, d):
    try: return int(_s(k, d))
    except: return int(d)
def _f(k, d):
    try: return float(_s(k, d))
    except: return float(d)

# ── Config ───────────────────────────────────────────────────────────
PAPER = args.paper or _b("PAPER_TRADE", "true")

CFG = {
    "paper":          PAPER,
    "index":          _s("INDEX",           "NIFTY").upper(),
    "lot_size":       _i("LOT_SIZE",         65),
    "strike_step":    _i("STRIKE_STEP",      50),
    "expiry_weekday": _s("EXPIRY_WEEKDAY",   "TUE").upper(),
    "expiry_type":    _s("EXPIRY_TYPE",      "Weekly"),
    "strike_mode":    _s("STRIKE_MODE",      "OTM1"),
    "option_type":    _s("OPTION_TYPE",      "AUTO"),
    "timeframe":      _i("TIMEFRAME",         5),
    "capital":        _f("MAX_CAPITAL",       10000),
    "max_trades":     _i("MAX_TRADES",        3),
    "max_daily_loss": _f("MAX_DAILY_LOSS",    3),
    "max_premium":    _f("MAX_PREMIUM",       450),
    "risk_pct":       _f("RISK_PCT",          2.0),
    "max_lots":       _i("MAX_LOTS",          1),
    "min_conf":       _i("MIN_CONFIRMATIONS", 5),
    "warmup_bars":    55,
}

# ── Imports (after PAPER env is set) ─────────────────────────────────
from signal_engine import SignalEngine, EngineConfig, SignalResult
from broker_dhan   import (
    broker, get_security_id, LOT_SIZES, STRIKE_STEPS,
    BUY as DHAN_BUY, SELL as DHAN_SELL
)
from data_feed       import CandleBuilder, Candle, LiveFeed, LTPPoller, WSWatchdog
from historical_dhan import warmup_engine

# ── Signal engine ────────────────────────────────────────────────────
engine = SignalEngine(EngineConfig(
    fast_len=9,  mid_len=21,  slow_len=50,  trend_len=200,
    st_atr_len=10, st_factor=2.5,
    rsi_len=14,  rsi_ob=65,   rsi_os=32,
    use_vwap=True,
    vol_mult=1.2, vol_required=False,
    adx_len=14,  adx_thresh=22,
    bb_len=20,   bb_std=2.0,
    min_confirmations=CFG["min_conf"],
    sl_mode="ATR",    sl_atr_mult=1.2,  sl_pct=0.5,
    tgt_mode="ATR",   tgt_atr_mult=2.5, tgt_pct=1.5,
    use_trail=True,   trail_trig=0.8,   trail_step=0.4,
    capital=CFG["capital"],
    risk_pct=CFG["risk_pct"],
    lot_size=CFG["lot_size"],
    max_lots=CFG["max_lots"],
    option_type=CFG["option_type"],
))


# ════════════════════════════════════════════════════════════════════
# TRADING STATE
# ════════════════════════════════════════════════════════════════════
@dataclass
class Position:
    security_id:  str
    option:       str       # CE | PE
    entry_ltp:    float
    sl_prem:      float
    tgt_prem:     float
    lots:         int
    qty:          int
    entry_oid:    str
    sl_oid:       str
    peak_ltp:     float = 0.0
    pnl:          float = 0.0
    status:       str   = "OPEN"
    entry_time:   str   = ""
    exit_time:    str   = ""
    exit_ltp:     float = 0.0
    partial_done: bool  = False
    be_done:      bool  = False


@dataclass
class DayState:
    trades:    int   = 0
    pnl:       float = 0.0
    halted:    bool  = False
    positions: dict  = field(default_factory=dict)
    trade_log: list  = field(default_factory=list)
    candles:   int   = 0
    _today:    date  = field(default_factory=date.today)

    def reset_if_new_day(self):
        today = date.today()
        if self._today != today:
            log.info("🗓 New trading day — resetting counters")
            self.trades  = 0
            self.pnl     = 0.0
            self.halted  = False
            self.candles = 0
            self._today  = today

    def can_trade(self) -> tuple:
        self.reset_if_new_day()
        if self.halted:
            return False, "Trading HALTED"
        if self.trades >= CFG["max_trades"]:
            return False, f"Max {CFG['max_trades']} trades/day reached"
        loss_limit = CFG["capital"] * CFG["max_daily_loss"] / 100
        if self.pnl <= -loss_limit:
            self.halted = True
            return False, f"Daily loss ₹{loss_limit:.0f} hit — HALTED"
        return True, "OK"

    def open_positions(self) -> dict:
        return {s: p for s, p in self.positions.items() if p.status == "OPEN"}


state      = DayState()
state_lock = threading.Lock()


# ════════════════════════════════════════════════════════════════════
# LOT CALCULATOR
# ════════════════════════════════════════════════════════════════════
def calc_lots(opt_ltp: float) -> int:
    if opt_ltp <= 0:
        return CFG["max_lots"]
    try:
        funds  = broker.get_funds()
        margin = min(float(funds.get("available", CFG["capital"])), CFG["capital"])
    except Exception:
        margin = CFG["capital"]
    usable  = margin * 0.80
    per_lot = opt_ltp * CFG["lot_size"]
    if per_lot <= 0:
        return 1
    lots = max(1, int(usable / per_lot))
    lots = min(lots, CFG["max_lots"])
    log.info(
        f"Lot calc: margin=₹{margin:.0f}  usable=₹{usable:.0f}  "
        f"per_lot=₹{per_lot:.0f}  → {lots} lot(s)"
    )
    return lots


# ════════════════════════════════════════════════════════════════════
# SQUARE OFF HELPERS
# ════════════════════════════════════════════════════════════════════
def _get_option_ltp(security_id: str, fallback: float) -> float:
    """Get option LTP — WebSocket first, REST fallback."""
    ltp = live_feed.get_option_ltp(security_id)
    if ltp > 0:
        return ltp
    ltp = broker.get_option_ltp(security_id)
    return ltp if ltp > 0 else fallback


def square_off(security_id: str, reason: str = "MANUAL") -> dict:
    with state_lock:
        pos = state.positions.get(security_id)
    if not pos or pos.status != "OPEN":
        return {"error": f"No open position: {security_id}"}
    try:
        ltp = _get_option_ltp(security_id, pos.entry_ltp)
        oid = broker.place_order(
            security_id, pos.qty, transaction_type=DHAN_SELL
        )
        if pos.sl_oid and not CFG["paper"]:
            broker.cancel_order(pos.sl_oid)
        pnl = (ltp - pos.entry_ltp) * pos.qty
        with state_lock:
            pos.status    = reason
            pos.pnl       = round(pnl, 2)
            pos.exit_ltp  = ltp
            pos.exit_time = datetime.now().isoformat()
            state.pnl    += pnl
        emoji = "✅" if pnl >= 0 else "❌"
        log.info(
            f"{emoji} EXIT [{reason}] sid={security_id} "
            f"| Entry ₹{pos.entry_ltp:.1f} → Exit ₹{ltp:.1f} "
            f"| P&L ₹{pnl:.0f}"
        )
        return {"security_id": security_id, "status": reason,
                "pnl": round(pnl, 2), "oid": oid}
    except Exception as e:
        log.error(f"square_off failed ({security_id}): {e}")
        return {"error": str(e)}


def square_off_all(reason: str = "MANUAL") -> list:
    with state_lock:
        sids = list(state.open_positions().keys())
    results = [square_off(s, reason) for s in sids]
    if results:
        log.info(f"Square-off all ({reason}): {len(results)} position(s)")
    return results


# ════════════════════════════════════════════════════════════════════
# TRADE EXECUTOR
# ════════════════════════════════════════════════════════════════════
def _place_with_retry(security_id, qty, tx, order_type="MARKET",
                      trigger=0.0, retries=3) -> str:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return broker.place_order(
                security_id, qty, transaction_type=tx,
                order_type=order_type, trigger_price=trigger
            )
        except Exception as e:
            last_err = e
            if attempt < retries:
                log.warning(f"Order attempt {attempt} failed ({e}) — retry in 1s")
                time.sleep(1)
    raise RuntimeError(f"Order failed after {retries} attempts: {last_err}")


def execute_trade(result: SignalResult) -> bool:
    """
    Places BUY CE or BUY PE based on signal.
    Steps:
      1. Filter by option_type config
      2. Time gate 9:30–15:00
      3. Day state checks
      4. Get Dhan security_id for the option
      5. Get option LTP
      6. Apply premium cap
      7. Calculate lots
      8. Place BUY (entry) order
      9. Place SL-M order
     10. Track position
    """
    option = "CE" if result.signal == "BUY_CE" else "PE"

    # Option type filter
    ot = CFG["option_type"]
    if ot == "CE Only" and option != "CE":
        return False
    if ot == "PE Only" and option != "PE":
        return False

    # Time gate  9:30 – 15:00 IST (ORB needs 9:15–9:30 to form)
    now     = datetime.now()
    bar_min = now.hour * 60 + now.minute
    if bar_min < 9 * 60 + 30:
        log.info("⏰ Before 9:30 — signal skipped (ORB forming)")
        return False
    if bar_min >= 15 * 60:
        log.info("⏰ After 15:00 — signal skipped")
        return False

    # Day state checks
    with state_lock:
        ok, reason = state.can_trade()
    if not ok:
        log.warning(f"🚫 {reason}")
        return False

    # Get security_id from Dhan instrument master
    sid, expiry, strike = get_security_id(
        index       = CFG["index"],
        option_type = option,
        underlying  = result.close,
        strike_mode = CFG["strike_mode"],
        expiry_type = CFG["expiry_type"],
    )

    if not sid:
        log.error(
            f"❌ No security_id for {CFG['index']} {expiry} "
            f"{strike}{option} — check instrument master"
        )
        return False

    # Duplicate position check
    with state_lock:
        if sid in state.positions and state.positions[sid].status == "OPEN":
            log.info(f"Already in sid={sid} — skip")
            return False

    # Subscribe to live LTP for this option
    live_feed.add_option(sid)
    time.sleep(0.5)   # brief wait for WS subscription

    # Get option LTP
    opt_ltp = _get_option_ltp(sid, 0.0)
    if opt_ltp <= 0:
        if CFG["paper"]:
            opt_ltp = round(result.atr * 1.5, 1) if result.atr > 0 else 150.0
            log.info(f"[PAPER] Synthetic LTP ₹{opt_ltp}")
        else:
            log.error(f"❌ LTP=0 for sid={sid} — check market hours / symbol")
            return False

    # Premium cap
    if opt_ltp > CFG["max_premium"] and not CFG["paper"]:
        log.warning(f"Premium ₹{opt_ltp:.0f} > cap ₹{CFG['max_premium']:.0f} "
                    f"→ try OTM2")
        sid2, expiry2, strike2 = get_security_id(
            CFG["index"], option, result.close,
            strike_mode="OTM2", expiry_type=CFG["expiry_type"]
        )
        if sid2:
            live_feed.add_option(sid2)
            time.sleep(0.3)
            opt_ltp2 = _get_option_ltp(sid2, 0.0)
            if 0 < opt_ltp2 <= CFG["max_premium"]:
                sid, expiry, strike, opt_ltp = sid2, expiry2, strike2, opt_ltp2
            else:
                log.error(f"❌ OTM2 ₹{opt_ltp2:.0f} still > cap — skip")
                return False

    # Lot calculation
    lots = calc_lots(opt_ltp)
    qty  = lots * CFG["lot_size"]
    cost = opt_ltp * qty

    if cost > CFG["capital"] * 0.90 and not CFG["paper"]:
        log.error(f"❌ Cost ₹{cost:.0f} > 90% capital — skip")
        return False

    # SL and Target (ATR-based option premium %)
    atr_pct  = (result.atr / result.close) if result.close > 0 else 0.004
    sl_pct   = min(0.35, max(0.15, atr_pct * 1.5))
    sl_prem  = max(10.0, round(opt_ltp * (1.0 - sl_pct), 1))
    tgt_prem = round(opt_ltp * (1.0 + sl_pct * 2.5), 1)   # 2.5:1 RR

    mode = "PAPER" if CFG["paper"] else "LIVE"
    log.info(
        f"\n{'='*62}\n"
        f"  [{mode}] {'BUY CALL (CE)' if option=='CE' else 'BUY PUT  (PE)'}\n"
        f"  SecurityId : {sid}\n"
        f"  Expiry     : {expiry}  Strike: {strike}{option}\n"
        f"  LTP        : ₹{opt_ltp:.1f}  Qty: {qty} ({lots}L)  Cost: ₹{cost:.0f}\n"
        f"  SL         : ₹{sl_prem:.1f}  Target: ₹{tgt_prem:.1f}\n"
        f"  Spot       : ₹{result.close:.1f}  Signal: {result.signal}\n"
        f"  Confs      : [{result.conf_str()}] {result.confirmations}/8\n"
        f"  Reason     : {result.signal_reason}\n"
        f"{'='*62}"
    )

    # Place ENTRY (BUY) order
    try:
        entry_oid = _place_with_retry(sid, qty, tx=DHAN_BUY)
    except Exception as e:
        log.error(f"Entry order FAILED: {e}")
        return False

    # Place SL-M order
    sl_oid = ""
    try:
        sl_trigger = round(sl_prem * 0.995, 1)
        sl_oid     = _place_with_retry(
            sid, qty, tx=DHAN_SELL,
            order_type="STOP_LOSS_MARKET",
            trigger=sl_trigger,
        )
        log.info(f"🛑 SL-M: {sl_oid} @ trig ₹{sl_trigger:.1f}")
    except Exception as e:
        log.warning(f"SL order failed — TrailMonitor will watch: {e}")

    # Track position
    pos = Position(
        security_id=sid, option=option,
        entry_ltp=opt_ltp, sl_prem=sl_prem, tgt_prem=tgt_prem,
        lots=lots, qty=qty,
        entry_oid=entry_oid, sl_oid=sl_oid,
        peak_ltp=opt_ltp,
        entry_time=datetime.now().isoformat(),
    )

    with state_lock:
        state.positions[sid] = pos
        state.trades += 1
        state.trade_log.append({
            "time":       pos.entry_time,
            "sid":        sid,
            "expiry":     str(expiry),
            "strike":     f"{strike}{option}",
            "signal":     result.signal,
            "option":     option,
            "entry_ltp":  opt_ltp,
            "sl_prem":    sl_prem,
            "tgt_prem":   tgt_prem,
            "lots":       lots,
            "qty":        qty,
            "cost":       round(cost, 2),
            "entry_oid":  entry_oid,
            "sl_oid":     sl_oid,
            "mode":       mode,
            "spot":       result.close,
            "confs":      result.confirmations,
            "reason":     result.signal_reason,
        })

    log.info(
        f"🚀 [{mode}] TRADE OPEN: sid={sid} {strike}{option} | "
        f"{lots}L @ ₹{opt_ltp:.1f} | Cost ₹{cost:.0f} | "
        f"SL ₹{sl_prem:.1f} | Tgt ₹{tgt_prem:.1f}"
    )
    return True


# ════════════════════════════════════════════════════════════════════
# TRAILING SL MONITOR
# ════════════════════════════════════════════════════════════════════
class TrailMonitor(threading.Thread):
    def __init__(self, interval: int = 5):
        super().__init__(daemon=True, name="TrailMonitor")
        self.interval = interval

    def run(self):
        log.info(f"🔁 TrailMonitor started (every {self.interval}s)")
        while True:
            try:
                self._tick()
            except Exception as e:
                log.error(f"TrailMonitor: {e}")
            time.sleep(self.interval)

    def _tick(self):
        # EOD at 15:10
        now = datetime.now()
        if now.weekday() < 5 and now.hour == 15 and 10 <= now.minute <= 12:
            if state.open_positions():
                log.info("⏰ 15:10 EOD — squaring off all")
                square_off_all("EOD")
            return

        with state_lock:
            open_pos = list(state.open_positions().items())

        for sid, pos in open_pos:
            try:
                ltp = _get_option_ltp(sid, 0.0)
                if ltp <= 0:
                    continue
                self._manage(pos, sid, ltp)
            except Exception as e:
                log.warning(f"Position check ({sid}): {e}")

    def _manage(self, pos: Position, sid: str, ltp: float):
        # Track peak
        if ltp > pos.peak_ltp:
            pos.peak_ltp = ltp

        # SL hit
        if ltp <= pos.sl_prem:
            log.info(f"🛑 SL HIT: sid={sid} ₹{ltp:.1f} <= ₹{pos.sl_prem:.1f}")
            square_off(sid, "SL_HIT")
            return

        # Breakeven shift — at 1:1 RR, move SL to entry + 0.5%
        sl_range   = pos.entry_ltp - pos.sl_prem
        be_trigger = pos.entry_ltp + sl_range
        if not pos.be_done and ltp >= be_trigger:
            new_be = round(pos.entry_ltp * 1.005, 1)
            if new_be > pos.sl_prem:
                old_sl = pos.sl_prem
                pos.sl_prem = new_be
                pos.be_done = True
                log.info(f"🔒 BREAKEVEN sid={sid}: SL ₹{old_sl:.1f} → ₹{new_be:.1f}")
                if pos.sl_oid and not CFG["paper"]:
                    new_oid = broker.modify_order(
                        pos.sl_oid, sid, pos.qty,
                        new_trigger=round(new_be * 0.995, 1)
                    )
                    with state_lock:
                        pos.sl_oid = new_oid

        # Partial exit — at 2× entry, sell 50%
        if not pos.partial_done and ltp >= pos.entry_ltp * 2.0:
            half_qty = (pos.qty // 2) if pos.qty > 1 else 0
            if half_qty > 0:
                try:
                    _place_with_retry(sid, half_qty, tx=DHAN_SELL)
                    pos.qty -= half_qty
                    pos.partial_done = True
                    partial_pnl = (ltp - pos.entry_ltp) * half_qty
                    with state_lock:
                        state.pnl += partial_pnl
                    log.info(
                        f"💰 PARTIAL EXIT sid={sid}: {half_qty} @ ₹{ltp:.1f} "
                        f"P&L ₹{partial_pnl:.0f} | keeping {pos.qty}"
                    )
                except Exception as e:
                    log.warning(f"Partial exit failed: {e}")

        # Full target (2.5× entry)
        if ltp >= pos.tgt_prem:
            log.info(f"🎯 TARGET: sid={sid} ₹{ltp:.1f} >= ₹{pos.tgt_prem:.1f}")
            square_off(sid, "TGT_HIT")
            return

        # Trail — activate at +50%, trail 18% below peak
        if ltp >= pos.entry_ltp * 1.5:
            new_sl = round(pos.peak_ltp * 0.82, 1)
            if new_sl > pos.sl_prem:
                old_sl = pos.sl_prem
                pos.sl_prem = new_sl
                log.info(
                    f"📈 TRAIL SL sid={sid}: ₹{old_sl:.1f} → ₹{new_sl:.1f} "
                    f"(peak ₹{pos.peak_ltp:.1f})"
                )
                if pos.sl_oid and not CFG["paper"]:
                    new_oid = broker.modify_order(
                        pos.sl_oid, sid, pos.qty,
                        new_trigger=round(new_sl * 0.995, 1)
                    )
                    with state_lock:
                        pos.sl_oid = new_oid


# ════════════════════════════════════════════════════════════════════
# CANDLE CLOSE HANDLER
# ════════════════════════════════════════════════════════════════════
def on_candle_close(candle: Candle):
    result = engine.update(
        timestamp=candle.ts,
        open_=candle.open, high=candle.high,
        low=candle.low,    close=candle.close,
        volume=candle.volume,
    )
    with state_lock:
        state.candles += 1

    _print_dashboard(result, candle)

    if len(engine.df) < CFG["warmup_bars"]:
        return

    if result.signal in ("BUY_CE", "BUY_PE"):
        execute_trade(result)


# ════════════════════════════════════════════════════════════════════
# TERMINAL DASHBOARD
# ════════════════════════════════════════════════════════════════════
GRN = "\033[92m"; RED = "\033[91m"; YEL = "\033[93m"
GRY = "\033[90m"; BLU = "\033[94m"; RST = "\033[0m"


def _print_dashboard(result: SignalResult, candle: Candle):
    now   = datetime.now().strftime("%H:%M:%S")
    mode  = f"{YEL}[PAPER]{RST}" if CFG["paper"] else f"{RED}[LIVE]{RST}"
    bars  = len(engine.df)
    need  = CFG["warmup_bars"]

    if bars < need:
        filled = int(bars / need * 20)
        print(
            f"\r{mode} {now} {CFG['index']} ₹{candle.close:.0f}  "
            f"{YEL}WARMUP [{'█'*filled}{'░'*(20-filled)}] {bars}/{need}{RST}",
            end="", flush=True
        )
        return

    sig = result.signal
    sc  = GRN if sig == "BUY_CE" else RED if sig == "BUY_PE" else GRY

    with state_lock:
        n_open  = len(state.open_positions())
        day_pnl = state.pnl
        trades  = state.trades

    pc = GRN if day_pnl >= 0 else RED
    print(
        f"\r{mode} {now} {CFG['index']:10s}"
        f" ₹{candle.close:>9.1f}"
        f"  RSI:{result.rsi:>5.1f}"
        f"  ADX:{result.adx:>5.1f}"
        f"  ATR:{result.atr:>6.1f}"
        f"  [{result.conf_str()}]{BLU}{result.confirmations}/8{RST}"
        f"  {sc}{sig:<8}{RST}"
        f"  T:{trades}/{CFG['max_trades']}"
        f"  Pos:{n_open}"
        f"  {pc}P&L:₹{day_pnl:.0f}{RST}",
        end="", flush=True
    )
    if sig != "NONE":
        print()


# ════════════════════════════════════════════════════════════════════
# MARKET HOURS GATE
# ════════════════════════════════════════════════════════════════════
def wait_for_market():
    logged = False
    while True:
        try:
            now = datetime.now()
            if now.weekday() >= 5:
                if not logged:
                    log.info("Weekend — waiting for Monday 9:15")
                    logged = True
                time.sleep(60)
                continue

            mo = now.replace(hour=9, minute=15, second=0, microsecond=0)
            mc = now.replace(hour=15, minute=30, second=0, microsecond=0)

            if mo <= now <= mc:
                log.info("🟢 Market is open")
                return

            if now > mc:
                if not logged:
                    log.info("Market closed — waiting for tomorrow 9:15")
                    logged = True
                time.sleep(60)
                continue

            secs = (mo - now).total_seconds()
            if not logged or int(secs) % 300 < 30:
                log.info(f"⏰ Market opens in {int(secs/60)}m {int(secs%60)}s")
                logged = True
            time.sleep(min(30, max(1, secs - 5)))

        except KeyboardInterrupt:
            log.info("Ctrl+C — exiting")
            sys.exit(0)


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    mode_lbl = (f"{YEL}PAPER TRADE{RST}" if CFG["paper"]
                else f"{RED}LIVE TRADE — REAL MONEY{RST}")
    print("=" * 65)
    print(f"  Dhan Options Auto-Trader  v1.0")
    print(f"  Mode         : {mode_lbl}")
    print(f"  Index        : {CFG['index']}")
    print(f"  Lot Size     : {CFG['lot_size']}")
    print(f"  Strike Step  : {CFG['strike_step']}")
    print(f"  Expiry       : {CFG['expiry_type']} {CFG['expiry_weekday']}")
    print(f"  Strike Mode  : {CFG['strike_mode']}")
    print(f"  Option Type  : {CFG['option_type']}")
    print(f"  Timeframe    : {CFG['timeframe']} min")
    print(f"  Capital      : ₹{CFG['capital']:,.0f}")
    print(f"  Max Trades   : {CFG['max_trades']}/day")
    print(f"  Loss Limit   : {CFG['max_daily_loss']}% = "
          f"₹{CFG['capital']*CFG['max_daily_loss']/100:.0f}")
    print(f"  Max Premium  : ₹{CFG['max_premium']:.0f}/share")
    print(f"  Min Confs    : {CFG['min_conf']}/8")
    print(f"  Warmup Bars  : {CFG['warmup_bars']}")
    print(f"  Price Feed   : Dhan WebSocket (primary) + REST fallback")
    print("=" * 65)

    missing = [k for k in ("DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN")
               if not os.getenv(k)]
    if missing:
        log.error(f"Missing .env keys: {missing}")
        sys.exit(1)

    if not CFG["paper"]:
        print(f"\n  {RED}⚠️  LIVE MODE — REAL MONEY ORDERS WILL BE PLACED!{RST}")
        print("  Ctrl+C within 5s to abort...")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("  Aborted.")
            sys.exit(0)

    # 1. Validate Dhan session
    from broker_dhan import session as dhan_session
    log.info("🔑 Validating Dhan session...")
    if not CFG["paper"] and not dhan_session.ping():
        log.error("❌ Dhan API ping failed — check DHAN_ACCESS_TOKEN")
        sys.exit(1)
    log.info("✅ Dhan session OK")

    # 2. Pre-load instrument master (background)
    threading.Thread(
        target=lambda: __import__("broker_dhan").instruments._load(),
        daemon=True, name="InstrumentMasterLoader"
    ).start()

    # 3. Build candle builder
    candle_builder = CandleBuilder(CFG["timeframe"], on_candle_close)

    # 4. Create live feed and REST poller
    live_feed = LiveFeed(CFG["index"], candle_builder)
    ltp_poller = LTPPoller(
        index=CFG["index"],
        candle_builder=candle_builder,
        live_feed=live_feed,
        interval=1.0,
        ws_stale_secs=10.0,
    )

    # 5. Historical warmup
    warmup_engine(engine, CFG["index"], CFG["timeframe"], days=5)

    # 6. Wait for market
    wait_for_market()
    log.info("🟢 Market open — Dhan auto-trader starting")

    # 7. Start background threads
    TrailMonitor(interval=5).start()
    live_feed.start()
    ltp_poller.start()
    WSWatchdog(live_feed).start()

    log.info("✅ Running — Ctrl+C to stop and square off all")

    try:
        while True:
            time.sleep(30)

            # EOD backup (TrailMonitor is primary)
            now = datetime.now()
            if now.weekday() < 5 and now.hour == 15 and 10 <= now.minute <= 14:
                if state.open_positions():
                    log.info("⏰ 15:10 EOD backup — squaring off")
                    square_off_all("EOD")
                    time.sleep(120)

    except KeyboardInterrupt:
        log.info("\nCtrl+C — squaring off all...")
        square_off_all("MANUAL")
        print()
        print("  ── Session Summary ──────────────────────")
        print(f"  Trades   : {state.trades}")
        print(f"  P&L      : ₹{state.pnl:.2f}")
        print(f"  Candles  : {state.candles}")
        print(f"  Log      : dhan_live.log")
        print()
