"""
signal_engine.py  v3.0  — ORB + VWAP + EMA (Proven Strategy)

Strategy: Opening Range Breakout anchored to 9:15–9:30 range,
confirmed by VWAP side, EMA alignment, RSI momentum, ADX trend,
Supertrend and candle body filter.

SEBI 2025 compliant: Nifty lot=65, weekly expiry Tuesday.
Pure pandas/numpy — no ta-lib, no external dependencies.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Literal
from datetime import datetime


# ─────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────
@dataclass
class EngineConfig:
    # EMA lengths
    fast_len:          int   = 9
    mid_len:           int   = 21
    slow_len:          int   = 50
    trend_len:         int   = 200
    # Supertrend
    st_atr_len:        int   = 10
    st_factor:         float = 2.5
    # RSI
    rsi_len:           int   = 14
    rsi_ob:            int   = 65
    rsi_os:            int   = 32
    # VWAP
    use_vwap:          bool  = True
    # Volume
    vol_mult:          float = 1.2
    vol_required:      bool  = False   # FIX: default False for index spot
    # ADX
    adx_len:           int   = 14
    adx_thresh:        int   = 22
    # Bollinger Bands
    bb_len:            int   = 20
    bb_std:            float = 2.0
    # Signal gate
    min_confirmations: int   = 6
    # SL
    sl_mode:           str   = "ATR"
    sl_atr_mult:       float = 1.2
    sl_pct:            float = 0.5
    # Target
    tgt_mode:          str   = "ATR"
    tgt_pct:           float = 1.5
    tgt_atr_mult:      float = 2.5
    # Trail
    use_trail:         bool  = True
    trail_trig:        float = 0.8
    trail_step:        float = 0.4
    # Sizing
    capital:           float = 10000.0
    risk_pct:          float = 2.0
    lot_size:          int   = 65
    max_lots:          int   = 1
    # Filter
    option_type:       str   = "AUTO"
    # Premium filter
    min_premium:       float = 30.0
    max_premium:       float = 450.0
    # Time filter  (SEBI: avoid opening 15-min chop)
    trade_start:       str   = "09:30"   # was 09:25; ORB needs 9:15-9:30 to form
    trade_end:         str   = "14:45"
    # ORB settings (Opening Range Breakout)
    orb_bars:          int   = 3         # bars that form the OR (3×5min = 9:15-9:30)
    orb_enabled:       bool  = True      # require ORB break before entry
    # OI (future use)
    use_oi:            bool  = False
    # Hard target vs trailing
    use_hard_target:   bool  = False


# ─────────────────────────────────────────────────────────────────────────
# SIGNAL RESULT
# ─────────────────────────────────────────────────────────────────────────
@dataclass
class SignalResult:
    signal:          Literal["BUY_CE", "BUY_PE", "NONE"]
    timestamp:       datetime
    close:           float
    sl:              float
    target:          float
    atr:             float
    rsi:             float
    adx:             float
    vwap:            float
    qty_lots:        int
    risk_amt:        float
    trail_sl:        float = 0.0
    signal_reason:   str   = ""
    bars_loaded:     int   = 0
    bars_needed:     int   = 50     # FIX: show 50-bar warmup progress, not 200
    conf_ema_cross:  bool  = False
    conf_trend:      bool  = False
    conf_supertrend: bool  = False
    conf_vwap:       bool  = False
    conf_rsi:        bool  = False
    conf_adx:        bool  = False
    conf_volume:     bool  = False
    conf_no_squeeze: bool  = False

    @property
    def confirmations(self) -> int:
        return sum([
            self.conf_ema_cross, self.conf_trend, self.conf_supertrend,
            self.conf_vwap,      self.conf_rsi,   self.conf_adx,
            self.conf_volume,    self.conf_no_squeeze,
        ])

    def conf_str(self) -> str:
        return "".join([
            "E" if self.conf_ema_cross  else "·",
            "T" if self.conf_trend      else "·",
            "S" if self.conf_supertrend else "·",
            "V" if self.conf_vwap       else "·",
            "R" if self.conf_rsi        else "·",
            "A" if self.conf_adx        else "·",
            "L" if self.conf_volume     else "·",
            "B" if self.conf_no_squeeze else "·",
        ])

    @property
    def warming_up(self) -> bool:
        # FIX: warm up based on slow_len (50) not trend_len (200)
        return self.bars_loaded < self.bars_needed


# ─────────────────────────────────────────────────────────────────────────
# INDICATOR FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────
def _ema(s, n):  return s.ewm(span=n, adjust=False).mean()
def _sma(s, n):  return s.rolling(n).mean()
def _rma(s, n):  return s.ewm(alpha=1/n, adjust=False).mean()

def _atr(h, l, c, n):
    pc = c.shift(1)
    tr = pd.concat([(h-l), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return _rma(tr, n)

def _rsi(c, n):
    d  = c.diff()
    up = _rma(d.clip(lower=0), n)
    dn = _rma((-d).clip(lower=0), n)
    rs = up / dn.replace(0, np.nan)
    return (100 - 100/(1+rs)).fillna(50)

def _adx(h, l, c, n):
    atr = _atr(h, l, c, n)
    up  = h.diff(); dn = -l.diff()
    p   = pd.Series(np.where((up>dn)&(up>0), up, 0.0), index=h.index)
    m   = pd.Series(np.where((dn>up)&(dn>0), dn, 0.0), index=h.index)
    dip = 100*_rma(p,n)/atr.replace(0,np.nan)
    din = 100*_rma(m,n)/atr.replace(0,np.nan)
    dx  = 100*(dip-din).abs()/(dip+din).replace(0,np.nan)
    return _rma(dx.fillna(0), n).fillna(0)

def _bbands(c, n, k):
    mid = _sma(c, n)
    std = c.rolling(n).std(ddof=0)
    return mid, mid+k*std, mid-k*std

def _supertrend(h, l, c, n, mult):
    atr  = _atr(h, l, c, n)
    hl2  = (h+l)/2
    bu   = (hl2 + mult*atr).values.copy()
    bl   = (hl2 - mult*atr).values.copy()
    cl   = c.values
    sz   = len(cl)
    st   = np.full(sz, np.nan)
    dr   = np.zeros(sz, dtype=int)
    for i in range(1, sz):
        if np.isnan(atr.iloc[i]): continue
        bl[i] = bl[i] if (bl[i]>bl[i-1] or cl[i-1]<bl[i-1]) else bl[i-1]
        bu[i] = bu[i] if (bu[i]<bu[i-1] or cl[i-1]>bu[i-1]) else bu[i-1]
        prev  = st[i-1] if not np.isnan(st[i-1]) else bu[i]
        if prev == bu[i-1]:
            if cl[i]<=bu[i]: st[i]=bu[i]; dr[i]=1
            else:            st[i]=bl[i]; dr[i]=-1
        else:
            if cl[i]>=bl[i]: st[i]=bl[i]; dr[i]=-1
            else:             st[i]=bu[i]; dr[i]=1
    return pd.Series(st, index=c.index), pd.Series(dr, index=c.index)

def _vwap(h, l, c, v):
    hlc3 = (h + l + c) / 3
    # FIX: h.index.date crashes if index is not DatetimeIndex.
    # Use normalize() (strips time → date-only Timestamp) which works on
    # DatetimeIndex regardless of tz-awareness, then group by that.
    try:
        idx = h.index
        if not isinstance(idx, pd.DatetimeIndex):
            idx = pd.DatetimeIndex(idx)
        g = idx.normalize()
    except Exception:
        # Last-resort fallback: treat all bars as same day (no intraday reset)
        g = pd.Series(0, index=h.index)
    cumtpv = (hlc3 * v).groupby(g).cumsum()
    cumvol = v.groupby(g).cumsum()
    return (cumtpv / cumvol.replace(0, np.nan)).fillna(hlc3)


# ─────────────────────────────────────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────────────────────────────────────
class SignalEngine:
    def __init__(self, cfg: EngineConfig = None):
        self.cfg       = cfg or EngineConfig()
        self.df        = pd.DataFrame(
            columns=["open","high","low","close","volume"], dtype=float)
        self.df.index.name = "timestamp"
        self._in_long  = False
        self._in_short = False
        self._entry_px = 0.0
        self._trail_sl = 0.0
        # FIX: warmup threshold is slow_len, not trend_len
        self._warmup_bars = max(self.cfg.slow_len + 5, self.cfg.bb_len + 5, 30)
        # Cooldown: bars since last signal fired (starts ≥3 so first signal is allowed)
        self._bars_since_signal = 99
        self._last_signal_dir   = "NONE"   # track last direction for anti-repeat
        # ORB state — reset each trading day
        self._orb_high   = 0.0
        self._orb_low    = float("inf")
        self._orb_formed = False
        self._orb_date   = None   # date when ORB was built

    def update(self, timestamp, open_, high, low, close, volume) -> SignalResult:
        # FIX: normalize timestamp — yfinance returns tz-aware Timestamps (e.g. UTC+5:30)
        # which breaks df.index.date comparisons and pd.concat alignment.
        # Strip timezone to naive local time so all index ops work consistently.
        try:
            ts = pd.Timestamp(timestamp)
            if ts.tzinfo is not None:
                ts = ts.tz_convert("Asia/Kolkata").tz_localize(None)
        except Exception:
            ts = pd.Timestamp(timestamp)

        new_row = pd.DataFrame(
            [{"open": float(open_), "high": float(high), "low": float(low),
              "close": float(close), "volume": float(volume)}],
            index=pd.DatetimeIndex([ts])   # FIX: always DatetimeIndex, never plain Index
        )
        if self.df.empty:
            self.df = new_row.astype(float)
        else:
            self.df = pd.concat([self.df, new_row]).astype(float)
            # FIX: ensure index stays DatetimeIndex after concat
            if not isinstance(self.df.index, pd.DatetimeIndex):
                self.df.index = pd.DatetimeIndex(self.df.index)
        if len(self.df) > 600:
            self.df = self.df.iloc[-600:]
        return self._evaluate()

    def _update_orb(self, df: "pd.DataFrame") -> None:
        """
        Build/update the Opening Range from the first orb_bars candles of the day.
        Called every bar. Resets automatically on a new trading day.

        FIX: df.index.date crashes with AttributeError when the DataFrame index
        is not a proper DatetimeIndex (e.g. when yfinance returns timezone-aware
        timestamps, or when the index is a plain Index after concat operations).
        Solution: ensure the index is a DatetimeIndex before calling .date,
        and fall back to iloc slicing if the conversion fails.
        """
        cfg     = self.cfg
        last_ts = df.index[-1]

        # FIX: safely extract date from various timestamp types
        today = None
        try:
            if hasattr(last_ts, "date"):
                today = last_ts.date()
            else:
                today = pd.Timestamp(last_ts).date()
        except Exception:
            today = None

        # Reset ORB on new day
        if today != self._orb_date:
            self._orb_date   = today
            self._orb_high   = 0.0
            self._orb_low    = float("inf")
            self._orb_formed = False

        # Find today's bars
        # FIX: df.index.date only works on DatetimeIndex — use normalize() approach
        # which works on both tz-aware and tz-naive DatetimeIndex, and falls back
        # to iloc-based slicing for any non-standard index type.
        try:
            if today is not None:
                # Ensure we have a DatetimeIndex before calling .date
                idx = df.index
                if not isinstance(idx, pd.DatetimeIndex):
                    idx = pd.DatetimeIndex(idx)
                # .normalize() strips time → compare date only
                today_mask = idx.normalize() == pd.Timestamp(today)
                today_df   = df[today_mask.values]
            else:
                today_df = df.iloc[-cfg.orb_bars:]
        except Exception:
            # Ultimate fallback: just use the last N bars
            today_df = df.iloc[-cfg.orb_bars * 3:]

        n_today = len(today_df)
        if n_today >= cfg.orb_bars and not self._orb_formed:
            orb_slice        = today_df.iloc[:cfg.orb_bars]
            self._orb_high   = float(orb_slice["high"].max())
            self._orb_low    = float(orb_slice["low"].min())
            self._orb_formed = True

    def _evaluate(self) -> SignalResult:
        df  = self.df
        cfg = self.cfg
        n   = len(df)

        if n < self._warmup_bars:
            return self._warmup(df)

        c  = df["close"].astype(float)
        h  = df["high"].astype(float)
        l  = df["low"].astype(float)
        v  = df["volume"].astype(float)
        px = float(c.iloc[-1])

        # ── Time filter — skip opening chop (9:15–9:30) ────────────────────
        last_ts  = df.index[-1]
        bar_min  = last_ts.hour * 60 + last_ts.minute if hasattr(last_ts, "hour") else 999
        # ORB forms during 9:15-9:30 — no entries during those first 3 bars
        no_trade_early = bar_min < 9 * 60 + 30   # before 9:30
        no_trade_late  = bar_min > 14 * 60 + 45  # after 14:45

        # ── Update ORB state ──────────────────────────────────────────────
        if cfg.orb_enabled:
            self._update_orb(df)

        # ── Indicators ─────────────────────────────────────────────────
        # EMAs
        ef   = float(_ema(c, cfg.fast_len).iloc[-1])
        ef1  = float(_ema(c, cfg.fast_len).iloc[-2])
        em   = float(_ema(c, cfg.mid_len).iloc[-1])
        em1  = float(_ema(c, cfg.mid_len).iloc[-2])
        es   = float(_ema(c, cfg.slow_len).iloc[-1])
        et   = float(_ema(c, min(cfg.trend_len, n-1)).iloc[-1]) if n > 10 \
               else float(_ema(c, cfg.slow_len).iloc[-1])

        atr     = float(_atr(h, l, c, cfg.adx_len).iloc[-1])
        rsi     = float(_rsi(c, cfg.rsi_len).iloc[-1])
        adx_val = float(_adx(h, l, c, cfg.adx_len).iloc[-1])

        # BB squeeze
        _, bbu, bbl = _bbands(c, cfg.bb_len, cfg.bb_std)
        bw      = (bbu - bbl) / c.replace(0, np.nan)
        squeeze = float(bw.iloc[-1]) < float(_sma(bw, cfg.bb_len).iloc[-1]) * 0.60

        # Supertrend
        try:
            _, st_dir = _supertrend(h, l, c, cfg.st_atr_len, cfg.st_factor)
            st_d = int(st_dir.iloc[-1])
        except Exception:
            st_d = 0

        # VWAP
        try:
            vwap_val = float(_vwap(h, l, c, v).iloc[-1])
        except Exception:
            vwap_val = px

        # Volume
        vol_avg   = float(_sma(v, 20).iloc[-1]) or 1.0
        vol_spike = float(v.iloc[-1]) > vol_avg * cfg.vol_mult

        # Candle body
        candle_body = abs(px - float(df["open"].iloc[-1]))
        body_ok     = candle_body >= atr * 0.25   # directional candle

        # ── ORB breakout checks ────────────────────────────────────────────
        orb_bull = (not cfg.orb_enabled) or (
            self._orb_formed and px > self._orb_high
        )
        orb_bear = (not cfg.orb_enabled) or (
            self._orb_formed and px < self._orb_low
        )

        # ── Core conditions ──────────────────────────────────────────────
        ema_bull   = ef > em > es           # EMA stack bullish: 9 > 21 > 50
        ema_bear   = ef < em < es           # EMA stack bearish: 9 < 21 < 50
        ema_cross_up = ef1 < em1 and ef >= em   # fresh cross-up
        ema_cross_dn = ef1 > em1 and ef <= em   # fresh cross-down
        vwap_bull  = (not cfg.use_vwap) or (px > vwap_val)
        vwap_bear  = (not cfg.use_vwap) or (px < vwap_val)
        rsi_buy    = 45 < rsi < cfg.rsi_ob
        rsi_sell   = cfg.rsi_os < rsi < 55
        trending   = adx_val > cfg.adx_thresh
        st_bull    = st_d == -1
        st_bear    = st_d == 1
        no_squeeze = not squeeze
        bull_trend = (px > et and ef > es) or ema_cross_up  # broader uptrend
        bear_trend = (px < et and ef < es) or ema_cross_dn

        # ── MANDATORY 3 + CONFIRMATION 2-of-4 structure ──────────────────────
        # BUY_CE mandatory: ORB breakout up + price above VWAP + EMA bullish
        ce_mandatory = orb_bull and vwap_bull and ema_bull
        # BUY_PE mandatory: ORB breakdown + price below VWAP + EMA bearish
        pe_mandatory = orb_bear and vwap_bear and ema_bear

        # Confirmation flags (need ≥2 of these 4 for both CE and PE)
        ce_conf_flags = [rsi_buy,  trending, st_bull, body_ok and no_squeeze]
        pe_conf_flags = [rsi_sell, trending, st_bear, body_ok and no_squeeze]
        ce_conf_cnt   = sum(ce_conf_flags)
        pe_conf_cnt   = sum(pe_conf_flags)
        min_conf      = cfg.min_confirmations - 3   # 5-3=2, so need 2 confirmations
        min_conf      = max(1, min_conf)

        buy_ok  = (
            ce_mandatory and ce_conf_cnt >= min_conf
            and cfg.option_type != "PE Only"
            and not no_trade_early and not no_trade_late
        )
        sell_ok = (
            pe_mandatory and pe_conf_cnt >= min_conf
            and cfg.option_type != "CE Only"
            and not no_trade_early and not no_trade_late
        )

        # Prefer stronger side if both fire
        if buy_ok and sell_ok:
            buy_ok  = ce_conf_cnt >= pe_conf_cnt
            sell_ok = not buy_ok

        signal = "BUY_CE" if buy_ok else "BUY_PE" if sell_ok else "NONE"

        # ── Cooldown: suppress signal for 3 bars after last fire ───────────
        self._bars_since_signal += 1
        if signal != "NONE":
            if self._bars_since_signal < 3:
                signal = "NONE"
            else:
                self._bars_since_signal = 0

        # ── Signal reason ───────────────────────────────────────────────
        orb_s = f"ORB:{self._orb_low:.0f}-{self._orb_high:.0f}" if self._orb_formed else "ORB:forming"

        if signal == "BUY_CE":
            conf_fired = ["RSI" if rsi_buy else "",
                          "ADX" if trending else "",
                          "ST" if st_bull else "",
                          "Body" if (body_ok and no_squeeze) else ""]
            reason = (f"BUY_CE | {orb_s} | VWAP↑ | EMA_stack | "
                      f"conf={ce_conf_cnt}/{min_conf} [{','.join(x for x in conf_fired if x)}]")
        elif signal == "BUY_PE":
            conf_fired = ["RSI" if rsi_sell else "",
                          "ADX" if trending else "",
                          "ST" if st_bear else "",
                          "Body" if (body_ok and no_squeeze) else ""]
            reason = (f"BUY_PE | {orb_s} | VWAP↓ | EMA_stack | "
                      f"conf={pe_conf_cnt}/{min_conf} [{','.join(x for x in conf_fired if x)}]")
        else:
            m_ce = [] ; m_pe = []
            if not orb_bull:   m_ce.append("ORB")
            if not vwap_bull:  m_ce.append("VWAP")
            if not ema_bull:   m_ce.append("EMA")
            if ce_conf_cnt < min_conf: m_ce.append(f"conf{ce_conf_cnt}<{min_conf}")
            if not orb_bear:   m_pe.append("ORB")
            if not vwap_bear:  m_pe.append("VWAP")
            if not ema_bear:   m_pe.append("EMA")
            if pe_conf_cnt < min_conf: m_pe.append(f"conf{pe_conf_cnt}<{min_conf}")
            time_str = "EARLY" if no_trade_early else ("LATE" if no_trade_late else "")
            reason = (f"NONE | CE miss: {','.join(m_ce[:3])} | PE miss: {','.join(m_pe[:3])}"
                      + (f" | {time_str}" if time_str else ""))

        # ── SL / Target ─────────────────────────────────────────────────
        swing_lo = float(l.iloc[-10:].min())
        swing_hi = float(h.iloc[-10:].max())

        if signal == "BUY_CE":
            sl  = (px - atr*cfg.sl_atr_mult)  if cfg.sl_mode == "ATR" else \
                  (px*(1-cfg.sl_pct/100))      if cfg.sl_mode == "Fixed %" else swing_lo
            rr  = 2.0 if cfg.tgt_mode == "2:1 RR" else 3.0 if cfg.tgt_mode == "3:1 RR" else 0
            tgt = (px*(1+cfg.tgt_pct/100))     if cfg.tgt_mode == "Fixed %" else \
                  (px+atr*cfg.tgt_atr_mult)    if cfg.tgt_mode == "ATR" else px+(px-sl)*rr
        elif signal == "BUY_PE":
            sl  = (px + atr*cfg.sl_atr_mult)  if cfg.sl_mode == "ATR" else \
                  (px*(1+cfg.sl_pct/100))      if cfg.sl_mode == "Fixed %" else swing_hi
            rr  = 2.0 if cfg.tgt_mode == "2:1 RR" else 3.0 if cfg.tgt_mode == "3:1 RR" else 0
            tgt = (px*(1-cfg.tgt_pct/100))    if cfg.tgt_mode == "Fixed %" else \
                  (px-atr*cfg.tgt_atr_mult)   if cfg.tgt_mode == "ATR" else px-(sl-px)*rr
        else:
            sl  = px - atr * cfg.sl_atr_mult
            tgt = px + atr * cfg.tgt_atr_mult * 2

        risk_amt = cfg.capital * cfg.risk_pct / 100
        risk_pts = max(abs(px - sl), 1.0)
        qty_lots = min(cfg.max_lots, max(1, int(risk_amt / (risk_pts * cfg.lot_size))))

        # Trail state
        if signal == "BUY_CE":
            self._in_long=True; self._in_short=False; self._entry_px=px; self._trail_sl=sl
        if signal == "BUY_PE":
            self._in_short=True; self._in_long=False; self._entry_px=px; self._trail_sl=sl
        if self._in_long and cfg.use_trail:
            if px >= self._entry_px + atr*cfg.trail_trig:
                self._trail_sl = max(self._trail_sl, px - atr*cfg.trail_step)
            if px <= self._trail_sl: self._in_long = False
        if self._in_short and cfg.use_trail:
            if px <= self._entry_px - atr*cfg.trail_trig:
                self._trail_sl = min(self._trail_sl, px + atr*cfg.trail_step)
            if px >= self._trail_sl: self._in_short = False

        # Map flags back to SignalResult fields (keeping backward compatibility)
        cross_up_or_dn = ema_cross_up or ema_cross_dn
        return SignalResult(
            signal=signal, timestamp=df.index[-1],
            close=round(px,2), sl=round(sl,2), target=round(tgt,2),
            atr=round(atr,2), rsi=round(rsi,1), adx=round(adx_val,1),
            vwap=round(vwap_val,2), qty_lots=qty_lots, risk_amt=round(risk_amt,0),
            trail_sl=round(self._trail_sl,2), signal_reason=reason,
            bars_loaded=n, bars_needed=self._warmup_bars,
            conf_ema_cross=cross_up_or_dn,      conf_trend=bull_trend or bear_trend,
            conf_supertrend=st_bull or st_bear,  conf_vwap=vwap_bull or vwap_bear,
            conf_rsi=rsi_buy or rsi_sell,        conf_adx=trending,
            conf_volume=vol_spike,               conf_no_squeeze=no_squeeze,
        )

    def _warmup(self, df) -> SignalResult:
        px = float(df["close"].iloc[-1]) if len(df) > 0 else 0.0
        ts = df.index[-1] if len(df) > 0 else datetime.now()
        return SignalResult(
            signal="NONE", timestamp=ts, close=px,
            sl=0, target=0, atr=0, rsi=0, adx=0, vwap=0,
            qty_lots=0, risk_amt=0,
            bars_loaded=len(df), bars_needed=self._warmup_bars,
            signal_reason=f"Warming up {len(df)}/{self._warmup_bars} bars",
        )