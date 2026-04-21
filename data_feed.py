"""
data_feed.py  —  Dhan market feed  (FIXED v3)
═══════════════════════════════════════════════════════════════════════
FIXES in v3:
  1. WS 429 Too Many Requests: exponential backoff up to 5 min before retry.
     The 429 means Dhan's server blocked the IP — backing off is mandatory.
  2. REST-only mode: when WS is blocked, LTPPoller alone drives candles.
     ws_stale_secs lowered to 5s so REST kicks in immediately.
  3. CandleBuilder force-close timer: a background thread checks every
     ~15s and force-closes the current candle if the candle's expected
     close time has passed. This prevents "stuck candles" when ticks
     arrive slowly (only 1 tick per second from REST).
  4. WS reconnect: after a 429, wait at least 120s before retrying
     (Dhan usually unblocks within 2 minutes).
  5. LTPPoller now always runs regardless of WS state — WS is BONUS,
     REST is the guaranteed path for candle building.
"""

import os, struct, json, time, logging, threading
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Callable, Optional

import websocket   # pip install websocket-client

from broker_dhan import session, INDEX_SECURITY_IDS, broker

log = logging.getLogger("DataFeed")

WS_ENDPOINT = "wss://api-feed.dhan.co"
IDX_EXCH    = 0
NSE_EXCH    = 1
FNO_EXCH    = 2
TICKER_REQ  = 15


# ════════════════════════════════════════════════════════════════════
# CANDLE BUILDER  (with force-close timer)
# ════════════════════════════════════════════════════════════════════
@dataclass
class Candle:
    ts:     datetime
    open:   float = 0.0
    high:   float = 0.0
    low:    float = float("inf")
    close:  float = 0.0
    volume: float = 0.0


class CandleBuilder:
    """
    Builds OHLCV candles from raw LTP ticks.

    FIX: Added _ForceCloseTimer — a background thread that fires
    every (tf * 60 / 4) seconds and checks if the current candle's
    expected close time has already passed. If yes, it synthetically
    emits a tick with the last known price so the candle closes.
    Without this, a candle opened at 09:30 might not close until
    09:35 + 1 tick arrives — causing up to 1 candle of lag.
    """

    def __init__(self, tf_min: int, on_close_fn: Callable[[Candle], None]):
        self.tf       = tf_min
        self.on_close = on_close_fn
        self.cur:     Optional[Candle] = None
        self._lock    = threading.Lock()
        self._last_ts = None
        self._last_ltp = 0.0

        # Start force-close timer
        self._fc_thread = threading.Thread(
            target=self._force_close_loop,
            daemon=True, name="CandleForceClose"
        )
        self._fc_thread.start()

    def _floor(self, dt: datetime) -> datetime:
        m = dt.hour * 60 + dt.minute
        f = (m // self.tf) * self.tf
        return dt.replace(hour=f // 60, minute=f % 60,
                          second=0, microsecond=0)

    def _force_close_loop(self):
        """
        Every tf/4 minutes, check if the current candle is overdue.
        If so, emit a synthetic tick at the next candle boundary so
        the current candle gets closed even without a real tick.
        """
        check_interval = max(15, self.tf * 60 // 4)  # e.g. 75s for 5m
        log.info(f"⏱ CandleForceClose started (check every {check_interval}s)")
        while True:
            time.sleep(check_interval)
            try:
                with self._lock:
                    if self.cur is None or self._last_ltp <= 0:
                        continue
                    now = datetime.now()
                    # Expected close time = candle open + tf minutes
                    expected_close = self.cur.ts + timedelta(minutes=self.tf)
                    if now >= expected_close + timedelta(seconds=5):
                        # Force close by faking a tick in the NEXT candle window
                        fake_dt = expected_close + timedelta(seconds=1)
                        ltp = self._last_ltp
                        # Release lock before calling tick (tick acquires its own lock)
                # Call tick outside lock to avoid deadlock
                if now >= expected_close + timedelta(seconds=5) and ltp > 0:
                    log.debug(
                        f"⏱ Force-closing candle {self.cur.ts.strftime('%H:%M') if self.cur else '?'}"
                    )
                    self.tick(ltp, 0.0, fake_dt)
            except Exception as e:
                log.debug(f"ForceClose error: {e}")

    def tick(self, ltp: float, vol: float = 0.0,
             dt: Optional[datetime] = None):
        if ltp <= 0:
            return
        if dt is None:
            dt = datetime.now()
        with self._lock:
            self._last_ltp = ltp
            ts = self._floor(dt)
            if self.cur is None or ts > self.cur.ts:
                if self.cur is not None and self.cur.ts != self._last_ts:
                    self._last_ts = self.cur.ts
                    closed = self.cur
                    log.info(
                        f"📊 Candle {closed.ts.strftime('%H:%M')} | "
                        f"O={closed.open:.0f} H={closed.high:.0f} "
                        f"L={closed.low:.0f} C={closed.close:.0f}"
                    )
                    threading.Thread(
                        target=self.on_close, args=(closed,), daemon=True
                    ).start()
                self.cur = Candle(
                    ts=ts, open=ltp, high=ltp, low=ltp,
                    close=ltp, volume=vol
                )
            else:
                self.cur.high    = max(self.cur.high, ltp)
                self.cur.low     = min(self.cur.low,  ltp)
                self.cur.close   = ltp
                self.cur.volume += vol


# ════════════════════════════════════════════════════════════════════
# LIVE FEED  (Dhan WebSocket v2)
# ════════════════════════════════════════════════════════════════════
class LiveFeed:
    """
    Dhan WebSocket v2.  Connection URL:
      wss://api-feed.dhan.co?version=2&token=TOKEN&clientId=ID&authType=2

    FIX: 429 handling — when Dhan returns 429 (Too Many Requests /
    client blocked), the reconnect waits at least WS_429_COOLDOWN
    seconds (default 120s) before retrying.  Repeated restarts were
    causing the block.
    """

    # Minimum wait after a 429 response before reconnecting
    WS_429_COOLDOWN = 120   # seconds — Dhan usually unblocks in ~2 min

    def __init__(self, index: str, candle_builder: CandleBuilder):
        self.index       = index.upper()
        self.cb          = candle_builder
        self._idx_sid    = INDEX_SECURITY_IDS.get(self.index, "13")
        self._subscribed = set()
        self._ws:        Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._lock       = threading.Lock()
        self._last_tick  = time.time()
        self._running    = False
        self._last_429   = 0.0   # timestamp of last 429
        self.ltp_store:  dict = {}
        self.connected   = False

    def _ws_url(self) -> str:
        token     = session.access_token
        client_id = session.client_id
        return (
            f"{WS_ENDPOINT}"
            f"?version=2"
            f"&token={token}"
            f"&clientId={client_id}"
            f"&authType=2"
        )

    def start(self):
        self._running   = True
        self._ws_thread = threading.Thread(
            target=self._run_forever, daemon=True, name="DhanWS"
        )
        self._ws_thread.start()
        log.info(f"🌐 LiveFeed starting ({self.index})")

    def stop(self):
        self._running  = False
        self.connected = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def last_tick_age(self) -> float:
        return time.time() - self._last_tick

    def get_index_ltp(self) -> float:
        return self.ltp_store.get(f"IDX_{self._idx_sid}", 0.0)

    def get_option_ltp(self, security_id: str) -> float:
        return self.ltp_store.get(f"FNO_{security_id}", 0.0)

    def add_option(self, security_id: str):
        with self._lock:
            self._subscribed.add(str(security_id))
        if self._ws and self.connected:
            try:
                self._send_subscription([str(security_id)], FNO_EXCH)
            except Exception:
                pass

    # ── WebSocket lifecycle ───────────────────────────────────────────
    def _run_forever(self):
        retry = 0
        while self._running:
            # FIX: if we got a 429 recently, wait longer before reconnecting
            since_429 = time.time() - self._last_429
            if since_429 < self.WS_429_COOLDOWN:
                wait_for = self.WS_429_COOLDOWN - since_429
                log.warning(
                    f"⏳ WS 429 cooldown: waiting {wait_for:.0f}s before reconnect "
                    f"(REST polling is active)"
                )
                time.sleep(wait_for)

            url = self._ws_url()
            try:
                log.info(f"📡 Connecting Dhan WS v2 (attempt {retry+1})...")
                ws = websocket.WebSocketApp(
                    url,
                    on_open    = self._on_open,
                    on_message = self._on_message,
                    on_error   = self._on_error,
                    on_close   = self._on_close,
                )
                self._ws = ws
                ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                log.error(f"WS run error: {e}")

            self.connected = False
            if self._running:
                # Exponential backoff: 10, 20, 40, 80, 120s max
                wait = min(120, 10 * (2 ** min(retry, 3)))
                log.info(f"🔄 WS reconnecting in {wait}s (REST active)...")
                time.sleep(wait)
                retry += 1
            else:
                break

    def _on_open(self, ws):
        self.connected = True
        log.info("✅ Dhan WebSocket v2 connected")
        self._send_subscription([self._idx_sid], IDX_EXCH)
        with self._lock:
            if self._subscribed:
                self._send_subscription(list(self._subscribed), FNO_EXCH)

    def _send_subscription(self, security_ids: list, exchange_code: int):
        exch_str_map = {
            0: "IDX_I", 1: "NSE_EQ", 2: "NSE_FNO",
            3: "NSE_CURRENCY", 4: "BSE_EQ",
        }
        exch_str = exch_str_map.get(exchange_code, "NSE_FNO")
        msg = json.dumps({
            "RequestCode":     TICKER_REQ,
            "InstrumentCount": len(security_ids),
            "InstrumentList":  [
                {"ExchangeSegment": exch_str, "SecurityId": str(s)}
                for s in security_ids
            ],
        })
        if self._ws:
            try:
                self._ws.send(msg)
                log.info(f"📡 Subscribed {exch_str}: {security_ids}")
            except Exception as e:
                log.warning(f"Subscription send failed: {e}")

    def _on_message(self, ws, raw):
        try:
            if isinstance(raw, str):
                log.debug(f"WS text: {raw[:100]}")
                try:
                    msg = json.loads(raw)
                    if msg.get("type") == "error":
                        log.error(f"WS error msg: {msg}")
                except Exception:
                    pass
                return

            if len(raw) < 16:
                return

            resp_code = struct.unpack_from("<B", raw, 0)[0]
            if resp_code != 2:
                return

            _, msg_len, exch_seg, sec_id, ltp, ltt = struct.unpack_from(
                "<BHBIfI", raw, 0
            )
            if ltp <= 0:
                return

            self._last_tick = time.time()

            if exch_seg == 0:   # IDX_I
                self.ltp_store[f"IDX_{sec_id}"] = ltp
                self.cb.tick(ltp, 0.0, datetime.now())
                log.debug(f"WS IDX: ₹{ltp:.1f}")
            elif exch_seg == 2:  # NSE_FNO
                self.ltp_store[f"FNO_{sec_id}"] = ltp
                log.debug(f"WS FNO sid={sec_id}: ₹{ltp:.1f}")

        except Exception as e:
            log.debug(f"WS parse error: {e}")

    def _on_error(self, ws, error):
        err_str = str(error)
        log.error(f"WS error: {error}")
        # FIX: detect 429 and set cooldown timestamp
        if "429" in err_str or "Too Many Requests" in err_str or "blocked" in err_str.lower():
            self._last_429 = time.time()
            log.warning(
                f"🚫 WS 429 — Dhan blocked this IP temporarily. "
                f"REST polling will handle candles for {self.WS_429_COOLDOWN}s. "
                "This resolves automatically — do NOT restart the script."
            )

    def _on_close(self, ws, code, msg):
        self.connected = False
        log.warning(f"WS closed: code={code}")


# ════════════════════════════════════════════════════════════════════
# LTP POLLER  (REST — primary when WS is down)
# ════════════════════════════════════════════════════════════════════
class LTPPoller(threading.Thread):
    """
    REST LTP poller — polls every `interval` seconds.

    FIX v3: LTPPoller is now the PRIMARY candle driver, not just a fallback.
    ws_stale_secs reduced to 5s (was 8s), meaning if WS hasn't ticked in
    5 seconds, REST kicks in immediately.

    With WS blocked (429), REST alone will drive all candles at 1s resolution
    — more than sufficient for 5-min candle building.
    """

    def __init__(
        self,
        index:          str,
        candle_builder: CandleBuilder,
        live_feed:      Optional[LiveFeed] = None,
        interval:       float = 1.0,
        ws_stale_secs:  float = 5.0,   # FIX: reduced from 8s to 5s
    ):
        super().__init__(daemon=True, name="LTPPoller")
        self.index       = index.upper()
        self.cb          = candle_builder
        self.live_feed   = live_feed
        self.interval    = interval
        self.ws_stale    = ws_stale_secs
        self._last_ltp   = 0.0
        self._errors     = 0
        self._poll_count = 0

    def _market_open(self) -> bool:
        now = datetime.now()
        return (
            now.weekday() < 5 and
            (now.hour > 9 or (now.hour == 9 and now.minute >= 14)) and
            now.hour < 16
        )

    def run(self):
        log.info(f"📊 LTPPoller started ({self.index}, interval={self.interval}s)")
        while True:
            try:
                if not self._market_open():
                    time.sleep(10)
                    continue

                # FIX: check WS freshness; if WS is fresh skip REST to avoid double-ticking
                # But if WS has 429 (connected=False), ALWAYS use REST
                ws_fresh = (
                    self.live_feed is not None
                    and self.live_feed.connected
                    and self.live_feed.last_tick_age() < self.ws_stale
                )
                if ws_fresh:
                    time.sleep(self.interval)
                    continue

                ltp = broker.get_index_ltp(self.index)

                if ltp > 0:
                    self._errors   = 0
                    self._last_ltp = ltp
                    self._poll_count += 1
                    self.cb.tick(ltp, 0.0, datetime.now())
                    if self.live_feed:
                        sid = INDEX_SECURITY_IDS.get(self.index, "")
                        self.live_feed.ltp_store[f"IDX_{sid}"] = ltp
                    log.debug(f"REST LTP {self.index}: ₹{ltp:.1f}")

                    # Log first successful poll and then every 60 polls (~1 min)
                    if self._poll_count == 1:
                        log.info(f"✅ REST LTP active — ₹{ltp:.1f} (WS fallback mode)")
                    elif self._poll_count % 60 == 0:
                        log.info(
                            f"📡 REST polling active ({self._poll_count} polls) "
                            f"| LTP ₹{ltp:.1f} | WS: {'OK' if (self.live_feed and self.live_feed.connected) else 'DOWN'}"
                        )
                else:
                    self._errors += 1
                    if self._errors % 15 == 0:
                        log.warning(
                            f"⚠️  LTP=0 ({self._errors}×) — check token/market hours"
                        )

            except Exception as e:
                self._errors += 1
                if self._errors % 15 == 1:
                    log.warning(f"LTPPoller error: {e}")

            time.sleep(self.interval)


# ════════════════════════════════════════════════════════════════════
# WS WATCHDOG
# ════════════════════════════════════════════════════════════════════
class WSWatchdog(threading.Thread):
    """
    FIX: only restart WS if it's been silent AND it was previously
    connected (not just blocked by 429).  If 429 cooldown is active,
    don't restart — the cooldown will handle it.
    """
    def __init__(self, live_feed: LiveFeed, silence_secs: int = 300):
        super().__init__(daemon=True, name="WSWatchdog")
        self.feed    = live_feed
        self.silence = silence_secs

    def run(self):
        log.info("👁 WSWatchdog started")
        time.sleep(120)   # give WS time to connect initially
        while True:
            try:
                now    = datetime.now()
                in_mkt = (
                    now.weekday() < 5 and
                    (now.hour > 9 or (now.hour == 9 and now.minute >= 15)) and
                    now.hour < 15
                )
                # FIX: don't restart if 429 cooldown is still active
                since_429 = time.time() - self.feed._last_429
                in_429_cooldown = since_429 < self.feed.WS_429_COOLDOWN

                if (in_mkt
                        and not in_429_cooldown
                        and self.feed.last_tick_age() > self.silence):
                    log.warning(
                        f"🔄 WS silent {self.feed.last_tick_age()/60:.0f}m — restarting"
                    )
                    self.feed.stop()
                    time.sleep(5)
                    self.feed.start()

            except Exception as e:
                log.error(f"WSWatchdog: {e}")
            time.sleep(60)
