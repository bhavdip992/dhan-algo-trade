"""
data_feed.py  —  Dhan WebSocket live market feed  (FIXED v2)
═══════════════════════════════════════════════════════════════════════
Fixes applied:
  1. WebSocket v2 URL: credentials go in query string, NOT in auth message
     wss://api-feed.dhan.co?version=2&token=<TOKEN>&clientId=<ID>&authType=2
  2. Binary packet: first byte is 2 (Ticker), struct = '<BHBIfI' (16 bytes)
     LTP is float32 (already in rupees, no /100 needed)
  3. Subscription message uses correct format from official SDK
  4. REST LTP poller is the reliable fallback (WS is bonus)
"""

import os, struct, json, time, logging, threading
from datetime import datetime
from dataclasses import dataclass
from typing import Callable, Optional

import websocket   # pip install websocket-client

from broker_dhan import session, INDEX_SECURITY_IDS, broker

log = logging.getLogger("DataFeed")

# Dhan WS v2 endpoint
WS_ENDPOINT = "wss://api-feed.dhan.co"

# Exchange segment codes (numeric, for binary packets and subscription)
IDX_EXCH  = 0   # IDX_I
NSE_EXCH  = 1   # NSE_EQ
FNO_EXCH  = 2   # NSE_FNO

# Request codes
TICKER_REQ = 15


# ════════════════════════════════════════════════════════════════════
# CANDLE BUILDER
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
    def __init__(self, tf_min: int, on_close_fn: Callable[[Candle], None]):
        self.tf       = tf_min
        self.on_close = on_close_fn
        self.cur:     Optional[Candle] = None
        self._lock    = threading.Lock()
        self._last_ts = None

    def _floor(self, dt: datetime) -> datetime:
        m = dt.hour * 60 + dt.minute
        f = (m // self.tf) * self.tf
        return dt.replace(hour=f // 60, minute=f % 60,
                          second=0, microsecond=0)

    def tick(self, ltp: float, vol: float = 0.0,
             dt: Optional[datetime] = None):
        if ltp <= 0:
            return
        if dt is None:
            dt = datetime.now()
        with self._lock:
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
    Dhan WebSocket v2 — credentials in URL, JSON subscription messages,
    binary response packets.

    Connection URL (FIXED):
      wss://api-feed.dhan.co?version=2&token=<ACCESS_TOKEN>
                              &clientId=<CLIENT_ID>&authType=2

    NO separate auth message needed for v2.

    Binary Ticker packet (16 bytes, little-endian):
      Byte 0:    response_code  (uint8)  — 2 = Ticker
      Bytes 1-2: message_length (uint16)
      Byte 3:    exchange_seg   (uint8)  — 0=IDX, 2=FNO
      Bytes 4-7: security_id   (uint32)
      Bytes 8-11: LTP          (float32) — rupees (no scaling needed)
      Bytes 12-15: LTT         (uint32)  — unix epoch
    Struct: '<BHBIfI'
    """

    def __init__(self, index: str, candle_builder: CandleBuilder):
        self.index      = index.upper()
        self.cb         = candle_builder
        self._idx_sid   = INDEX_SECURITY_IDS.get(self.index, "13")
        self._subscribed = set()
        self._ws:       Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._lock      = threading.Lock()
        self._last_tick = time.time()
        self._running   = False
        self.ltp_store: dict = {}   # {"IDX_13": 23500.0, "FNO_12345": 150.0}

    def _ws_url(self) -> str:
        """
        FIXED: v2 requires credentials in the URL query string.
        """
        token     = session.access_token
        client_id = session.client_id
        return (
            f"{WS_ENDPOINT}"
            f"?version=2"
            f"&token={token}"
            f"&clientId={client_id}"
            f"&authType=2"
        )

    # ── Public API ────────────────────────────────────────────────────
    def start(self):
        self._running  = True
        self._ws_thread = threading.Thread(
            target=self._run_forever, daemon=True, name="DhanWS"
        )
        self._ws_thread.start()
        log.info(f"🌐 LiveFeed starting ({self.index})")

    def stop(self):
        self._running = False
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
        if self._ws:
            try:
                if self._ws.sock and self._ws.sock.connected:
                    self._send_subscription([str(security_id)], FNO_EXCH)
            except Exception:
                pass

    # ── WebSocket lifecycle ───────────────────────────────────────────
    def _run_forever(self):
        retry = 0
        while self._running:
            url = self._ws_url()
            try:
                log.info(f"📡 Connecting Dhan WebSocket v2 (attempt {retry+1})...")
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
                log.error(f"WebSocket run error: {e}")

            if not self._running:
                break

            # Exponential backoff: 30s, 60s, 120s, 120s max
            # Avoids Dhan 429 "Too many requests from this IP"
            wait = min(120, 30 * (2 ** min(retry, 2)))
            log.info(f"🔄 Reconnecting in {wait}s...")
            time.sleep(wait)
            retry += 1

    def _on_open(self, ws):
        log.info("✅ Dhan WebSocket v2 connected")
        # v2: NO auth message needed — auth is in URL
        # Subscribe index immediately
        self._send_subscription([self._idx_sid], IDX_EXCH)
        # Subscribe any queued options
        with self._lock:
            if self._subscribed:
                self._send_subscription(list(self._subscribed), FNO_EXCH)

    def _send_subscription(self, security_ids: list, exchange_code: int):
        """
        Send JSON subscription message — v2 format from official SDK.
        exchange_code: 0=IDX_I, 2=NSE_FNO
        """
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
                log.info(
                    f"📡 Subscribed {exch_str}: {security_ids}"
                )
            except Exception as e:
                log.warning(f"Subscription send failed: {e}")

    def _on_message(self, ws, raw):
        """
        Parse Dhan v2 binary Ticker packet.
        Struct: '<BHBIfI' (16 bytes)
          [0] uint8  — response_code  (2 = Ticker)
          [1] uint16 — message_length
          [2] uint8  — exchange_segment (0=IDX, 2=FNO)
          [3] uint32 — security_id
          [4] float32 — LTP (rupees, no scaling)
          [5] uint32 — LTT (unix epoch seconds)
        """
        try:
            if isinstance(raw, str):
                # JSON control message (auth ACK, disconnect notice etc.)
                log.debug(f"WS text msg: {raw[:100]}")
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

            # Dhan v1 uses response code 2 for Ticker
            # Dhan v2 also uses 2 for Ticker (from process_data in SDK)
            if resp_code != 2:
                # Not a Ticker packet (could be prev_close=6, status=7 etc.)
                return

            # Unpack full packet
            _, msg_len, exch_seg, sec_id, ltp, ltt = struct.unpack_from(
                "<BHBIfI", raw, 0
            )

            if ltp <= 0:
                return

            self._last_tick = time.time()

            # Route to store
            if exch_seg == 0:  # IDX_I
                key = f"IDX_{sec_id}"
                self.ltp_store[key] = ltp
                self.cb.tick(ltp, 0.0, datetime.now())
                log.debug(f"WS IDX LTP: ₹{ltp:.1f}")
            elif exch_seg == 2:  # NSE_FNO
                key = f"FNO_{sec_id}"
                self.ltp_store[key] = ltp
                log.debug(f"WS FNO sid={sec_id} LTP: ₹{ltp:.1f}")

        except Exception as e:
            log.debug(f"WS message parse error: {e}")

    def _on_error(self, ws, error):
        log.error(f"WS error: {error}")

    def _on_close(self, ws, code, msg):
        log.warning(f"WS closed: code={code}")


# ════════════════════════════════════════════════════════════════════
# LTP POLLER  (REST fallback — always reliable)
# ════════════════════════════════════════════════════════════════════
class LTPPoller(threading.Thread):
    """
    REST polling fallback.
    Runs every `interval` seconds.
    Skips polling when WebSocket has ticked within `ws_stale_secs`.
    """

    def __init__(
        self,
        index:          str,
        candle_builder: CandleBuilder,
        live_feed:      Optional[LiveFeed] = None,
        interval:       float = 1.0,
        ws_stale_secs:  float = 8.0,
    ):
        super().__init__(daemon=True, name="LTPPoller")
        self.index       = index.upper()
        self.cb          = candle_builder
        self.live_feed   = live_feed
        self.interval    = interval
        self.ws_stale    = ws_stale_secs
        self._last_ltp   = 0.0
        self._errors     = 0

    def _market_open(self) -> bool:
        now = datetime.now()
        return (
            now.weekday() < 5 and
            (now.hour > 9 or (now.hour == 9 and now.minute >= 14)) and
            now.hour < 16
        )

    def run(self):
        log.info(
            f"📊 LTPPoller started ({self.index}, "
            f"interval={self.interval}s)"
        )
        while True:
            try:
                if not self._market_open():
                    time.sleep(10)
                    continue

                # Skip if WebSocket is fresh
                if (self.live_feed and
                        self.live_feed.last_tick_age() < self.ws_stale):
                    time.sleep(self.interval)
                    continue

                ltp = broker.get_index_ltp(self.index)

                if ltp > 0:
                    self._errors   = 0
                    self._last_ltp = ltp
                    self.cb.tick(ltp, 0.0, datetime.now())
                    # Mirror into live_feed store
                    if self.live_feed:
                        sid = INDEX_SECURITY_IDS.get(self.index, "")
                        self.live_feed.ltp_store[f"IDX_{sid}"] = ltp
                    log.debug(f"REST LTP {self.index}: ₹{ltp:.1f}")
                else:
                    self._errors += 1
                    if self._errors % 15 == 0:
                        log.warning(
                            f"⚠️  Index LTP=0 ({self._errors}× REST) — "
                            "check token / market hours"
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
    def __init__(self, live_feed: LiveFeed, silence_secs: int = 300):
        super().__init__(daemon=True, name="WSWatchdog")
        self.feed    = live_feed
        self.silence = silence_secs

    def run(self):
        log.info("👁 WSWatchdog started")
        time.sleep(90)
        while True:
            try:
                now    = datetime.now()
                in_mkt = (
                    now.weekday() < 5 and
                    (now.hour > 9 or (now.hour == 9 and now.minute >= 15)) and
                    now.hour < 15
                )
                if in_mkt and self.feed.last_tick_age() > self.silence:
                    log.warning(
                        f"🔄 WS silent {self.feed.last_tick_age()/60:.0f}m "
                        "— restarting"
                    )
                    self.feed.stop()
                    time.sleep(3)
                    self.feed.start()
            except Exception as e:
                log.error(f"WSWatchdog: {e}")
            time.sleep(60)