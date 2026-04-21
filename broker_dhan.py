"""
broker_dhan.py  —  Dhan API integration layer
═══════════════════════════════════════════════════════════════════════
Auto token refresh via Dhan Renew Token API:
  POST https://api.dhan.co/v2/token/renew
  Body: { "access_token": "<current>", "totp": "<6-digit>" }
  Docs: https://dhanhq.co/docs/v2/authentication/#renew-token

.env keys:
  DHAN_CLIENT_ID=...
  DHAN_ACCESS_TOKEN=...      # current token (auto-updated on refresh)
  DHAN_TOTP_SECRET=...       # Base32 TOTP secret from Dhan 2FA setup
                             # Dhan Web → Profile → Security → 2FA
                             # → "Can't scan?" → copy Base32 secret
"""

import os, io, time, logging, threading, re
from datetime import date, datetime, timedelta
from typing import Optional
from pathlib import Path

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("DhanBroker")

# ── Constants ────────────────────────────────────────────────────────
NSE_FNO   = "NSE_FNO"
NSE_IDX   = "IDX_I"
NSE_EQ    = "NSE_EQ"
MARKET    = "MARKET"
SL_M      = "STOP_LOSS_MARKET"
SL        = "STOP_LOSS"
INTRA     = "INTRADAY"
DAY       = "DAY"
BUY       = "BUY"
SELL      = "SELL"

INDEX_SECURITY_IDS = {
    "NIFTY":      "13",
    "BANKNIFTY":  "25",
    "FINNIFTY":   "27",
    "MIDCPNIFTY": "26",
    "SENSEX":     "1",
}

STRIKE_STEPS = {
    "NIFTY": 50, "BANKNIFTY": 100,
    "FINNIFTY": 50, "MIDCPNIFTY": 25, "SENSEX": 100,
}

LOT_SIZES = {
    "NIFTY": 65, "BANKNIFTY": 30,
    "FINNIFTY": 65, "MIDCPNIFTY": 120, "SENSEX": 20,
}

_EXPIRY_WDAYS = {
    "NIFTY": 1, "BANKNIFTY": 2,
    "FINNIFTY": 3, "MIDCPNIFTY": 3, "SENSEX": 4,
}


# ════════════════════════════════════════════════════════════════════
# DHAN SESSION  —  auto token refresh
# ════════════════════════════════════════════════════════════════════
class DhanSession:
    """
    Renew Token API (Dhan docs v2):
      POST https://api.dhan.co/v2/token/renew
      Headers: Content-Type: application/json
      Body:    { "access_token": "<current_jwt>", "totp": "<6-digit>" }
      Returns: { "access_token": "<new_jwt>" }

    Refreshes daily at 08:45 IST. Also refreshes immediately on startup
    if the current token expires within 2 hours.
    """
    RENEW_URL = "https://api.dhan.co/v2/token/renew"
    ENV_FILE  = Path(__file__).parent / ".env"

    def __init__(self):
        self._lock           = threading.Lock()
        self._refresh_thread = None

    # ── Lazy properties ───────────────────────────────────────────────
    @property
    def client_id(self) -> str:
        return os.getenv("DHAN_CLIENT_ID", "").strip()

    @property
    def access_token(self) -> str:
        return os.getenv("DHAN_ACCESS_TOKEN", "").strip()

    @property
    def headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Accept":       "application/json",
            "access-token": self.access_token,
            "client-id":    self.client_id,
        }

    # ── JWT expiry ────────────────────────────────────────────────────
    def _token_expires_at(self) -> Optional[datetime]:
        try:
            import base64, json as _json
            parts   = self.access_token.split(".")
            if len(parts) != 3:
                return None
            pad     = parts[1] + "=" * (4 - len(parts[1]) % 4)
            claims  = _json.loads(base64.urlsafe_b64decode(pad))
            exp     = claims.get("exp")
            return datetime.utcfromtimestamp(exp) if exp else None
        except Exception:
            return None

    def _is_expiring_soon(self, within_hours: float = 2.0) -> bool:
        exp = self._token_expires_at()
        if exp is None:
            return True
        return exp < datetime.utcnow() + timedelta(hours=within_hours)

    # ── Core refresh ─────────────────────────────────────────────────
    def refresh_token(self) -> bool:
        """
        POST /v2/token/renew  with current access_token + TOTP.
        No password required.
        """
        totp_secret = os.getenv("DHAN_TOTP_SECRET", "").strip()
        if not totp_secret:
            log.warning("DHAN_TOTP_SECRET not set — token auto-refresh disabled")
            return False

        try:
            import pyotp
        except ImportError:
            log.error("Run: pip install pyotp")
            return False

        with self._lock:
            totp = pyotp.TOTP(totp_secret).now()
            log.info(f"🔑 Renewing token (TOTP={totp})...")
            try:
                resp = requests.post(
                    self.RENEW_URL,
                    headers={"Content-Type": "application/json"},
                    json={
                        "access_token": self.access_token,
                        "totp":         totp,
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                data  = resp.json()
                token = (
                    data.get("access_token") or
                    data.get("accessToken")  or
                    data.get("jwtToken")     or ""
                )
                if not token:
                    log.error(f"Renew: no token in response: {data}")
                    return False

                os.environ["DHAN_ACCESS_TOKEN"] = token
                self._write_token_to_env(token)
                exp = self._token_expires_at()
                log.info(
                    f"✅ Token renewed"
                    f"{f' | expires {exp.strftime("%H:%M UTC")}' if exp else ''}"
                )
                return True

            except requests.HTTPError as e:
                log.error(
                    f"Token renew HTTP {e.response.status_code}: "
                    f"{e.response.text[:300]}"
                )
                return False
            except Exception as e:
                log.error(f"Token renew error: {e}")
                return False

    def _write_token_to_env(self, token: str):
        try:
            text     = self.ENV_FILE.read_text(encoding="utf-8")
            new_text = re.sub(
                r"^DHAN_ACCESS_TOKEN=.*$",
                f"DHAN_ACCESS_TOKEN={token}",
                text, flags=re.MULTILINE,
            )
            self.ENV_FILE.write_text(new_text, encoding="utf-8")
            log.info("📝 .env updated with new token")
        except Exception as e:
            log.warning(f"Could not write token to .env: {e}")

    # ── Daily scheduler ───────────────────────────────────────────────
    def _scheduler_loop(self):
        log.info("⏰ Token refresh scheduler started")
        while True:
            try:
                now    = datetime.now()
                target = now.replace(hour=8, minute=45, second=0, microsecond=0)
                if now >= target:
                    target += timedelta(days=1)
                while target.weekday() >= 5:          # skip weekends
                    target += timedelta(days=1)
                secs = (target - datetime.now()).total_seconds()
                log.info(
                    f"⏰ Next token refresh: "
                    f"{target.strftime('%Y-%m-%d %H:%M')} (in {secs/3600:.1f}h)"
                )
                time.sleep(max(secs, 1))
                if not self.refresh_token():
                    log.warning("Refresh failed — retrying in 5 min")
                    time.sleep(300)
                    self.refresh_token()
            except Exception as e:
                log.error(f"Scheduler error: {e}")
                time.sleep(60)

    # ── Public startup call ───────────────────────────────────────────
    def start_auto_refresh(self):
        if self._is_expiring_soon(within_hours=2):
            exp = self._token_expires_at()
            log.warning(
                f"⚠️  Token expires soon "
                f"({exp.strftime('%H:%M UTC') if exp else 'unknown'}) "
                f"— refreshing now"
            )
            self.refresh_token()
        else:
            exp = self._token_expires_at()
            log.info(f"✅ Token valid until {exp.strftime('%H:%M UTC') if exp else '?'}")

        if self._refresh_thread is None or not self._refresh_thread.is_alive():
            self._refresh_thread = threading.Thread(
                target=self._scheduler_loop, daemon=True, name="TokenRefresh"
            )
            self._refresh_thread.start()

    def ping(self) -> bool:
        try:
            r = requests.get(
                "https://api.dhan.co/v2/fundlimit",
                headers=self.headers, timeout=8
            )
            if r.status_code == 200:
                return True
            log.warning(f"Ping {r.status_code}: {r.text[:100]}")
            return False
        except Exception as e:
            log.warning(f"Dhan ping failed: {e}")
            return False

    def ensure_valid(self):
        if self._is_expiring_soon(within_hours=0.08):   # < 5 min
            log.warning("Token expiring in <5min — refreshing now")
            self.refresh_token()


session = DhanSession()


# ════════════════════════════════════════════════════════════════════
# INSTRUMENT MASTER
# ════════════════════════════════════════════════════════════════════
class DhanInstruments:
    BASE_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

    def __init__(self):
        self._df:        pd.DataFrame = pd.DataFrame()
        self._loaded_on: date         = None
        self._lock = threading.Lock()

    def _load(self):
        today = date.today()
        with self._lock:
            if self._loaded_on == today and not self._df.empty:
                return
            log.info("📥 Downloading Dhan instrument master...")
            try:
                r = requests.get(self.BASE_URL, timeout=30)
                r.raise_for_status()
                df = pd.read_csv(io.StringIO(r.text), low_memory=False)
                df.columns = [c.strip() for c in df.columns]
                self._df        = df
                self._loaded_on = today
                log.info(f"✅ Instrument master loaded: {len(df):,} rows")
                log.debug(f"Columns: {list(df.columns[:15])}")
            except Exception as e:
                log.error(f"Instrument master load failed: {e}")

    def security_id_for(
        self,
        index:       str,
        expiry_date: date,
        strike:      int,
        opt_type:    str,
    ) -> Optional[str]:
        self._load()
        if self._df.empty:
            return None

        df = self._df
        try:
            cols = list(df.columns)
            log.debug(f"Instrument master columns: {cols[:20]}")

            # Find correct columns — Dhan CSV uses these names:
            # SEM_TRADING_SYMBOL, SEM_EXPIRY_DATE, SEM_STRIKE_PRICE,
            # SEM_OPTION_TYPE, SEM_SMST_SECURITY_ID
            def find_col(*candidates):
                for c in candidates:
                    if c in cols:
                        return c
                return None

            sym_col    = find_col("SEM_TRADING_SYMBOL", "SM_SYMBOL_NAME",
                                  "pSymbolName", "SYMBOL")
            exp_col    = find_col("SEM_EXPIRY_DATE", "pExpiryDate",
                                  "EXPIRY_DATE", "expiryDate")
            strike_col = find_col("SEM_STRIKE_PRICE", "dStrikePrice",
                                  "STRIKE_PRICE", "strikePrice",
                                  "dStrikePrice;")
            opt_col    = find_col("SEM_OPTION_TYPE", "pOptionType",
                                  "OPTION_TYPE", "optionType")
            sec_col    = find_col("SEM_SMST_SECURITY_ID", "pSymbol",
                                  "SECURITY_ID", "securityId")

            if not all([sym_col, exp_col, strike_col, opt_col, sec_col]):
                log.error(
                    f"Could not find required columns.\n"
                    f"  sym={sym_col} exp={exp_col} "
                    f"strike={strike_col} opt={opt_col} sec={sec_col}\n"
                    f"  Available: {cols[:25]}"
                )
                return None

            # Filter: index name in symbol, option type match, strike match
            mask = (
                df[sym_col].astype(str).str.upper()
                           .str.contains(index.upper(), na=False, regex=False)
                & df[opt_col].astype(str).str.upper()
                             .str.strip().str.startswith(opt_type[:2].upper())
            )

            # Strike filter
            try:
                mask &= (df[strike_col].astype(float) == float(strike))
            except Exception:
                mask &= (df[strike_col].astype(str).str.strip() == str(strike))

            hits = df[mask].copy()

            if hits.empty:
                log.warning(
                    f"No match for {index} {strike}{opt_type} "
                    f"(checked {len(df)} rows)"
                )
                return None

            # Among hits, find closest expiry on or after today
            hits["_exp_ts"] = pd.to_datetime(
                hits[exp_col], errors="coerce", dayfirst=True
            )
            today_ts = pd.Timestamp(date.today())
            future   = hits[hits["_exp_ts"] >= today_ts]

            if future.empty:
                future = hits  # fallback: take any

            # Pick nearest expiry
            best = future.sort_values("_exp_ts").iloc[0]
            sid  = str(int(float(best[sec_col])))

            log.info(
                f"✅ security_id={sid} "
                f"({index} {best['_exp_ts'].date() if pd.notna(best['_exp_ts']) else '?'} "
                f"{strike}{opt_type})"
            )
            return sid

        except Exception as e:
            log.error(f"security_id lookup error: {e}", exc_info=True)
            return None


instruments = DhanInstruments()


# ════════════════════════════════════════════════════════════════════
# SYMBOL BUILDER
# ════════════════════════════════════════════════════════════════════
def next_expiry(index: str, expiry_type: str = "Weekly") -> date:
    idx    = index.upper()
    exp_wd = _EXPIRY_WDAYS.get(idx, 3)
    today  = date.today()

    if expiry_type == "Monthly":
        y, m  = today.year, today.month
        cands = [date(y, m, d) for d in range(1, 32)
                 if _safe_date(y, m, d)
                 and date(y, m, d).weekday() == exp_wd]
        return cands[-1] if cands else today

    ahead = (exp_wd - today.weekday()) % 7
    if ahead == 0:
        ahead = 7
    return today + timedelta(days=ahead)


def _safe_date(y, m, d) -> bool:
    try:
        date(y, m, d)
        return True
    except ValueError:
        return False


def get_security_id(
    index:       str,
    option_type: str,
    underlying:  float,
    strike_mode: str = "ATM",
    expiry_type: str = "Weekly",
) -> tuple:
    step    = STRIKE_STEPS.get(index.upper(), 50)
    atm     = int(round(underlying / step) * step)
    offsets = {"ATM": 0, "OTM1": 1, "OTM2": 2, "ITM1": -1, "ITM2": -2}
    direct  = 1 if option_type.upper() == "CE" else -1
    strike  = atm + offsets.get(strike_mode.upper(), 0) * step * direct
    expiry  = next_expiry(index, expiry_type)

    sid = instruments.security_id_for(
        index.upper(), expiry, strike, option_type.upper()
    )
    return sid, expiry, strike


# ════════════════════════════════════════════════════════════════════
# DHAN BROKER
# ════════════════════════════════════════════════════════════════════
class DhanBroker:
    BASE = "https://api.dhan.co/v2"

    def __init__(self):
        self._paper = os.getenv("PAPER_TRADE", "true").lower() == "true"

    def _post(self, path: str, payload: dict) -> dict:
        r = requests.post(
            self.BASE + path,
            headers=session.headers,
            json=payload,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def _get(self, path: str) -> dict:
        r = requests.get(
            self.BASE + path,
            headers=session.headers,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    # ── LTP ──────────────────────────────────────────────────────────
    def get_index_ltp(self, index: str) -> float:
        if self._paper:
            return 0.0
        sid = INDEX_SECURITY_IDS.get(index.upper(), "")
        if not sid:
            return 0.0
        try:
            data = self._post("/marketfeed/ltp", {"IDX_I": [int(sid)]})
            inner = (data.get("data", {})
                        .get("IDX_I", {})
                        .get(str(sid), {}))
            ltp = float(inner.get("last_price", 0) or 0)
            if ltp > 0:
                log.debug(f"Index LTP {index}: ₹{ltp:.1f}")
            return ltp
        except requests.HTTPError as e:
            log.debug(f"Index LTP ({index}): {e.response.status_code} "
                      f"{e.response.text[:80]}")
            return 0.0
        except Exception as e:
            log.debug(f"Index LTP ({index}): {e}")
            return 0.0

    def get_option_ltp(self, security_id: str) -> float:
        if self._paper:
            return 0.0
        try:
            data = self._post("/marketfeed/ltp", {"NSE_FNO": [int(security_id)]})
            inner = (data.get("data", {})
                        .get("NSE_FNO", {})
                        .get(str(security_id), {}))
            return float(inner.get("last_price", 0) or 0)
        except Exception as e:
            log.debug(f"Option LTP ({security_id}): {e}")
            return 0.0

    # ── Funds ─────────────────────────────────────────────────────────
    def get_funds(self) -> dict:
        if self._paper:
            cap = float(os.getenv("MAX_CAPITAL", "100000"))
            return {"available": cap}
        try:
            data = self._get("/fundlimit")
            avail = float(
                data.get("availabelBalance", 0) or
                data.get("availableBalance",  0) or
                data.get("net", 0) or 0
            )
            return {"available": avail, "raw": data}
        except Exception as e:
            log.error(f"Fund limits error: {e}")
            return {"available": float(os.getenv("MAX_CAPITAL", "100000"))}

    # ── Place order ───────────────────────────────────────────────────
    def place_order(
        self,
        security_id:      str,
        qty:              int,
        transaction_type: str   = BUY,
        order_type:       str   = MARKET,
        price:            float = 0,
        trigger_price:    float = 0,
    ) -> str:
        if self._paper:
            oid = f"PAPER_{transaction_type}_{security_id[-6:]}_{int(time.time())}"
            log.info(f"[PAPER] {transaction_type} {qty}×sid={security_id} → {oid}")
            return oid
        try:
            payload = {
                "dhanClientId":      session.client_id,   # required in body too
                "transactionType":   transaction_type,
                "exchangeSegment":   NSE_FNO,
                "productType":       INTRA,
                "orderType":         order_type,
                "validity":          DAY,
                "securityId":        str(security_id),
                "quantity":          qty,
                "disclosedQuantity": 0,
                "price":             price if price else 0,
                "triggerPrice":      trigger_price if trigger_price else 0,
                "afterMarketOrder":  False,
            }
            resp = self._post("/orders", payload)
            oid  = str(resp.get("orderId") or resp.get("order_id") or "")
            log.info(f"✅ Order placed: {oid} | "
                     f"{transaction_type} {qty}×sid={security_id}")
            return oid
        except requests.HTTPError as e:
            log.error(f"place_order {e.response.status_code}: "
                      f"{e.response.text[:300]}")
            raise
        except Exception as e:
            log.error(f"place_order error: {e}")
            raise

    # ── Place SL-M order ──────────────────────────────────────────────
    def place_sl_order(
        self,
        security_id:      str,
        qty:              int,
        trigger_price:    float,
        transaction_type: str = SELL,
    ) -> str:
        if self._paper:
            oid = f"PAPER_SL_{security_id[-6:]}_{int(time.time())}"
            log.info(f"[PAPER] SL-M {qty}×sid={security_id} "
                     f"trig=₹{trigger_price:.1f} → {oid}")
            return oid
        try:
            payload = {
                "dhanClientId":      session.client_id,
                "transactionType":   transaction_type,
                "exchangeSegment":   NSE_FNO,
                "productType":       INTRA,
                "orderType":         SL_M,
                "validity":          DAY,
                "securityId":        str(security_id),
                "quantity":          qty,
                "disclosedQuantity": 0,
                "price":             0,
                "triggerPrice":      round(trigger_price, 1),
                "afterMarketOrder":  False,
            }
            resp = self._post("/orders", payload)
            oid  = str(resp.get("orderId") or resp.get("order_id") or "")
            log.info(f"🛑 SL-M placed: {oid} @ ₹{trigger_price:.1f}")
            return oid
        except Exception as e:
            log.error(f"place_sl_order error: {e}")
            raise

    # ── Modify order ──────────────────────────────────────────────────
    def modify_order(
        self,
        order_id:    str,
        security_id: str,
        qty:         int,
        new_trigger: float,
        new_price:   float = 0,
    ) -> str:
        if self._paper:
            log.info(f"[PAPER] MODIFY {order_id} trig=₹{new_trigger:.1f}")
            return order_id
        try:
            payload = {
                "dhanClientId":      session.client_id,
                "orderId":           order_id,
                "orderType":         SL_M,
                "legName":           "ENTRY_LEG",
                "quantity":          qty,
                "price":             new_price if new_price else 0,
                "disclosedQuantity": 0,
                "triggerPrice":      round(new_trigger, 1),
                "validity":          DAY,
            }
            resp    = self._post(f"/orders/{order_id}", payload)
            new_oid = str(resp.get("orderId") or order_id)
            log.info(f"📝 Modified: {order_id} → ₹{new_trigger:.1f}")
            return new_oid
        except Exception as e:
            log.error(f"modify_order error: {e}")
            return order_id

    # ── Cancel order ──────────────────────────────────────────────────
    def cancel_order(self, order_id: str) -> bool:
        if self._paper:
            return True
        try:
            r = requests.delete(
                f"{self.BASE}/orders/{order_id}",
                headers=session.headers,
                timeout=8,
            )
            return r.status_code in (200, 202)
        except Exception as e:
            log.error(f"cancel_order error: {e}")
            return False

    # ── Positions ─────────────────────────────────────────────────────
    def get_positions(self) -> list:
        if self._paper:
            return []
        try:
            return self._get("/positions") or []
        except Exception as e:
            log.error(f"positions error: {e}")
            return []


broker = DhanBroker()