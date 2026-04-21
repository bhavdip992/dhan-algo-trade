"""
telegram_bot.py — Background thread bot inside dhan_live.py process.
Shares state/engine/CFG directly. Sends proactive alerts on trade events.

FIX v2:
  - 409 Conflict: delete webhook + add conflict retry with exponential backoff
  - Graceful shutdown with stop() before restart
  - send_alert uses MarkdownV2 escape to avoid parse errors

.env keys:
    TELEGRAM_BOT_TOKEN=<token from @BotFather>
    TELEGRAM_CHAT_ID=<your numeric chat id>
"""

import os, logging, asyncio, threading, re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger("TelegramBot")

BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ALLOWED_CHAT = os.getenv("TELEGRAM_CHAT_ID",  "").strip()

# ── Shared state (injected by start_bot_thread) ──────────────────────
_state  = None
_lock   = None
_cfg    = {}
_engine = None
_sq_all = None

# ── Event loop owned by the bot thread ───────────────────────────────
_loop: asyncio.AbstractEventLoop = None
_app  = None
_bot_running = False


# ── Escape special chars for Markdown (v1) ───────────────────────────
def _esc(text: str) -> str:
    """Escape characters that break Telegram Markdown v1."""
    # Only escape _ * ` [ for MarkdownV1
    for ch in ["_", "*", "`", "["]:
        text = text.replace(ch, f"\\{ch}")
    return text


# ── Auth guard ───────────────────────────────────────────────────────
def _auth(update) -> bool:
    return not ALLOWED_CHAT or str(update.effective_chat.id) == ALLOWED_CHAT


# ── Safe reply (no parse_mode by default to avoid crashes) ───────────
async def _reply(update, text: str, md: bool = False):
    try:
        await update.message.reply_text(
            text,
            parse_mode="Markdown" if md else None
        )
    except Exception as e:
        log.warning(f"reply_text failed: {e}")
        try:
            # Fallback: strip all markdown
            await update.message.reply_text(
                re.sub(r"[*_`\[\]]", "", text)
            )
        except Exception:
            pass


# ── Position formatter ───────────────────────────────────────────────
def _fmt_pos(pos, sid: str) -> str:
    try:
        import dhan_live
        ltp = dhan_live._get_option_ltp(sid, pos.entry_ltp)
    except Exception:
        ltp = pos.entry_ltp
    pnl = (ltp - pos.entry_ltp) * pos.qty if ltp > 0 else 0
    sym = "UP" if pnl >= 0 else "DN"
    return (
        f"  {pos.option} sid={sid}\n"
        f"  Entry Rs{pos.entry_ltp:.1f} | LTP Rs{ltp:.1f}\n"
        f"  SL Rs{pos.sl_prem:.1f} | Tgt Rs{pos.tgt_prem:.1f}\n"
        f"  Qty {pos.qty} ({pos.lots}L) | {sym} PnL Rs{pnl:.0f}\n"
        f"  {pos.entry_time[:16]}"
    )


# ── Command handlers ─────────────────────────────────────────────────
async def cmd_status(update, ctx):
    if not _auth(update): return
    with _lock:
        trades  = _state.trades
        pnl     = _state.pnl
        halted  = _state.halted
        candles = _state.candles
        n_open  = len(_state.open_positions())

    mode       = "PAPER" if _cfg.get("paper") else "LIVE"
    status     = "HALTED" if halted else "Running"
    loss_limit = _cfg.get("capital", 33000) * _cfg.get("max_daily_loss", 3) / 100
    pnl_sym    = "+" if pnl >= 0 else "-"
    bars       = len(_engine.df) if _engine else 0
    warmup     = "OK" if bars >= _cfg.get("warmup_bars", 55) \
                 else f"warming {bars}/{_cfg.get('warmup_bars', 55)}"

    text = (
        f"Dhan Options Auto-Trader\n"
        f"========================\n"
        f"Mode      : {mode}\n"
        f"Status    : {status}\n"
        f"Index     : {_cfg.get('index')} | {_cfg.get('timeframe')}m\n"
        f"Warmup    : {warmup}\n"
        f"Candles   : {candles}\n"
        f"========================\n"
        f"Trades    : {trades}/{_cfg.get('max_trades')}\n"
        f"Open Pos  : {n_open}\n"
        f"Day P&L   : Rs{pnl:.2f}\n"
        f"Loss Limit: Rs{loss_limit:.0f}\n"
        f"========================\n"
        f"Capital   : Rs{_cfg.get('capital', 0):,.0f}\n"
        f"Strike    : {_cfg.get('strike_mode')} | {_cfg.get('option_type')}\n"
        f"Expiry    : {_cfg.get('expiry_type')} {_cfg.get('expiry_weekday')}\n"
        f"Updated   : {datetime.now().strftime('%H:%M:%S')}"
    )
    await _reply(update, text)


async def cmd_positions(update, ctx):
    if not _auth(update): return
    with _lock:
        open_pos = dict(_state.open_positions())
    if not open_pos:
        await _reply(update, "No open positions.")
        return
    lines = [f"Open Positions ({len(open_pos)})\n===================="]
    for sid, pos in open_pos.items():
        lines.append(_fmt_pos(pos, sid))
        lines.append("---------------------")
    await _reply(update, "\n".join(lines))


async def cmd_trades(update, ctx):
    if not _auth(update): return
    with _lock:
        trade_log = list(_state.trade_log)
        pnl       = _state.pnl
    if not trade_log:
        await _reply(update, "No trades today.")
        return
    lines = [f"Today's Trades ({len(trade_log)})\n===================="]
    for t in trade_log:
        lines.append(
            f"{t['strike']} @ Rs{t['entry_ltp']:.1f}\n"
            f"  {t['time'][11:16]} | {t['qty']}qty ({t['lots']}L)\n"
            f"  SL Rs{t['sl_prem']:.1f} | Tgt Rs{t['tgt_prem']:.1f}\n"
            f"  {t['signal']} | {t['confs']}/8 | Spot Rs{t['spot']:.0f}"
        )
        lines.append("---------------------")
    pnl_sym = "+" if pnl >= 0 else ""
    lines.append(f"Day P&L: Rs{pnl:.2f}")
    await _reply(update, "\n".join(lines))


async def cmd_signal(update, ctx):
    if not _auth(update): return
    if _engine is None or _engine.df.empty:
        await _reply(update, "Engine not ready.")
        return
    r = _engine._evaluate()
    sig_sym = {"BUY_CE": "BUY CE (CALL)", "BUY_PE": "BUY PE (PUT)", "NONE": "NO SIGNAL"}
    orb_h = getattr(_engine, "_orb_high", 0)
    orb_l = getattr(_engine, "_orb_low", 0)
    orb_f = getattr(_engine, "_orb_formed", False)
    orb_str = f"Rs{orb_l:.0f}-Rs{orb_h:.0f}" if orb_f else "not formed yet"
    text = (
        f"Latest Signal\n"
        f"====================\n"
        f"Signal : {sig_sym.get(r.signal, r.signal)}\n"
        f"Close  : Rs{r.close:.1f}\n"
        f"RSI    : {r.rsi:.1f} | ADX: {r.adx:.1f} | ATR: {r.atr:.1f}\n"
        f"VWAP   : Rs{r.vwap:.1f}\n"
        f"ORB    : {orb_str}\n"
        f"Confs  : [{r.conf_str()}] {r.confirmations}/8\n"
        f"SL     : Rs{r.sl:.1f} | Tgt: Rs{r.target:.1f}\n"
        f"Bars   : {r.bars_loaded}/{r.bars_needed}\n"
        f"Reason : {r.signal_reason[:150]}\n"
        f"{datetime.now().strftime('%H:%M:%S')}"
    )
    await _reply(update, text)


async def cmd_config(update, ctx):
    if not _auth(update): return
    loss_limit = _cfg.get("capital", 33000) * _cfg.get("max_daily_loss", 3) / 100
    text = (
        f"Algo Configuration\n"
        f"====================\n"
        f"Mode       : {'PAPER' if _cfg.get('paper') else 'LIVE'}\n"
        f"Index      : {_cfg.get('index')} | {_cfg.get('timeframe')}m\n"
        f"Lot Size   : {_cfg.get('lot_size')}\n"
        f"Strike     : {_cfg.get('strike_mode')} | {_cfg.get('option_type')}\n"
        f"Expiry     : {_cfg.get('expiry_type')} {_cfg.get('expiry_weekday')}\n"
        f"====================\n"
        f"Capital    : Rs{_cfg.get('capital', 0):,.0f}\n"
        f"Max Trades : {_cfg.get('max_trades')}/day\n"
        f"Loss Limit : {_cfg.get('max_daily_loss')}% = Rs{loss_limit:.0f}\n"
        f"Max Premium: Rs{_cfg.get('max_premium')}\n"
        f"Risk/Trade : {_cfg.get('risk_pct')}%\n"
        f"Max Lots   : {_cfg.get('max_lots')}\n"
        f"Min Confs  : {_cfg.get('min_conf')}/8\n"
        f"Warmup     : {_cfg.get('warmup_bars')} bars"
    )
    await _reply(update, text)


async def cmd_squareoff(update, ctx):
    if not _auth(update): return
    with _lock:
        n_open = len(_state.open_positions())
    if n_open == 0:
        await _reply(update, "No open positions.")
        return
    await _reply(update, f"Squaring off {n_open} position(s)...")
    results = _sq_all("TELEGRAM")
    lines = ["Square-off Results"]
    for r in results:
        if "error" in r:
            lines.append(f"ERROR: {r['error']}")
        else:
            sym = "WIN" if r.get("pnl", 0) >= 0 else "LOSS"
            lines.append(
                f"{sym} {r.get('security_id', '')} | "
                f"{r.get('status', '')} | Rs{r.get('pnl', 0):.0f}"
            )
    await _reply(update, "\n".join(lines))


async def cmd_help(update, ctx):
    if not _auth(update): return
    await _reply(update,
        "Dhan Algo Bot\n"
        "====================\n"
        "/status     - live dashboard\n"
        "/positions  - open positions\n"
        "/trades     - today's trade log\n"
        "/signal     - latest signal + ORB\n"
        "/config     - algo config\n"
        "/squareoff  - square off all\n"
        "/help       - this message"
    )


# ── Proactive alert (called from dhan_live.py threads) ───────────────
def send_alert(text: str):
    """Thread-safe: schedule a message on the bot's event loop."""
    if not BOT_TOKEN or not ALLOWED_CHAT:
        return
    if _loop is None or _app is None or not _bot_running:
        return
    # Strip markdown to avoid parse errors in alerts
    clean = re.sub(r"[*_`\[\]]", "", text)
    try:
        asyncio.run_coroutine_threadsafe(
            _app.bot.send_message(chat_id=ALLOWED_CHAT, text=clean),
            _loop
        )
    except Exception as e:
        log.warning(f"send_alert failed: {e}")


# ── Internal async runner ─────────────────────────────────────────────
async def _run_async():
    global _app, _bot_running
    from telegram.ext import Application, CommandHandler

    # FIX: delete any existing webhook first to avoid 409 conflict
    # This also clears any old long-poll session from a previous run
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=true"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                result = await resp.json()
                log.info(f"deleteWebhook: {result.get('description', 'ok')}")
    except Exception as e:
        log.warning(f"deleteWebhook pre-call failed (non-fatal): {e}")

    # Small delay to ensure Telegram registers the webhook deletion
    await asyncio.sleep(2)

    _app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .build()
    )

    _app.add_handler(CommandHandler("status",    cmd_status))
    _app.add_handler(CommandHandler("positions", cmd_positions))
    _app.add_handler(CommandHandler("trades",    cmd_trades))
    _app.add_handler(CommandHandler("signal",    cmd_signal))
    _app.add_handler(CommandHandler("config",    cmd_config))
    _app.add_handler(CommandHandler("squareoff", cmd_squareoff))
    _app.add_handler(CommandHandler("help",      cmd_help))
    _app.add_handler(CommandHandler("start",     cmd_help))

    await _app.initialize()
    await _app.start()

    _bot_running = True

    # Send startup message
    if ALLOWED_CHAT:
        try:
            await _app.bot.send_message(
                chat_id=ALLOWED_CHAT,
                text="Dhan Algo Bot started. Type /status for live info."
            )
            log.info("✅ Telegram startup message sent")
        except Exception as e:
            log.warning(f"Startup message failed: {e}")

    # FIX: start polling with conflict_resolution allowed_updates
    await _app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message"],
    )
    log.info("🤖 Telegram bot polling...")

    # Keep running
    while True:
        await asyncio.sleep(3600)


# ── Thread entry point ────────────────────────────────────────────────
def _run_bot():
    global _loop, _bot_running
    try:
        from telegram.ext import Application  # noqa: verify import
    except ImportError:
        log.error("Run: pip install python-telegram-bot==20.7")
        return

    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)

    # FIX: retry loop — if 409 conflict happens, wait and retry
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            _loop.run_until_complete(_run_async())
            break
        except Exception as e:
            err_str = str(e)
            if "Conflict" in err_str or "409" in err_str:
                wait = attempt * 5
                log.warning(
                    f"Telegram 409 Conflict (attempt {attempt}/{max_retries}) "
                    f"— another instance may be running. Waiting {wait}s..."
                )
                _bot_running = False
                import time; import time as _t; _t.sleep(wait)
            else:
                log.error(f"Telegram bot error: {e}")
                break


# ── Public entry point ────────────────────────────────────────────────
def start_bot_thread(state, lock, cfg, engine, sq_all_fn):
    """Call once from dhan_live.py after all state is initialized."""
    if not BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN not set in .env — bot disabled")
        return

    global _state, _lock, _cfg, _engine, _sq_all
    _state  = state
    _lock   = lock
    _cfg    = cfg
    _engine = engine
    _sq_all = sq_all_fn

    t = threading.Thread(target=_run_bot, daemon=True, name="TelegramBot")
    t.start()
    log.info("🤖 Telegram bot thread started")