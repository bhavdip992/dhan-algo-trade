"""
telegram_bot.py — Background thread bot inside dhan_live.py process.
Shares state/engine/CFG directly. Sends proactive alerts on trade events.

.env keys:
    TELEGRAM_BOT_TOKEN=<token from @BotFather>
    TELEGRAM_CHAT_ID=<your numeric chat id>
"""

import os, logging, asyncio, threading
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


# ── Auth guard ───────────────────────────────────────────────────────
def _auth(update) -> bool:
    return not ALLOWED_CHAT or str(update.effective_chat.id) == ALLOWED_CHAT


# ── Position formatter ───────────────────────────────────────────────
def _fmt_pos(pos, sid: str) -> str:
    try:
        import dhan_live
        ltp = dhan_live._get_option_ltp(sid, pos.entry_ltp)
    except Exception:
        ltp = pos.entry_ltp
    pnl = (ltp - pos.entry_ltp) * pos.qty if ltp > 0 else 0
    sym = "🟢" if pnl >= 0 else "🔴"
    return (
        f"  *{pos.option}* `{sid}`\n"
        f"  Entry ₹{pos.entry_ltp:.1f} | LTP ₹{ltp:.1f}\n"
        f"  SL ₹{pos.sl_prem:.1f} | Tgt ₹{pos.tgt_prem:.1f}\n"
        f"  Qty {pos.qty} ({pos.lots}L) | {sym} P&L ₹{pnl:.0f}\n"
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

    mode       = "📄 PAPER" if _cfg.get("paper") else "💰 LIVE"
    status     = "🛑 HALTED" if halted else "🟢 Running"
    loss_limit = _cfg.get("capital", 33000) * _cfg.get("max_daily_loss", 3) / 100
    pnl_sym    = "🟢" if pnl >= 0 else "🔴"
    bars       = len(_engine.df) if _engine else 0
    warmup     = "✅" if bars >= _cfg.get("warmup_bars", 55) \
                 else f"⏳ {bars}/{_cfg.get('warmup_bars', 55)}"

    await update.message.reply_text(
        f"*Dhan Options Auto\\-Trader*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode      : {mode}\n"
        f"Status    : {status}\n"
        f"Index     : {_cfg.get('index')} | {_cfg.get('timeframe')}m\n"
        f"Warmup    : {warmup}\n"
        f"Candles   : {candles}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Trades    : {trades}/{_cfg.get('max_trades')}\n"
        f"Open Pos  : {n_open}\n"
        f"{pnl_sym} Day P&L : ₹{pnl:.2f}\n"
        f"Loss Limit: ₹{loss_limit:.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Capital   : ₹{_cfg.get('capital', 0):,.0f}\n"
        f"Strike    : {_cfg.get('strike_mode')} | {_cfg.get('option_type')}\n"
        f"Expiry    : {_cfg.get('expiry_type')} {_cfg.get('expiry_weekday')}\n"
        f"Updated   : {datetime.now().strftime('%H:%M:%S')}",
        parse_mode="Markdown"
    )


async def cmd_positions(update, ctx):
    if not _auth(update): return
    with _lock:
        open_pos = dict(_state.open_positions())
    if not open_pos:
        await update.message.reply_text("📭 No open positions.")
        return
    lines = [f"*Open Positions ({len(open_pos)})*\n━━━━━━━━━━━━━━━━━━━━"]
    for sid, pos in open_pos.items():
        lines.append(_fmt_pos(pos, sid))
        lines.append("─────────────────────")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_trades(update, ctx):
    if not _auth(update): return
    with _lock:
        trade_log = list(_state.trade_log)
        pnl       = _state.pnl
    if not trade_log:
        await update.message.reply_text("📭 No trades today.")
        return
    lines = [f"*Today's Trades ({len(trade_log)})*\n━━━━━━━━━━━━━━━━━━━━"]
    for t in trade_log:
        sym = "📄" if t.get("mode") == "PAPER" else "💰"
        lines.append(
            f"{sym} *{t['strike']}* @ ₹{t['entry_ltp']:.1f}\n"
            f"  {t['time'][11:16]} | {t['qty']}qty ({t['lots']}L)\n"
            f"  SL ₹{t['sl_prem']:.1f} | Tgt ₹{t['tgt_prem']:.1f}\n"
            f"  {t['signal']} | {t['confs']}/8 | Spot ₹{t['spot']:.0f}"
        )
        lines.append("─────────────────────")
    pnl_sym = "🟢" if pnl >= 0 else "🔴"
    lines.append(f"{pnl_sym} *Day P&L: ₹{pnl:.2f}*")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_signal(update, ctx):
    if not _auth(update): return
    if _engine is None or _engine.df.empty:
        await update.message.reply_text("⚠️ Engine not ready.")
        return
    r = _engine._evaluate()
    sig_sym = {"BUY_CE": "🟢 BUY CE", "BUY_PE": "🔴 BUY PE", "NONE": "⚪ NONE"}
    await update.message.reply_text(
        f"*Latest Signal*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Signal : {sig_sym.get(r.signal, r.signal)}\n"
        f"Close  : ₹{r.close:.1f}\n"
        f"RSI    : {r.rsi:.1f} | ADX: {r.adx:.1f} | ATR: {r.atr:.1f}\n"
        f"VWAP   : ₹{r.vwap:.1f}\n"
        f"Confs  : [{r.conf_str()}] {r.confirmations}/8\n"
        f"SL     : ₹{r.sl:.1f} | Tgt: ₹{r.target:.1f}\n"
        f"Bars   : {r.bars_loaded}/{r.bars_needed}\n"
        f"{r.signal_reason[:120]}\n"
        f"{datetime.now().strftime('%H:%M:%S')}",
        parse_mode="Markdown"
    )


async def cmd_config(update, ctx):
    if not _auth(update): return
    loss_limit = _cfg.get("capital", 33000) * _cfg.get("max_daily_loss", 3) / 100
    await update.message.reply_text(
        f"*Algo Configuration*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode       : {'PAPER' if _cfg.get('paper') else 'LIVE'}\n"
        f"Index      : {_cfg.get('index')} | {_cfg.get('timeframe')}m\n"
        f"Lot Size   : {_cfg.get('lot_size')}\n"
        f"Strike     : {_cfg.get('strike_mode')} | {_cfg.get('option_type')}\n"
        f"Expiry     : {_cfg.get('expiry_type')} {_cfg.get('expiry_weekday')}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Capital    : ₹{_cfg.get('capital', 0):,.0f}\n"
        f"Max Trades : {_cfg.get('max_trades')}/day\n"
        f"Loss Limit : {_cfg.get('max_daily_loss')}% = ₹{loss_limit:.0f}\n"
        f"Max Premium: ₹{_cfg.get('max_premium')}\n"
        f"Risk/Trade : {_cfg.get('risk_pct')}%\n"
        f"Max Lots   : {_cfg.get('max_lots')}\n"
        f"Min Confs  : {_cfg.get('min_conf')}/8\n"
        f"Warmup     : {_cfg.get('warmup_bars')} bars",
        parse_mode="Markdown"
    )


async def cmd_squareoff(update, ctx):
    if not _auth(update): return
    with _lock:
        n_open = len(_state.open_positions())
    if n_open == 0:
        await update.message.reply_text("📭 No open positions.")
        return
    await update.message.reply_text(f"⚡ Squaring off {n_open} position(s)...")
    results = _sq_all("TELEGRAM")
    lines = ["*Square-off Results*"]
    for r in results:
        if "error" in r:
            lines.append(f"❌ {r['error']}")
        else:
            sym = "🟢" if r.get("pnl", 0) >= 0 else "🔴"
            lines.append(
                f"{sym} {r.get('security_id', '')} | "
                f"{r.get('status', '')} | ₹{r.get('pnl', 0):.0f}"
            )
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_help(update, ctx):
    if not _auth(update): return
    await update.message.reply_text(
        "*Dhan Algo Bot*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/status     — live dashboard\n"
        "/positions  — open positions\n"
        "/trades     — today's trade log\n"
        "/signal     — latest signal\n"
        "/config     — algo config\n"
        "/squareoff  — square off all\n"
        "/help       — this message",
        parse_mode="Markdown"
    )


# ── Proactive alert (called from dhan_live.py threads) ───────────────
def send_alert(text: str):
    """Thread-safe: schedule a message on the bot's event loop."""
    if not BOT_TOKEN or not ALLOWED_CHAT:
        return
    if _loop is None or _app is None:
        return
    try:
        asyncio.run_coroutine_threadsafe(
            _app.bot.send_message(chat_id=ALLOWED_CHAT, text=text,
                                  parse_mode="Markdown"),
            _loop
        )
    except Exception as e:
        log.warning(f"send_alert failed: {e}")


# ── Internal async runner ─────────────────────────────────────────────
async def _run_async():
    global _app
    from telegram.ext import Application, CommandHandler

    _app = Application.builder().token(BOT_TOKEN).build()

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

    # Send startup message now that bot is ready
    if ALLOWED_CHAT:
        try:
            await _app.bot.send_message(
                chat_id=ALLOWED_CHAT,
                text="🤖 *Dhan Algo Bot started*\nType /status for live info.",
                parse_mode="Markdown"
            )
            log.info("✅ Telegram startup message sent")
        except Exception as e:
            log.warning(f"Startup message failed: {e}")

    await _app.updater.start_polling(drop_pending_updates=True)
    log.info("🤖 Telegram bot polling...")

    # Keep running until the thread is killed (daemon)
    while True:
        await asyncio.sleep(3600)


# ── Thread entry point ────────────────────────────────────────────────
def _run_bot():
    global _loop
    try:
        from telegram.ext import Application  # noqa: verify import
    except ImportError:
        log.error("Run: pip install python-telegram-bot==20.7")
        return

    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    try:
        _loop.run_until_complete(_run_async())
    except Exception as e:
        log.error(f"Telegram bot error: {e}")


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
