"""
telegram_bot.py — Telegram status bot for Dhan Options Auto-Trader

Commands:
  /status   — live dashboard (mode, index, P&L, trades, positions)
  /positions — open positions with entry/SL/target/current LTP
  /trades   — today's trade log
  /config   — algo config from .env
  /signal   — latest signal engine output
  /squareoff — square off all open positions (LIVE mode only)
  /help     — command list

Run alongside dhan_live.py:
  python telegram_bot.py

Add to .env:
  TELEGRAM_BOT_TOKEN=<your_bot_token>
  TELEGRAM_CHAT_ID=<your_chat_id>   # optional: restrict to one chat
"""

import os, time, logging, threading
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

try:
    from telegram import Update
    from telegram.ext import Application, CommandHandler, ContextTypes
except ImportError:
    raise SystemExit("Install: pip install python-telegram-bot")

log = logging.getLogger("TelegramBot")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")   # "" = allow all

if not BOT_TOKEN:
    raise SystemExit("Set TELEGRAM_BOT_TOKEN in .env")


# ── Import shared state from dhan_live (must be imported after dhan_live starts)
def _get_live_state():
    """Lazy import so bot can also run standalone for testing."""
    try:
        import dhan_live as live
        return live.state, live.state_lock, live.CFG, live.engine, live.square_off_all
    except Exception as e:
        log.warning(f"dhan_live not running: {e}")
        return None, None, {}, None, None


def _auth(update: Update) -> bool:
    if ALLOWED_CHAT and str(update.effective_chat.id) != ALLOWED_CHAT:
        return False
    return True


def _fmt_pos(pos, sid: str, live_ltp_fn=None) -> str:
    ltp = 0.0
    if live_ltp_fn:
        try:
            ltp = live_ltp_fn(sid, pos.entry_ltp)
        except Exception:
            ltp = pos.entry_ltp
    pnl = (ltp - pos.entry_ltp) * pos.qty if ltp > 0 else 0
    pnl_sym = "🟢" if pnl >= 0 else "🔴"
    return (
        f"  *{pos.option}* sid={sid}\n"
        f"  Entry ₹{pos.entry_ltp:.1f} | LTP ₹{ltp:.1f}\n"
        f"  SL ₹{pos.sl_prem:.1f} | Tgt ₹{pos.tgt_prem:.1f}\n"
        f"  Qty {pos.qty} ({pos.lots}L) | {pnl_sym} P&L ₹{pnl:.0f}\n"
        f"  Status: {pos.status} | Entry: {pos.entry_time[:16]}"
    )


# ── /status ──────────────────────────────────────────────────────────
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _auth(update): return
    state, lock, cfg, engine, _ = _get_live_state()

    if state is None:
        await update.message.reply_text("⚠️ Algo not running.")
        return

    with lock:
        trades   = state.trades
        pnl      = state.pnl
        halted   = state.halted
        candles  = state.candles
        n_open   = len(state.open_positions())

    mode  = "📄 PAPER" if cfg.get("paper") else "💰 LIVE"
    halt  = "🛑 HALTED" if halted else "🟢 Running"
    loss_limit = cfg.get("capital", 10000) * cfg.get("max_daily_loss", 3) / 100
    pnl_sym = "🟢" if pnl >= 0 else "🔴"

    bars_loaded = len(engine.df) if engine else 0
    warmup_done = "✅" if bars_loaded >= cfg.get("warmup_bars", 55) else f"⏳ {bars_loaded}/{cfg.get('warmup_bars',55)}"

    msg = (
        f"*Dhan Options Auto-Trader*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode     : {mode}\n"
        f"Status   : {halt}\n"
        f"Index    : {cfg.get('index','?')} | TF: {cfg.get('timeframe','?')}m\n"
        f"Warmup   : {warmup_done}\n"
        f"Candles  : {candles}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Trades   : {trades}/{cfg.get('max_trades','?')}\n"
        f"Open Pos : {n_open}\n"
        f"{pnl_sym} Day P&L : ₹{pnl:.2f}\n"
        f"Loss Limit: ₹{loss_limit:.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Capital  : ₹{cfg.get('capital',0):,.0f}\n"
        f"Max Lots : {cfg.get('max_lots','?')}\n"
        f"Strike   : {cfg.get('strike_mode','?')} | {cfg.get('option_type','?')}\n"
        f"Expiry   : {cfg.get('expiry_type','?')} {cfg.get('expiry_weekday','?')}\n"
        f"_Updated: {datetime.now().strftime('%H:%M:%S')}_"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


# ── /positions ───────────────────────────────────────────────────────
async def cmd_positions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _auth(update): return
    state, lock, cfg, _, _ = _get_live_state()

    if state is None:
        await update.message.reply_text("⚠️ Algo not running.")
        return

    try:
        from dhan_live import _get_option_ltp
        ltp_fn = _get_option_ltp
    except Exception:
        ltp_fn = None

    with lock:
        open_pos = dict(state.open_positions())

    if not open_pos:
        await update.message.reply_text("📭 No open positions.")
        return

    lines = [f"*Open Positions ({len(open_pos)})*\n━━━━━━━━━━━━━━━━━━━━"]
    for sid, pos in open_pos.items():
        lines.append(_fmt_pos(pos, sid, ltp_fn))
        lines.append("─────────────────────")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /trades ──────────────────────────────────────────────────────────
async def cmd_trades(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _auth(update): return
    state, lock, cfg, _, _ = _get_live_state()

    if state is None:
        await update.message.reply_text("⚠️ Algo not running.")
        return

    with lock:
        log_copy = list(state.trade_log)
        pnl      = state.pnl

    if not log_copy:
        await update.message.reply_text("📭 No trades today.")
        return

    lines = [f"*Today's Trades ({len(log_copy)})*\n━━━━━━━━━━━━━━━━━━━━"]
    for t in log_copy:
        mode_sym = "📄" if t.get("mode") == "PAPER" else "💰"
        lines.append(
            f"{mode_sym} *{t['strike']}* @ ₹{t['entry_ltp']:.1f}\n"
            f"  Time: {t['time'][11:16]} | Qty: {t['qty']} ({t['lots']}L)\n"
            f"  SL ₹{t['sl_prem']:.1f} | Tgt ₹{t['tgt_prem']:.1f}\n"
            f"  Signal: {t['signal']} | Confs: {t['confs']}/8\n"
            f"  Spot: ₹{t['spot']:.1f} | Cost: ₹{t['cost']:.0f}"
        )
        lines.append("─────────────────────")

    pnl_sym = "🟢" if pnl >= 0 else "🔴"
    lines.append(f"{pnl_sym} *Day P&L: ₹{pnl:.2f}*")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /signal ──────────────────────────────────────────────────────────
async def cmd_signal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _auth(update): return
    _, _, cfg, engine, _ = _get_live_state()

    if engine is None or engine.df.empty:
        await update.message.reply_text("⚠️ Engine not ready.")
        return

    df = engine.df
    if len(df) < 2:
        await update.message.reply_text("⏳ Warming up...")
        return

    # Re-evaluate last bar
    last = df.iloc[-1]
    result = engine._evaluate()

    sig_sym = {"BUY_CE": "🟢 BUY CE", "BUY_PE": "🔴 BUY PE", "NONE": "⚪ NONE"}
    msg = (
        f"*Latest Signal*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Signal  : {sig_sym.get(result.signal, result.signal)}\n"
        f"Close   : ₹{result.close:.1f}\n"
        f"RSI     : {result.rsi:.1f}\n"
        f"ADX     : {result.adx:.1f}\n"
        f"ATR     : {result.atr:.1f}\n"
        f"VWAP    : ₹{result.vwap:.1f}\n"
        f"Confs   : [{result.conf_str()}] {result.confirmations}/8\n"
        f"SL      : ₹{result.sl:.1f}\n"
        f"Target  : ₹{result.target:.1f}\n"
        f"Bars    : {result.bars_loaded}/{result.bars_needed}\n"
        f"Reason  : `{result.signal_reason[:120]}`\n"
        f"_Time: {datetime.now().strftime('%H:%M:%S')}_"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


# ── /config ──────────────────────────────────────────────────────────
async def cmd_config(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _auth(update): return
    _, _, cfg, _, _ = _get_live_state()

    if not cfg:
        await update.message.reply_text("⚠️ Config not available.")
        return

    loss_limit = cfg.get("capital", 10000) * cfg.get("max_daily_loss", 3) / 100
    msg = (
        f"*Algo Configuration*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Mode       : {'PAPER' if cfg.get('paper') else 'LIVE'}\n"
        f"Index      : {cfg.get('index')}\n"
        f"Lot Size   : {cfg.get('lot_size')}\n"
        f"Strike Step: {cfg.get('strike_step')}\n"
        f"Expiry     : {cfg.get('expiry_type')} {cfg.get('expiry_weekday')}\n"
        f"Strike Mode: {cfg.get('strike_mode')}\n"
        f"Option Type: {cfg.get('option_type')}\n"
        f"Timeframe  : {cfg.get('timeframe')}m\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Capital    : ₹{cfg.get('capital',0):,.0f}\n"
        f"Max Trades : {cfg.get('max_trades')}/day\n"
        f"Loss Limit : {cfg.get('max_daily_loss')}% = ₹{loss_limit:.0f}\n"
        f"Max Premium: ₹{cfg.get('max_premium')}\n"
        f"Risk/Trade : {cfg.get('risk_pct')}%\n"
        f"Max Lots   : {cfg.get('max_lots')}\n"
        f"Min Confs  : {cfg.get('min_conf')}/8\n"
        f"Warmup Bars: {cfg.get('warmup_bars')}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


# ── /squareoff ───────────────────────────────────────────────────────
async def cmd_squareoff(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _auth(update): return
    state, lock, cfg, _, sq_all = _get_live_state()

    if sq_all is None:
        await update.message.reply_text("⚠️ Algo not running.")
        return

    with lock:
        n_open = len(state.open_positions())

    if n_open == 0:
        await update.message.reply_text("📭 No open positions to square off.")
        return

    await update.message.reply_text(f"⚡ Squaring off {n_open} position(s)...")
    results = sq_all("TELEGRAM")

    lines = ["*Square-off Results*"]
    for r in results:
        if "error" in r:
            lines.append(f"❌ {r['error']}")
        else:
            sym = "🟢" if r.get("pnl", 0) >= 0 else "🔴"
            lines.append(f"{sym} sid={r['sid']} | {r['status']} | P&L ₹{r.get('pnl',0):.0f}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ── /help ────────────────────────────────────────────────────────────
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _auth(update): return
    msg = (
        "*Dhan Algo Bot Commands*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "/status     — live dashboard\n"
        "/positions  — open positions with LTP\n"
        "/trades     — today's trade log\n"
        "/signal     — latest signal engine output\n"
        "/config     — algo configuration\n"
        "/squareoff  — square off all positions\n"
        "/help       — this message"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


# ── Startup alert ────────────────────────────────────────────────────
async def _send_startup(app):
    if not ALLOWED_CHAT:
        return
    try:
        await app.bot.send_message(
            chat_id=ALLOWED_CHAT,
            text="🤖 *Dhan Algo Bot started*\nType /status for live info.",
            parse_mode="Markdown"
        )
    except Exception as e:
        log.warning(f"Startup message failed: {e}")


# ── Main ─────────────────────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("trades",    cmd_trades))
    app.add_handler(CommandHandler("signal",    cmd_signal))
    app.add_handler(CommandHandler("config",    cmd_config))
    app.add_handler(CommandHandler("squareoff", cmd_squareoff))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("start",     cmd_help))

    app.post_init = _send_startup

    log.info("🤖 Telegram bot polling...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
