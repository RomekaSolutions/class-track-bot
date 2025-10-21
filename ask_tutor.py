"""Tutor question workflow for students and admins."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import Any, Dict, Iterable, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from data_store import load_students
from helpers import try_ack

try:  # pragma: no cover - circular import guard when running tests
    from class_track_bot import BASE_TZ, safe_edit_or_send
except Exception:  # pragma: no cover - fallback when imported before main module
    BASE_TZ = timezone(timedelta(hours=7))

    async def safe_edit_or_send(  # type: ignore
        query,
        text: str,
        reply_markup=None,
        parse_mode=None,
        disable_web_page_preview: bool = True,
    ) -> None:
        target = getattr(query, "message", None) or getattr(query, "effective_message", None)
        if target and hasattr(target, "reply_text"):
            await target.reply_text(text, reply_markup=reply_markup)
        elif hasattr(query, "reply_text"):
            await query.reply_text(text, reply_markup=reply_markup)


ASKS_FILE = "asks.json"
MAX_MESSAGES = 10
CONV_END = getattr(ConversationHandler, "END", -1)
CONV_TIMEOUT = getattr(ConversationHandler, "TIMEOUT", object())


class AskStates(IntEnum):
    """Conversation states for the student ask flow."""

    COLLECTING = 100
    CONFIRM = 101


@dataclass
class AskSession:
    """Transient storage while a student is composing an ask."""

    student_id: str
    student_name: str
    student_handle: Optional[str]
    chat_id: int
    messages: List[Dict[str, Any]]
    message_ids: List[int]


ADMIN_IDS: List[int] = []


def load_asks() -> List[Dict[str, Any]]:
    """Return the persisted asks list from disk."""

    if not os.path.exists(ASKS_FILE):
        return []
    try:
        with open(ASKS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        logging.warning("Failed to read asks.json; starting from empty list")
        return []
    if not isinstance(data, list):
        logging.warning("asks.json malformed; resetting to empty list")
        return []
    cleaned: List[Dict[str, Any]] = []
    for entry in data:
        if isinstance(entry, dict):
            cleaned.append(entry)
    return cleaned


def save_asks(asks: Iterable[Dict[str, Any]]) -> None:
    """Persist ``asks`` atomically following the data store pattern."""

    data = list(asks)
    tmp_path = f"{ASKS_FILE}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
            try:
                fh.flush()
                os.fsync(fh.fileno())
            except Exception:
                pass
        os.replace(tmp_path, ASKS_FILE)
    except Exception as exc:
        logging.error("Failed to persist asks.json atomically: %s", exc)
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def _prepare_session(student_id: str, chat_id: int) -> AskSession:
    students = load_students()
    student = students.get(str(student_id), {}) if isinstance(students, dict) else {}
    name = student.get("name") or f"Student {student_id}"
    handle = student.get("telegram_handle")
    if handle and not handle.startswith("@"):
        handle = f"@{handle}"
    return AskSession(
        student_id=str(student_id),
        student_name=str(name),
        student_handle=handle,
        chat_id=int(chat_id),
        messages=[],
        message_ids=[],
    )


def _get_session(context: ContextTypes.DEFAULT_TYPE) -> Optional[AskSession]:
    data = context.user_data.get("ask_tutor")
    if isinstance(data, AskSession):
        return data
    return None


def _store_session(context: ContextTypes.DEFAULT_TYPE, session: AskSession) -> None:
    context.user_data["ask_tutor"] = session


def _clear_session(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("ask_tutor", None)


def _format_summary(ask: Dict[str, Any]) -> str:
    status = ask.get("status", "new")
    status_icon = {
        "new": "🆕",
        "seen": "👀",
        "resolved": "✅",
    }.get(status, "📬")
    handle = ask.get("student_handle")
    student_line = ask.get("student_name") or ask.get("student_id")
    if handle:
        student_line = f"{student_line} ({handle})"
    submitted = ask.get("submitted_at")
    seen_at = ask.get("seen_at")
    resolved_at = ask.get("resolved_at")
    lines = [
        f"{status_icon} Ask #{ask.get('id')}",
        f"Student: {student_line}",
        f"Submitted: {submitted}",
        f"Messages: {len(ask.get('messages', []))}",
    ]
    if seen_at:
        lines.append(f"Seen: {seen_at}")
    if resolved_at:
        lines.append(f"Resolved: {resolved_at}")
    lines.append("")
    lines.append("Messages:")
    for idx, item in enumerate(ask.get("messages", []), start=1):
        mtype = item.get("type")
        if mtype == "text":
            text = item.get("text", "").strip()
            if not text:
                text = "(empty)"
            lines.append(f"{idx}. 📝 {text}")
        elif mtype == "photo":
            caption = item.get("caption")
            suffix = f" — {caption}" if caption else ""
            lines.append(f"{idx}. 🖼️ Photo{suffix}")
        elif mtype == "voice":
            duration = item.get("duration")
            if duration:
                lines.append(f"{idx}. 🎙️ Voice ({duration}s)")
            else:
                lines.append(f"{idx}. 🎙️ Voice")
        else:
            lines.append(f"{idx}. {mtype or 'message'}")
    return "\n".join(lines)


def _build_detail_keyboard(ask: Dict[str, Any]) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    ask_id = ask.get("id")
    status = ask.get("status")
    if status != "resolved":
        buttons.append([InlineKeyboardButton("👁️ Mark Seen", callback_data=f"ask:seen:{ask_id}")])
        buttons.append([InlineKeyboardButton("✅ Resolve", callback_data=f"ask:resolved:{ask_id}")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="admin:asks")])
    return InlineKeyboardMarkup(buttons)


async def start_ask(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Entry point for the student ask flow."""

    query = update.callback_query
    if not query or not query.data:
        return CONV_END
    await try_ack(query)
    parts = query.data.split(":", maxsplit=2)
    if len(parts) != 3:
        await safe_edit_or_send(query, "Unable to start the ask session right now.")
        return CONV_END
    _, _, student_id = parts
    chat = update.effective_chat
    chat_id = getattr(chat, "id", None)
    if chat_id is None:
        await safe_edit_or_send(query, "Unable to determine chat for ask session.")
        return CONV_END
    session = _prepare_session(student_id, chat_id)
    _store_session(context, session)
    message = (
        "📝 Leave a message in Gregor's tray (text/image/voice). "
        "He'll check it before your next class. Type /done when finished."
    )
    await safe_edit_or_send(query, message)
    return AskStates.COLLECTING


async def collect_ask_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Collect a single message from the student."""

    session = _get_session(context)
    if not session:
        await update.message.reply_text("Please tap 'Ask Tutor' to start a new question.")
        return ConversationHandler.END
    if len(session.messages) >= MAX_MESSAGES:
        await update.message.reply_text(
            "You've reached the limit of 10 messages. Type /done to send your ask."
        )
        return AskStates.COLLECTING

    message = update.message
    if message is None:
        return AskStates.COLLECTING

    payload: Dict[str, Any]
    if message.text:
        payload = {"type": "text", "text": message.text}
    elif message.photo:
        file_id = message.photo[-1].file_id if message.photo else None
        if not file_id:
            await message.reply_text("Couldn't read that photo. Please try again.")
            return AskStates.COLLECTING
        payload = {
            "type": "photo",
            "file_id": file_id,
            "caption": message.caption,
        }
    elif message.voice:
        payload = {
            "type": "voice",
            "file_id": message.voice.file_id,
            "duration": message.voice.duration,
        }
    else:
        await message.reply_text("Please send text, photo, or voice messages only.")
        return AskStates.COLLECTING

    session.messages.append(payload)
    if message.message_id is not None:
        session.message_ids.append(message.message_id)
    _store_session(context, session)
    await message.reply_text(
        f"Saved message {len(session.messages)}/{MAX_MESSAGES}. "
        "Send more or /done when ready."
    )
    return AskStates.COLLECTING


async def finish_ask(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Prompt the student to confirm submission."""

    session = _get_session(context)
    if not session or not session.messages:
        await update.message.reply_text(
            "Please send at least one message before finishing your ask."
        )
        return AskStates.COLLECTING
    count = len(session.messages)
    summary = f"You submitted {count} message{'s' if count != 1 else ''}. Send now?"
    buttons = [
        [
            InlineKeyboardButton("Yes", callback_data=f"ask:confirm:{session.student_id}"),
            InlineKeyboardButton("Cancel", callback_data=f"ask:cancel:{session.student_id}"),
        ]
    ]
    await update.message.reply_text(summary, reply_markup=InlineKeyboardMarkup(buttons))
    return AskStates.CONFIRM


def _next_ask_id(asks: List[Dict[str, Any]]) -> int:
    max_id = 0
    for entry in asks:
        try:
            max_id = max(max_id, int(entry.get("id", 0)))
        except Exception:
            continue
    return max_id + 1


def _build_record(session: AskSession, asks: List[Dict[str, Any]]) -> Dict[str, Any]:
    now = datetime.now(BASE_TZ).isoformat()
    return {
        "id": _next_ask_id(asks),
        "student_id": session.student_id,
        "student_name": session.student_name,
        "student_handle": session.student_handle,
        "submitted_at": now,
        "status": "new",
        "seen_at": None,
        "resolved_at": None,
        "messages": list(session.messages),
        "origin": {
            "chat_id": session.chat_id,
            "message_ids": list(session.message_ids),
        },
    }


async def notify_admins_new_ask(
    context: ContextTypes.DEFAULT_TYPE, ask: Dict[str, Any]
) -> None:
    """Inform admins that a new ask has arrived."""

    if not ADMIN_IDS:
        return
    bot = getattr(context, "bot", None)
    if bot is None:
        application = getattr(context, "application", None)
        bot = getattr(application, "bot", None)
    if bot is None:
        return
    text = (
        f"📩 New ask from {ask.get('student_name')} "
        f"({ask.get('student_handle') or ask.get('student_id')})."
    )
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("View Ask", callback_data=f"ask:view:{ask.get('id')}")]]
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=markup)
        except Exception as exc:  # pragma: no cover - network/telegram errors
            logging.warning("Failed to notify admin %s of new ask: %s", admin_id, exc)


async def confirm_send_ask(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Persist the ask and notify admins."""

    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await try_ack(query)
    session = _get_session(context)
    if not session or not session.messages:
        await safe_edit_or_send(query, "This ask session has expired. Please start again.")
        _clear_session(context)
        return ConversationHandler.END
    asks = load_asks()
    record = _build_record(session, asks)
    asks.append(record)
    save_asks(asks)
    await safe_edit_or_send(
        query,
        "✅ I'll pop this in Gregor's tray and he'll read it before your next class",
    )
    await notify_admins_new_ask(context, record)
    _clear_session(context)
    return CONV_END


async def cancel_ask(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Abort the current ask session."""

    query = getattr(update, "callback_query", None)
    if query:
        await try_ack(query)
        await safe_edit_or_send(query, "Ask cancelled")
    else:
        if update.message:
            await update.message.reply_text("Ask cancelled")
    _clear_session(context)
    return CONV_END


async def ask_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle ask conversation timeout."""

    _clear_session(context)
    chat = update.effective_chat
    bot = getattr(context, "bot", None)
    if bot and chat:
        try:
            await bot.send_message(
                chat.id,
                "⏱️ Session timed out. Tap 'Ask Tutor' again when ready.",
            )
        except Exception as exc:  # pragma: no cover - depends on telegram
            logging.debug("Failed to send timeout message: %s", exc)
    return CONV_END


async def admin_view_asks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List outstanding asks for admins."""

    query = update.callback_query
    if not query:
        return
    await try_ack(query)
    asks = load_asks()
    pending = [a for a in asks if a.get("status") in {"new", "seen"}]
    pending.sort(key=lambda a: (a.get("status") != "new", a.get("submitted_at") or ""))
    lines = ["📥 Tutor Inbox"]
    buttons: List[List[InlineKeyboardButton]] = []
    if not pending:
        lines.append("No pending asks. Enjoy your day! ✨")
    else:
        for ask in pending:
            status = ask.get("status")
            icon = "🆕" if status == "new" else "👀"
            label = f"{icon} #{ask.get('id')} {ask.get('student_name')}"
            buttons.append(
                [InlineKeyboardButton(label, callback_data=f"ask:view:{ask.get('id')}")]
            )
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="admin:root")])
    await safe_edit_or_send(query, "\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons))


async def _send_media_copy(
    query, context: ContextTypes.DEFAULT_TYPE, ask: Dict[str, Any]
) -> None:
    bot = getattr(context, "bot", None)
    if bot is None:
        application = getattr(context, "application", None)
        bot = getattr(application, "bot", None)
    if bot is None:
        return
    origin = ask.get("origin") or {}
    from_chat = origin.get("chat_id")
    message_ids = origin.get("message_ids") or []
    if not from_chat or not isinstance(message_ids, list):
        return
    chat_id = getattr(query.message, "chat_id", None)
    if chat_id is None:
        return
    for item, message_id in zip(ask.get("messages", []), message_ids):
        if item.get("type") == "text":
            continue
        try:
            await bot.copy_message(chat_id, from_chat, message_id)
        except Exception as exc:  # pragma: no cover - depends on telegram
            logging.debug("Failed to copy ask message %s: %s", message_id, exc)


async def admin_view_ask_detail(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the detail for a specific ask."""

    query = update.callback_query
    if not query or not query.data:
        return
    await try_ack(query)
    try:
        ask_id = int(query.data.split(":")[-1])
    except Exception:
        await safe_edit_or_send(query, "Unable to open that ask right now.")
        return
    asks = load_asks()
    ask = next((a for a in asks if a.get("id") == ask_id), None)
    if not ask:
        await safe_edit_or_send(query, "Ask not found (it may have been resolved).")
        return
    text = _format_summary(ask)
    await safe_edit_or_send(query, text, reply_markup=_build_detail_keyboard(ask))
    await _send_media_copy(query, context, ask)


def _update_status(ask: Dict[str, Any], status: str) -> None:
    now = datetime.now(BASE_TZ).isoformat()
    ask["status"] = status
    if status == "seen" and not ask.get("seen_at"):
        ask["seen_at"] = now
    if status == "resolved":
        if not ask.get("seen_at"):
            ask["seen_at"] = now
        ask["resolved_at"] = now


async def admin_mark_seen(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mark an ask as seen."""

    query = update.callback_query
    if not query or not query.data:
        return
    await try_ack(query)
    try:
        ask_id = int(query.data.split(":")[-1])
    except Exception:
        return
    asks = load_asks()
    ask = next((a for a in asks if a.get("id") == ask_id), None)
    if not ask:
        await safe_edit_or_send(query, "Ask not found (it may have been resolved).")
        return
    if ask.get("status") != "resolved":
        _update_status(ask, "seen")
        save_asks(asks)
    text = _format_summary(ask)
    await safe_edit_or_send(query, text, reply_markup=_build_detail_keyboard(ask))


async def admin_mark_resolved(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mark an ask as resolved."""

    query = update.callback_query
    if not query or not query.data:
        return
    await try_ack(query)
    try:
        ask_id = int(query.data.split(":")[-1])
    except Exception:
        return
    asks = load_asks()
    ask = next((a for a in asks if a.get("id") == ask_id), None)
    if not ask:
        await safe_edit_or_send(query, "Ask not found (it may have been resolved).")
        return
    _update_status(ask, "resolved")
    save_asks(asks)
    text = _format_summary(ask)
    await safe_edit_or_send(query, text, reply_markup=_build_detail_keyboard(ask))


def register_handlers(application: Application, admin_ids: List[int]) -> None:
    """Register conversation and admin handlers for the ask tutor module."""

    global ADMIN_IDS
    ADMIN_IDS = [int(a) for a in admin_ids]

    ask_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(
                start_ask,
                pattern=r"^ask:start:\d+$",
                block=True,
            )
        ],
        states={
            AskStates.COLLECTING: [
                CommandHandler("done", finish_ask, filters=filters.ChatType.PRIVATE),
                MessageHandler(
                    filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
                    collect_ask_message,
                ),
                MessageHandler(
                    filters.ChatType.PRIVATE & filters.PHOTO,
                    collect_ask_message,
                ),
                MessageHandler(
                    filters.ChatType.PRIVATE & filters.VOICE,
                    collect_ask_message,
                ),
            ],
            AskStates.CONFIRM: [
                CallbackQueryHandler(
                    confirm_send_ask, pattern=r"^ask:confirm:\d+$", block=True
                ),
                CallbackQueryHandler(
                    cancel_ask, pattern=r"^ask:cancel:\d+$", block=True
                ),
            ],
            CONV_TIMEOUT: [
                MessageHandler(filters.ChatType.PRIVATE & filters.ALL, ask_timeout)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_ask, filters=filters.ChatType.PRIVATE)],
        conversation_timeout=600,
        allow_reentry=True,
        name="ask_tutor",
    )
    application.add_handler(ask_conversation)
    application.add_handler(CallbackQueryHandler(admin_view_asks, pattern=r"^admin:asks$"))
    application.add_handler(CallbackQueryHandler(admin_view_ask_detail, pattern=r"^ask:view:\d+$"))
    application.add_handler(CallbackQueryHandler(admin_mark_seen, pattern=r"^ask:seen:\d+$"))
    application.add_handler(
        CallbackQueryHandler(admin_mark_resolved, pattern=r"^ask:resolved:\d+$")
    )

