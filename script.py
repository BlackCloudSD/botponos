# script_fixed.py
import asyncio
import io
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Dict, Optional, Set, List, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters.command import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.exceptions import TelegramBadRequest, TelegramAPIError

# ----------------- Логи -----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ----------------- Конфиг -----------------
TOKEN = ("8263730440:AAGz5I3gBjAD2FwrhIcrL5ZIZ-nkzShXfak")


# Замените на ваши админ ID
ADMINS: Set[int] = {6958734279, 1865077709}

TOTAL_SECONDS = 86400                # 24 часа — для статуса
SHORT_ANIMATION_SECONDS = 10         # 10 секунд — короткая анимация
LONG_WORKER_INTERVAL = 60            # частота обновления long worker (сек)
PERSIST_FILE = "requests_db.json"
TELEGRAM_MSG_LIMIT = 4000

# ----------------- Инициализация -----------------
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

requests_db: Dict[int, Dict[str, Any]] = {}
_next_request_id: int = 1

requests_lock = asyncio.Lock()
_next_id_lock = asyncio.Lock()
file_lock = asyncio.Lock()

tasks_by_request: Dict[int, Dict[str, Optional[asyncio.Task]]] = {}
blocked_users: Dict[int, str] = {}

# FSM
class AdminManage(StatesGroup):
    waiting_block_reason = State()

class SendMessage(StatesGroup):
    waiting_for_text = State()


# ----------------- UI -----------------
def keyboard_button(text: str) -> KeyboardButton:
    return KeyboardButton(text=text)

def main_keyboard(is_admin: bool = False) -> ReplyKeyboardMarkup:
    kb: List[List[KeyboardButton]] = [
        [keyboard_button("📤 Отправить сообщение")],
        [keyboard_button("📖 Туториал")],
        [keyboard_button("📊 Статус")]
    ]
    if is_admin:
        kb.append([keyboard_button("🛠 Админ панель")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [keyboard_button("👥 Список запросов")],
        [keyboard_button("🚫 Управление пользователями")],
        [keyboard_button("⬅ Назад")]
    ], resize_keyboard=True)

def generate_progress_bar(percent: int, blocks: int = 5) -> str:
    percent = max(0, min(100, percent))
    filled = int((percent * blocks) / 100)
    filled = min(blocks, max(0, filled))
    return "✅" * filled + "⬜" * (blocks - filled) + f" {percent}%"

def format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M UTC")


# ----------------- Persistence -----------------
async def save_requests_db() -> None:
    async with file_lock:
        try:
            safe: Dict[str, Dict[str, Any]] = {}
            async with requests_lock:
                for rid, r in requests_db.items():
                    copy = r.copy()
                    ca = copy.get("created_at")
                    if isinstance(ca, datetime):
                        copy["created_at"] = ca.isoformat()
                    safe[str(rid)] = copy
            tmp_path = PERSIST_FILE + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(safe, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, PERSIST_FILE)
            logger.debug("requests_db saved")
        except Exception:
            logger.exception("Ошибка при сохранении requests_db")

async def load_requests_db() -> None:
    global _next_request_id
    if not os.path.exists(PERSIST_FILE):
        return
    async with file_lock:
        try:
            with open(PERSIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for k, v in data.items():
                rid = int(k)
                ca = v.get("created_at")
                if isinstance(ca, str):
                    try:
                        v["created_at"] = datetime.fromisoformat(ca)
                    except Exception:
                        v["created_at"] = datetime.now(timezone.utc)
                requests_db[rid] = v
            if requests_db:
                _next_request_id = max(requests_db.keys()) + 1
            logger.info("requests_db loaded, items=%d", len(requests_db))
        except Exception:
            logger.exception("Ошибка при загрузке requests_db")


# ----------------- Текстовые утилиты -----------------
def truncate_text_for_telegram(text: str, limit: int = TELEGRAM_MSG_LIMIT):
    if not text:
        return "", False
    if len(text) <= limit:
        return text, False
    safe_cut = max(0, limit - 200)
    preview = text[:safe_cut].rstrip()
    preview += "\n\n[...Текст слишком длинный — нажмите «Показать полностью»]"
    return preview, True

async def send_full_text_as_file(chat_id: int, filename: str, content: str):
    """
    Попытка отправить BytesIO; fallback -> временный файл.
    """
    bio = io.BytesIO()
    bio.write(content.encode("utf-8"))
    bio.seek(0)

    # Попробуем отправить BytesIO (работает в большинстве версий aiogram)
    try:
        await bot.send_document(chat_id=chat_id, document=bio, filename=filename)
        return
    except TypeError as e:
        logger.info("send_document(BytesIO) вызвал TypeError, fallback to temp file: %s", e)
    except TelegramAPIError:
        logger.exception("Telegram API error при отправке BytesIO, fallback to temp file.")
    except Exception as e:
        logger.exception("Не удалось отправить BytesIO (попытка fallback): %s", e)

    # Fallback: временный файл
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tf:
            tf.write(content.encode("utf-8"))
            tmp_path = tf.name
        await bot.send_document(chat_id=chat_id, document=tmp_path)
    except Exception:
        logger.exception("Не удалось отправить файл по временному пути.")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                logger.exception("Не удалось удалить временный файл %s", tmp_path)


# ----------------- Safe update message -----------------
async def safe_update_progress_message(rid: int, text: str) -> None:
    async with requests_lock:
        req = requests_db.get(rid)
        if not req:
            return
        chat_id = req["chat_id"]
        msg_id = req.get("progress_message_id")

    # Для safety: обрезаем текст если слишком большой (прогресс обычно небольшой — на всякий случай)
    safe_text, _ = truncate_text_for_telegram(text, limit=TELEGRAM_MSG_LIMIT)

    if msg_id is None:
        try:
            sent = await bot.send_message(chat_id, safe_text)
            async with requests_lock:
                if rid in requests_db:
                    requests_db[rid]["progress_message_id"] = sent.message_id
            await save_requests_db()
            return
        except Exception:
            logger.exception("Не удалось отправить новое прогресс-сообщение")
            return

    try:
        await bot.edit_message_text(text=safe_text, chat_id=chat_id, message_id=msg_id)
    except TelegramBadRequest as e:
        logger.info("Не удалось редактировать прогресс-сообщение для #%s: %s — отправляю новое", rid, e)
        try:
            sent = await bot.send_message(chat_id, safe_text)
            async with requests_lock:
                if rid in requests_db:
                    requests_db[rid]["progress_message_id"] = sent.message_id
            await save_requests_db()
        except Exception:
            logger.exception("Не удалось отправить новое прогресс-сообщение после ошибки редактирования")
    except TelegramAPIError:
        logger.exception("Telegram API error при редактировании сообщения для #%s", rid)
    except Exception:
        logger.exception("Неожиданная ошибка при safe_update_progress_message для #%s", rid)


async def update_progress_message(rid: int) -> None:
    async with requests_lock:
        req = requests_db.get(rid)
        if not req:
            return
        percent = int(req.get("progress", 0))
        status = req.get("status", "processing")
    status_text = {
        "processing": "⏳ В обработке",
        "done": "✅ Отправлено",
        "failed": "❌ Не удалось"
    }.get(status, status)
    text = (
        f"{status_text} — Запрос #{rid}\n"
        f"{generate_progress_bar(percent)}\n\n"
        f"Отправлен: {format_dt(req['created_at'])}\n"
    )
    await safe_update_progress_message(rid, text)


# ----------------- Mark done / failed -----------------
async def mark_request_done(rid: int, by_admin: bool = False) -> None:
    async with requests_lock:
        r = requests_db.get(rid)
        if not r:
            return
        if r.get("status") == "done":
            return
        r["status"] = "done"
        r["progress"] = 100
    entry = tasks_by_request.pop(rid, {})
    for t in entry.values():
        if t and not t.done():
            t.cancel()
    await update_progress_message(rid)
    try:
        async with requests_lock:
            chat_id = requests_db[rid]["chat_id"]
        await bot.send_message(chat_id, f"✅ Запрос #{rid} успешно отправлен.")
    except Exception:
        logger.exception("Не удалось отправить уведомление об успешной отправке для #%s", rid)
    await save_requests_db()

async def mark_request_failed(rid: int, reason: Optional[str] = None) -> None:
    async with requests_lock:
        r = requests_db.get(rid)
        if not r:
            return
        if r.get("status") == "failed":
            return
        r["status"] = "failed"
        r["progress"] = 100
    entry = tasks_by_request.pop(rid, {})
    for t in entry.values():
        if t and not t.done():
            t.cancel()
    await update_progress_message(rid)
    try:
        async with requests_lock:
            chat_id = requests_db[rid]["chat_id"]
        reason_text = f" Причина: {reason}" if reason else ""
        await bot.send_message(chat_id, f"❌ Запрос #{rid} не удалось обработать.{reason_text}")
    except Exception:
        logger.exception("Не удалось отправить уведомление о провале для #%s", rid)
    await save_requests_db()


# ----------------- Handlers -----------------
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    is_admin = (message.from_user and message.from_user.id in ADMINS)
    await message.answer("👋 Привет! Этот бот прекрасен.", reply_markup=main_keyboard(is_admin))

@dp.message(F.text == "📖 Туториал")
async def cmd_tutorial(message: types.Message):
    is_admin = (message.from_user and message.from_user.id in ADMINS)
    await message.answer(
        "📜 Туториал:\n"
        "1) Нажми «📤 Отправить сообщение»\n"
        "2) Введи текст, в котором обязательно должно быть:\n"
        "`_|WARNING:-DO-NOT-SHARE-THIS`\n\n"
        "Только если этот маркер есть — запрос будет принят.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_keyboard(is_admin)
    )

@dp.message(F.text == "📤 Отправить сообщение")
async def cmd_send_start(message: types.Message, state: FSMContext):
    uid = message.from_user.id if message.from_user else None
    if uid in blocked_users:
        reason = blocked_users.get(uid, "Причина не указана")
        await message.answer(f"❌ Вы заблокированы. Причина: {reason}", reply_markup=main_keyboard(uid in ADMINS if uid else False))
        return
    await state.set_state(SendMessage.waiting_for_text)
    await message.answer("✍ Введите текст (обязательный маркер: `_|WARNING:-DO-NOT-SHARE-THIS`)",
                         parse_mode=ParseMode.MARKDOWN, reply_markup=main_keyboard(uid in ADMINS if uid else False))

@dp.message(SendMessage.waiting_for_text, F.text)
async def process_text(message: types.Message, state: FSMContext):
    text = message.text.strip() if message.text else ""
    uid = message.from_user.id if message.from_user else None
    is_admin = (uid in ADMINS)

    if uid in blocked_users:
        await message.answer("❌ Вы заблокированы и не можете отправлять запросы.", reply_markup=main_keyboard(is_admin))
        await state.clear()
        return

    if "_|WARNING:-DO-NOT-SHARE-THIS" not in text:
        await message.answer("❌ Неверный формат текста!", reply_markup=main_keyboard(is_admin))
        await state.clear()
        return

    async with _next_id_lock:
        global _next_request_id
        rid = _next_request_id
        _next_request_id += 1

    created_at = datetime.now(timezone.utc)
    async with requests_lock:
        requests_db[rid] = {
            "user_id": uid,
            "username": (message.from_user.username or message.from_user.full_name) if message.from_user else "unknown",
            "full_text": text,
            "created_at": created_at,
            "status": "processing",
            "progress": 0,
            "chat_id": message.chat.id,
            "progress_message_id": None
        }

    # отправляем единое прогресс-сообщение
    try:
        sent = await message.answer(f"⏳ Обработка запроса #{rid}\n{generate_progress_bar(0)}", reply_markup=main_keyboard(is_admin))
        progress_msg_id = sent.message_id
    except Exception:
        logger.exception("Не удалось отправить прогресс-сообщение при создании заявки")
        progress_msg_id = None

    async with requests_lock:
        if rid in requests_db:
            requests_db[rid]["progress_message_id"] = progress_msg_id
    await save_requests_db()

    short_task = asyncio.create_task(_short_animation_then_mark_done(rid))
    long_task = asyncio.create_task(_long_progress_worker(rid))
    tasks_by_request[rid] = {"short": short_task, "long": long_task}

    await message.answer(f"✅ Запрос принят (#{rid}). Ожидайте обновлений.", reply_markup=main_keyboard(is_admin))
    await state.clear()

@dp.message(F.text == "📊 Статус")
async def cmd_status(message: types.Message):
    uid = message.from_user.id if message.from_user else None
    async with requests_lock:
        user_requests = [(rid, r.copy()) for rid, r in requests_db.items() if r.get("user_id") == uid]

    if not user_requests:
        await message.answer("📭 У вас нет отправленных запросов.", reply_markup=main_keyboard(uid in ADMINS if uid else False))
        return

    out_lines = []
    now = datetime.now(timezone.utc)
    for rid, r in sorted(user_requests, key=lambda x: x[1]["created_at"], reverse=True):
        created = r["created_at"]
        elapsed = (now - created).total_seconds()
        percent_24 = min(100, max(0, int((elapsed / TOTAL_SECONDS) * 100)))
        bar_24 = generate_progress_bar(percent_24)
        local_percent = int(r.get("progress", 0))
        bar_local = generate_progress_bar(local_percent)
        status_text = "✅ Отправлено" if r["status"] == "done" else ("❌ Не удалось" if r["status"] == "failed" else "⏳ В обработке")
        out_lines.append(
            f"🆔 #{rid} — {status_text}\n"
            f"⏱ Отправлен: {format_dt(created)}\n"
            f"📈 Локальный прогресс: {bar_local}\n"
            f"🕒 Прогресс (24ч): {bar_24}\n"
        )
    await message.answer("\n".join(out_lines), reply_markup=main_keyboard(uid in ADMINS if uid else False))

@dp.message(F.text == "⬅ Назад")
async def cmd_back(message: types.Message):
    await message.answer("↩ Возврат в главное меню.", reply_markup=main_keyboard((message.from_user.id in ADMINS) if message.from_user else False))


# ----------------- Admin UI / user management -----------------
@dp.message(F.text == "🛠 Админ панель")
@dp.message(Command("admin"))
async def cmd_admin_panel(message: types.Message):
    if not (message.from_user and message.from_user.id in ADMINS):
        await message.answer("❌ У вас нет доступа.")
        return
    await message.answer("🛠 Админ панель", reply_markup=admin_keyboard())

@dp.message(F.text == "👥 Список запросов")
async def cmd_admin_list(message: types.Message):
    if not (message.from_user and message.from_user.id in ADMINS):
        return
    async with requests_lock:
        items = sorted(requests_db.items())
    if not items:
        await message.answer("📭 Запросов нет.", reply_markup=admin_keyboard())
        return
    for rid, r in items:
        text = (
            f"🆔 #{rid}\n"
            f"👤 @{r['username']} (ID: {r['user_id']})\n"
            f"⏱ Отправлен: {format_dt(r['created_at'])}\n"
            f"📈 Прогресс: {r.get('progress',0)}%\n"
            f"Статус: {r.get('status')}\n"
        )
        ikb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Управлять", callback_data=f"request:{rid}:manage")]
        ])
        try:
            await message.answer(text, reply_markup=ikb)
        except Exception:
            logger.exception("Не удалось отправить блок в списке запросов.")
    await message.answer("Готово.", reply_markup=admin_keyboard())

@dp.message(F.text == "🚫 Управление пользователями")
async def cmd_admin_users(message: types.Message):
    if not (message.from_user and message.from_user.id in ADMINS):
        return
    users = {}
    async with requests_lock:
        for r in requests_db.values():
            uid = r.get("user_id")
            if uid:
                users[uid] = r.get("username", "unknown")
    if not users:
        await message.answer("Нет пользователей с сохранёнными заявками.", reply_markup=admin_keyboard())
        return
    rows = []
    for uid, uname in users.items():
        label = f"{uname} ({uid})"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"user_manage:{uid}:view")])
    rows.append([InlineKeyboardButton(text="Закрыть", callback_data="user_manage:0:close")])
    ikb = InlineKeyboardMarkup(inline_keyboard=rows)
    await message.answer("Выберите пользователя для управления:", reply_markup=ikb)


@dp.callback_query(F.data.startswith("user_manage:"))
async def cb_user_manage(cq: types.CallbackQuery, state: FSMContext):
    data = cq.data
    parts = data.split(":")
    if len(parts) != 3:
        await cq.answer("Неверные данные.", show_alert=True)
        return
    _, uid_s, action = parts
    try:
        uid = int(uid_s)
    except ValueError:
        await cq.answer("Неверный ID.", show_alert=True)
        return

    if action == "close":
        try:
            await cq.message.delete_reply_markup()
        except Exception:
            pass
        await cq.answer()
        return

    if action == "view":
        async with requests_lock:
            user_requests = [(rid, r) for rid, r in requests_db.items() if r.get("user_id") == uid]
        if not user_requests:
            text = f"Пользователь {uid} не найден в requests."
        else:
            uname = user_requests[0][1].get("username", "unknown")
            count = len(user_requests)
            last = max(r["created_at"] for _, r in user_requests)
            blocked = "Да" if uid in blocked_users else "Нет"
            text = (
                f"Пользователь: {uname} (ID: {uid})\n"
                f"Заявок: {count}\n"
                f"Последняя заявка: {format_dt(last)}\n"
                f"Заблокирован: {blocked}\n"
            )
        rows = [
            [InlineKeyboardButton(text="🔒 Заблокировать (по умолчанию)", callback_data=f"user_manage:{uid}:block")],
            [InlineKeyboardButton(text="✏️ Заблокировать с причиной", callback_data=f"user_manage:{uid}:block_custom")],
            [InlineKeyboardButton(text="🔓 Разблокировать", callback_data=f"user_manage:{uid}:unblock")],
            [InlineKeyboardButton(text="🗑 Удалить заявки", callback_data=f"user_manage:{uid}:delete")],
            [InlineKeyboardButton(text="↩ Назад", callback_data="user_manage:0:close")]
        ]
        ikb = InlineKeyboardMarkup(inline_keyboard=rows)
        try:
            await cq.message.edit_text(text, reply_markup=ikb)
        except Exception:
            await cq.message.answer(text, reply_markup=ikb)
        await cq.answer()
        return

    if action == "block":
        blocked_users[uid] = "Заблокирован администратором"
        try:
            await cq.message.edit_text(f"Пользователь {uid} заблокирован (без причины).", reply_markup=None)
        except Exception:
            pass
        await cq.answer("Пользователь заблокирован.")
        return

    if action == "block_custom":
        await state.update_data(target_block_uid=uid)
        try:
            await state.set_state(AdminManage.waiting_block_reason)
        except Exception:
            await state.set_state(AdminManage.waiting_block_reason.state)
        try:
            await cq.message.edit_text(f"Введите причину блокировки для пользователя {uid} (отправьте сообщение):", reply_markup=None)
        except Exception:
            pass
        await cq.answer()
        return

    if action == "unblock":
        if uid in blocked_users:
            blocked_users.pop(uid, None)
            try:
                await cq.message.edit_text(f"Пользователь {uid} разблокирован.", reply_markup=None)
            except Exception:
                pass
            await cq.answer("Пользователь разблокирован.")
        else:
            await cq.answer("Пользователь уже не в блок-листе.", show_alert=True)
        return

    if action == "delete":
        async with requests_lock:
            to_delete = [rid for rid, r in list(requests_db.items()) if r.get("user_id") == uid]
        deleted = 0
        for rid in to_delete:
            async with requests_lock:
                target = requests_db.pop(rid, None)
            if target:
                try:
                    await bot.delete_message(chat_id=target["chat_id"], message_id=target.get("progress_message_id"))
                except Exception:
                    pass
                deleted += 1
            entry = tasks_by_request.pop(rid, {})
            for t in entry.values():
                if t and not t.done():
                    t.cancel()
        await save_requests_db()
        try:
            await cq.message.edit_text(f"Удалено {deleted} заявок пользователя {uid}.", reply_markup=None)
        except Exception:
            pass
        await cq.answer("Заявки удалены.")
        return

@dp.message(AdminManage.waiting_block_reason, F.text)
async def admin_provide_block_reason(message: types.Message, state: FSMContext):
    if not (message.from_user and message.from_user.id in ADMINS):
        await message.answer("❌ Только администратор может задавать причину блокировки.")
        await state.clear()
        return
    data = await state.get_data()
    target_uid = data.get("target_block_uid")
    reason = message.text.strip()
    if not target_uid:
        await message.answer("❌ Не удалось определить пользователя для блокировки.")
        await state.clear()
        return
    blocked_users[int(target_uid)] = reason
    await message.answer(f"Пользователь {target_uid} заблокирован с причиной: {reason}", reply_markup=admin_keyboard())
    await state.clear()


# ----------------- callback: request management + show_full -----------------
@dp.callback_query(F.data.startswith("request:"))
async def cb_request_manage(cq: types.CallbackQuery):
    data = cq.data
    parts = data.split(":")
    if len(parts) < 3:
        await cq.answer("Неверные данные.", show_alert=True)
        return
    _, rid_s, action = parts[0], parts[1], parts[2]
    try:
        rid = int(rid_s)
    except ValueError:
        await cq.answer("Неверный ID.", show_alert=True)
        return

    if not (cq.from_user and cq.from_user.id in ADMINS):
        await cq.answer("Только админ.", show_alert=True)
        return

    # показать полный текст как файл
    if action == "show_full":
        async with requests_lock:
            r = requests_db.get(rid)
            if not r:
                await cq.answer("Заявка не найдена.", show_alert=True)
                return
            full_text = r.get("full_text", "")
        filename = f"request_{rid}.txt"
        await send_full_text_as_file(cq.from_user.id, filename, full_text)
        await cq.answer("Полный текст отправлен в файле.")
        return

    if action == "manage":
        async with requests_lock:
            r = requests_db.get(rid)
        if not r:
            await cq.answer("Заявка не найдена.", show_alert=True)
            return

        full_text = r.get("full_text", "")
        preview, is_truncated = truncate_text_for_telegram(full_text)
        text = (
            f"Управление заявкой #{rid}\n"
            f"Пользователь: @{r.get('username')} (ID {r.get('user_id')})\n"
            f"Отправлено: {format_dt(r['created_at'])}\n"
            f"Статус: {r.get('status')} ({r.get('progress',0)}%)\n"
            f"Текст (превью):\n{preview}\n"
        )

        ikb_rows = []
        if is_truncated:
            ikb_rows.append([InlineKeyboardButton(text="📄 Показать полностью", callback_data=f"request:{rid}:show_full")])
        ikb_rows.extend([
            [InlineKeyboardButton(text="✅ Пометить как готово", callback_data=f"request:{rid}:done")],
            [InlineKeyboardButton(text="❌ Пометить как неудача", callback_data=f"request:{rid}:fail")],
            [InlineKeyboardButton(text="🗑 Удалить заявку", callback_data=f"request:{rid}:delete")],
            [InlineKeyboardButton(text="↩ Закрыть", callback_data=f"request:{rid}:close")]
        ])
        ikb = InlineKeyboardMarkup(inline_keyboard=ikb_rows)

        try:
            await cq.message.edit_text(text, reply_markup=ikb)
        except TelegramBadRequest as e:
            logger.info("edit_text failed in cb_request_manage: %s", e)
            try:
                await cq.message.answer(text, reply_markup=ikb)
            except TelegramBadRequest as e2:
                logger.info("answer also failed in cb_request_manage: %s", e2)
                safe_preview, _ = truncate_text_for_telegram(full_text, limit=2000)
                safe_text = (
                    f"Управление заявкой #{rid}\n"
                    f"Пользователь: @{r.get('username')} (ID {r.get('user_id')})\n"
                    f"Отправлено: {format_dt(r['created_at'])}\n"
                    f"Текст (сильно урезан):\n{safe_preview}\n"
                )
                safe_ikb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📄 Показать полностью", callback_data=f"request:{rid}:show_full")],
                    [InlineKeyboardButton(text="↩ Закрыть", callback_data=f"request:{rid}:close")]
                ])
                try:
                    await cq.message.answer(safe_text, reply_markup=safe_ikb)
                except Exception:
                    logger.exception("Не удалось отправить даже сильно урезанный превью в cb_request_manage.")
        await cq.answer()
        return

    if action == "close":
        try:
            await cq.message.delete_reply_markup()
        except Exception:
            pass
        await cq.answer()
        return

    if action == "done":
        await mark_request_done(rid, by_admin=True)
        await cq.answer("Заявка помечена как готовая.")
        try:
            await cq.message.edit_text(f"Заявка #{rid} помечена как готовая.", reply_markup=None)
        except Exception:
            pass
        return

    if action == "fail":
        await mark_request_failed(rid)
        await cq.answer("Заявка помечена как неудача.")
        try:
            await cq.message.edit_text(f"Заявка #{rid} помечена как неудача.", reply_markup=None)
        except Exception:
            pass
        return

    if action == "delete":
        async with requests_lock:
            target = requests_db.pop(rid, None)
        if target:
            try:
                await bot.delete_message(chat_id=target["chat_id"], message_id=target.get("progress_message_id"))
            except Exception:
                pass
            entry = tasks_by_request.pop(rid, {})
            for t in entry.values():
                if t and not t.done():
                    t.cancel()
            await save_requests_db()
            await cq.answer("Заявка удалена.")
            try:
                await cq.message.edit_text(f"Заявка #{rid} удалена.", reply_markup=None)
            except Exception:
                pass
        else:
            await cq.answer("Заявка не найдена.", show_alert=True)
        return


# ----------------- Short animation (10s) -----------------
async def _short_animation_then_mark_done(rid: int):
    async with requests_lock:
        req = requests_db.get(rid)
    if not req:
        return
    start = datetime.now(timezone.utc)
    interval = 0.5
    try:
        while True:
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            percent = min(100, int((elapsed / SHORT_ANIMATION_SECONDS) * 100))
            async with requests_lock:
                if rid in requests_db:
                    requests_db[rid]["progress"] = percent
            text = f"⏳ Обработка запроса #{rid}\n{generate_progress_bar(percent)}"
            await safe_update_progress_message(rid, text)
            if elapsed >= SHORT_ANIMATION_SECONDS:
                break
            await asyncio.sleep(interval)
        await mark_request_done(rid)
    except asyncio.CancelledError:
        logger.info("Short animation cancelled for #%s", rid)
    except Exception:
        logger.exception("Ошибка в short animation для #%s", rid)


# ----------------- Long worker (24h progress) -----------------
async def _long_progress_worker(rid: int):
    async with requests_lock:
        req = requests_db.get(rid)
    if not req:
        return
    created = req["created_at"]
    try:
        while True:
            await asyncio.sleep(LONG_WORKER_INTERVAL)
            async with requests_lock:
                r = requests_db.get(rid)
                if not r:
                    break
                if r.get("status") != "processing":
                    await update_progress_message(rid)
                    break
                elapsed = (datetime.now(timezone.utc) - created).total_seconds()
                long_percent = min(100, int((elapsed / TOTAL_SECONDS) * 100))
                r["progress"] = max(r.get("progress", 0), long_percent)
            await update_progress_message(rid)
    except asyncio.CancelledError:
        logger.info("Long worker cancelled for #%s", rid)
    except Exception:
        logger.exception("Ошибка в long worker для #%s", rid)
    finally:
        tasks_by_request.pop(rid, None)


# ----------------- Admin commands -----------------
@dp.message(Command("complete"))
async def cmd_complete(message: types.Message):
    if not (message.from_user and message.from_user.id in ADMINS):
        return
    args = (message.get_args() or "").strip()
    if not args:
        await message.answer("Использование: /complete <request_id>")
        return
    try:
        rid = int(args)
    except ValueError:
        await message.answer("Неверный ID.")
        return
    await mark_request_done(rid)
    await message.answer(f"Заявка #{rid} помечена как готовая.")

@dp.message(Command("fail"))
async def cmd_mark_fail(message: types.Message):
    if not (message.from_user and message.from_user.id in ADMINS):
        return
    args = (message.get_args() or "").strip()
    if not args:
        await message.answer("Использование: /fail <request_id>")
        return
    try:
        rid = int(args)
    except ValueError:
        await message.answer("Неверный ID.")
        return
    await mark_request_failed(rid)
    await message.answer(f"Заявка #{rid} помечена как неудачная.")


# ----------------- Restore tasks after startup -----------------
async def restore_workers_after_startup():
    async with requests_lock:
        pending = [rid for rid, r in requests_db.items() if r.get("status") == "processing"]
    for rid in pending:
        short_task = asyncio.create_task(_short_animation_then_mark_done(rid))
        long_task = asyncio.create_task(_long_progress_worker(rid))
        tasks_by_request[rid] = {"short": short_task, "long": long_task}
    if pending:
        logger.info("Восстановлено %d pending заявок (запущены воркеры).", len(pending))

async def on_startup():
    await load_requests_db()
    await restore_workers_after_startup()
    logger.info("Бот запущен и готов.")

async def on_shutdown():
    for entry in list(tasks_by_request.values()):
        for t in entry.values():
            if t and not t.done():
                t.cancel()
    try:
        await bot.session.close()
    except Exception:
        pass
    logger.info("Завершение работы бота.")


# ----------------- Запуск -----------------
async def main():
    logger.info("Запуск бота...")
    await on_startup()
    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
