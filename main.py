import logging
import os
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import asyncio
import asyncpg
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    CallbackQueryHandler
)
import requests
import random
import string
from generator import LicenseGenerator
import types
import csv
import io
import pytz

# --- Настройка логирования ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Отключаем лишние логи ---
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("telegram").setLevel(logging.ERROR)

# --- Переменные окружения ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')

if not BOT_TOKEN:
    raise ValueError("❌ Не задана переменная окружения BOT_TOKEN")
if not DATABASE_URL:
    raise ValueError("❌ Не задана переменная окружения DATABASE_URL")


# --- Класс для управления конфигурацией из БД ---
class Config:
    """Класс для кеширования и обновления данных из БД"""

    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._last_update: Optional[datetime] = None
        self._update_interval = timedelta(minutes=5)  # Обновление каждые 5 минут
        self._lock = asyncio.Lock()

    async def get_value(self, column_name: str) -> Optional[str]:
        """
        Получает значение из кеша или из БД при необходимости
        """
        # Проверяем, нужно ли обновить кеш
        if self._needs_update():
            await self._refresh_cache()

        return self._cache.get(column_name.lower())

    def _needs_update(self) -> bool:
        """Проверяет, нужно ли обновить кеш"""
        if not self._last_update:
            return True
        return datetime.now() - self._last_update > self._update_interval

    async def _refresh_cache(self):
        """Обновляет кеш данными из БД"""
        async with self._lock:
            # Повторная проверка после получения блокировки
            if not self._needs_update():
                return

            conn = None
            try:
                conn = await asyncpg.connect(DATABASE_URL)

                # Получаем все поля из первой записи
                row = await conn.fetchrow("SELECT * FROM tokens LIMIT 1")

                if row:
                    # Преобразуем запись в словарь с ключами в нижнем регистре
                    self._cache = {key.lower(): value for key, value in dict(row).items()}
                else:
                    logger.error("❌ Таблица tokens пуста!")
                    self._cache = {}

                self._last_update = datetime.now()
                logger.info("✅ Кеш конфигурации обновлен")

            except Exception as e:
                logger.error(f"❌ Ошибка обновления кеша: {e}")
            finally:
                if conn:
                    await conn.close()

    async def force_refresh(self):
        """Принудительное обновление кеша"""
        self._last_update = None
        await self._refresh_cache()


# --- Создаем глобальный экземпляр конфига ---
config = Config()


# --- Вспомогательные функции для получения значений ---
async def get_git_token() -> Optional[str]:
    """Получает GitHub токен"""
    return await config.get_value("gittoken")


async def get_gist_id() -> Optional[str]:
    """Получает Gist ID"""
    return await config.get_value("gistid")


async def get_gist_filename() -> Optional[str]:
    """Получает имя файла в Gist"""
    return await config.get_value("gistfilename")


async def get_admin_password() -> Optional[str]:
    """Получает пароль администратора"""
    return await config.get_value("adminpassword")


async def get_admin_ids() -> tuple:
    """Получает ID администраторов"""
    admin1 = await config.get_value("adminid1")
    admin2 = await config.get_value("adminid2")

    # Преобразуем в int, если возможно
    admin_ids = []
    for aid in [admin1, admin2]:
        if aid:
            try:
                admin_ids.append(int(aid))
            except (ValueError, TypeError):
                pass

    return tuple(admin_ids)


# --- Вспомогательный класс для имитации callback_query ---
class DummyQuery:
    def __init__(self, data, from_user, message, chat_id=None):
        self.data = data
        self.from_user = from_user
        self.message = message
        self.chat_id = chat_id

    async def answer(self, *args, **kwargs):
        pass

    async def edit_message_text(self, *args, **kwargs):
        if self.message:
            await self.message.reply_text(*args, **kwargs)
        elif self.chat_id and hasattr(self, 'bot'):
            await self.bot.send_message(chat_id=self.chat_id, *args, **kwargs)


# --- Функции для работы с Gist (используют get_git_token, get_gist_id, get_gist_filename) ---
async def get_licenses_from_gist() -> dict:
    """Получает лицензии из Gist"""
    git_token = await get_git_token()
    gist_id = await get_gist_id()
    gist_filename = await get_gist_filename()

    if not all([git_token, gist_id, gist_filename]):
        logger.error("❌ Отсутствуют данные для подключения к Gist")
        return {}

    headers = {
        "Authorization": f"token {git_token}",
        "Accept": "application/vnd.github.v3+json"
    }

    try:
        response = requests.get(f"https://api.github.com/gists/{gist_id}", headers=headers)
        if response.status_code == 200:
            gist_data = response.json()
            if gist_filename in gist_data['files']:
                licenses_content = gist_data['files'][gist_filename]['content']
                raw_dict = json.loads(licenses_content)
                return {k: v for k, v in raw_dict.items() if v and isinstance(v, dict) and v.get('key')}
        return {}
    except Exception as e:
        logger.error(f"Ошибка получения Gist: {e}")
        return {}


async def save_licenses_to_gist(licenses: dict) -> bool:
    """Сохраняет лицензии в Gist"""
    git_token = await get_git_token()
    gist_id = await get_gist_id()
    gist_filename = await get_gist_filename()

    if not all([git_token, gist_id, gist_filename]):
        logger.error("❌ Отсутствуют данные для сохранения в Gist")
        return False

    headers = {
        "Authorization": f"token {git_token}",
        "Accept": "application/vnd.github.v3+json"
    }

    data = {
        "files": {
            gist_filename: {
                "content": json.dumps(licenses, indent=2, ensure_ascii=False)
            }
        }
    }

    try:
        response = requests.patch(f"https://api.github.com/gists/{gist_id}", headers=headers, json=data)
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Ошибка сохранения Gist: {e}")
        return False


# --- АВТОРИЗАЦИЯ ---
async def check_admin_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяет авторизацию администратора"""
    user_id = update.effective_user.id if update.effective_user else None
    admin_ids = await get_admin_ids()

    if user_id in admin_ids:
        return True

    if context.user_data and context.user_data.get('admin_authenticated'):
        return True

    if update.message and update.message.text:
        if context.user_data and context.user_data.get('awaiting_password'):
            password = update.message.text.strip()
            admin_password = await get_admin_password()

            if password == admin_password:
                context.user_data['admin_authenticated'] = True
                context.user_data['awaiting_password'] = False
                await show_main_menu(update, context, "✅ Доступ разрешён!")
                return True
            else:
                await update.message.reply_text("❌ Неверный пароль. Попробуйте ещё раз.")
                return False

    if update.message:
        await update.message.reply_text("🔒 Введите пароль администратора:")
    elif update.callback_query:
        await update.callback_query.edit_message_text("🔒 Введите пароль администратора:")

    if context.user_data is not None:
        context.user_data['awaiting_password'] = True

    return False


# --- ГЛАВНОЕ МЕНЮ ---
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, info_msg: str = None):
    """Показывает главное меню"""
    lic_dict = await get_licenses_from_gist()
    total = len(lic_dict) if lic_dict else 0
    active = sum(1 for v in lic_dict.values() if v.get('hasActive')) if lic_dict else 0
    demo = sum(1 for v in lic_dict.values() if v.get('customer') == 'demo') if lic_dict else 0
    annulled = sum(1 for v in lic_dict.values() if
                   not v.get('hasActive') and v.get('expired') == '' and v.get('customer') != 'demo') if lic_dict else 0

    text = (
        "<b>🌟 Админ-панель управления лицензиями 🌟</b>\n"
        "<b>──────────────</b>\n"
        f"Всего: <b>{total}</b> | 🟢 <b>{active}</b> | 🟡 <b>{demo}</b> | ⚫️ <b>{annulled}</b>\n"
        "<b>──────────────</b>\n"
        "Выберите действие:\n"
    )

    if info_msg is not None:
        text = f"{info_msg}\n\n{text}"

    keyboard = [
        [InlineKeyboardButton("📋 Список ключей", callback_data="key_list")],
        [InlineKeyboardButton("🔍 Поиск по нику", callback_data="find_key")],
        [InlineKeyboardButton("🔍 Поиск по ключу", callback_data="find_by_key")],
        [InlineKeyboardButton("🔑 Сгенерировать ключ", callback_data="generate_key")],
        [InlineKeyboardButton("📊 Экспорт/Импорт", callback_data="export_import_menu")],
        [InlineKeyboardButton("🔄 Обновить данные из БД", callback_data="refresh_config")],
        [InlineKeyboardButton("❓ Помощь / FAQ", callback_data="help_faq")]
    ]

    if update.message:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    elif update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode='HTML')


# --- АКТИВАЦИЯ КЛЮЧА ---
async def toggle_active_license(update, context):
    """Включает/отключает лицензию"""
    query = update.callback_query
    key = query.data.split('_', 2)[2] if query and hasattr(query, 'data') and query.data else None
    lic_dict = await get_licenses_from_gist()

    if not lic_dict or key not in lic_dict or lic_dict[key] is None:
        await query.answer("Ключ не найден!", show_alert=True)
        return

    info = lic_dict[key]
    if info.get('hasActive'):
        # Отключить лицензию
        info['hasActive'] = False
        info['hwid'] = ""
        info['issued'] = ""
        info['expired'] = ""
        await query.answer(f"🔴 Лицензия {key} отключена!", show_alert=True)
    else:
        # Включить лицензию
        moscow_tz = pytz.timezone('Europe/Moscow')
        now = datetime.now(moscow_tz)
        period = info.get('period', 0)
        try:
            period = int(float(period))
        except Exception:
            period = 0

        info['issued'] = now.strftime('%d.%m.%Y %H:%M')
        if period > 0:
            info['expired'] = (now + timedelta(days=period)).strftime('%d.%m.%Y %H:%M')
        else:
            info['expired'] = ''
        info['hasActive'] = True
        await query.answer(f"🟢 Лицензия {key} включена!", show_alert=True)

    await save_licenses_to_gist(lic_dict)

    # Обновляем окно редактирования
    dummy_update = types.SimpleNamespace()
    dummy_update.callback_query = DummyQuery(f"edit_{key}", query.from_user, query.message,
                                             getattr(query.message, 'chat_id', None))
    await edit_key_menu(dummy_update, context)


# --- СПИСОК КЛЮЧЕЙ ---
KEYS_PER_PAGE = 10


async def key_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список ключей"""
    if not await check_admin_auth(update, context):
        return

    lic_dict = await get_licenses_from_gist()
    if not lic_dict or not isinstance(lic_dict, dict):
        await show_main_menu(update, context, "Нет ключей.")
        return

    # Фильтры и сортировка
    filter_val = context.user_data.get('keylist_filter', 'all')
    sort_val = context.user_data.get('keylist_sort', 'created_desc')
    bulk_mode = context.user_data.get('bulk_mode', False)
    selected_keys = context.user_data.get('bulk_selected', [])

    filtered = list(lic_dict.items())

    # Применяем фильтры
    if filter_val == 'active':
        filtered = [(k, v) for k, v in filtered if v.get('hasActive')]
    elif filter_val == 'demo':
        filtered = [(k, v) for k, v in filtered if v.get('customer') == 'demo']
    elif filter_val == 'annulled':
        filtered = [(k, v) for k, v in filtered if
                    not v.get('hasActive') and v.get('expired') == '' and v.get('customer') != 'demo']
    elif filter_val == 'soon_expired':
        now = datetime.now()

        def is_soon(v):
            exp = v.get('expired')
            if exp and isinstance(exp, str):
                try:
                    exp_str = exp.split()[0] if len(exp) > 10 else exp
                    if exp_str:
                        exp_date = datetime.strptime(exp_str, "%d.%m.%Y")
                        return 0 <= (exp_date - now).days <= 7
                except Exception:
                    pass
            return False

        filtered = [(k, v) for k, v in filtered if is_soon(v)]

    # Применяем сортировку
    if sort_val == 'created_desc':
        filtered.sort(key=lambda x: datetime.strptime(x[1].get('created', '01.01.2000'), "%d.%m.%Y %H:%M") if x[1].get(
            'created') else datetime.min, reverse=True)
    elif sort_val == 'created_asc':
        filtered.sort(key=lambda x: datetime.strptime(x[1].get('created', '01.01.2000'), "%d.%m.%Y %H:%M") if x[1].get(
            'created') else datetime.min)

    keys = [k for k, v in filtered]

    # Пагинация
    page = context.user_data.get('keylist_page', 0)

    if update.callback_query and getattr(update.callback_query, 'data', None):
        cb_data = update.callback_query.data
        if cb_data and isinstance(cb_data, str) and cb_data.startswith('keylist_page_'):
            try:
                page = int(cb_data.split('_')[-1])
                context.user_data['keylist_page'] = page
            except Exception:
                page = 0

    total_pages = max(1, (len(keys) - 1) // KEYS_PER_PAGE + 1 if keys else 1)
    start = page * KEYS_PER_PAGE
    end = start + KEYS_PER_PAGE
    page_keys = keys[start:end]

    keyboard = []
    for key in page_keys:
        info = lic_dict[key]

        # Статус эмодзи
        if info.get('hasActive'):
            emoji = '🟢'
        elif info.get('customer') == 'demo':
            emoji = '🟡'
        elif not info.get('hasActive') and info.get('expired') == '':
            emoji = '⚫️'
        else:
            emoji = '🔴'

        if bulk_mode:
            checked = '✅' if key in selected_keys else '⬜️'
            button_text = f"{checked} {emoji} {key}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"bulk_toggle_{key}")])
        else:
            button_text = f"{emoji} {key} | {info.get('customer', '?')}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_{key}")])

    # Навигация
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"keylist_page_{page - 1}"))
    if end < len(keys):
        nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"keylist_page_{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"keylist_page_{page}")])
    keyboard.append([InlineKeyboardButton("⚙️ Фильтр/Сортировка", callback_data="keylist_filter_menu")])

    if bulk_mode:
        keyboard.append([
            InlineKeyboardButton("🗑️ Удалить выбранные", callback_data="bulk_confirm_delete"),
            InlineKeyboardButton("🚫 Аннулировать выбранные", callback_data="bulk_confirm_annul")
        ])
        keyboard.append([InlineKeyboardButton("❌ Сбросить выбор", callback_data="bulk_cancel")])
    else:
        keyboard.append([InlineKeyboardButton("☑️ Массовый выбор", callback_data="bulk_mode_on")])

    keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="main_menu")])

    text = f"<b>📋 Список ключей</b> (стр. {page + 1}/{total_pages}):\nФильтр: <b>{filter_val}</b>"
    if bulk_mode:
        text += f"\n\nВыбрано: {len(selected_keys)} ключей"

    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                          parse_mode='HTML')
        except Exception as e:
            if "Message is not modified" not in str(e):
                raise
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


# --- ПОИСК ПО НИКУ ---
async def find_key_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрос ника для поиска"""
    if not await check_admin_auth(update, context):
        return

    keyboard = [[InlineKeyboardButton("↩️ Назад", callback_data="main_menu")]]
    text = "Введите ник для поиска (отправьте сообщением):"

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    context.user_data['awaiting_nick'] = True


async def find_by_key_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрос ключа для поиска"""
    if not await check_admin_auth(update, context):
        return

    keyboard = [[InlineKeyboardButton("↩️ Назад", callback_data="main_menu")]]
    text = "Введите ключ для поиска (отправьте сообщением):"

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    context.user_data['awaiting_key'] = True


# --- МЕНЮ РЕДАКТИРОВАНИЯ КЛЮЧА ---
async def edit_key_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню редактирования ключа"""
    query = update.callback_query
    if not query or not hasattr(query, 'data') or query.data is None:
        return

    await query.answer()

    user_id = query.from_user.id if query.from_user else None
    admin_ids = await get_admin_ids()

    if user_id not in admin_ids and not (context.user_data and context.user_data.get('admin_authenticated')):
        await query.answer("🔒 Нет доступа", show_alert=True)
        return

    key = query.data.split('_', 1)[1] if query.data else None
    lic_dict = await get_licenses_from_gist()

    if not key or not lic_dict or key not in lic_dict or lic_dict[key] is None:
        await query.answer("Ключ не найден!", show_alert=True)
        return

    info = lic_dict[key]

    status = '🟢 Активен' if info.get('hasActive') else ('🟡 Демо' if info.get('customer') == 'demo' else '🔴 Неактивен')

    text = (
        f"<b>🔑 Ключ:</b> <code>{key}</code>\n"
        f"<b>👤 Клиент:</b> <code>{info.get('customer', '?')}</code>\n"
        f"<b>📅 Создан:</b> <code>{info.get('created', '?')}</code>\n"
        f"<b>⏳ Истекает:</b> <code>{info.get('expired', '?')}</code>\n"
        f"<b>📆 Срок:</b> <code>{info.get('period', '?')}</code> дней\n"
        f"<b>💽 HWID:</b> <code>{info.get('hwid', '-')}</code>\n"
        f"<b>Статус:</b> {status}\n"
    )

    keyboard = [
        [InlineKeyboardButton("🗑️ Удалить", callback_data=f"confirm_delete_{key}"),
         InlineKeyboardButton(
             ("🔴 Отключить" if info.get('hasActive') else "🟢 Включить"),
             callback_data=f"toggle_active_{key}")],
        [InlineKeyboardButton("📅 Изменить дату", callback_data=f"setexp_{key}"),
         InlineKeyboardButton("🔄 Изменить период", callback_data=f"setperiod_{key}")],
        [InlineKeyboardButton(f"💽 Проверка диска: {'✅' if info.get('disableDiskCheck') else '❌'}",
                              callback_data=f"diskcheck_{key}")],
        [InlineKeyboardButton("🏠 В меню", callback_data="main_menu")]
    ]

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


# --- ОБНОВЛЕНИЕ КОНФИГА ---
async def refresh_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительное обновление конфигурации из БД"""
    await config.force_refresh()

    admin_ids = await get_admin_ids()
    git_token = await get_git_token()
    gist_id = await get_gist_id()
    gist_filename = await get_gist_filename()

    text = (
        "✅ Конфигурация обновлена из БД:\n\n"
        f"👤 Админы: {admin_ids}\n"
        f"🔑 GitHub токен: {git_token[:10]}...\n"
        f"📦 Gist ID: {gist_id}\n"
        f"📄 Gist файл: {gist_filename}"
    )

    if update.callback_query:
        await update.callback_query.answer("Конфигурация обновлена!")
        await show_main_menu(update, context, text)
    else:
        await update.message.reply_text(text)


# --- ОСТАЛЬНЫЕ ФУНКЦИИ (сокращены для краткости, но они должны быть здесь) ---
# Важно: все функции, которые используют get_licenses_from_gist() и save_licenses_to_gist()
# должны быть async и использовать await при вызове этих функций

# --- CALLBACK ROUTER ---
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Роутер для callback запросов"""
    query = update.callback_query
    data = query.data if query and hasattr(query, 'data') and query.data else None

    if data == "main_menu":
        await show_main_menu(update, context)
    elif data == "key_list" or (data and data.startswith("keylist_page_")):
        await key_list(update, context)
    elif data == "find_key":
        await find_key_prompt(update, context)
    elif data == "find_by_key":
        await find_by_key_prompt(update, context)
    elif data == "refresh_config":
        await refresh_config(update, context)
    elif data and data.startswith("edit_"):
        await edit_key_menu(update, context)
    elif data and data.startswith("toggle_active_"):
        await toggle_active_license(update, context)
    # ... остальные callback handlers ...


# --- ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ---
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    if context.user_data and context.user_data.get('awaiting_nick'):
        search_nick = update.message.text.strip().lower()
        lic_dict = await get_licenses_from_gist()
        found = [(k, v) for k, v in lic_dict.items() if v and v.get('customer', '').lower() == search_nick]
        context.user_data['awaiting_nick'] = False

        if len(found) == 1:
            dummy_update = types.SimpleNamespace()
            dummy_update.callback_query = DummyQuery(f"edit_{found[0][0]}", update.effective_user, update.message,
                                                     update.effective_chat.id)
            await edit_key_menu(dummy_update, context)
        elif found:
            text = "Найденные ключи:\n"
            keyboard = []
            for k, v in found:
                keyboard.append([InlineKeyboardButton(f"✏️ Редактировать {k}", callback_data=f"edit_{k}")])
            keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="main_menu")])
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text("Ключи не найдены.")
        return

    elif context.user_data and context.user_data.get('awaiting_key'):
        search_key = update.message.text.strip()
        lic_dict = await get_licenses_from_gist()
        found = [(k, v) for k, v in lic_dict.items() if k == search_key]
        context.user_data['awaiting_key'] = False

        if len(found) == 1:
            dummy_update = types.SimpleNamespace()
            dummy_update.callback_query = DummyQuery(f"edit_{found[0][0]}", update.effective_user, update.message,
                                                     update.effective_chat.id)
            await edit_key_menu(dummy_update, context)
        elif found:
            text = "Найденный ключ:\n"
            keyboard = []
            for k, v in found:
                keyboard.append([InlineKeyboardButton(f"✏️ Редактировать {k}", callback_data=f"edit_{k}")])
            keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="main_menu")])
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text("Ключ не найден.")
        return

    # Проверка пароля
    await check_admin_auth(update, context)


# --- MAIN ---
async def post_init(application):
    """Действия после инициализации бота"""
    logger.info("🚀 Бот запускается...")
    # Первое обновление конфига
    await config.force_refresh()
    logger.info("✅ Конфигурация загружена из БД")


def main() -> None:
    """Главная функция"""
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # Добавляем обработчики
    application.add_handler(CommandHandler("start", show_main_menu))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    application.add_handler(CallbackQueryHandler(callback_router))

    logger.info("✅ Бот успешно запущен!")
    application.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()