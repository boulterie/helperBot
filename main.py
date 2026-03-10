import logging
import os
import json
from datetime import datetime, timedelta
from typing import Optional
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
import types
import csv
import io
import pytz
from generator import LicenseGenerator

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


# --- Класс для работы с БД ---
class Database:
    """Класс для работы с базой данных"""

    def __init__(self):
        self.pool = None
        self._cache = {}
        self._last_update = None
        self._update_interval = timedelta(minutes=5)

    async def init_pool(self):
        """Инициализация пула соединений"""
        try:
            self.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
            logger.info("✅ Пул соединений с БД создан")
            await self._load_config()
        except Exception as e:
            logger.error(f"❌ Ошибка создания пула: {e}")
            raise

    async def close_pool(self):
        """Закрытие пула соединений"""
        if self.pool:
            await self.pool.close()
            logger.info("✅ Пул соединений закрыт")

    async def _load_config(self):
        """Загрузка конфигурации из БД"""
        try:
            async with self.pool.acquire() as conn:
                # Проверяем существование таблицы
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS tokens (
                        id SERIAL PRIMARY KEY,
                        gitToken TEXT NOT NULL,
                        gistId TEXT NOT NULL,
                        gistFileName TEXT NOT NULL DEFAULT 'licenses.json',
                        adminPassword TEXT NOT NULL,
                        adminId1 TEXT,
                        adminId2 TEXT
                    )
                """)

                # Получаем первую запись
                row = await conn.fetchrow("SELECT * FROM tokens LIMIT 1")

                if row:
                    self._cache = {
                        'gittoken': row['gittoken'],
                        'gistid': row['gistid'],
                        'gistfilename': row['gistfilename'],
                        'adminpassword': row['adminpassword'],
                        'adminid1': row['adminid1'],
                        'adminid2': row['adminid2']
                    }
                    self._last_update = datetime.now()
                    logger.info("✅ Конфигурация загружена из БД")
                else:
                    logger.error("❌ Таблица tokens пуста!")

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфигурации: {e}")

    async def get_value(self, column_name: str) -> Optional[str]:
        """Получение значения по имени колонки"""
        # Проверяем, нужно ли обновить кеш
        if not self._last_update or datetime.now() - self._last_update > self._update_interval:
            await self._load_config()

        return self._cache.get(column_name.lower())

    async def refresh(self):
        """Принудительное обновление конфигурации"""
        self._last_update = None
        await self._load_config()


# --- Создаем глобальный экземпляр БД ---
db = Database()


# --- Вспомогательные функции для получения данных ---
async def get_github_token() -> Optional[str]:
    """Получает GitHub токен"""
    return await db.get_value('gittoken')


async def get_gist_id() -> Optional[str]:
    """Получает Gist ID"""
    return await db.get_value('gistid')


async def get_gist_filename() -> Optional[str]:
    """Получает имя файла в Gist"""
    return await db.get_value('gistfilename')


async def get_admin_password() -> Optional[str]:
    """Получает пароль администратора"""
    return await db.get_value('adminpassword')


async def get_admin_ids() -> tuple:
    """Получает ID администраторов"""
    admin1 = await db.get_value('adminid1')
    admin2 = await db.get_value('adminid2')

    admin_ids = []
    for aid in [admin1, admin2]:
        if aid:
            try:
                admin_ids.append(int(aid))
            except (ValueError, TypeError):
                pass

    return tuple(admin_ids)


# --- Функции для работы с Gist (используют данные из БД) ---
async def get_licenses_from_gist() -> dict:
    """Получает лицензии из Gist"""
    github_token = await get_github_token()
    gist_id = await get_gist_id()
    gist_filename = await get_gist_filename()

    if not all([github_token, gist_id, gist_filename]):
        logger.error("❌ Отсутствуют данные для подключения к Gist")
        return {}

    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }

    try:
        response = requests.get(f"https://api.github.com/gists/{gist_id}", headers=headers, timeout=10)
        if response.status_code == 200:
            gist_data = response.json()
            if gist_filename in gist_data['files']:
                licenses_content = gist_data['files'][gist_filename]['content']
                raw_dict = json.loads(licenses_content)
                return {k: v for k, v in raw_dict.items() if v and isinstance(v, dict) and v.get('key')}
            else:
                logger.error(f"❌ Файл {gist_filename} не найден в Gist")
                return {}
        else:
            logger.error(f"❌ Ошибка получения Gist: {response.status_code}")
            return {}
    except Exception as e:
        logger.error(f"❌ Ошибка при получении лицензий: {e}")
        return {}


async def save_licenses_to_gist(licenses: dict) -> bool:
    """Сохраняет лицензии в Gist"""
    github_token = await get_github_token()
    gist_id = await get_gist_id()
    gist_filename = await get_gist_filename()

    if not all([github_token, gist_id, gist_filename]):
        logger.error("❌ Отсутствуют данные для сохранения в Gist")
        return False

    headers = {
        "Authorization": f"token {github_token}",
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
        response = requests.patch(f"https://api.github.com/gists/{gist_id}", headers=headers, json=data, timeout=10)
        if response.status_code == 200:
            logger.info("✅ Лицензии сохранены в Gist")
            return True
        else:
            logger.error(f"❌ Ошибка сохранения в Gist: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении в Gist: {e}")
        return False


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


# --- ВКЛЮЧЕНИЕ/ОТКЛЮЧЕНИЕ ЛИЦЕНЗИИ ---
async def toggle_active_license(update, context):
    query = update.callback_query
    key = query.data.split('_', 2)[2] if query and hasattr(query, 'data') and query.data else None
    lic_dict = await get_licenses_from_gist()

    if not lic_dict or key not in lic_dict or lic_dict[key] is None:
        await query.answer("Ключ не найден!", show_alert=True)
        return

    info = lic_dict[key]
    moscow_tz = pytz.timezone('Europe/Moscow')

    if info.get('hasActive'):
        # Отключить лицензию
        info['hasActive'] = False
        info['hwid'] = ""
        info['issued'] = ""
        info['expired'] = ""
        await query.answer(f"🔴 Лицензия {key} отключена!", show_alert=True)
    else:
        # Включить лицензию
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


# --- АВТОРИЗАЦИЯ ---
async def check_admin_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяет авторизацию администратора"""
    user_id = update.effective_user.id if update.effective_user else None
    admin_ids = await get_admin_ids()
    admin_password = await get_admin_password()

    if user_id in admin_ids:
        return True

    if context.user_data and context.user_data.get('admin_authenticated'):
        return True

    if update.message and update.message.text:
        if context.user_data and context.user_data.get('awaiting_password'):
            password = update.message.text.strip()
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

    if info_msg:
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    if not await check_admin_auth(update, context):
        return
    await show_main_menu(update, context)


# --- ОБНОВЛЕНИЕ КОНФИГА ---
async def refresh_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Принудительное обновление конфигурации из БД"""
    await db.refresh()

    github_token = await get_github_token()
    gist_id = await get_gist_id()
    gist_filename = await get_gist_filename()
    admin_ids = await get_admin_ids()

    text = (
        "✅ Конфигурация обновлена из БД:\n\n"
        f"🔑 GitHub токен: {github_token[:10] if github_token else 'Не задан'}...\n"
        f"📦 Gist ID: {gist_id or 'Не задан'}\n"
        f"📄 Gist файл: {gist_filename or 'licenses.json'}\n"
        f"👤 Админы: {admin_ids}\n"
    )

    if update.callback_query:
        await update.callback_query.answer("Конфигурация обновлена!")
        await show_main_menu(update, context, text)
    else:
        await update.message.reply_text(text)


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
    elif filter_val == 'diskcheck_on':
        filtered = [(k, v) for k, v in filtered if not v.get('disableDiskCheck', False)]
    elif filter_val == 'diskcheck_off':
        filtered = [(k, v) for k, v in filtered if v.get('disableDiskCheck', False)]

    # Применяем сортировку
    if sort_val == 'created_desc':
        def parse_created(info):
            try:
                created_val = info.get('created', '')
                return datetime.strptime(created_val, "%d.%m.%Y %H:%M") if created_val else datetime.min
            except Exception:
                return datetime.min

        filtered.sort(key=lambda x: parse_created(x[1]), reverse=True)
    elif sort_val == 'created_asc':
        def parse_created(info):
            try:
                created_val = info.get('created', '')
                return datetime.strptime(created_val, "%d.%m.%Y %H:%M") if created_val else datetime.min
            except Exception:
                return datetime.min

        filtered.sort(key=lambda x: parse_created(x[1]))
    elif sort_val == 'forever':
        filtered = [(k, v) for k, v in filtered if v.get('period', 0) >= 3650]
        filtered.sort(key=lambda x: x[1].get('created', ''), reverse=True)
    elif sort_val == 'month':
        filtered = [(k, v) for k, v in filtered if v.get('period', 0) == 30]
        filtered.sort(key=lambda x: x[1].get('created', ''), reverse=True)
    elif sort_val == 'week':
        filtered = [(k, v) for k, v in filtered if v.get('period', 0) == 7]
        filtered.sort(key=lambda x: x[1].get('created', ''), reverse=True)
    elif sort_val == 'demo':
        filtered = [(k, v) for k, v in filtered if 0 < v.get('period', 0) < 1]
        filtered.sort(key=lambda x: x[1].get('created', ''), reverse=True)

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

    keyboard.append([InlineKeyboardButton("🗑️ Удалить все демо-ключи", callback_data="delete_demo_keys")])
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


# --- МАССОВЫЕ ДЕЙСТВИЯ ---
async def bulk_mode_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['bulk_mode'] = True
    context.user_data['bulk_selected'] = []
    await key_list(update, context)


async def bulk_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['bulk_mode'] = False
    context.user_data['bulk_selected'] = []
    await key_list(update, context)


async def bulk_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = update.callback_query.data.split('_', 2)[2]
    selected = context.user_data.get('bulk_selected', [])
    if key in selected:
        selected.remove(key)
    else:
        selected.append(key)
    context.user_data['bulk_selected'] = selected
    await key_list(update, context)


async def bulk_confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    selected = context.user_data.get('bulk_selected', [])
    if not selected:
        await key_list(update, context)
        return
    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data="bulk_delete")],
        [InlineKeyboardButton("❌ Нет, назад", callback_data="key_list")]
    ]
    text = f"⚠️ Удалить {len(selected)} выбранных ключей?"
    await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def bulk_confirm_annul(update: Update, context: ContextTypes.DEFAULT_TYPE):
    selected = context.user_data.get('bulk_selected', [])
    if not selected:
        await key_list(update, context)
        return
    keyboard = [
        [InlineKeyboardButton("✅ Да, аннулировать", callback_data="bulk_annul")],
        [InlineKeyboardButton("❌ Нет, назад", callback_data="key_list")]
    ]
    text = f"⚠️ Аннулировать {len(selected)} выбранных ключей?"
    await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def bulk_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    selected = context.user_data.get('bulk_selected', [])
    lic_dict = await get_licenses_from_gist() or {}
    for k in list(selected):
        if k in lic_dict:
            del lic_dict[k]
    await save_licenses_to_gist(lic_dict)
    context.user_data['bulk_mode'] = False
    context.user_data['bulk_selected'] = []
    await show_main_menu(update, context, f"🗑️ Удалено {len(selected)} ключей.")


async def bulk_annul(update: Update, context: ContextTypes.DEFAULT_TYPE):
    selected = context.user_data.get('bulk_selected', [])
    lic_dict = await get_licenses_from_gist() or {}
    for k in list(selected):
        if k in lic_dict and lic_dict[k].get('hasActive'):
            lic_dict[k]['hasActive'] = False
            lic_dict[k]['hwid'] = ""
            lic_dict[k]['issued'] = ""
            lic_dict[k]['expired'] = ""
    await save_licenses_to_gist(lic_dict)
    context.user_data['bulk_mode'] = False
    context.user_data['bulk_selected'] = []
    await show_main_menu(update, context, f"🚫 Аннулировано {len(selected)} ключей.")


# --- ПОИСК ПО НИКУ ---
async def find_key_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


# --- ПОДТВЕРЖДЕНИЯ ---
async def confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    key = query.data.split('_', 2)[2] if query and hasattr(query, 'data') and query.data else None
    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_{key}"),
         InlineKeyboardButton("❌ Нет, назад", callback_data=f"edit_{key}")]
    ]
    if query:
        await query.edit_message_text(
            f"<b>⚠️ Вы уверены, что хотите удалить ключ</b> <code>{key}</code>?\nЭто действие <b>необратимо</b>!",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def confirm_annul(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    key = query.data.split('_', 2)[2] if query and hasattr(query, 'data') and query.data else None
    keyboard = [
        [InlineKeyboardButton("✅ Да, аннулировать", callback_data=f"annul_{key}"),
         InlineKeyboardButton("❌ Нет, назад", callback_data=f"edit_{key}")]
    ]
    if query:
        await query.edit_message_text(
            f"<b>⚠️ Вы уверены, что хотите аннулировать ключ</b> <code>{key}</code>?\nПосле аннуляции ключ станет неактивным.",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


# --- УДАЛЕНИЕ КЛЮЧА ---
async def delete_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    key = query.data.split('_', 1)[1] if query and hasattr(query, 'data') and query.data else None
    lic_dict = await get_licenses_from_gist()

    if not lic_dict or key not in lic_dict or lic_dict[key] is None:
        await query.answer("Ключ не найден!", show_alert=True)
        return

    if key in lic_dict:
        del lic_dict[key]
        await save_licenses_to_gist(lic_dict)
        await query.answer(f"🗑️ Ключ {key} удалён!", show_alert=True)
        await show_main_menu(update, context, f"🗑️ Ключ <code>{key}</code> успешно удалён!")


# --- АННУЛИРОВАНИЕ КЛЮЧА ---
async def annul_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    key = query.data.split('_', 1)[1] if query and hasattr(query, 'data') and query.data else None
    lic_dict = await get_licenses_from_gist()

    if not lic_dict or key not in lic_dict or lic_dict[key] is None:
        await query.answer("Ключ не найден!", show_alert=True)
        return

    lic_dict[key]['hasActive'] = False
    lic_dict[key]['hwid'] = ""
    lic_dict[key]['issued'] = ""
    lic_dict[key]['expired'] = ""
    await save_licenses_to_gist(lic_dict)
    await query.answer(f"🚫 Ключ {key} аннулирован!", show_alert=True)
    await show_main_menu(update, context, f"🚫 Ключ <code>{key}</code> аннулирован.")


# --- АКТИВАЦИЯ КЛЮЧА ---
async def activate_key(key, context):
    """Активирует ключ и отправляет уведомление админам"""
    lic_dict = await get_licenses_from_gist()
    admin_ids = await get_admin_ids()

    if not lic_dict or key not in lic_dict or lic_dict[key] is None:
        return

    info = lic_dict[key]
    moscow_tz = pytz.timezone('Europe/Moscow')

    if not info.get('issued') or info.get('issued') in (0, '', None):
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
    await save_licenses_to_gist(lic_dict)

    # Отправка уведомлений админам
    for admin_id in admin_ids:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"🔔 Ключ `{key}` был активирован!"
            )
        except Exception:
            pass


# --- ИЗМЕНЕНИЕ ДАТЫ И PERIOD ---
async def set_expired(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    key = query.data.split('_', 1)[1] if query and hasattr(query, 'data') and query.data else None
    context.user_data['edit_key'] = key
    context.user_data['edit_field'] = 'expired'
    keyboard = [[InlineKeyboardButton("↩️ Назад", callback_data=f"edit_{key}")]]
    if query:
        await query.edit_message_text(f"<b>Введите новую дату истечения (ДД.ММ.ГГГГ):</b>",
                                      reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def set_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    key = query.data.split('_', 1)[1] if query and hasattr(query, 'data') and query.data else None
    context.user_data['edit_key'] = key
    context.user_data['edit_field'] = 'period'
    keyboard = [[InlineKeyboardButton("↩️ Назад", callback_data=f"edit_{key}")]]
    if query:
        await query.edit_message_text(f"<b>Введите новый period (в днях):</b>",
                                      reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


# --- DISK CHECK ---
async def toggle_diskcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    key = query.data.split('_', 1)[1] if query and hasattr(query, 'data') and query.data else None
    lic_dict = await get_licenses_from_gist()

    if not lic_dict or key not in lic_dict or lic_dict[key] is None:
        await query.answer("Ключ не найден!", show_alert=True)
        return

    lic_dict[key]['disableDiskCheck'] = not lic_dict[key].get('disableDiskCheck', False)
    await save_licenses_to_gist(lic_dict)
    await edit_key_menu(update, context)


# --- ГЕНЕРАЦИЯ КЛЮЧА ---
async def generate_key_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало генерации ключа"""
    if not await check_admin_auth(update, context):
        return

    context.user_data['gen_step'] = 'customer'
    keyboard = [[InlineKeyboardButton("↩️ Назад", callback_data="main_menu")]]

    if update.callback_query:
        await update.callback_query.edit_message_text("Введите имя клиента:",
                                                      reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text("Введите имя клиента:", reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_generate_key_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода для генерации ключа"""
    if not await check_admin_auth(update, context):
        return

    if context.user_data.get('gen_step') == 'customer':
        customer = update.message.text.strip()
        context.user_data['gen_customer'] = customer
        context.user_data['gen_step'] = 'time'
        keyboard = [
            [InlineKeyboardButton("Неделя", callback_data="gen_time_week"),
             InlineKeyboardButton("Месяц", callback_data="gen_time_month")],
            [InlineKeyboardButton("Год", callback_data="gen_time_year"),
             InlineKeyboardButton("Бессрочно", callback_data="gen_time_unlimited")],
            [InlineKeyboardButton("Демо-ключ", callback_data="gen_time_demo")],
            [InlineKeyboardButton("↩️ Назад", callback_data="main_menu")]
        ]
        await update.message.reply_text("Выберите срок действия:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    await handle_text_input(update, context)


async def send_client_window(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str):
    """Отправляет сообщение для клиента с ключом"""
    text = (
        f"Ваш ключ активации: <code>{key}</code>\n\n"
        "Внимательно читайте инструкцию перед применением и не запускайте программы в архиве — сначала распакуйте.\n"
        "Прежде чем задавать вопросы по работе программы, читайте инструкцию и проверяйте, всё ли вы правильно сделали.\n"
        "\n"
        "<b>ИНСТРУКЦИЯ ЕСТЬ В ОТПРАВЛЕННОМ ВАМ АРХИВЕ</b>"
    )

    if update.callback_query:
        sent = await update.callback_query.message.reply_text(text, parse_mode='HTML')
    else:
        sent = await update.message.reply_text(text, parse_mode='HTML')

    await asyncio.sleep(15)
    try:
        await sent.delete()
    except Exception:
        pass


async def handle_generate_key_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора срока для ключа"""
    if not await check_admin_auth(update, context):
        return

    query = update.callback_query
    if not query or not hasattr(query, 'data') or not query.data.startswith('gen_time_'):
        return

    time_val = query.data.replace('gen_time_', '')
    customer = context.user_data.get('gen_customer', 'unknown')

    # Обновляем конфиг перед генерацией
    await db.refresh()

    generator = LicenseGenerator()
    # Передаем токены в генератор
    generator.GITHUB_TOKEN = await get_github_token()
    generator.GIST_ID = await get_gist_id()
    generator.LICENSE_FILE_IN_GIST = await get_gist_filename()

    await asyncio.to_thread(generator.load_licenses)
    license = generator.create_license(customer=customer, time=time_val)

    if license and license.get('key'):
        generator.valid_licenses[license['key']] = license
        success = await asyncio.to_thread(generator.save_licenses)

        if success:
            logger.info(f"Создан ключ для клиента '{customer}': {license.get('key')}")
            text = (
                f"✅ *Ключ успешно сгенерирован!*\n\n"
                f"🔹 *Клиент:* {license.get('customer', '?')}\n"
                f"🔹 *Срок:* {license.get('period', '?')} дней\n"
                f"🔹 *Дата генерации:* {license.get('created', '?')} (МСК)\n\n"
                f"🔑 *Сгенерированный ключ:* `{license.get('key')}`"
            )
            keyboard = [
                [InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_{license.get('key')}")],
                [InlineKeyboardButton("📤 Показать окно для клиента",
                                      callback_data=f"show_client_window_{license.get('key')}")],
                [InlineKeyboardButton("↩️ В меню", callback_data="main_menu")]
            ]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            await query.edit_message_text("❌ Ошибка сохранения ключа!", parse_mode='Markdown')
    else:
        await query.edit_message_text("❌ Ошибка генерации ключа!", parse_mode='Markdown')

    context.user_data.pop('gen_step', None)
    context.user_data.pop('gen_customer', None)


# --- ДЕМО-КЛЮЧ ---
async def create_demo_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создание демо-ключа"""
    if not await check_admin_auth(update, context):
        return

    await db.refresh()

    generator = LicenseGenerator()
    generator.GITHUB_TOKEN = await get_github_token()
    generator.GIST_ID = await get_gist_id()
    generator.LICENSE_FILE_IN_GIST = await get_gist_filename()

    await asyncio.to_thread(generator.load_licenses)
    license = generator.create_license(customer="demo", time="demo")

    if license and 'key' in license:
        key = license['key']
        license['customer'] = 'demo'
        license['disableDiskCheck'] = True
        generator.valid_licenses[key] = license
        success = await asyncio.to_thread(generator.save_licenses)

        if success:
            logger.info(f"Создан демо-ключ: {key}")
            text = f"🆕 Демо-ключ создан: <code>{key}</code>\nДействует 10 минут.\n<i>Нажмите на ключ, чтобы скопировать</i>"
            keyboard = [
                [InlineKeyboardButton("📤 Показать окно для клиента", callback_data=f"show_client_window_{key}")],
                [InlineKeyboardButton("↩️ В меню", callback_data="main_menu")]
            ]
            if update.callback_query:
                await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                              parse_mode='HTML')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        else:
            await show_main_menu(update, context, "❌ Ошибка сохранения демо-ключа!")
    else:
        await show_main_menu(update, context, "❌ Ошибка генерации демо-ключа!")


# --- УДАЛЕНИЕ ВСЕХ ДЕМО-КЛЮЧЕЙ ---
async def delete_demo_keys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет все демо-ключи"""
    lic_dict = await get_licenses_from_gist() or {}
    demo_keys = [k for k, v in lic_dict.items() if v and v.get('customer') == 'demo']

    for k in demo_keys:
        del lic_dict[k]

    await save_licenses_to_gist(lic_dict)
    await show_main_menu(update, context, f"🗑️ Удалено демо-ключей: {len(demo_keys)}")


# --- ПОМОЩЬ ---
async def help_faq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню помощи"""
    if not await check_admin_auth(update, context):
        return

    text = (
        "<b>❓ FAQ и помощь</b>\n\n"
        "<b>🟢 Активный</b> — ключ действующий, привязан к клиенту.\n"
        "<b>🟡 Демо</b> — тестовый ключ, действует 10 минут.\n"
        "<b>⚫️ Аннулирован</b> — ключ был аннулирован и не может быть использован.\n"
        "<b>🔴 Неактивен</b> — срок действия истёк или ключ не активирован.\n\n"
        "<b>Основные действия:</b>\n"
        "- <b>Список ключей</b>: фильтрация, сортировка, массовые действия.\n"
        "- <b>Карточка ключа</b>: подробная информация, быстрые действия.\n"
        "- <b>Массовые действия</b>: выделяйте несколько ключей для удаления/аннуляции.\n"
        "- <b>Обновить данные из БД</b>: принудительное обновление конфигурации.\n\n"
        "<b>Если возникли вопросы — обращайтесь к администратору.</b>"
    )

    keyboard = [[InlineKeyboardButton("↩️ В меню", callback_data="main_menu")]]

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


# --- МЕНЮ ФИЛЬТРОВ ---
async def keylist_filter_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню фильтров и сортировки"""
    text = (
        "<b>🏠 Главное меню</b> / <b>📋 Список ключей</b> / <b>⚙️ Фильтр/Сортировка</b>\n\n"
        "Выберите категорию:"
    )
    keyboard = [
        [InlineKeyboardButton("🔍 Фильтры", callback_data="filter_menu")],
        [InlineKeyboardButton("📊 Сортировка", callback_data="sort_menu")],
        [InlineKeyboardButton("📅 По сроку", callback_data="duration_menu")],
        [InlineKeyboardButton("↩️ Назад", callback_data="key_list")]
    ]

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def filter_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню фильтров"""
    text = (
        "<b>🏠 Главное меню</b> / <b>📋 Список ключей</b> / <b>⚙️ Фильтр/Сортировка</b> / <b>🔍 Фильтры</b>\n\n"
        "Выберите фильтр:"
    )
    keyboard = [
        [InlineKeyboardButton("Все", callback_data="set_filter_all"),
         InlineKeyboardButton("Активные", callback_data="set_filter_active")],
        [InlineKeyboardButton("Демо", callback_data="set_filter_demo"),
         InlineKeyboardButton("Аннулированные", callback_data="set_filter_annulled")],
        [InlineKeyboardButton("Истекают (7д)", callback_data="set_filter_soon_expired")],
        [InlineKeyboardButton("Проверка диска: ВКЛ", callback_data="set_filter_diskcheck_on"),
         InlineKeyboardButton("Проверка диска: ВЫКЛ", callback_data="set_filter_diskcheck_off")],
        [InlineKeyboardButton("↩️ Назад", callback_data="keylist_filter_menu")]
    ]

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def sort_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню сортировки"""
    text = (
        "<b>🏠 Главное меню</b> / <b>📋 Список ключей</b> / <b>⚙️ Фильтр/Сортировка</b> / <b>📊 Сортировка</b>\n\n"
        "Выберите сортировку:"
    )
    keyboard = [
        [InlineKeyboardButton("По созданию ⬇️", callback_data="set_sort_created_desc"),
         InlineKeyboardButton("По созданию ⬆️", callback_data="set_sort_created_asc")],
        [InlineKeyboardButton("По истечению ⬇️", callback_data="set_sort_expired_desc"),
         InlineKeyboardButton("По истечению ⬆️", callback_data="set_sort_expired_asc")],
        [InlineKeyboardButton("По клиенту ⬇️", callback_data="set_sort_customer_desc"),
         InlineKeyboardButton("По клиенту ⬆️", callback_data="set_sort_customer_asc")],
        [InlineKeyboardButton("↩️ Назад", callback_data="keylist_filter_menu")]
    ]

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def duration_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню выбора по сроку"""
    text = (
        "<b>🏠 Главное меню</b> / <b>📋 Список ключей</b> / <b>⚙️ Фильтр/Сортировка</b> / <b>📅 По сроку</b>\n\n"
        "Выберите срок действия:"
    )
    keyboard = [
        [InlineKeyboardButton("Все периоды", callback_data="set_duration_all")],
        [InlineKeyboardButton("🔑 Навсегда", callback_data="set_sort_forever"),
         InlineKeyboardButton("📅 Месяц", callback_data="set_sort_month")],
        [InlineKeyboardButton("📆 Неделя", callback_data="set_sort_week"),
         InlineKeyboardButton("🎯 Демо", callback_data="set_sort_demo")],
        [InlineKeyboardButton("↩️ Назад", callback_data="keylist_filter_menu")]
    ]

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


# --- ЭКСПОРТ/ИМПОРТ ---
async def export_import_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню экспорта/импорта"""
    if not await check_admin_auth(update, context):
        return

    text = (
        "<b>🏠 Главное меню</b> / <b>📊 Экспорт/Импорт</b>\n\n"
        "Выберите действие:"
    )
    keyboard = [
        [InlineKeyboardButton("📤 Экспорт в CSV", callback_data="export_csv")],
        [InlineKeyboardButton("📥 Импорт из CSV", callback_data="import_csv")],
        [InlineKeyboardButton("↩️ Назад", callback_data="main_menu")]
    ]

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт в CSV"""
    if not await check_admin_auth(update, context):
        return

    lic_dict = await get_licenses_from_gist()
    if not lic_dict:
        await show_main_menu(update, context, "Нет данных для экспорта.")
        return

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        'Ключ', 'Клиент', 'Создан', 'Истекает', 'Период (дней)',
        'Статус', 'HWID', 'Примечания'
    ])

    now = datetime.now()

    for key, info in lic_dict.items():
        if not info:
            continue

        expired_str = info.get('expired', '')
        expired = None
        if expired_str and isinstance(expired_str, str):
            try:
                exp_str = expired_str.split()[0] if len(expired_str) > 10 else expired_str
                if exp_str:
                    expired = datetime.strptime(exp_str, "%d.%m.%Y")
            except Exception:
                expired = None

        if info.get('hasActive'):
            status = 'Активен'
        elif info.get('customer') == 'demo':
            status = 'Демо'
        elif not info.get('hasActive') and info.get('expired') == '':
            status = 'Аннулирован'
        elif expired and expired < now:
            status = 'Истек'
        else:
            status = 'Неактивен'

        period = info.get('period', 0)
        notes = ""
        if info.get('customer') == 'demo':
            notes = "Демо-ключ"
        elif period >= 3650:
            notes = "Навсегда"
        elif period == 30:
            notes = "Месяц"
        elif period == 7:
            notes = "Неделя"

        writer.writerow([
            key,
            info.get('customer', ''),
            info.get('created', ''),
            info.get('expired', ''),
            period,
            status,
            info.get('hwid', ''),
            notes
        ])

    csv_content = output.getvalue()
    output.close()

    csv_file = io.BytesIO(csv_content.encode('utf-8-sig'))
    csv_file.name = f'licenses_backup_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'

    text = f"📊 Бэкап завершен!\n\nВсего ключей: {len(lic_dict)}"

    if update.callback_query:
        await update.callback_query.message.reply_document(
            document=csv_file,
            caption=text,
            filename=csv_file.name
        )
        await update.callback_query.answer("Файл отправлен!")
    else:
        await update.message.reply_document(
            document=csv_file,
            caption=text,
            filename=csv_file.name
        )


async def import_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Импорт из CSV"""
    if not await check_admin_auth(update, context):
        return

    keyboard = [[InlineKeyboardButton("↩️ Назад", callback_data="export_import_menu")]]
    text = (
        "📥 <b>Импорт из CSV</b>\n\n"
        "Отправьте CSV файл с лицензиями.\n"
        "Формат: ключ,клиент,создан,истекает,период,статус,hwid\n\n"
        "⚠️ Внимание: существующие ключи будут перезаписаны!"
    )

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                                      parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    context.user_data['awaiting_csv_import'] = True


async def handle_csv_import(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка импорта CSV"""
    if not context.user_data or not context.user_data.get('awaiting_csv_import'):
        return

    if not update.message or not update.message.document:
        await update.message.reply_text("❌ Пожалуйста, отправьте CSV файл.")
        return

    try:
        file = await context.bot.get_file(update.message.document.file_id)
        file_content = await file.download_as_bytearray()
        content = file_content.decode('utf-8')

        reader = csv.reader(io.StringIO(content))
        next(reader)  # Пропускаем заголовки

        lic_dict = await get_licenses_from_gist() or {}
        imported_count = 0

        for row in reader:
            if len(row) >= 7:
                key, customer, created, expired, period, status, hwid = row[:7]

                license_data = {
                    "key": key,
                    "customer": customer,
                    "created": created,
                    "expired": expired,
                    "period": float(period) if period else 0,
                    "hwid": hwid,
                    "hasActive": status.lower() == 'активен',
                    "disableDiskCheck": True
                }

                lic_dict[key] = license_data
                imported_count += 1

        if await save_licenses_to_gist(lic_dict):
            await show_main_menu(update, context, f"✅ Импортировано {imported_count} лицензий!")
        else:
            await show_main_menu(update, context, "❌ Ошибка сохранения!")

    except Exception as e:
        await show_main_menu(update, context, f"❌ Ошибка импорта: {str(e)}")

    context.user_data['awaiting_csv_import'] = False


# --- ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ---
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Основной обработчик текстовых сообщений"""

    # Поиск по нику
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
            keyboard = []
            for k, v in found:
                keyboard.append([InlineKeyboardButton(f"✏️ Редактировать {k}", callback_data=f"edit_{k}")])
            keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="main_menu")])
            await update.message.reply_text("Найденные ключи:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text("Ключи не найдены.")
        return

    # Поиск по ключу
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
            keyboard = []
            for k, v in found:
                keyboard.append([InlineKeyboardButton(f"✏️ Редактировать {k}", callback_data=f"edit_{k}")])
            keyboard.append([InlineKeyboardButton("↩️ Назад", callback_data="main_menu")])
            await update.message.reply_text("Найденный ключ:", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text("Ключ не найден.")
        return

    # Импорт CSV
    elif context.user_data and context.user_data.get('awaiting_csv_import'):
        await handle_csv_import(update, context)
        return

    # Изменение даты/period
    if context.user_data and 'edit_key' in context.user_data and 'edit_field' in context.user_data:
        key = context.user_data['edit_key']
        field = context.user_data['edit_field']
        value = update.message.text.strip()
        lic_dict = await get_licenses_from_gist()

        if key in lic_dict and lic_dict[key] is not None:
            if field == 'expired':
                try:
                    datetime.strptime(value, "%d.%m.%Y")
                    lic_dict[key]['expired'] = value

                    issued_str = lic_dict[key].get('issued')
                    if issued_str:
                        try:
                            issued_date = datetime.strptime(issued_str, "%d.%m.%Y")
                            expired_date = datetime.strptime(value, "%d.%m.%Y")
                            lic_dict[key]['period'] = (expired_date - issued_date).days
                        except Exception:
                            pass

                except ValueError:
                    await update.message.reply_text("❌ Неверный формат даты!")
                    return

            elif field == 'period':
                try:
                    period = int(value)
                    lic_dict[key]['period'] = period

                    issued_str = lic_dict[key].get('issued')
                    if issued_str:
                        try:
                            issued_date = datetime.strptime(issued_str, "%d.%m.%Y")
                            expired_date = issued_date + timedelta(days=period)
                            lic_dict[key]['expired'] = expired_date.strftime("%d.%m.%Y")
                        except Exception:
                            pass

                except ValueError:
                    await update.message.reply_text("❌ Неверный формат числа!")
                    return

            await save_licenses_to_gist(lic_dict)
            await update.message.reply_text(f"✅ Параметр {field} обновлён для ключа <code>{key}</code>!",
                                            parse_mode='HTML')

            context.user_data.pop('edit_key', None)
            context.user_data.pop('edit_field', None)
            await show_main_menu(update, context)
            return

    # Проверка пароля
    await check_admin_auth(update, context)


# --- CALLBACK ROUTER ---
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Роутер для callback запросов"""
    query = update.callback_query
    data = query.data if query and hasattr(query, 'data') and query.data else None

    if not data:
        return

    # Главное меню
    if data == "main_menu":
        await show_main_menu(update, context)

    # Список ключей
    elif data == "key_list" or (data and data.startswith("keylist_page_")):
        await key_list(update, context)

    # Поиск
    elif data == "find_key":
        await find_key_prompt(update, context)
    elif data == "find_by_key":
        await find_by_key_prompt(update, context)

    # Генерация ключа
    elif data == "generate_key":
        await generate_key_start(update, context)
    elif data and data.startswith("gen_time_"):
        await handle_generate_key_time(update, context)
    elif data and data.startswith("show_client_window_"):
        key = data.replace("show_client_window_", "")
        await send_client_window(update, context, key)

    # Редактирование ключа
    elif data and data.startswith("edit_"):
        await edit_key_menu(update, context)
    elif data and data.startswith("confirm_delete_"):
        await confirm_delete(update, context)
    elif data and data.startswith("delete_"):
        await delete_key(update, context)
    elif data and data.startswith("confirm_annul_"):
        await confirm_annul(update, context)
    elif data and data.startswith("annul_"):
        await annul_key(update, context)
    elif data and data.startswith("setexp_"):
        await set_expired(update, context)
    elif data and data.startswith("setperiod_"):
        await set_period(update, context)
    elif data and data.startswith("diskcheck_"):
        await toggle_diskcheck(update, context)
    elif data and data.startswith("toggle_active_"):
        await toggle_active_license(update, context)

    # Фильтры и сортировка
    elif data == "keylist_filter_menu":
        await keylist_filter_menu(update, context)
    elif data == "filter_menu":
        await filter_menu(update, context)
    elif data == "sort_menu":
        await sort_menu(update, context)
    elif data == "duration_menu":
        await duration_menu(update, context)

    # Установка фильтров
    elif data == "set_filter_all":
        context.user_data['keylist_filter'] = 'all'
        await key_list(update, context)
    elif data == "set_filter_active":
        context.user_data['keylist_filter'] = 'active'
        await key_list(update, context)
    elif data == "set_filter_demo":
        context.user_data['keylist_filter'] = 'demo'
        await key_list(update, context)
    elif data == "set_filter_annulled":
        context.user_data['keylist_filter'] = 'annulled'
        await key_list(update, context)
    elif data == "set_filter_soon_expired":
        context.user_data['keylist_filter'] = 'soon_expired'
        await key_list(update, context)
    elif data == "set_filter_diskcheck_on":
        context.user_data['keylist_filter'] = 'diskcheck_on'
        await key_list(update, context)
    elif data == "set_filter_diskcheck_off":
        context.user_data['keylist_filter'] = 'diskcheck_off'
        await key_list(update, context)

    # Установка сортировки
    elif data == "set_sort_created_desc":
        context.user_data['keylist_sort'] = 'created_desc'
        await key_list(update, context)
    elif data == "set_sort_created_asc":
        context.user_data['keylist_sort'] = 'created_asc'
        await key_list(update, context)
    elif data == "set_sort_forever":
        context.user_data['keylist_sort'] = 'forever'
        await key_list(update, context)
    elif data == "set_sort_month":
        context.user_data['keylist_sort'] = 'month'
        await key_list(update, context)
    elif data == "set_sort_week":
        context.user_data['keylist_sort'] = 'week'
        await key_list(update, context)
    elif data == "set_sort_demo":
        context.user_data['keylist_sort'] = 'demo'
        await key_list(update, context)
    elif data == "set_duration_all":
        context.user_data['keylist_sort'] = 'created_desc'
        await key_list(update, context)

    # Массовые действия
    elif data == "bulk_mode_on":
        await bulk_mode_on(update, context)
    elif data and data.startswith("bulk_toggle_"):
        await bulk_toggle(update, context)
    elif data == "bulk_cancel":
        await bulk_cancel(update, context)
    elif data == "bulk_confirm_delete":
        await bulk_confirm_delete(update, context)
    elif data == "bulk_confirm_annul":
        await bulk_confirm_annul(update, context)
    elif data == "bulk_delete":
        await bulk_delete(update, context)
    elif data == "bulk_annul":
        await bulk_annul(update, context)

    # Демо-ключи
    elif data == "delete_demo_keys":
        await delete_demo_keys(update, context)

    # Экспорт/Импорт
    elif data == "export_import_menu":
        await export_import_menu(update, context)
    elif data == "export_csv":
        await export_csv(update, context)
    elif data == "import_csv":
        await import_csv(update, context)

    # Обновление конфига
    elif data == "refresh_config":
        await refresh_config(update, context)

    # Помощь
    elif data == "help_faq":
        await help_faq(update, context)


# --- ИНИЦИАЛИЗАЦИЯ ПРИ ЗАПУСКЕ ---
async def post_init(application):
    """Действия после инициализации бота"""
    logger.info("🚀 Бот запускается...")
    await db.init_pool()
    logger.info("✅ База данных инициализирована")


async def shutdown(application):
    """Действия при остановке бота"""
    logger.info("🛑 Бот останавливается...")
    await db.close_pool()
    logger.info("✅ Соединения с БД закрыты")


# --- MAIN ---
def main() -> None:
    """Главная функция"""
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(shutdown)
        .build()
    )

    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_generate_key_input))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_csv_import))
    application.add_handler(CallbackQueryHandler(callback_router))

    logger.info("✅ Бот успешно запущен и ожидает команды!")
    application.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()