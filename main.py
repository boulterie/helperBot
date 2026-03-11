import asyncio
import os

import asyncpg
import logging
import time
import subprocess
import shlex
import re
import json
import requests
from typing import Dict, Any, Optional, List, Tuple

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Импортируем конфигурацию
from environment import BOT_VERSION, SESSION_DURATION
BOT_TOKEN = os.getenv('BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Создаем экземпляры бота и диспетчера
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


# Состояния для FSM
class EditStates(StatesGroup):
    waiting_for_period = State()
    waiting_for_customer = State()
    waiting_for_search_key = State()
    waiting_for_search_user = State()


# Состояния пользователей
user_states = {}  # user_id -> {'state': 'waiting_customer', 'customer': None}
# Хранилище для пагинации ключей
user_pages = {}  # user_id -> current_page
# Временное хранение для редактирования
edit_data = {}  # user_id -> {'key': key, 'action': action}


# Класс для управления сессиями
class SessionManager:
    def __init__(self, duration_minutes: int = 30):
        self.sessions = {}
        self.duration = duration_minutes * 60

    def create_session(self, user_id: int) -> None:
        expiry = time.time() + self.duration
        self.sessions[user_id] = expiry
        logger.info(f"Session created for user {user_id}")

    def is_session_valid(self, user_id: int) -> bool:
        if user_id not in self.sessions:
            return False
        expiry = self.sessions[user_id]
        if time.time() > expiry:
            del self.sessions[user_id]
            return False
        return True

    def refresh_session(self, user_id: int) -> None:
        if user_id in self.sessions:
            expiry = time.time() + self.duration
            self.sessions[user_id] = expiry

    def end_session(self, user_id: int) -> None:
        if user_id in self.sessions:
            del self.sessions[user_id]


# Класс для работы с БД и Gist
class LicenseBotDB:
    def __init__(self):
        self.pool = None
        self.tokens = {}

    async def init_db(self):
        try:
            self.pool = await asyncpg.create_pool(DATABASE_URL)
            logger.info("Connected to database")
            await self.ensure_table_exists()
            await self.load_tokens()
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    async def ensure_table_exists(self):
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tokens')")
            if not exists:
                await conn.execute("""
                    CREATE TABLE tokens (
                        id SERIAL PRIMARY KEY,
                        gittoken VARCHAR(100),
                        gistid VARCHAR(100),
                        gistfilename VARCHAR(100),
                        adminid1 VARCHAR(100),
                        adminid2 VARCHAR(100),
                        adminid3 VARCHAR(100),
                        adminpassword VARCHAR(100)
                    )
                """)
                await conn.execute("""
                    INSERT INTO tokens (gittoken, gistid, gistfilename, adminid1, adminid2, adminid3, adminpassword)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """, "test", "test", "test.json", "123456789", "", "", "admin123")

    async def load_tokens(self):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tokens LIMIT 1")
            if row:
                self.tokens = dict(row)
                logger.info(f"Tokens loaded")

    async def is_admin_by_id(self, user_id: int) -> bool:
        str_id = str(user_id)
        return str_id == self.tokens.get('adminid1') or str_id == self.tokens.get(
            'adminid2') or str_id == self.tokens.get('adminid3')

    async def check_password(self, password: str) -> bool:
        return password == self.tokens.get('adminpassword')

    async def get_gist_data(self) -> Optional[Dict]:
        """Получает данные из Gist"""
        try:
            gittoken = self.tokens.get('gittoken')
            gistid = self.tokens.get('gistid')
            filename = self.tokens.get('gistfilename')

            if not all([gittoken, gistid, filename]):
                logger.error("Missing GitHub credentials")
                return None

            headers = {
                'Authorization': f'token {gittoken}',
                'Accept': 'application/vnd.github.v3+json'
            }

            url = f'https://api.github.com/gists/{gistid}'
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                gist_data = response.json()
                content = gist_data['files'][filename]['content']
                return json.loads(content)
            else:
                logger.error(f"Failed to fetch gist: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error fetching gist: {e}")
            return None

    async def update_gist_data(self, data: Dict) -> bool:
        """Обновляет данные в Gist"""
        try:
            gittoken = self.tokens.get('gittoken')
            gistid = self.tokens.get('gistid')
            filename = self.tokens.get('gistfilename')

            if not all([gittoken, gistid, filename]):
                logger.error("Missing GitHub credentials")
                return False

            headers = {
                'Authorization': f'token {gittoken}',
                'Accept': 'application/vnd.github.v3+json'
            }

            url = f'https://api.github.com/gists/{gistid}'
            payload = {
                'files': {
                    filename: {
                        'content': json.dumps(data, indent=4, ensure_ascii=False)
                    }
                }
            }

            response = requests.patch(url, headers=headers, json=payload)

            if response.status_code == 200:
                logger.info("Gist updated successfully")
                return True
            else:
                logger.error(f"Failed to update gist: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Error updating gist: {e}")
            return False


# Класс для работы с ключами
class LicenseManager:
    def __init__(self, db: LicenseBotDB):
        self.db = db
        self.keys_per_page = 10
        self.cache = None

    async def _refresh_cache(self):
        """Обновляет кэш ключей"""
        self.cache = await self.db.get_gist_data()

    async def get_all_keys(self) -> List[Dict]:
        """Получает все ключи из Gist"""
        await self._refresh_cache()
        if not self.cache:
            return []

        keys = []
        for key, value in self.cache.items():
            value['key'] = key  # Добавляем ключ в данные
            keys.append(value)

        return keys

    async def get_keys_page(self, page: int = 1) -> Tuple[List[Dict], int, int]:
        """Возвращает ключи для указанной страницы"""
        all_keys = await self.get_all_keys()
        total_keys = len(all_keys)
        total_pages = (total_keys + self.keys_per_page - 1) // self.keys_per_page

        start = (page - 1) * self.keys_per_page
        end = start + self.keys_per_page

        page_keys = all_keys[start:end]

        return page_keys, page, total_pages

    def get_status_emoji(self, key_data: Dict) -> str:
        """Возвращает эмодзи статуса ключа"""
        from datetime import datetime

        # Сначала проверяем, истек ли ключ (даже если он активен)
        expired = key_data.get('expired', 0)
        if expired != 0 and isinstance(expired, str):
            try:
                # Парсим дату истечения
                expired_date = datetime.strptime(expired, "%d.%m.%Y")
                current_date = datetime.now()

                # Если дата истечения меньше текущей даты - ключ истек
                if expired_date < current_date:
                    return "🔴"  # Истек
            except Exception as e:
                logger.error(f"Error parsing expired date: {e}")

        # Если ключ активен и не истек
        if key_data.get('hasActive'):
            return "🟢"  # Активен

        # Во всех остальных случаях - желтый
        return "🟡"  # Не активирован

    async def search_by_key(self, search_key: str) -> List[Dict]:
        """Ищет ключ по точному совпадению"""
        all_keys = await self.get_all_keys()
        results = []
        for key_data in all_keys:
            if key_data['key'].lower() == search_key.lower():
                results.append(key_data)
                break
        return results

    async def search_by_customer(self, customer_name: str) -> List[Dict]:
        """Ищет ключи по имени пользователя (частичное совпадение)"""
        all_keys = await self.get_all_keys()
        results = []
        customer_lower = customer_name.lower()
        for key_data in all_keys:
            customer = key_data.get('customer', '')
            if customer and customer_lower in customer.lower():
                results.append(key_data)
        return results

    async def update_key_period(self, key: str, new_period: int) -> bool:
        """Обновляет срок действия ключа с учетом его статуса"""
        try:
            await self._refresh_cache()
            if not self.cache or key not in self.cache:
                return False

            key_data = self.cache[key]
            old_period = key_data.get('period', 0)

            # Обновляем period
            key_data['period'] = new_period

            # Если ключ активирован (issued не 0 и не пустой)
            issued = key_data.get('issued', 0)
            if issued != 0 and issued != "0" and issued:
                from datetime import datetime, timedelta
                try:
                    # Парсим дату активации
                    issued_date = datetime.strptime(issued, "%d.%m.%Y")
                    # Вычисляем новую дату истечения
                    expired_date = issued_date + timedelta(days=new_period)
                    # Форматируем обратно в строку
                    key_data['expired'] = expired_date.strftime("%d.%m.%Y")
                except Exception as e:
                    logger.error(f"Error calculating expired date: {e}")
                    # Если ошибка парсинга, оставляем expired без изменений
                    pass

            # Сохраняем в Gist
            return await self.db.update_gist_data(self.cache)
        except Exception as e:
            logger.error(f"Error updating key period: {e}")
            return False

    async def update_key(self, key: str, updates: Dict) -> bool:
        """Обновляет данные ключа и сохраняет в Gist"""
        try:
            await self._refresh_cache()
            if not self.cache or key not in self.cache:
                return False

            # Обновляем данные
            for field, value in updates.items():
                self.cache[key][field] = value

            # Сохраняем в Gist
            return await self.db.update_gist_data(self.cache)
        except Exception as e:
            logger.error(f"Error updating key: {e}")
            return False

    async def delete_key(self, key: str) -> bool:
        """Удаляет ключ из Gist"""
        try:
            await self._refresh_cache()
            if not self.cache or key not in self.cache:
                return False

            # Удаляем ключ
            del self.cache[key]

            # Сохраняем в Gist
            return await self.db.update_gist_data(self.cache)
        except Exception as e:
            logger.error(f"Error deleting key: {e}")
            return False


# Создаем экземпляры
db = LicenseBotDB()
session_manager = SessionManager(duration_minutes=SESSION_DURATION)
license_manager = LicenseManager(db)


# Функция для парсинга и форматирования вывода генератора
def parse_generator_output(output: str) -> str:
    """Извлекает только нужные строки из вывода генератора и форматирует для Telegram"""
    lines = output.strip().split('\n')
    result_lines = []

    for line in lines:
        # Пропускаем debug строки
        if '[DEBUG]' in line:
            continue

        # Форматируем каждую строку
        line = line.strip()
        if 'Сгенерированный ключ:' in line:
            key = line.split('Сгенерированный ключ:')[1].strip()
            result_lines.append(f"🔑 *Сгенерированный ключ:* `{key}`")
        elif 'Покупатель:' in line:
            customer = line.split('Покупатель:')[1].strip()
            result_lines.append(f"👤 *Покупатель:* {customer}")
        elif 'Срок действия:' in line:
            period = line.split('Срок действия:')[1].strip()
            result_lines.append(f"⏰ *Срок действия:* {period}")
        elif 'Время генерации:' in line:
            created = line.split('Время генерации:')[1].strip()
            result_lines.append(f"✅ *Время генерации:* {created}")

    return '\n'.join(result_lines) if result_lines else output


# Функция запуска генератора
def run_generator(customer: str, period: str) -> str:
    """Запускает cmdGen.py и возвращает отформатированный вывод"""
    try:
        # Экранируем кавычки в имени пользователя
        safe_customer = customer.replace('"', '\\"')
        cmd = f'python cmdGen.py -c "{safe_customer}" -t {period}'
        logger.info(f"Running: {cmd}")

        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30,
            encoding='utf-8'
        )

        if result.returncode == 0:
            # Парсим вывод
            return parse_generator_output(result.stdout)
        else:
            return f"Ошибка: {result.stderr}"
    except Exception as e:
        return f"Ошибка: {str(e)}"


# Клавиатуры
def get_main_keyboard():
    """Главное меню в 2 столбца"""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📋 Список ключей", callback_data="list"),
        InlineKeyboardButton(text="🔍 Поиск по ключу", callback_data="search_key"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="👤 Поиск по пользователю", callback_data="search_user"),
        InlineKeyboardButton(text="➕ Генерация ключа", callback_data="generate"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="❓ FAQ", callback_data="faq"),
        InlineKeyboardButton(text="🚪 Выйти", callback_data="logout"),
        width=2
    )
    return builder.as_markup()


def get_period_keyboard():
    """Клавиатура выбора периода в 2 столбца"""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📅 Неделя", callback_data="period:week"),
        InlineKeyboardButton(text="📅 Месяц", callback_data="period:month"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="📅 Год", callback_data="period:year"),
        InlineKeyboardButton(text="♾️ Бессрочно", callback_data="period:unlimited"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="🔙 Отмена", callback_data="back"),
        width=1
    )
    return builder.as_markup()


def get_back_keyboard():
    """Кнопка назад"""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back"))
    return builder.as_markup()


def get_to_main_keyboard():
    """Кнопка возврата в главное меню"""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🏠 В главное меню", callback_data="back"))
    return builder.as_markup()


def get_keys_list_keyboard(page_keys: List[Dict], current_page: int, total_pages: int, search_mode: bool = False,
                           search_query: str = ""):
    """Клавиатура для списка ключей с пагинацией"""
    builder = InlineKeyboardBuilder()

    # Добавляем кнопки ключей (по одной в ряд)
    for key_data in page_keys:
        key = key_data['key']
        status_emoji = license_manager.get_status_emoji(key_data)
        button_text = f"{status_emoji} {key}"
        builder.row(InlineKeyboardButton(text=button_text, callback_data=f"key:{key}"))

    # Кнопки пагинации (всегда 3 кнопки в ряд)
    pagination_buttons = []

    # Левая кнопка (назад)
    if current_page > 1:
        if search_mode:
            pagination_buttons.append(
                InlineKeyboardButton(text="🔙", callback_data=f"search_page:{search_query}:{current_page - 1}"))
        else:
            pagination_buttons.append(InlineKeyboardButton(text="🔙", callback_data=f"page:{current_page - 1}"))
    else:
        pagination_buttons.append(InlineKeyboardButton(text="✖️", callback_data="noop"))

    # Средняя кнопка (домой)
    pagination_buttons.append(InlineKeyboardButton(text="🏠", callback_data="back"))

    # Правая кнопка (вперед)
    if current_page < total_pages:
        if search_mode:
            pagination_buttons.append(
                InlineKeyboardButton(text="🔜", callback_data=f"search_page:{search_query}:{current_page + 1}"))
        else:
            pagination_buttons.append(InlineKeyboardButton(text="🔜", callback_data=f"page:{current_page + 1}"))
    else:
        pagination_buttons.append(InlineKeyboardButton(text="✖️", callback_data="noop"))

    builder.row(*pagination_buttons, width=3)

    return builder.as_markup()


def get_key_details_keyboard(key: str):
    """Клавиатура для детальной информации о ключе"""
    builder = InlineKeyboardBuilder()

    # Кнопки действий
    builder.row(
        InlineKeyboardButton(text="📅 Изменить срок", callback_data=f"edit:period:{key}"),
        InlineKeyboardButton(text="🧹 Удалить HWID", callback_data=f"edit:clear_hwid:{key}"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="💾 Диск: переключить", callback_data=f"edit:toggle_disk:{key}"),
        InlineKeyboardButton(text="🔴 Деактивировать", callback_data=f"edit:deactivate:{key}"),
        width=2
    )
    builder.row(
        InlineKeyboardButton(text="👤 Изменить имя", callback_data=f"edit:customer:{key}"),
        InlineKeyboardButton(text="🗑️ Удалить ключ", callback_data=f"edit:delete:{key}"),
        width=2
    )

    # Кнопки навигации (7 строка, в два столбца)
    builder.row(
        InlineKeyboardButton(text="📋 К списку", callback_data="list"),
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="back"),
        width=2
    )

    return builder.as_markup()


def get_confirm_delete_keyboard(key: str):
    """Клавиатура подтверждения удаления"""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete:{key}"),
        width=1
    )
    builder.row(
        InlineKeyboardButton(text="❌ Нет, отмена", callback_data=f"key:{key}"),
        width=1
    )
    return builder.as_markup()


# Проверка сессии
async def check_session(user_id: int) -> bool:
    return session_manager.is_session_valid(user_id)


# Старт
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    name = message.from_user.first_name

    session_manager.end_session(user_id)
    await state.clear()

    if await db.is_admin_by_id(user_id):
        session_manager.create_session(user_id)
        await message.answer(
            f"👋 *Добро пожаловать, {name}!*\n✅ Вы вошли как администратор.\n\n📋 *Выберите действие:*",
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown"
        )
    else:
        await message.answer("🔐 Для доступа к боту введите пароль администратора:")


# Ввод пароля и обработка текстовых сообщений
@dp.message()
async def handle_text(message: Message, state: FSMContext):
    user_id = message.from_user.id
    text = message.text

    # Проверяем состояние FSM для редактирования
    current_state = await state.get_state()

    # Редактирование периода
    if current_state == EditStates.waiting_for_period:
        if user_id in edit_data:
            key = edit_data[user_id]['key']
            try:
                days = int(text)

                # Используем специальный метод для обновления периода
                if await license_manager.update_key_period(key, days):
                    await message.answer(
                        f"✅ *Срок действия ключа обновлен на {days} дней*",
                        parse_mode="Markdown"
                    )
                    # Показываем обновленную информацию о ключе
                    await show_key_details_direct(message, key)
                else:
                    await message.answer("❌ Ошибка при обновлении ключа")

                # Очищаем данные
                del edit_data[user_id]
                await state.clear()
                return
            except ValueError:
                await message.answer("❌ Введите число (количество дней)")
                return

    # Редактирование имени пользователя
    elif current_state == EditStates.waiting_for_customer:
        if user_id in edit_data:
            key = edit_data[user_id]['key']

            # Обновляем имя пользователя
            updates = {'customer': text}
            if await license_manager.update_key(key, updates):
                await message.answer(
                    f"✅ *Имя пользователя изменено на:* {text}",
                    parse_mode="Markdown"
                )
                # Показываем обновленную информацию о ключе
                await show_key_details_direct(message, key)
            else:
                await message.answer("❌ Ошибка при обновлении имени")

            # Очищаем данные
            del edit_data[user_id]
            await state.clear()
            return

    # Поиск по ключу
    elif current_state == EditStates.waiting_for_search_key:
        results = await license_manager.search_by_key(text)
        await state.clear()

        if not results:
            await message.answer(
                f"🔍 *Результаты поиска по ключу*\n\n❌ Ключ `{text}` не найден",
                reply_markup=get_back_keyboard(),
                parse_mode="Markdown"
            )
            return  # <-- ВАЖНО: добавляем return

        # Показываем результаты поиска
        await show_search_results(message, results, "ключу", text)
        return  # <-- ВАЖНО: добавляем return

    # Поиск по пользователю
    elif current_state == EditStates.waiting_for_search_user:
        results = await license_manager.search_by_customer(text)
        await state.clear()

        if not results:
            await message.answer(
                f"👤 *Результаты поиска по пользователю*\n\n❌ Пользователь `{text}` не найден",
                reply_markup=get_back_keyboard(),
                parse_mode="Markdown"
            )
            return  # <-- ВАЖНО: добавляем return

        # Показываем результаты поиска
        await show_search_results(message, results, "пользователю", text)
        return  # <-- ВАЖНО: добавляем return

    # Проверяем состояние пользователя для генерации
    elif user_id in user_states:
        state_data = user_states[user_id]

        if state_data['state'] == 'waiting_customer':
            # Сохраняем имя покупателя
            user_states[user_id]['customer'] = text
            user_states[user_id]['state'] = 'waiting_period'
            await message.answer(
                f"👤 *Покупатель:* {text}\n\nВыберите срок действия:",
                reply_markup=get_period_keyboard(),
                parse_mode="Markdown"
            )
            return  # <-- ВАЖНО: добавляем return

    # Если нет состояния - проверяем пароль
    if await db.check_password(text):
        session_manager.create_session(user_id)
        await message.answer(
            f"👋 *Добро пожаловать!*\n🔑 Вы вошли по паролю.\n\n📋 *Выберите действие:*",
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown"
        )
    else:
        await message.answer("❌ Неверный пароль. Попробуйте снова или используйте /start")


# Обработка кнопок
@dp.callback_query()
async def handle_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = callback.data

    # Проверка сессии для всех действий
    if not await check_session(user_id):
        await callback.message.edit_text(
            "⏰ *Сессия истекла*\n\nИспользуйте /start для нового входа.",
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    session_manager.refresh_session(user_id)

    # Заглушка для неактивных кнопок
    if data == "noop":
        await callback.answer("❌ Нет доступных страниц")
        return

    # Главное меню
    if data == "back":
        if user_id in user_states:
            del user_states[user_id]
        if user_id in user_pages:
            del user_pages[user_id]
        if user_id in edit_data:
            del edit_data[user_id]
        await state.clear()
        await callback.message.edit_text(
            "📋 *Главное меню*\n\nВыберите действие:",
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    # Выход
    if data == "logout":
        session_manager.end_session(user_id)
        if user_id in user_states:
            del user_states[user_id]
        if user_id in user_pages:
            del user_pages[user_id]
        if user_id in edit_data:
            del edit_data[user_id]
        await state.clear()
        await callback.message.edit_text(
            "👋 Вы вышли из системы.\nДля нового входа используйте /start"
        )
        await callback.answer()
        return

    # Список ключей
    if data == "list":
        await show_keys_page(callback, user_id, 1)
        return

    # Пагинация для обычного списка
    if data.startswith("page:"):
        page = int(data.split(":")[1])
        await show_keys_page(callback, user_id, page)
        return

    # Пагинация для результатов поиска
    if data.startswith("search_page:"):
        parts = data.split(":")
        search_query = parts[1]
        page = int(parts[2])

        # Определяем тип поиска (по ключу или по пользователю)
        results = await license_manager.search_by_customer(search_query)
        if not results:
            results = await license_manager.search_by_key(search_query)

        if results:
            await show_search_results_page(callback, results, page, search_query)
        else:
            await callback.message.edit_text(
                "❌ Результаты не найдены",
                reply_markup=get_back_keyboard()
            )
        await callback.answer()
        return

    # Просмотр ключа
    if data.startswith("key:"):
        key = data.split(":")[1]
        await show_key_details(callback, key)
        return

    # Редактирование ключа
    if data.startswith("edit:"):
        parts = data.split(":")
        action = parts[1]
        key = parts[2]

        # Изменение срока действия
        if action == "period":
            edit_data[user_id] = {'key': key, 'action': 'period'}
            await state.set_state(EditStates.waiting_for_period)

            # Получаем текущие данные ключа для информации
            all_keys = await license_manager.get_all_keys()
            key_data = None
            for k in all_keys:
                if k['key'] == key:
                    key_data = k
                    break

            status_info = ""
            if key_data:
                if key_data.get('issued') and key_data.get('issued') != 0 and key_data.get('issued') != "0":
                    status_info = f"\n\n🔔 *Ключ активирован* {key_data.get('issued')}\nПри изменении срока expired будет пересчитан автоматически."
                else:
                    status_info = "\n\n⚪ *Ключ не активирован*\nБудет изменен только период."

            await callback.message.edit_text(
                f"📅 *Введите новый срок действия (в днях)*\n\nДля ключа: `{key}`{status_info}",
                reply_markup=get_back_keyboard(),
                parse_mode="Markdown"
            )
            await callback.answer()
            return

        # Удаление HWID
        elif action == "clear_hwid":
            updates = {'hwid': ""}
            if await license_manager.update_key(key, updates):
                await callback.message.edit_text(
                    f"✅ *HWID успешно удален*",
                    parse_mode="Markdown"
                )
                # Показываем обновленную информацию
                await show_key_details(callback, key)
            else:
                await callback.message.edit_text(
                    "❌ Ошибка при удалении HWID",
                    reply_markup=get_back_keyboard()
                )
                await callback.answer()
            return

        # Переключение проверки диска
        elif action == "toggle_disk":
            all_keys = await license_manager.get_all_keys()
            key_data = None
            for k in all_keys:
                if k['key'] == key:
                    key_data = k
                    break

            if key_data:
                current = key_data.get('disableDiskCheck', False)
                updates = {'disableDiskCheck': not current}
                if await license_manager.update_key(key, updates):
                    new_status = "🔒 Вкл" if not updates['disableDiskCheck'] else "🔓 Выкл"
                    await callback.message.edit_text(
                        f"✅ *Проверка диска изменена*\nНовый статус: {new_status}",
                        parse_mode="Markdown"
                    )
                    # Показываем обновленную информацию
                    await show_key_details(callback, key)
                else:
                    await callback.message.edit_text(
                        "❌ Ошибка при изменении проверки диска",
                        reply_markup=get_back_keyboard()
                    )
            else:
                await callback.message.edit_text("❌ Ключ не найден")
            await callback.answer()
            return

        # Деактивация ключа
        elif action == "deactivate":
            updates = {
                'hasActive': False,
                'hwid': "",
                'issued': 0,
                'expired': 0
            }
            if await license_manager.update_key(key, updates):
                await callback.message.edit_text(
                    f"✅ *Ключ деактивирован*",
                    parse_mode="Markdown"
                )
                # Показываем обновленную информацию
                await show_key_details(callback, key)
            else:
                await callback.message.edit_text(
                    "❌ Ошибка при деактивации ключа",
                    reply_markup=get_back_keyboard()
                )
            await callback.answer()
            return

        # Изменение имени пользователя
        elif action == "customer":
            edit_data[user_id] = {'key': key, 'action': 'customer'}
            await state.set_state(EditStates.waiting_for_customer)
            await callback.message.edit_text(
                f"👤 *Введите новое имя пользователя*\n\nДля ключа: `{key}`",
                reply_markup=get_back_keyboard(),
                parse_mode="Markdown"
            )
            await callback.answer()
            return

        # Удаление ключа (с подтверждением)
        elif action == "delete":
            await callback.message.edit_text(
                f"⚠️ *Вы уверены, что хотите удалить ключ?*\n\n`{key}`\n\nЭто действие нельзя отменить.",
                reply_markup=get_confirm_delete_keyboard(key),
                parse_mode="Markdown"
            )
            await callback.answer()
            return

    # Подтверждение удаления
    if data.startswith("confirm_delete:"):
        key = data.split(":")[1]
        if await license_manager.delete_key(key):
            await callback.message.edit_text(
                f"✅ *Ключ успешно удален*",
                parse_mode="Markdown"
            )
            # Возвращаемся к списку ключей
            await show_keys_page(callback, user_id, 1)
        else:
            await callback.message.edit_text(
                "❌ Ошибка при удалении ключа",
                reply_markup=get_back_keyboard()
            )
        await callback.answer()
        return

    # Генерация ключа
    if data == "generate":
        user_states[user_id] = {'state': 'waiting_customer', 'customer': None}
        await callback.message.edit_text(
            "➕ *Генерация ключа*\n\nВведите имя покупателя (любые символы):",
            reply_markup=get_back_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    # Выбор периода для генерации
    if data.startswith("period:"):
        period = data.split(":")[1]

        if user_id not in user_states or user_states[user_id]['state'] != 'waiting_period':
            await callback.message.edit_text(
                "❌ Ошибка, начните заново",
                reply_markup=get_back_keyboard()
            )
            await callback.answer()
            return

        customer = user_states[user_id]['customer']

        # Показываем ожидание
        await callback.message.edit_text(
            "🔄 *Генерация ключа...*\n\nПожалуйста, подождите.",
            parse_mode="Markdown"
        )

        # Запускаем генератор
        result = run_generator(customer, period)

        # Очищаем состояние
        del user_states[user_id]

        # Отправляем результат с одной кнопкой "в главное меню"
        await callback.message.answer(
            f"{result}",
            reply_markup=get_to_main_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    # Поиск по ключу - запрос ввода
    if data == "search_key":
        await state.set_state(EditStates.waiting_for_search_key)
        await callback.message.edit_text(
            "🔍 *Поиск по ключу*\n\nВведите ключ для поиска:",
            reply_markup=get_back_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    # Поиск по пользователю - запрос ввода
    elif data == "search_user":
        await state.set_state(EditStates.waiting_for_search_user)
        await callback.message.edit_text(
            "👤 *Поиск по пользователю*\n\nВведите имя пользователя для поиска:",
            reply_markup=get_back_keyboard(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    # FAQ
    elif data == "faq":
        faq_text = f"""
❓ *FAQ* (v{BOT_VERSION})

📋 *Команды:*
• /start — вход в систему
• /cancel — отмена действия

⏰ *Сессия:* {SESSION_DURATION} минут
• Активна пока вы пользуетесь ботом
• Обновляется при каждом действии
• Автоматически завершается через {SESSION_DURATION} минут бездействия

📱 *Функции меню:*
• Список ключей — просмотр всех ключей
• Поиск по ключу — найти конкретный ключ
• Поиск по пользователю — найти ключи пользователя
• Генерация ключа — создать новый ключ
• Выйти — завершить сессию

🔑 *Генерация ключа:*
• Неделя (7 дней)
• Месяц (30 дней)
• Год (365 дней)
• Бессрочно
"""
        await callback.message.edit_text(
            faq_text,
            reply_markup=get_back_keyboard(),
            parse_mode="Markdown"
        )

    await callback.answer()


async def show_keys_page(callback: CallbackQuery, user_id: int, page: int):
    """Показывает страницу со списком ключей"""
    try:
        # Получаем ключи для страницы
        page_keys, current_page, total_pages = await license_manager.get_keys_page(page)

        if not page_keys:
            await callback.message.edit_text(
                "📋 *Список ключей*\n\n❌ Нет доступных ключей",
                reply_markup=get_back_keyboard(),
                parse_mode="Markdown"
            )
            await callback.answer()
            return

        # Сохраняем текущую страницу
        user_pages[user_id] = current_page

        # Формируем заголовок
        header = f"📋 *Список ключей*\nСтраница {current_page} из {total_pages}\n\n"

        await callback.message.edit_text(
            header,
            reply_markup=get_keys_list_keyboard(page_keys, current_page, total_pages, search_mode=False),
            parse_mode="Markdown"
        )
        await callback.answer()

    except Exception as e:
        logger.error(f"Error showing keys page: {e}")
        await callback.message.edit_text(
            "❌ Ошибка при загрузке ключей",
            reply_markup=get_back_keyboard()
        )
        await callback.answer()


async def show_search_results(message: Message, results: List[Dict], search_type: str, query: str):
    """Показывает результаты поиска с пагинацией"""
    if len(results) == 1:
        # Если найден один ключ, сразу показываем его детали
        key_data = results[0]

        # Экранируем специальные символы
        def escape_markdown(text):
            """Экранирует только проблемные символы для Markdown V2"""
            if not isinstance(text, str):
                return text
            # Для Markdown V2 нужно экранировать: _ * [ ] ( ) ~ ` > # + - = | { } . !
            # Но мы будем использовать Markdown V1, где точки не нужно экранировать
            special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '!']
            for char in special_chars:
                text = text.replace(char, f'\\{char}')
            return text

        activated = "✅ Да" if key_data.get('hasActive') else "❌ Нет"
        disk_check = "🔒 Вкл" if not key_data.get('disableDiskCheck') else "🔓 Выкл"
        demo = "🎮 Да" if key_data.get('isDemo') else "🚫 Нет"

        customer = escape_markdown(str(key_data.get('customer', 'Неизвестно')))
        created = escape_markdown(str(key_data.get('created', 'Неизвестно')))
        period = escape_markdown(str(key_data.get('period', 'Неизвестно')))
        expired = escape_markdown(str(key_data.get('expired', '0')))
        hwid = escape_markdown(str(key_data.get('hwid', '')))

        info_text = f"""
🔑 *Ключ:* `{key_data['key']}`

👤 *Пользователь:* {customer}
📅 *Создан:* {created}
⏰ *Срок действия:* {period} дней
❌ *Истекает:* {expired}
💻 *HWID:* {hwid}

*Статусы:*
• Активирован: {activated}
• Проверка диска: {disk_check}
• Демо: {demo}
"""

        await message.answer(
            info_text,
            reply_markup=get_key_details_keyboard(key_data['key']),
            parse_mode="Markdown"
        )
    else:
        # Если несколько ключей, показываем первую страницу
        await show_search_results_page(message, results, 1, query)


async def show_search_results_page(message_or_callback, results: List[Dict], page: int, query: str):
    """Показывает страницу результатов поиска"""
    items_per_page = 10
    total_results = len(results)
    total_pages = (total_results + items_per_page - 1) // items_per_page

    start = (page - 1) * items_per_page
    end = start + items_per_page
    page_keys = results[start:end]

    # Определяем тип поиска по первому ключу (для заголовка)
    search_type = "ключу"
    if results and results[0].get('customer', '').lower() == query.lower():
        search_type = "пользователю"

    header = f"🔍 *Результаты поиска по {search_type}* `{query}`\nСтраница {page} из {total_pages}\n\n"

    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(
            header,
            reply_markup=get_keys_list_keyboard(page_keys, page, total_pages, search_mode=True, search_query=query),
            parse_mode="Markdown"
        )
    else:
        await message_or_callback.answer(
            header,
            reply_markup=get_keys_list_keyboard(page_keys, page, total_pages, search_mode=True, search_query=query),
            parse_mode="Markdown"
        )


async def show_key_details(callback: CallbackQuery, key: str):
    """Показывает детальную информацию о ключе"""
    try:
        # Получаем все ключи
        all_keys = await license_manager.get_all_keys()

        # Ищем нужный ключ
        key_data = None
        for k in all_keys:
            if k['key'] == key:
                key_data = k
                break

        if not key_data:
            await callback.message.edit_text(
                "❌ Ключ не найден",
                reply_markup=get_back_keyboard()
            )
            await callback.answer()
            return

        # Экранируем специальные символы в тексте
        def escape_markdown(text):
            """Экранирует только проблемные символы для Markdown V2"""
            if not isinstance(text, str):
                return text
            # Для Markdown V2 нужно экранировать: _ * [ ] ( ) ~ ` > # + - = | { } . !
            # Но мы будем использовать Markdown V1, где точки не нужно экранировать
            special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '!']
            for char in special_chars:
                text = text.replace(char, f'\\{char}')
            return text

        # Формируем информацию о ключе с экранированием
        activated = "✅ Да" if key_data.get('hasActive') else "❌ Нет"
        disk_check = "🔒 Вкл" if not key_data.get('disableDiskCheck') else "🔓 Выкл"
        demo = "🎮 Да" if key_data.get('isDemo') else "🚫 Нет"

        customer = escape_markdown(str(key_data.get('customer', 'Неизвестно')))
        created = escape_markdown(str(key_data.get('created', 'Неизвестно')))
        period = escape_markdown(str(key_data.get('period', 'Неизвестно')))
        expired = escape_markdown(str(key_data.get('expired', '0')))
        hwid = escape_markdown(str(key_data.get('hwid', '')))

        info_text = f"""
🔑 *Ключ:* `{key_data['key']}`

👤 *Пользователь:* {customer}
📅 *Создан:* {created}
⏰ *Срок действия:* {period} дней
❌ *Истекает:* {expired}
💻 *HWID:* {hwid}

*Статусы:*
• Активирован: {activated}
• Проверка диска: {disk_check}
• Демо: {demo}
"""

        await callback.message.edit_text(
            info_text,
            reply_markup=get_key_details_keyboard(key),
            parse_mode="Markdown"
        )
        await callback.answer()

    except Exception as e:
        logger.error(f"Error showing key details: {e}")
        # В случае ошибки пробуем отправить без Markdown
        try:
            await callback.message.edit_text(
                f"🔑 Ключ: {key}\n\nПроизошла ошибка при форматировании. Пожалуйста, попробуйте еще раз.",
                reply_markup=get_back_keyboard()
            )
        except:
            pass
        await callback.answer()


async def show_key_details_direct(message: Message, key: str):
    """Показывает детальную информацию о ключе (для прямых сообщений)"""
    try:
        # Получаем все ключи
        all_keys = await license_manager.get_all_keys()

        # Ищем нужный ключ
        key_data = None
        for k in all_keys:
            if k['key'] == key:
                key_data = k
                break

        if not key_data:
            await message.answer("❌ Ключ не найден")
            return

        # Экранируем специальные символы в тексте
        def escape_markdown(text):
            """Экранирует только проблемные символы для Markdown V2"""
            if not isinstance(text, str):
                return text
            # Для Markdown V2 нужно экранировать: _ * [ ] ( ) ~ ` > # + - = | { } . !
            # Но мы будем использовать Markdown V1, где точки не нужно экранировать
            special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '!']
            for char in special_chars:
                text = text.replace(char, f'\\{char}')
            return text

        # Формируем информацию о ключе с экранированием
        activated = "✅ Да" if key_data.get('hasActive') else "❌ Нет"
        disk_check = "🔒 Вкл" if not key_data.get('disableDiskCheck') else "🔓 Выкл"
        demo = "🎮 Да" if key_data.get('isDemo') else "🚫 Нет"

        customer = escape_markdown(str(key_data.get('customer', 'Неизвестно')))
        created = escape_markdown(str(key_data.get('created', 'Неизвестно')))
        period = escape_markdown(str(key_data.get('period', 'Неизвестно')))
        expired = escape_markdown(str(key_data.get('expired', '0')))
        hwid = escape_markdown(str(key_data.get('hwid', '')))

        info_text = f"""
🔑 *Ключ:* `{key_data['key']}`

👤 *Пользователь:* {customer}
📅 *Создан:* {created}
⏰ *Срок действия:* {period} дней
❌ *Истекает:* {expired}
💻 *HWID:* {hwid}

*Статусы:*
• Активирован: {activated}
• Проверка диска: {disk_check}
• Демо: {demo}
"""

        await message.answer(
            info_text,
            reply_markup=get_key_details_keyboard(key),
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Error showing key details: {e}")
        await message.answer(
            f"❌ Ошибка при загрузке информации о ключе\n\n🔑 Ключ: {key}",
            reply_markup=get_back_keyboard()
        )


# Команда /cancel
@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    user_id = message.from_user.id

    if user_id in user_states:
        del user_states[user_id]
    if user_id in user_pages:
        del user_pages[user_id]
    if user_id in edit_data:
        del edit_data[user_id]

    await state.clear()

    if await check_session(user_id):
        await message.answer(
            "❌ Действие отменено.",
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown"
        )
    else:
        await message.answer("❌ Действие отменено.")


# Ошибки
@dp.error()
async def errors_handler(event: types.ErrorEvent):
    logger.error(f"Error: {event.exception}")


async def main():
    await db.init_db()
    logger.info(f"Bot v{BOT_VERSION} started")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())