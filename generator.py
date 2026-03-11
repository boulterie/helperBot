import hashlib
import secrets
import string
import json
import os
import requests
import asyncio
import asyncpg
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import pytz

# --- Настройка временной зоны ---
moscow_tz = pytz.timezone('Europe/Moscow')

# Импортируем конфигурацию
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    raise ValueError("❌ Не задана переменная окружения DATABASE_URL")


# --- Класс для получения данных из БД ---
class GistConfig:
    """Класс для получения GitHub токена, Gist ID и имени файла из БД"""

    def __init__(self):
        self._cache = {}
        self._last_update = None

    async def _fetch_from_db(self):
        """Получает данные из БД"""
        conn = None
        try:
            conn = await asyncpg.connect(DATABASE_URL)

            # Получаем только нужные поля из первой записи
            row = await conn.fetchrow("""
                SELECT gittoken, gistid, gistfilename 
                FROM tokens 
                LIMIT 1
            """)

            if row:
                self._cache = {
                    'github_token': row['gittoken'],
                    'gist_id': row['gistid'],
                    'gist_filename': row.get('gistfilename', 'licenses.json')
                }
                self._last_update = datetime.now()
                return True
            else:
                print("❌ Таблица tokens пуста!")
                return False

        except Exception as e:
            print(f"❌ Ошибка получения данных из БД: {e}")
            return False
        finally:
            if conn:
                await conn.close()

    async def get_github_token(self) -> Optional[str]:
        """Возвращает GitHub токен"""
        if not self._cache or not self._last_update:
            await self._fetch_from_db()
        return self._cache.get('github_token')

    async def get_gist_id(self) -> Optional[str]:
        """Возвращает Gist ID"""
        if not self._cache or not self._last_update:
            await self._fetch_from_db()
        return self._cache.get('gist_id')

    async def get_gist_filename(self) -> str:
        """Возвращает имя файла в Gist (по умолчанию licenses.json)"""
        if not self._cache or not self._last_update:
            await self._fetch_from_db()
        return self._cache.get('gist_filename', 'licenses.json')

    async def refresh(self):
        """Принудительное обновление данных"""
        self._last_update = None
        await self._fetch_from_db()


# --- Создаем глобальный экземпляр конфига ---
gist_config = GistConfig()


# --- Вспомогательная функция для синхронного получения данных (для обратной совместимости) ---
def get_sync_config():
    """Синхронная обертка для получения конфига (для существующего кода)"""
    loop = None
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        # Если цикл уже запущен, создаем задачу
        future = asyncio.run_coroutine_threadsafe(
            asyncio.gather(
                gist_config.get_github_token(),
                gist_config.get_gist_id(),
                gist_config.get_gist_filename()
            ),
            loop
        )
        token, gist_id, filename = future.result()
    else:
        # Если цикла нет, запускаем новый
        token, gist_id, filename = loop.run_until_complete(
            asyncio.gather(
                gist_config.get_github_token(),
                gist_config.get_gist_id(),
                gist_config.get_gist_filename()
            )
        )

    return token, gist_id, filename


# --- Обновляем глобальные переменные (будут обновляться при импорте) ---
try:
    GITHUB_TOKEN, GIST_ID, LICENSE_FILE_IN_GIST = get_sync_config()
except Exception as e:
    print(f"⚠️ Не удалось загрузить конфиг при импорте: {e}")
    GITHUB_TOKEN = ""
    GIST_ID = ""
    LICENSE_FILE_IN_GIST = "licenses.json"


class LicenseGenerator:
    def __init__(self):
        self.key_pattern = "XXXX-XXXX-XXXX-XXXX"
        self.valid_licenses: Dict[str, Dict[str, Any]] = {}
        # Обновляем глобальные переменные при инициализации
        self._refresh_config()
        self.load_licenses()

    def _refresh_config(self):
        """Обновляет конфигурацию из БД"""
        global GITHUB_TOKEN, GIST_ID, LICENSE_FILE_IN_GIST
        try:
            GITHUB_TOKEN, GIST_ID, LICENSE_FILE_IN_GIST = get_sync_config()
        except Exception as e:
            print(f"⚠️ Ошибка обновления конфига: {e}")

    def generate_key(self, length: int = 16, segments: int = 4, max_attempts: int = 100) -> str:
        """Генерация уникального лицензионного ключа"""
        segment_length = length // segments
        chars = string.ascii_uppercase + string.digits

        attempt = 0
        while attempt < max_attempts:
            # Генерируем части ключа
            segments_list = []
            for _ in range(segments):
                segment = ''.join(secrets.choice(chars) for _ in range(segment_length))
                segments_list.append(segment)

            # Собираем полный ключ
            key = '-'.join(segments_list)

            # Проверяем уникальность
            if not self._key_exists(key):
                return key
            attempt += 1

        raise RuntimeError(f"Не удалось сгенерировать уникальный ключ после {max_attempts} попыток")

    def _key_exists(self, key: str) -> bool:
        """Проверяет существование ключа в памяти и в файле"""
        return key in self.valid_licenses

    def create_license(self, customer: str = "", time: str = "") -> dict:
        try:
            license_key = self.generate_key()
            current_time = datetime.now(moscow_tz)
            curtime_str = current_time.strftime("%d.%m.%Y %H:%M")
            demo = False
            disableDisk = False

            if time == 'demo':
                period = 0
                demo = True
                disableDisk = True
            elif time == 'week':
                period = 7
            elif time == 'month':
                period = 30
            elif time == 'year':
                period = 365
            elif time == 'unlimited':
                period = 3650
            else:
                raise ValueError("Неверный срок действия")

            print(f"[DEBUG] Генерация ключа: {license_key} | DATE: {curtime_str}")

            license_data = {
                "key": license_key,
                "customer": customer,
                "created": curtime_str,
                "period": period,
                "issued": 0,
                "expired": 0,
                "hwid": "",
                "hash": self._hash_key(license_key),
                "disableDiskCheck": disableDisk,
                "hasActive": False,
                "isDemo": demo,
                "demoActivationCount": 0
            }
            self.valid_licenses[license_key] = license_data
            return license_data
        except RuntimeError as e:
            print(f"Ошибка: {e}")
            return {}

    def _hash_key(self, key: str) -> str:
        """Создает SHA-256 хэш ключа"""
        return hashlib.sha256(key.encode()).hexdigest()

    def save_licenses(self) -> bool:
        """Сохраняет лицензии в GitHub Gist через API, сохраняя существующие записи."""
        # Обновляем конфиг перед сохранением
        self._refresh_config()

        if not GITHUB_TOKEN or not GIST_ID:
            print("GITHUB_TOKEN или GIST_ID не заданы!")
            return False

        # Сначала получаем текущие лицензии из Gist
        current_licenses = self.get_licenses_from_gist()

        # Объединяем старые и новые лицензии
        merged_licenses = {**current_licenses, **self.valid_licenses}

        url = f"https://api.github.com/gists/{GIST_ID}"
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        payload = {
            "files": {
                LICENSE_FILE_IN_GIST: {
                    "content": json.dumps(merged_licenses, indent=4, ensure_ascii=False)
                }
            }
        }

        try:
            response = requests.patch(url, headers=headers, json=payload)
            if response.status_code == 200:
                # Обновляем локальную копию
                self.valid_licenses = merged_licenses
                return True
            else:
                print(f"Ошибка сохранения в Gist! Код: {response.status_code}\nОтвет: {response.text}")
                return False
        except requests.RequestException as e:
            print(f"Ошибка при сохранении в Gist: {e}")
            return False

    def get_licenses_from_gist(self) -> dict:
        """Получает текущие лицензии из GitHub Gist"""
        # Обновляем конфиг перед получением
        self._refresh_config()

        if not GITHUB_TOKEN or not GIST_ID:
            return {}

        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }

        try:
            response = requests.get(f"https://api.github.com/gists/{GIST_ID}", headers=headers)
            if response.status_code == 200:
                gist_data = response.json()
                if LICENSE_FILE_IN_GIST in gist_data['files']:
                    licenses_content = gist_data['files'][LICENSE_FILE_IN_GIST]['content']
                    return json.loads(licenses_content)
                else:
                    print(f"❌ Файл {LICENSE_FILE_IN_GIST} не найден в Gist")
                    return {}
            else:
                print(f"❌ Ошибка получения Gist: {response.status_code}")
                return {}
        except Exception as e:
            print(f"❌ Ошибка при получении лицензий: {e}")
            return {}

    def load_licenses(self) -> bool:
        """Загружает лицензии из GitHub Gist."""
        self.valid_licenses = self.get_licenses_from_gist()
        return bool(self.valid_licenses)

    def validate_key(self, key: str) -> bool:
        """Проверяет валидность ключа"""
        return key in self.valid_licenses and not self.valid_licenses[key].get('hasActive', False)

    def get_license_info(self, key: str) -> Optional[dict]:
        """Возвращает информацию о лицензии"""
        return self.valid_licenses.get(key)


# --- Асинхронная версия для использования в Telegram боте ---
async def get_gist_config_async():
    """Асинхронное получение конфигурации"""
    token = await gist_config.get_github_token()
    gist_id = await gist_config.get_gist_id()
    filename = await gist_config.get_gist_filename()
    return token, gist_id, filename


# --- Функция для обновления конфига в рантайме ---
def refresh_config():
    """Обновляет глобальные переменные из БД"""
    global GITHUB_TOKEN, GIST_ID, LICENSE_FILE_IN_GIST
    try:
        GITHUB_TOKEN, GIST_ID, LICENSE_FILE_IN_GIST = get_sync_config()
        print("✅ Конфигурация обновлена из БД")
        return True
    except Exception as e:
        print(f"❌ Ошибка обновления конфига: {e}")
        return False


# Пример использования
if __name__ == "__main__":
    # Обновляем конфиг перед использованием
    refresh_config()

    generator = LicenseGenerator()

    # Генерация лицензий
    license2 = generator.create_license(customer="test", time="month")

    # Сохранение в Gist
    generator.save_licenses()