"""
База данных и парсер запросов.
Pattern matching для распознавания естественного языка.
"""

import re
import asyncpg
import os
from datetime import datetime
from typing import Optional, Tuple


# ==================== Pattern Matching ====================

MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
    "январь": 1, "февраль": 2, "март": 3, "апрель": 4, "май": 5, "июнь": 6,
    "июль": 7, "август": 8, "сентябрь": 9, "октябрь": 10, "ноябрь": 11, "декабрь": 12,
}


def parse_russian_date(date_str: str) -> Optional[datetime]:
    """Парсинг даты: '28 ноября 2025' или '28 ноября' (год=2025)."""
    date_str = date_str.strip().lower()
    match = re.search(
        r"(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря|январь|февраль|март|апрель|май|июнь|июль|август|сентябрь|октябрь|ноябрь|декабрь)(?:\s+(\d{4}))?",
        date_str,
    )
    if match:
        day = int(match.group(1))
        month = MONTHS_RU[match.group(2)]
        year = int(match.group(3)) if match.group(3) else 2025
        try:
            return datetime(year, month, day)
        except ValueError:
            return None
    return None


def parse_date_range(text: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Парсинг диапазона: 'с 1 по 5 ноября 2025'."""
    text = text.lower()
    match = re.search(
        r"с\s+(\d{1,2})\s+(?:по\s+)?(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)(?:\s+(\d{4}))?",
        text,
    )
    if match:
        start_day = int(match.group(1))
        end_day = int(match.group(2))
        month = MONTHS_RU[match.group(3)]
        year = int(match.group(4)) if match.group(4) else 2025
        try:
            start_date = datetime(year, month, start_day)
            end_date = datetime(year, month, end_day + 1)
            return start_date, end_date
        except ValueError:
            return None, None
    return None, None


def extract_creator_id(text: str) -> Optional[str]:
    """Извлечение UUID креатора."""
    match = re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text, re.IGNORECASE)
    if match:
        return match.group(0)
    match = re.search(r"[0-9a-f]{32}", text, re.IGNORECASE)
    if match:
        return match.group(0)
    return None


def extract_number(text: str) -> Optional[int]:
    """Извлечение числа (порог просмотров)."""
    text_cleaned = re.sub(r"(\d)\s+(\d{3})", r"\1\2", text)
    match = re.search(r"\d+", text_cleaned)
    if match:
        return int(match.group(0))
    return None


def parse_query(text: str) -> dict:
    """
    Парсинг запроса через regex patterns.
    Поддерживает 5 типов запросов из задания.
    """
    text_lower = text.lower()

    # Видео с новыми просмотрами на дату
    if re.search(r"сколько.*разн.*видео.*получали.*просмотр", text_lower) or re.search(r"сколько.*видео.*нов.*просмотр", text_lower):
        date = parse_russian_date(text)
        if date:
            return {"type": "videos_with_new_views_on_date", "date": date}

    # Сумма просмотров на дату
    if re.search(r"на сколько.*просмотр.*вырос", text_lower) or re.search(r"сумма.*просмотр.*дата", text_lower):
        date = parse_russian_date(text)
        if date:
            return {"type": "total_views_on_date", "date": date}

    # Всего видео
    if re.search(r"сколько всего видео", text_lower) or re.search(r"сколько.*видео.*систем", text_lower):
        return {"type": "total_videos"}

    # Видео креатора по датам
    if re.search(r"сколько видео у креатора", text_lower) or re.search(r"сколько видео.*креатор", text_lower):
        creator_id = extract_creator_id(text)
        start_date, end_date = parse_date_range(text)
        if creator_id and start_date and end_date:
            return {
                "type": "videos_by_creator_date",
                "creator_id": creator_id,
                "start_date": start_date,
                "end_date": end_date,
            }

    # Видео с порогом просмотров
    if re.search(r"(набрало|набрали).*(больше|более).*просмотр", text_lower) or re.search(r"просмотров.*больше|более.*\d", text_lower):
        threshold = extract_number(text)
        if threshold:
            return {"type": "videos_with_views_threshold", "threshold": threshold}

    return {"type": "unknown"}


# ==================== База данных ====================

class Database:
    def __init__(self):
        self.conn: Optional[asyncpg.Connection] = None

    async def connect(self):
        """Подключение к PostgreSQL."""
        self.conn = await asyncpg.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            database=os.getenv("DB_NAME", "tpsh_db"),
        )

    async def close(self):
        """Закрытие соединения."""
        if self.conn:
            await self.conn.close()

    async def get_total_videos(self) -> int:
        """Сколько всего видео в системе."""
        result = await self.conn.fetchval("SELECT COUNT(*) FROM videos")
        return result or 0

    async def get_videos_by_creator_and_date(
        self, creator_id: str, start_date: datetime, end_date: datetime
    ) -> int:
        """Сколько видео у креатора в диапазоне дат."""
        result = await self.conn.fetchval(
            "SELECT COUNT(*) FROM videos WHERE creator_id = $1 AND video_created_at >= $2 AND video_created_at < $3",
            creator_id, start_date, end_date,
        )
        return result or 0

    async def get_videos_with_views_more_than(self, threshold: int) -> int:
        """Сколько видео набрали больше порога просмотров."""
        result = await self.conn.fetchval(
            "SELECT COUNT(*) FROM videos WHERE views_count > $1", threshold
        )
        return result or 0

    async def get_total_views_on_date(self, date: datetime) -> int:
        """Сумма прироста просмотров за дату."""
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        result = await self.conn.fetchval(
            "SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE created_at >= $1 AND created_at <= $2",
            start_of_day, end_of_day,
        )
        return result or 0

    async def get_videos_with_new_views_on_date(self, date: datetime) -> int:
        """Сколько разных видео получили новые просмотры за дату."""
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        result = await self.conn.fetchval(
            "SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at >= $1 AND created_at <= $2 AND delta_views_count > 0",
            start_of_day, end_of_day,
        )
        return result or 0
