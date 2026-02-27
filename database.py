"""
База данных и AI-парсер запросов.
DeepSeek AI - основной парсер, pattern matching - fallback.
"""

import re
import json
import asyncpg
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple

try:
    import httpx
    DEEPSEEK_AVAILABLE = True
except ImportError:
    DEEPSEEK_AVAILABLE = False


# ==================== Настройки DeepSeek AI ====================

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
USE_AI = DEEPSEEK_AVAILABLE and DEEPSEEK_API_KEY and DEEPSEEK_API_KEY != "your_deepseek_api_key_here"

# УНИВЕРСАЛЬНЫЙ промпт - учит принципам SQL для любой задачи
AI_SYSTEM_PROMPT = """
You are an expert SQL query generator for a Telegram bot about video statistics.

=== DATABASE SCHEMA ===

TABLE: videos (FINAL statistics per video)
- id (UUID) - unique video identifier
- creator_id (UUID) - creator identifier
- video_created_at (TIMESTAMP WITH TIME ZONE) - publication date
- views_count (INTEGER) - total views (final)
- likes_count (INTEGER) - total likes (final)
- comments_count (INTEGER) - total comments (final)
- reports_count (INTEGER) - total reports (final)

TABLE: video_snapshots (HOURLY changes)
- id (UUID) - snapshot identifier
- video_id (UUID) - references videos.id
- created_at (TIMESTAMP WITH TIME ZONE) - snapshot timestamp
- views_count, likes_count, comments_count, reports_count (INTEGER) - values at that hour
- delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count (INTEGER) - change from previous hour

=== UNIVERSAL QUERY CONSTRUCTION PRINCIPLES ===

1. AGGREGATION FUNCTIONS:
   - COUNT(*) → count rows/videos
   - COUNT(DISTINCT column) → count unique values
   - SUM(column) → sum values
   - COALESCE(SUM(...), 0) → sum with NULL protection

2. TABLE SELECTION LOGIC:
   - videos table: final totals, video counts, creator stats, publication dates
   - video_snapshots table: changes over time, hourly deltas, "growth on date"

3. FILTER TYPES:
   - creator_id = 'uuid' → filter by specific creator
   - video_created_at >= X AND < Y → filter by publication period
   - views_count > N → threshold filter on final stats
   - delta_views_count > 0 → had growth (from snapshots)
   - created_at >= X AND < Y → filter snapshot date

4. DATE HANDLING:
   - Single day "28 ноября 2025": >= '2025-11-28' AND < '2025-11-29'
   - Month "за май 2025", "в мае": >= '2025-05-01' AND < '2025-06-01'
   - Range "с 1 по 5 ноября": >= '2025-11-01' AND < '2025-11-06'
   - Default year: 2025

5. COMBINING CONDITIONS:
   - Use AND to combine multiple filters
   - Can combine: creator + date + threshold + any metric
   - Order: SELECT → FROM → WHERE (conditions with AND)

=== QUERY PATTERNS BY INTENT ===

COUNT queries:
- "сколько всего видео" → SELECT COUNT(*) FROM videos
- "сколько видео у креатора" → SELECT COUNT(*) FROM videos WHERE creator_id = '...'
- "сколько видео набрали больше N" → SELECT COUNT(*) FROM videos WHERE views_count > N
- "сколько разных видео получили просмотры" → SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE delta_views_count > 0

SUM queries (final stats from videos):
- "сколько лайков набрали все видео" → SELECT COALESCE(SUM(likes_count), 0) FROM videos
- "сколько просмотров у видео за [месяц]" → SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE video_created_at >= ... AND < ...
- "сколько комментариев у креатора" → SELECT COALESCE(SUM(comments_count), 0) FROM videos WHERE creator_id = '...'

SUM queries (changes from snapshots):
- "на сколько выросли просмотры 28 ноября" → SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE created_at >= '2025-11-28' AND < '2025-11-29'
- "прирост лайков за дату" → SELECT COALESCE(SUM(delta_likes_count), 0) FROM video_snapshots WHERE created_at >= ... AND < ...

COMBINED queries (multiple conditions):
- "сколько видео у креатора X набрали больше N просмотров" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X' AND views_count > N
- "сколько просмотров у видео креатора X за июнь" → SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE creator_id = 'X' AND video_created_at >= '2025-06-01' AND < '2025-07-01'
- "сколько видео у креатора X опубликованных в мае набрали больше N лайков" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X' AND video_created_at >= '2025-05-01' AND < '2025-06-01' AND likes_count > N

=== KEY RUSSIAN PHRASES → SQL CONCEPTS ===

- "сколько всего", "общее количество" → COUNT or SUM
- "набрали", "имеют" → final stats from videos table
- "выросли", "прирост", "изменились" → delta from video_snapshots
- "у креатора", "creator id" → WHERE creator_id = 'UUID'
- "за [месяц]", "в [месяце]", "опубликованные в" → WHERE video_created_at (month range)
- "больше N", "более N" → WHERE metric > N
- "разных видео" → COUNT(DISTINCT video_id)
- "получили новые [метрика]" → WHERE delta_[metric] > 0

=== EXAMPLES ===

Simple:
Q: "Сколько всего видео?" → {"sql": "SELECT COUNT(*) FROM videos"}
Q: "Сколько лайков у всех видео?" → {"sql": "SELECT COALESCE(SUM(likes_count), 0) FROM videos"}

With threshold:
Q: "Сколько видео набрало больше 100000 просмотров?" → {"sql": "SELECT COUNT(*) FROM videos WHERE views_count > 100000"}
Q: "Сколько видео имеют больше 1000 лайков?" → {"sql": "SELECT COUNT(*) FROM videos WHERE likes_count > 1000"}

With date:
Q: "Сколько просмотров набрали видео за июнь 2025?" → {"sql": "SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE video_created_at >= '2025-06-01' AND video_created_at < '2025-07-01'"}
Q: "На сколько просмотров выросли видео 28 ноября 2025?" → {"sql": "SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE created_at >= '2025-11-28' AND created_at < '2025-11-29'"}

With creator:
Q: "Сколько видео у креатора с id abc123...?" → {"sql": "SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123...'"}
Q: "Сколько видео у креатора abc123... опубликованы в мае?" → {"sql": "SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123...' AND video_created_at >= '2025-05-01' AND video_created_at < '2025-06-01'"}

COMBINED (creator + threshold):
Q: "Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 набрали больше 10000 просмотров?" → {"sql": "SELECT COUNT(*) FROM videos WHERE creator_id = 'aca1061a9d324ecf8c3fa2bb32d7be63' AND views_count > 10000"}

COMBINED (creator + date + threshold):
Q: "Сколько видео у креатора abc123... за июнь набрали больше 50000 просмотров?" → {"sql": "SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123...' AND video_created_at >= '2025-06-01' AND video_created_at < '2025-07-01' AND views_count > 50000"}

COMBINED (creator + date + SUM):
Q: "Сколько просмотров у видео креатора abc123... за июль 2025?" → {"sql": "SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE creator_id = 'abc123...' AND video_created_at >= '2025-07-01' AND video_created_at < '2025-08-01'"}

Distinct:
Q: "Сколько разных видео получили новые просмотры 27 ноября?" → {"sql": "SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at >= '2025-11-27' AND created_at < '2025-11-28' AND delta_views_count > 0"}

=== YOUR TASK ===

1. Parse the Russian query
2. Identify: metric (COUNT/SUM), table (videos/snapshots), filters (creator/date/threshold)
3. Construct SQL following the patterns above
4. Return ONLY JSON: {"sql": "SELECT ..."}
5. If cannot parse: {"sql": "UNKNOWN"}

DO NOT include explanations. Return ONLY the JSON object.
"""


def parse_with_ai(text: str) -> Optional[str]:
    """Parse query using DeepSeek AI and return SQL."""
    if not USE_AI:
        return None
    
    try:
        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": AI_SYSTEM_PROMPT},
                {"role": "user", "content": f"Generate SQL for this Russian query: {text}"}
            ],
            "temperature": 0.05,
            "max_tokens": 300
        }
        
        with httpx.Client() as client:
            response = client.post(
                "https://api.deepseek.com/chat/completions",
                headers=headers,
                json=payload,
                timeout=15.0
            )
            response.raise_for_status()
        
        result_text = response.json()["choices"][0]["message"]["content"].strip()
        
        # Extract JSON from response
        json_match = re.search(r'\{[^}]*"sql"[^}]*\}', result_text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group(0))
            sql = result.get("sql", "")
            if sql and sql != "UNKNOWN":
                print(f"[AI] {text[:50]}... → {sql[:80]}")
                return sql
    
    except Exception as e:
        print(f"AI error: {e}")
    
    return None


# ==================== Pattern Matching (fallback) ====================

MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
    "январь": 1, "февраль": 2, "март": 3, "апрель": 4, "май": 5, "июнь": 6,
    "июль": 7, "август": 8, "сентябрь": 9, "октябрь": 10, "ноябрь": 11, "декабрь": 12,
    "мае": 5, "июне": 6, "июле": 7, "августе": 8, "сентябре": 9, "октябре": 10, "ноябре": 11, "декабре": 12,
}


def get_month_range(month_name: str, year: int = 2025) -> Tuple[datetime, datetime]:
    """Get start and end dates for a month."""
    month = MONTHS_RU.get(month_name.lower())
    if not month:
        return None, None
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)
    return start, end


def extract_uuid(text: str) -> Optional[str]:
    """Extract UUID from text."""
    match = re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text, re.IGNORECASE)
    if match:
        return match.group(0)
    match = re.search(r"[0-9a-f]{32}", text, re.IGNORECASE)
    if match:
        return match.group(0)
    return None


def extract_threshold(text: str) -> Optional[int]:
    """Extract threshold number after 'больше' or 'более'."""
    match = re.search(r"(больше|более)\s+([0-9\s]+)\s*(просмотр|лайк|коммент|жалоб)", text, re.IGNORECASE)
    if match:
        num_str = match.group(2).replace(" ", "")
        return int(num_str)
    return None


def parse_with_patterns(text: str) -> Optional[str]:
    """Fallback pattern matching."""
    text_lower = text.lower()
    creator_id = extract_uuid(text)
    threshold = extract_threshold(text)
    
    # Extract single date
    date_match = re.search(r"(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})?", text_lower)
    single_date = None
    if date_match:
        day = int(date_match.group(1))
        month = MONTHS_RU.get(date_match.group(2))
        year = int(date_match.group(3)) if date_match.group(3) else 2025
        if month:
            single_date = (datetime(year, month, day), datetime(year, month, day + 1))
    
    # Extract month range
    month_match = re.search(r"(за|в|на|опубликован|вышедш)\s*(мая|май|мае|июня|июнь|июне|июля|июль|июле|августа|август|августе|сентября|сентябрь|сентябре|октября|октябрь|октябре|ноября|ноябрь|ноябре|декабря|декабрь|декабре|января|январь|январе|февраля|февраль|феврале|марта|март|марте|апреля|апрель|апреле)\s+(\d{4})?", text_lower)
    month_range = None
    if month_match:
        month_range = get_month_range(month_match.group(2), int(month_match.group(3)) if month_match.group(3) else 2025)
    
    # Snapshot delta queries
    if re.search(r"(вырос|прирост|дельта)", text_lower) and single_date:
        if re.search(r"просмотр", text_lower):
            return f"SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE created_at >= '{single_date[0].strftime('%Y-%m-%d')}' AND created_at < '{single_date[1].strftime('%Y-%m-%d')}'"
        if re.search(r"лайк", text_lower):
            return f"SELECT COALESCE(SUM(delta_likes_count), 0) FROM video_snapshots WHERE created_at >= '{single_date[0].strftime('%Y-%m-%d')}' AND created_at < '{single_date[1].strftime('%Y-%m-%d')}'"
    
    # COUNT DISTINCT videos with new metrics
    if re.search(r"сколько.*разн.*видео", text_lower) and single_date:
        if re.search(r"просмотр", text_lower):
            return f"SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at >= '{single_date[0].strftime('%Y-%m-%d')}' AND created_at < '{single_date[1].strftime('%Y-%m-%d')}' AND delta_views_count > 0"
        if re.search(r"лайк", text_lower):
            return f"SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at >= '{single_date[0].strftime('%Y-%m-%d')}' AND created_at < '{single_date[1].strftime('%Y-%m-%d')}' AND delta_likes_count > 0"
    
    # COMBINED: creator + threshold
    if creator_id and threshold:
        if re.search(r"просмотр", text_lower):
            return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{creator_id}' AND views_count > {threshold}"
        if re.search(r"лайк", text_lower):
            return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{creator_id}' AND likes_count > {threshold}"
    
    # COMBINED: creator + month + threshold
    if creator_id and month_range and threshold:
        date_filter = f"video_created_at >= '{month_range[0].strftime('%Y-%m-%d')}' AND video_created_at < '{month_range[1].strftime('%Y-%m-%d')}'"
        if re.search(r"просмотр", text_lower):
            return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{creator_id}' AND {date_filter} AND views_count > {threshold}"
        if re.search(r"лайк", text_lower):
            return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{creator_id}' AND {date_filter} AND likes_count > {threshold}"
    
    # COMBINED: creator + month (SUM)
    if creator_id and month_range:
        date_filter = f"video_created_at >= '{month_range[0].strftime('%Y-%m-%d')}' AND video_created_at < '{month_range[1].strftime('%Y-%m-%d')}'"
        if re.search(r"просмотр", text_lower):
            return f"SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE creator_id = '{creator_id}' AND {date_filter}"
        if re.search(r"лайк", text_lower):
            return f"SELECT COALESCE(SUM(likes_count), 0) FROM videos WHERE creator_id = '{creator_id}' AND {date_filter}"
    
    # Simple threshold
    if threshold:
        if re.search(r"просмотр", text_lower):
            return f"SELECT COUNT(*) FROM videos WHERE views_count > {threshold}"
        if re.search(r"лайк", text_lower):
            return f"SELECT COUNT(*) FROM videos WHERE likes_count > {threshold}"
    
    # Simple month (SUM)
    if month_range:
        date_filter = f"video_created_at >= '{month_range[0].strftime('%Y-%m-%d')}' AND video_created_at < '{month_range[1].strftime('%Y-%m-%d')}'"
        if re.search(r"просмотр", text_lower):
            return f"SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE {date_filter}"
        if re.search(r"лайк", text_lower):
            return f"SELECT COALESCE(SUM(likes_count), 0) FROM videos WHERE {date_filter}"
    
    # Simple creator + date (COUNT)
    if creator_id and month_range:
        date_filter = f"video_created_at >= '{month_range[0].strftime('%Y-%m-%d')}' AND video_created_at < '{month_range[1].strftime('%Y-%m-%d')}'"
        return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{creator_id}' AND {date_filter}"
    
    # Simple counts
    if re.search(r"сколько всего видео", text_lower):
        return "SELECT COUNT(*) FROM videos"
    
    # Simple sums
    if re.search(r"лайк", text_lower) and re.search(r"(сколько|сумма|общее)", text_lower):
        return "SELECT COALESCE(SUM(likes_count), 0) FROM videos"
    if re.search(r"просмотр", text_lower) and re.search(r"(сколько|сумма|общее)", text_lower):
        return "SELECT COALESCE(SUM(views_count), 0) FROM videos"
    
    return None


def parse_query(text: str) -> Optional[str]:
    """Parse query: AI first, then pattern matching."""
    if USE_AI:
        sql = parse_with_ai(text)
        if sql:
            return sql
    return parse_with_patterns(text)


# ==================== Database Class ====================

class Database:
    def __init__(self):
        self.conn: Optional[asyncpg.Connection] = None

    async def connect(self):
        """Connect to PostgreSQL."""
        self.conn = await asyncpg.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            database=os.getenv("DB_NAME", "tpsh_db"),
        )

    async def close(self):
        """Close connection."""
        if self.conn:
            await self.conn.close()

    async def execute_sql(self, sql: str) -> int:
        """Execute SQL and return result."""
        try:
            result = await self.conn.fetchval(sql)
            return result if result is not None else 0
        except Exception as e:
            print(f"SQL Error: {e}")
            return -1
