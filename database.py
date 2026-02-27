"""
База данных и AI-парсер запросов.
DeepSeek AI генерирует SQL запросы напрямую.
"""

import re
import json
import asyncpg
import os
from datetime import datetime
from typing import Optional, Tuple

try:
    import httpx
    DEEPSEEK_AVAILABLE = True
except ImportError:
    DEEPSEEK_AVAILABLE = False


# ==================== Настройки DeepSeek AI ====================

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
USE_AI = DEEPSEEK_AVAILABLE and DEEPSEEK_API_KEY and DEEPSEEK_API_KEY != "your_deepseek_api_key_here"

# Полный промпт для AI - описывает схему БД и принципы генерации SQL
AI_SYSTEM_PROMPT = """
You are a SQL query generator for a Telegram bot about video statistics.

=== DATABASE SCHEMA ===

TABLE: videos (one row per video with final statistics)
- id (UUID) - unique video identifier
- creator_id (UUID) - who created the video
- video_created_at (TIMESTAMP WITH TIME ZONE) - when video was published
- views_count (INTEGER) - total number of views
- likes_count (INTEGER) - total number of likes
- comments_count (INTEGER) - total number of comments
- reports_count (INTEGER) - total number of reports

TABLE: video_snapshots (hourly statistics snapshots)
- id (UUID) - unique snapshot identifier
- video_id (UUID) - links to videos.id
- created_at (TIMESTAMP WITH TIME ZONE) - when snapshot was taken
- views_count, likes_count, comments_count, reports_count (INTEGER) - values at snapshot time
- delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count (INTEGER) - change from previous hour

=== HOW TO BUILD QUERIES ===

STEP 1: Identify what to COUNT/SUM
- "сколько видео", "количество видео" → COUNT(*) or COUNT(video_id)
- "сколько просмотров", "сумма просмотров", "общее количество просмотров" → SUM(views_count)
- "сколько лайков", "сумма лайков" → SUM(likes_count)
- "сколько комментариев", "сумма комментариев" → SUM(comments_count)
- "сколько жалоб", "сумма жалоб" → SUM(reports_count)
- "на сколько выросли", "прирост просмотров", "дельта" → SUM(delta_views_count) from video_snapshots
- "на сколько выросли лайки" → SUM(delta_likes_count) from video_snapshots
- "сколько разных видео" → COUNT(DISTINCT video_id)

STEP 2: Identify which TABLE
- Use `videos` table for: final statistics, video counts, creator info, total metrics
- Use `video_snapshots` table for: hourly changes, deltas, growth on specific dates, "на сколько выросли"

STEP 3: Identify FILTERS
- "у креатора", "creator id" → WHERE creator_id = 'UUID'
- "за май 2025", "в июне 2025", "published in June" → WHERE video_created_at >= 'YYYY-MM-01' AND < 'YYYY-MM+1-01'
- "28 ноября 2025", "on November 28" → WHERE created_at >= '2025-11-28' AND < '2025-11-29' (for snapshots)
- "больше 100000", "more than N views" → WHERE views_count > N
- "больше N likes" → WHERE likes_count > N
- "получили новые просмотры", "had new views" → WHERE delta_views_count > 0
- "получили новые лайки" → WHERE delta_likes_count > 0

STEP 4: DATE PARSING
- "28 ноября 2025" → single day: >= '2025-11-28' AND < '2025-11-29'
- "за май 2025", "в мае 2025", "июнь 2025" → full month: >= '2025-05-01' AND < '2025-06-01'
- "с 1 по 5 ноября" → date range: >= '2025-11-01' AND < '2025-11-06'
- Default year: 2025

=== QUERY CONSTRUCTION RULES ===

1. For SUM queries, ALWAYS use COALESCE(SUM(...), 0) to return 0 instead of NULL
2. For date ranges, use >= start AND < end (exclusive end)
3. For "за [месяц]", "в [месяце]" use full month: 1st to 1st of next month
4. Return ONLY JSON: {"sql": "SELECT ..."}
5. If cannot generate: {"sql": "UNKNOWN"}

=== EXAMPLES ===

Q: "Сколько всего видео?"
A: {"sql": "SELECT COUNT(*) FROM videos"}

Q: "Сколько лайков набрали все видео?"
A: {"sql": "SELECT COALESCE(SUM(likes_count), 0) FROM videos"}

Q: "Сколько видео у креатора с id abc123... с 1 по 5 ноября?"
A: {"sql": "SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123...' AND video_created_at >= '2025-11-01' AND video_created_at < '2025-11-06'"}

Q: "Сколько просмотров набрали видео, опубликованные в июне 2025?"
A: {"sql": "SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE video_created_at >= '2025-06-01' AND video_created_at < '2025-07-01'"}

Q: "Какое количество лайков у видео, вышедших в июле 2025?"
A: {"sql": "SELECT COALESCE(SUM(likes_count), 0) FROM videos WHERE video_created_at >= '2025-07-01' AND video_created_at < '2025-08-01'"}

Q: "На сколько просмотров выросли все видео 28 ноября 2025?"
A: {"sql": "SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE created_at >= '2025-11-28' AND created_at < '2025-11-29'"}

Q: "На сколько выросли лайки 27 ноября?"
A: {"sql": "SELECT COALESCE(SUM(delta_likes_count), 0) FROM video_snapshots WHERE created_at >= '2025-11-27' AND created_at < '2025-11-28'"}

Q: "Сколько разных видео получили новые просмотры 27 ноября?"
A: {"sql": "SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at >= '2025-11-27' AND created_at < '2025-11-28' AND delta_views_count > 0"}

Q: "Сколько разных видео получили новые лайки 28 ноября?"
A: {"sql": "SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at >= '2025-11-28' AND created_at < '2025-11-29' AND delta_likes_count > 0"}

Q: "Сколько видео набрало больше 100000 просмотров?"
A: {"sql": "SELECT COUNT(*) FROM videos WHERE views_count > 100000"}

Q: "Сколько видео имеют больше 1000 лайков?"
A: {"sql": "SELECT COUNT(*) FROM videos WHERE likes_count > 1000"}

Q: "Сколько комментариев набрали видео за август 2025?"
A: {"sql": "SELECT COALESCE(SUM(comments_count), 0) FROM videos WHERE video_created_at >= '2025-08-01' AND video_created_at < '2025-09-01'"}

Q: "Сколько жалоб получили видео за сентябрь 2025?"
A: {"sql": "SELECT COALESCE(SUM(reports_count), 0) FROM videos WHERE video_created_at >= '2025-09-01' AND video_created_at < '2025-10-01'"}

NOW GENERATE SQL FOR THE USER'S QUERY. RETURN ONLY JSON.
"""


def parse_with_ai(text: str) -> Optional[str]:
    """
    Parse query using DeepSeek AI and return SQL.
    Returns SQL string or None if AI unavailable/failed.
    """
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
            "temperature": 0.1,
            "max_tokens": 200
        }
        
        with httpx.Client() as client:
            response = client.post(
                "https://api.deepseek.com/chat/completions",
                headers=headers,
                json=payload,
                timeout=10.0
            )
            response.raise_for_status()
        
        result_text = response.json()["choices"][0]["message"]["content"].strip()
        
        # Extract JSON from response
        json_match = re.search(r'\{[^}]*"sql"[^}]*\}', result_text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group(0))
            sql = result.get("sql", "")
            if sql and sql != "UNKNOWN":
                print(f"[AI] {text[:50]}... → {sql[:60]}")
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


def parse_with_patterns(text: str) -> Optional[str]:
    """
    Parse query using patterns and return SQL.
    Returns SQL string or None if no pattern matched.
    """
    text_lower = text.lower()
    
    # Total videos
    if re.search(r"сколько всего видео", text_lower):
        return "SELECT COUNT(*) FROM videos"
    
    # Videos with views threshold (ПРИОРИТЕТ)
    match = re.search(r"(больше|более).*?(\d+).*?просмотр", text_lower)
    if match:
        threshold = match.group(2)
        return f"SELECT COUNT(*) FROM videos WHERE views_count > {threshold}"
    match = re.search(r"набрал.*больше.*?(\d+).*?просмотр", text_lower)
    if match:
        threshold = match.group(1)
        return f"SELECT COUNT(*) FROM videos WHERE views_count > {threshold}"
    
    # Videos with likes threshold
    match = re.search(r"(больше|более).*?(\d+).*?лайк", text_lower)
    if match:
        threshold = match.group(2)
        return f"SELECT COUNT(*) FROM videos WHERE likes_count > {threshold}"
    match = re.search(r"имеют.*больше.*?(\d+).*?лайк", text_lower)
    if match:
        threshold = match.group(1)
        return f"SELECT COUNT(*) FROM videos WHERE likes_count > {threshold}"
    
    # COMBINED: Views/Likes/Comments/Reports for videos in month
    month_match = re.search(r"(за|в|на|вышедш|опубликован)\s*(мая|май|мае|июня|июнь|июне|июля|июль|июле|августа|август|августе|сентября|сентябрь|сентябре|октября|октябрь|октябре|ноября|ноябрь|ноябре|декабря|декабрь|декабре|января|январь|январе|февраля|февраль|феврале|марта|март|марте|апреля|апрель|апреле)\s+(\d{4})?", text_lower)
    if month_match:
        month_str = month_match.group(2)
        year = int(month_match.group(3)) if month_match.group(3) else 2025
        start, end = get_month_range(month_str, year)
        if start and end:
            date_filter = f"video_created_at >= '{start.strftime('%Y-%m-%d')}' AND video_created_at < '{end.strftime('%Y-%m-%d')}'"
            # Views
            if re.search(r"просмотр", text_lower) and re.search(r"(суммарн|общ|сколько|дай|набрал)", text_lower):
                return f"SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE {date_filter}"
            # Likes
            if re.search(r"лайк", text_lower) and re.search(r"(суммарн|общ|сколько|дай|количеств)", text_lower):
                return f"SELECT COALESCE(SUM(likes_count), 0) FROM videos WHERE {date_filter}"
            # Comments
            if re.search(r"коммент", text_lower) and re.search(r"(суммарн|общ|сколько|дай|набрал)", text_lower):
                return f"SELECT COALESCE(SUM(comments_count), 0) FROM videos WHERE {date_filter}"
            # Reports
            if re.search(r"(жалоб|репорт)", text_lower) and re.search(r"(суммарн|общ|сколько|дай|получил)", text_lower):
                return f"SELECT COALESCE(SUM(reports_count), 0) FROM videos WHERE {date_filter}"
    
    # Total likes
    if re.search(r"лайк", text_lower) and re.search(r"(сколько|посчитай|сумма|общее|дай)", text_lower):
        return "SELECT COALESCE(SUM(likes_count), 0) FROM videos"
    
    # Total views (but not "на сколько выросли")
    if re.search(r"просмотр", text_lower) and re.search(r"(сколько|посчитай|сумма|общее|дай)", text_lower) and not re.search(r"вырос|на сколько", text_lower):
        return "SELECT COALESCE(SUM(views_count), 0) FROM videos"
    
    # Total comments
    if re.search(r"коммент", text_lower) and re.search(r"(сколько|посчитай|сумма|общее|дай)", text_lower):
        return "SELECT COALESCE(SUM(comments_count), 0) FROM videos"
    
    # Total reports
    if re.search(r"(жалоб|репорт)", text_lower) and re.search(r"(сколько|посчитай|сумма|общее|дай)", text_lower):
        return "SELECT COALESCE(SUM(reports_count), 0) FROM videos"
    
    # Videos appeared in month (COUNT not SUM)
    match = re.search(r"(за|в)\s+(мая|май|мае|июня|июнь|июне|июля|июль|июле|августа|август|августе|сентября|сентябрь|сентябре|октября|октябрь|октябре|ноября|ноябрь|ноябре|декабря|декабрь|декабре|января|январь|январе|февраля|февраль|феврале|марта|март|марте|апреля|апрель|апреле)\s+(\d{4})?", text_lower)
    if match and re.search(r"сколько.*видео", text_lower):
        month_str = match.group(2)
        year = int(match.group(3)) if match.group(3) else 2025
        start, end = get_month_range(month_str, year)
        if start and end:
            return f"SELECT COUNT(*) FROM videos WHERE video_created_at >= '{start.strftime('%Y-%m-%d')}' AND video_created_at < '{end.strftime('%Y-%m-%d')}'"
    
    # SNAPSHOT QUERIES: "на сколько выросли", "прирост"
    # "На сколько просмотров выросли все видео 28 ноября 2025?"
    date_match = re.search(r"(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})?", text_lower)
    if date_match and re.search(r"(вырос|прирост|дельта)", text_lower):
        day = int(date_match.group(1))
        month_str = date_match.group(2)
        year = int(date_match.group(3)) if date_match.group(3) else 2025
        month = MONTHS_RU.get(month_str)
        if month:
            start = datetime(year, month, day)
            end = datetime(year, month, day + 1)
            # Views delta
            if re.search(r"просмотр", text_lower):
                return f"SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE created_at >= '{start.strftime('%Y-%m-%d')}' AND created_at < '{end.strftime('%Y-%m-%d')}'"
            # Likes delta
            if re.search(r"лайк", text_lower):
                return f"SELECT COALESCE(SUM(delta_likes_count), 0) FROM video_snapshots WHERE created_at >= '{start.strftime('%Y-%m-%d')}' AND created_at < '{end.strftime('%Y-%m-%d')}'"
    
    return None


def parse_query(text: str) -> Optional[str]:
    """
    Parse query: try AI first, then pattern matching.
    Returns SQL string or None.
    """
    # Try AI first
    if USE_AI:
        sql = parse_with_ai(text)
        if sql:
            return sql
    
    # Fall back to pattern matching
    return parse_with_patterns(text)


# ==================== Database Class ====================

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

    async def execute_sql(self, sql: str) -> int:
        """Execute SQL and return result as int."""
        try:
            result = await self.conn.fetchval(sql)
            return result if result is not None else 0
        except Exception as e:
            print(f"SQL Error: {e}")
            return -1
