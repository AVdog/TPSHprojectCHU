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

# Полный промпт для AI - описывает схему БД и как генерировать SQL
AI_SYSTEM_PROMPT = """
You are a SQL query generator for a Telegram bot that answers questions about video statistics.

DATABASE SCHEMA:
1. Table `videos` - final statistics per video:
   - id (UUID) - video identifier
   - creator_id (UUID) - creator identifier
   - video_created_at (TIMESTAMP WITH TIME ZONE) - when video was published
   - views_count (INT) - total views
   - likes_count (INT) - total likes
   - comments_count (INT) - total comments
   - reports_count (INT) - total reports

2. Table `video_snapshots` - hourly snapshots:
   - id (UUID) - snapshot identifier
   - video_id (UUID) - reference to videos.id
   - created_at (TIMESTAMP WITH TIME ZONE) - snapshot time
   - views_count, likes_count, comments_count, reports_count (INT) - current values
   - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count (INT) - change from previous hour

YOUR TASK:
1. Parse the Russian natural language query
2. Generate appropriate SQL query based on the question
3. Return ONLY a JSON object with "sql" field

QUERY PATTERNS:

| Question Type | Example | SQL |
|--------------|---------|-----|
| Total videos | "Сколько всего видео?" | SELECT COUNT(*) FROM videos |
| Total likes (all videos) | "Сколько лайков набрали все видео?" | SELECT COALESCE(SUM(likes_count), 0) FROM videos |
| Total views (all videos) | "Сколько просмотров у всех видео?" | SELECT COALESCE(SUM(views_count), 0) FROM videos |
| Total comments | "Сколько комментариев?" | SELECT COALESCE(SUM(comments_count), 0) FROM videos |
| Total reports | "Сколько жалоб?" | SELECT COALESCE(SUM(reports_count), 0) FROM videos |
| Videos by creator + date | "Сколько видео у креатора с id ... с 1 по 5 ноября 2025" | SELECT COUNT(*) FROM videos WHERE creator_id = 'UUID' AND video_created_at >= '2025-11-01' AND video_created_at < '2025-11-06' |
| Videos in date range | "Сколько видео появилось за май 2025" | SELECT COUNT(*) FROM videos WHERE video_created_at >= '2025-05-01' AND video_created_at < '2025-06-01' |
| Videos with views > N | "Сколько видео набрало больше 100000 просмотров?" | SELECT COUNT(*) FROM videos WHERE views_count > 100000 |
| Views gained on date | "На сколько просмотров выросли видео 28 ноября 2025" | SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE created_at >= '2025-11-28' AND created_at < '2025-11-29' |
| Videos with new views on date | "Сколько видео получили новые просмотры 27 ноября" | SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at >= '2025-11-27' AND created_at < '2025-11-28' AND delta_views_count > 0 |

DATE PARSING:
- "28 ноября 2025" → '2025-11-28'
- "за май 2025", "в мае 2025" → >= '2025-05-01' AND < '2025-06-01'
- "с 1 по 5 ноября" → >= '2025-11-01' AND < '2025-11-06'
- Default year is 2025

RULES:
1. ALWAYS use COALESCE(..., 0) for SUM to return 0 instead of NULL
2. For date ranges, use >= start AND < end (exclusive end)
3. For "за май", "в мае" use full month range
4. Extract UUIDs as-is (32 hex chars or 8-4-4-4-12 format)
5. Return ONLY valid JSON: {"sql": "SELECT ..."}
6. If you cannot generate SQL: {"sql": "UNKNOWN"}
7. NEVER include explanations, only JSON

EXAMPLES:
Input: "Сколько всего видео есть в системе?"
Output: {"sql": "SELECT COUNT(*) FROM videos"}

Input: "Какое общее количество лайков набрали все видео?"
Output: {"sql": "SELECT COALESCE(SUM(likes_count), 0) FROM videos"}

Input: "Сколько видео появилось на платформе за май 2025"
Output: {"sql": "SELECT COUNT(*) FROM videos WHERE video_created_at >= '2025-05-01' AND video_created_at < '2025-06-01'"}
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
    
    # Total likes
    if re.search(r"лайк", text_lower) and re.search(r"(сколько|посчитай|сумма|общее|дай)", text_lower):
        return "SELECT COALESCE(SUM(likes_count), 0) FROM videos"
    
    # Total views
    if re.search(r"просмотр", text_lower) and re.search(r"(сколько|посчитай|сумма|общее|дай)", text_lower) and not re.search(r"вырос|на сколько", text_lower):
        return "SELECT COALESCE(SUM(views_count), 0) FROM videos"
    
    # Total comments
    if re.search(r"коммент", text_lower) and re.search(r"(сколько|посчитай|сумма|общее|дай)", text_lower):
        return "SELECT COALESCE(SUM(comments_count), 0) FROM videos"
    
    # Total reports
    if re.search(r"(жалоб|репорт)", text_lower) and re.search(r"(сколько|посчитай|сумма|общее|дай)", text_lower):
        return "SELECT COALESCE(SUM(reports_count), 0) FROM videos"
    
    # Videos with views threshold
    match = re.search(r"(больше|более).*?(\d+).*?просмотр", text_lower)
    if match:
        threshold = match.group(2)
        return f"SELECT COUNT(*) FROM videos WHERE views_count > {threshold}"
    
    # Videos appeared in month (e.g., "за май 2025", "в мае 2025")
    match = re.search(r"(за|в)\s+(мая|май|мае|июня|июнь|июне|июля|июль|июле|августа|август|августе|сентября|сентябрь|сентябре|октября|октябрь|октябре|ноября|ноябрь|ноябре|декабря|декабрь|декабре|января|январь|январе|февраля|февраль|феврале|марта|март|марте|апреля|апрель|апреле)\s+(\d{4})?", text_lower)
    if match and re.search(r"сколько.*видео", text_lower):
        month_str = match.group(2)
        year = int(match.group(3)) if match.group(3) else 2025
        start, end = get_month_range(month_str, year)
        if start and end:
            return f"SELECT COUNT(*) FROM videos WHERE video_created_at >= '{start.strftime('%Y-%m-%d')}' AND video_created_at < '{end.strftime('%Y-%m-%d')}'"
    
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
