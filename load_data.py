"""
Загрузка данных из videos.json в PostgreSQL.
"""

import json
import asyncio
import asyncpg
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()


def parse_iso_datetime(dt_string: str) -> datetime:
    """Парсинг ISO 8601 дат."""
    dt_string = dt_string.replace("+00:00", "+0000").replace("-00:00", "+0000")
    try:
        return datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        try:
            return datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%S%z")
        except ValueError:
            dt_string = dt_string.split("+")[0].split("-")[0]
            return datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)


async def load_data():
    """Загрузка videos.json в базу данных."""
    conn = await asyncpg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        database=os.getenv("DB_NAME", "tpsh_db"),
    )

    try:
        with open("videos.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        videos = data.get("videos", [])
        print(f"Found {len(videos)} videos to load")

        video_insert = """
            INSERT INTO videos (id, creator_id, video_created_at, views_count, likes_count,
                comments_count, reports_count, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO NOTHING
        """

        snapshot_insert = """
            INSERT INTO video_snapshots (id, video_id, views_count, likes_count, comments_count,
                reports_count, delta_views_count, delta_likes_count, delta_comments_count,
                delta_reports_count, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO NOTHING
        """

        total_snapshots = 0
        processed = 0

        async with conn.transaction():
            for video in videos:
                await conn.execute(
                    video_insert,
                    video["id"],
                    video["creator_id"],
                    parse_iso_datetime(video["video_created_at"]),
                    video["views_count"],
                    video["likes_count"],
                    video["comments_count"],
                    video["reports_count"],
                    parse_iso_datetime(video["created_at"]),
                    parse_iso_datetime(video["updated_at"]),
                )

                snapshots = video.get("snapshots", [])
                for snapshot in snapshots:
                    await conn.execute(
                        snapshot_insert,
                        snapshot["id"],
                        snapshot["video_id"],
                        snapshot["views_count"],
                        snapshot["likes_count"],
                        snapshot["comments_count"],
                        snapshot["reports_count"],
                        snapshot["delta_views_count"],
                        snapshot["delta_likes_count"],
                        snapshot["delta_comments_count"],
                        snapshot["delta_reports_count"],
                        parse_iso_datetime(snapshot["created_at"]),
                        parse_iso_datetime(snapshot["updated_at"]),
                    )
                    total_snapshots += 1

                processed += 1
                if processed % 50 == 0:
                    print(f"Processed {processed}/{len(videos)} videos...")

        print(f"Successfully loaded {len(videos)} videos with {total_snapshots} snapshots")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(load_data())
