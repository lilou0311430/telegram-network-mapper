"""
Telegram Network Mapper - Persistent Cache (SQLite)
Stores crawl results for incremental crawling and crash recovery.
"""

import hashlib
import logging
import os
import sqlite3
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
DB_PATH = os.path.join(DB_DIR, 'crawl_cache.db')

_SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    username TEXT PRIMARY KEY,
    title TEXT,
    subscribers INTEGER DEFAULT 0,
    description TEXT DEFAULT '',
    photo_url TEXT DEFAULT '',
    is_verified INTEGER DEFAULT 0,
    peer_id INTEGER DEFAULT 0,
    scraped_at TEXT,
    session_id TEXT
);

CREATE TABLE IF NOT EXISTS edges (
    source TEXT,
    target TEXT,
    link_type TEXT,
    count INTEGER DEFAULT 1,
    first_seen TEXT,
    last_seen TEXT,
    session_id TEXT,
    PRIMARY KEY (source, target)
);

CREATE TABLE IF NOT EXISTS page_cache (
    url_hash TEXT PRIMARY KEY,
    url TEXT,
    html TEXT,
    cached_at REAL
);

CREATE INDEX IF NOT EXISTS idx_channels_session ON channels(session_id);
CREATE INDEX IF NOT EXISTS idx_edges_session ON edges(session_id);
CREATE INDEX IF NOT EXISTS idx_page_cache_time ON page_cache(cached_at);
"""


class CrawlCache:
    """SQLite-backed persistent cache for crawl results and page HTML."""

    def __init__(self, db_path: str = None, page_ttl: int = 3600):
        self.db_path = db_path or DB_PATH
        self.page_ttl = page_ttl
        self._local = threading.local()

        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a thread-local connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, timeout=10)
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def _init_schema(self):
        conn = self._get_conn()
        conn.executescript(_SCHEMA)
        conn.commit()

    # ── Channel operations ──

    def save_channel(self, username: str, title: str = "", subscribers: int = 0,
                     description: str = "", photo_url: str = "", is_verified: bool = False,
                     peer_id: int = 0, scraped_at: str = "", session_id: str = ""):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO channels
            (username, title, subscribers, description, photo_url, is_verified, peer_id, scraped_at, session_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (username, title, subscribers, description[:500], photo_url,
              int(is_verified), peer_id, scraped_at, session_id))
        conn.commit()

    def save_channel_from_info(self, info, session_id: str = ""):
        """Save a ChannelInfo object."""
        self.save_channel(
            username=info.username,
            title=info.title,
            subscribers=info.subscribers,
            description=info.description,
            photo_url=info.photo_url,
            is_verified=info.is_verified,
            peer_id=info.peer_id,
            scraped_at=info.scraped_at or "",
            session_id=session_id,
        )

    def get_cached_channels(self, session_id: str = None) -> dict:
        """Get all cached channels, optionally filtered by session.
        Returns dict of username -> row dict."""
        conn = self._get_conn()
        if session_id:
            rows = conn.execute(
                "SELECT * FROM channels WHERE session_id = ? AND scraped_at IS NOT NULL AND scraped_at != ''",
                (session_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM channels WHERE scraped_at IS NOT NULL AND scraped_at != ''"
            ).fetchall()
        return {row['username']: dict(row) for row in rows}

    def get_all_channels(self) -> dict:
        """Get all channels (including unscraped placeholders)."""
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM channels").fetchall()
        return {row['username']: dict(row) for row in rows}

    def channel_count(self) -> int:
        conn = self._get_conn()
        row = conn.execute("SELECT COUNT(*) as cnt FROM channels WHERE scraped_at IS NOT NULL AND scraped_at != ''").fetchone()
        return row['cnt'] if row else 0

    # ── Edge operations ──

    def save_edge(self, source: str, target: str, link_type: str = "",
                  count: int = 1, first_seen: str = "", last_seen: str = "",
                  session_id: str = ""):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO edges (source, target, link_type, count, first_seen, last_seen, session_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, target) DO UPDATE SET
                count = count + excluded.count,
                link_type = CASE
                    WHEN link_type NOT LIKE '%' || excluded.link_type || '%'
                    THEN link_type || ',' || excluded.link_type
                    ELSE link_type
                END,
                first_seen = CASE
                    WHEN excluded.first_seen < first_seen OR first_seen = '' THEN excluded.first_seen
                    ELSE first_seen
                END,
                last_seen = CASE
                    WHEN excluded.last_seen > last_seen OR last_seen = '' THEN excluded.last_seen
                    ELSE last_seen
                END
        """, (source, target, link_type, count, first_seen or "", last_seen or "", session_id))
        conn.commit()

    def save_edge_from_link(self, link, session_id: str = ""):
        """Save a ChannelLink object."""
        link_type = ','.join(set(link.link_types)) if link.link_types else ''
        self.save_edge(
            source=link.source,
            target=link.target,
            link_type=link_type,
            count=link.count,
            first_seen=link.first_seen or "",
            last_seen=link.last_seen or "",
            session_id=session_id,
        )

    def get_cached_edges(self, session_id: str = None) -> list:
        """Get all cached edges."""
        conn = self._get_conn()
        if session_id:
            rows = conn.execute("SELECT * FROM edges WHERE session_id = ?", (session_id,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM edges").fetchall()
        return [dict(row) for row in rows]

    def edge_count(self) -> int:
        conn = self._get_conn()
        row = conn.execute("SELECT COUNT(*) as cnt FROM edges").fetchone()
        return row['cnt'] if row else 0

    # ── Page cache operations (replaces file-based PageCache) ──

    def get_page(self, url: str) -> Optional[str]:
        """Get cached HTML for a URL, or None if expired/missing."""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        conn = self._get_conn()
        row = conn.execute(
            "SELECT html, cached_at FROM page_cache WHERE url_hash = ?",
            (url_hash,)
        ).fetchone()
        if row:
            age = time.time() - row['cached_at']
            if age < self.page_ttl:
                return row['html']
            else:
                conn.execute("DELETE FROM page_cache WHERE url_hash = ?", (url_hash,))
                conn.commit()
        return None

    def set_page(self, url: str, html: str):
        """Cache HTML for a URL."""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO page_cache (url_hash, url, html, cached_at)
            VALUES (?, ?, ?, ?)
        """, (url_hash, url, html, time.time()))
        conn.commit()

    def prune_pages(self, max_size: int = 2000):
        """Remove expired page cache entries and enforce max size."""
        conn = self._get_conn()
        conn.execute("DELETE FROM page_cache WHERE cached_at < ?", (time.time() - self.page_ttl,))
        count = conn.execute("SELECT COUNT(*) as cnt FROM page_cache").fetchone()['cnt']
        if count > max_size:
            conn.execute("""
                DELETE FROM page_cache WHERE url_hash IN (
                    SELECT url_hash FROM page_cache ORDER BY cached_at ASC LIMIT ?
                )
            """, (count - max_size,))
        conn.commit()

    # ── Session management ──

    def get_sessions(self) -> list:
        """List distinct crawl sessions with stats."""
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT session_id,
                   COUNT(*) as channel_count,
                   MIN(scraped_at) as started,
                   MAX(scraped_at) as last_update
            FROM channels
            WHERE session_id != '' AND scraped_at IS NOT NULL AND scraped_at != ''
            GROUP BY session_id
            ORDER BY last_update DESC
        """).fetchall()
        return [dict(row) for row in rows]

    # ── Cache management ──

    def clear_all(self):
        """Clear all cached data."""
        conn = self._get_conn()
        conn.execute("DELETE FROM channels")
        conn.execute("DELETE FROM edges")
        conn.execute("DELETE FROM page_cache")
        conn.commit()
        logger.info("Cache cleared")

    def clear_pages(self):
        """Clear only the page cache."""
        conn = self._get_conn()
        conn.execute("DELETE FROM page_cache")
        conn.commit()

    def stats(self) -> dict:
        """Get cache statistics."""
        conn = self._get_conn()
        channels = conn.execute("SELECT COUNT(*) as cnt FROM channels WHERE scraped_at IS NOT NULL AND scraped_at != ''").fetchone()['cnt']
        placeholders = conn.execute("SELECT COUNT(*) as cnt FROM channels WHERE scraped_at IS NULL OR scraped_at = ''").fetchone()['cnt']
        edges = conn.execute("SELECT COUNT(*) as cnt FROM edges").fetchone()['cnt']
        pages = conn.execute("SELECT COUNT(*) as cnt FROM page_cache").fetchone()['cnt']
        return {
            "channels_scraped": channels,
            "channels_placeholder": placeholders,
            "edges": edges,
            "pages_cached": pages,
        }

    def close(self):
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
