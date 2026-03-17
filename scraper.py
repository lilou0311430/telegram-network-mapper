"""
Telegram Network Mapper - Scraping Module (Optimized)
Supports two methods:
  1. Telethon (API-based, requires credentials)
  2. Web scraping (public channels only, no auth needed)

Optimizations:
  - lxml parser (5-10x faster than html.parser)
  - Connection pooling with persistent session
  - Combined info+messages fetch (single page load when possible)
  - In-memory cache to avoid re-fetching
  - Adaptive delay based on error rate
  - Retry with exponential backoff
  - Optional proxy rotation
  - Robust CSS selector fallbacks
  - Cache auto-pruning
"""

import re
import asyncio
import logging
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Use lxml if available (much faster), fallback to html.parser
try:
    import lxml
    HTML_PARSER = 'lxml'
    logger.info("Using lxml parser (fast)")
except ImportError:
    HTML_PARSER = 'html.parser'
    logger.info("Using html.parser (install lxml for 5-10x speed)")

# Regex patterns to detect Telegram channel references (pre-compiled)
TG_LINK_PATTERNS = [
    re.compile(r'https?://t\.me/(?:s/)?([a-zA-Z_][\w]{3,30})(?:/(\d+))?'),
    re.compile(r'https?://telegram\.me/([a-zA-Z_][\w]{3,30})(?:/(\d+))?'),
    re.compile(r'tg://resolve\?domain=([a-zA-Z_][\w]{3,30})'),
    re.compile(r'@([a-zA-Z_][\w]{3,30})'),
]

# Pattern to detect (and skip) invite links
TG_INVITE_PATTERN = re.compile(r'https?://t\.me/\+[\w-]+')

# Channels to ignore (bots, Telegram internal paths, common non-channel usernames)
IGNORE_LIST = {
    'telegram', 'botfather', 'stickers', 'gamee', 'gif',
    'like', 'share', 'vote', 'quiz', 'donate', 'premium',
    'joinchat', 'addstickers', 'addemoji', 'invoice',
    'addlist', 'addtheme', 'setlanguage', 'confirmphone',
    'socks', 'proxy', 'login', 'share_game', 'contact',
    'boost', 'giftcode', 'message', 'folder',
    # Additional Telegram internal paths
    'username', 'privacy', 'tos', 'settings', 'channel',
    'group', 'c', 's', 'iv', 'web', 'dl', 'cdn',
}


@dataclass
class ChannelInfo:
    """Metadata about a Telegram channel."""
    username: str
    title: str = ""
    description: str = ""
    subscribers: int = 0
    photo_url: str = ""
    is_verified: bool = False
    scraped_at: Optional[str] = None

    def to_dict(self):
        return {
            "username": self.username,
            "title": self.title,
            "description": self.description,
            "subscribers": self.subscribers,
            "photo_url": self.photo_url,
            "is_verified": self.is_verified,
            "scraped_at": self.scraped_at,
        }


@dataclass
class ChannelLink:
    """A directed link from one channel to another."""
    source: str
    target: str
    count: int = 1
    message_ids: list = field(default_factory=list)
    link_types: list = field(default_factory=list)  # 'forward', 'mention', 'link'
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None

    def to_dict(self):
        return {
            "source": self.source,
            "target": self.target,
            "count": self.count,
            "link_types": list(set(self.link_types)),
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


def extract_channel_references(text: str) -> list[tuple[str, str]]:
    """Extract channel usernames from text. Returns list of (username, link_type)."""
    refs = []
    seen = set()
    for pattern in TG_LINK_PATTERNS:
        for match in pattern.finditer(text):
            username = match.group(1).lower()
            if (username not in IGNORE_LIST
                    and len(username) >= 4
                    and len(username) <= 32
                    and not username.isdigit()
                    and username not in seen):
                link_type = 'mention' if match.group(0).startswith('@') else 'link'
                refs.append((username, link_type))
                seen.add(username)
    return refs


# ────────────────────────────────────────────────────────────
# Simple disk cache for scraped pages (with auto-pruning)
# ────────────────────────────────────────────────────────────
class PageCache:
    """Simple file-based cache to avoid re-fetching the same pages."""

    def __init__(self, cache_dir: str = None, ttl: int = 3600, max_size: int = 500):
        self.ttl = ttl  # Cache validity in seconds
        self.max_size = max_size
        if cache_dir:
            self.cache_dir = cache_dir
        else:
            self.cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        self.prune()

    def _key(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def get(self, url: str) -> Optional[str]:
        """Get cached HTML for a URL, or None if not cached/expired."""
        path = os.path.join(self.cache_dir, self._key(url))
        if os.path.exists(path):
            age = time.time() - os.path.getmtime(path)
            if age < self.ttl:
                logger.debug(f"Cache hit: {url}")
                with open(path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                os.remove(path)  # Expired
        return None

    def set(self, url: str, html: str):
        """Cache HTML for a URL."""
        path = os.path.join(self.cache_dir, self._key(url))
        with open(path, 'w', encoding='utf-8') as f:
            f.write(html)

    def prune(self):
        """Remove expired cache entries. If still over max_size, remove oldest."""
        now = time.time()
        entries = []
        for filename in os.listdir(self.cache_dir):
            path = os.path.join(self.cache_dir, filename)
            if not os.path.isfile(path):
                continue
            mtime = os.path.getmtime(path)
            age = now - mtime
            if age > self.ttl:
                os.remove(path)
            else:
                entries.append((mtime, path))

        # If still over max_size, remove oldest entries
        if len(entries) > self.max_size:
            entries.sort()  # oldest first
            for _, path in entries[:len(entries) - self.max_size]:
                try:
                    os.remove(path)
                except OSError:
                    pass

    def clear(self):
        """Clear all cache."""
        import shutil
        if os.path.exists(self.cache_dir):
            shutil.rmtree(self.cache_dir)
            os.makedirs(self.cache_dir, exist_ok=True)


# ────────────────────────────────────────────────────────────
# WebScraper — optimized
# ────────────────────────────────────────────────────────────
class WebScraper:
    """Scrape public Telegram channels via t.me web previews."""

    # Robust CSS selector fallbacks (easy to update if Telegram changes HTML)
    TITLE_SELECTORS = [
        '.tgme_page_title span',
        '.tgme_page_title',
        '.tgme_header_title',
        'meta[property="og:title"]',
        'title',
    ]
    DESCRIPTION_SELECTORS = [
        '.tgme_page_description',
        'meta[property="og:description"]',
    ]
    SUBSCRIBERS_SELECTORS = [
        '.tgme_page_extra',
        '.tgme_header_counter',
    ]
    PHOTO_SELECTORS = [
        '.tgme_page_photo_image img',
        'meta[property="og:image"]',
    ]
    VERIFIED_SELECTORS = [
        '.verified-icon',
        '.verified',
    ]
    MESSAGE_SELECTORS = [
        '.tgme_widget_message_wrap',
        '.tgme_widget_message',
        '[data-post]',
    ]

    def __init__(self, max_messages: int = 100, delay: float = 0.8,
                 concurrency: int = 5, use_cache: bool = True, cache_ttl: int = 3600,
                 max_retries: int = 3, proxies: list = None,
                 max_age_days: int = 0, link_types: list = None):
        # Validation
        if max_messages < 1 or max_messages > 5000:
            raise ValueError("max_messages must be between 1 and 5000")
        if delay < 0:
            raise ValueError("delay must be non-negative")
        if concurrency < 1 or concurrency > 20:
            raise ValueError("concurrency must be between 1 and 20")

        self.max_messages = max_messages
        self.base_delay = delay
        self.current_delay = delay
        self.concurrency = concurrency
        self.max_retries = max_retries
        self.max_age_days = max_age_days
        self.link_types = link_types or ['forward', 'mention', 'link', 'description']
        self.session: Optional[aiohttp.ClientSession] = None
        self.cache = PageCache(ttl=cache_ttl) if use_cache else None
        # Adaptive delay tracking
        self._error_count = 0
        self._success_count = 0
        # Semaphore for concurrent requests
        self._semaphore = asyncio.Semaphore(concurrency)
        # Proxy rotation
        self._proxies = proxies or []
        self._proxy_index = 0

    def _get_proxy(self) -> Optional[str]:
        """Get next proxy in round-robin rotation."""
        if not self._proxies:
            return None
        proxy = self._proxies[self._proxy_index % len(self._proxies)]
        self._proxy_index += 1
        return proxy

    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.concurrency * 2,  # Connection pool
                ttl_dns_cache=300,  # DNS cache 5min
                enable_cleanup_closed=True,
            )
            self.session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/131.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                },
                cookie_jar=aiohttp.CookieJar(),
            )

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    def _adapt_delay(self, success: bool):
        """Adjust delay based on success/error rate."""
        if success:
            self._success_count += 1
            # Speed up if many consecutive successes
            if self._success_count > 5:
                self.current_delay = max(0.3, self.current_delay * 0.85)
        else:
            self._error_count += 1
            self._success_count = 0
            # Slow down on errors
            self.current_delay = min(5.0, self.current_delay * 1.5)

    async def _fetch_html(self, url: str) -> Optional[str]:
        """Fetch HTML with caching, retry, rate limiting, and error handling."""
        # Check cache first
        if self.cache:
            cached = self.cache.get(url)
            if cached:
                return cached

        await self._ensure_session()

        async with self._semaphore:
            for attempt in range(self.max_retries):
                try:
                    proxy = self._get_proxy()
                    async with self.session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=12),
                        allow_redirects=True,
                        proxy=proxy,
                    ) as resp:
                        if resp.status == 429:  # Rate limited
                            retry_after = int(resp.headers.get('Retry-After', 5))
                            logger.warning(f"Rate limited, waiting {retry_after}s (attempt {attempt + 1}/{self.max_retries})")
                            await asyncio.sleep(retry_after)
                            self.current_delay = min(5.0, self.current_delay * 2)
                            continue  # Retry

                        if resp.status >= 500:  # Server error — retryable
                            backoff = self.base_delay * (2 ** attempt)
                            logger.warning(f"HTTP {resp.status} for {url}, retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.max_retries})")
                            await asyncio.sleep(backoff)
                            self._adapt_delay(False)
                            continue  # Retry

                        if resp.status != 200:  # 4xx — not retryable
                            logger.warning(f"HTTP {resp.status} for {url}")
                            self._adapt_delay(False)
                            return None

                        html = await resp.text()
                        self._adapt_delay(True)

                        # Cache the result
                        if self.cache:
                            self.cache.set(url, html)

                        return html

                except asyncio.TimeoutError:
                    if attempt < self.max_retries - 1:
                        backoff = self.base_delay * (2 ** attempt)
                        logger.warning(f"Timeout: {url}, retrying in {backoff:.1f}s (attempt {attempt + 1}/{self.max_retries})")
                        await asyncio.sleep(backoff)
                    else:
                        logger.warning(f"Timeout: {url} (all {self.max_retries} attempts failed)")
                    self._adapt_delay(False)
                except Exception as e:
                    logger.error(f"Fetch error {url}: {e} (attempt {attempt + 1}/{self.max_retries})")
                    self._adapt_delay(False)
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.base_delay * (2 ** attempt))
                    else:
                        return None

            return None  # All retries exhausted

    def _parse_subscribers(self, text: str) -> int:
        """Parse subscriber count from various text formats."""
        if not text:
            return 0
        text = text.replace('\xa0', ' ').replace(',', '').strip()
        # Handle "1.2K", "15K", "2.3M" etc.
        m = re.search(r'([\d\s.]+)\s*([KkMm])?', text)
        if m:
            num_str = m.group(1).replace(' ', '')
            if not num_str:
                return 0
            try:
                num = float(num_str)
                suffix = (m.group(2) or '').upper()
                if suffix == 'K':
                    num *= 1000
                elif suffix == 'M':
                    num *= 1000000
                return int(num)
            except ValueError:
                return 0
        return 0

    async def _fetch_info_and_messages(self, username: str) -> tuple[Optional[str], Optional[str]]:
        """Fetch both info page and messages page concurrently."""
        url_info = f"https://t.me/{username}"
        url_msgs = f"https://t.me/s/{username}"

        # Launch both requests in parallel
        html_info, html_msgs = await asyncio.gather(
            self._fetch_html(url_info),
            self._fetch_html(url_msgs),
        )
        return html_info, html_msgs

    def _select_first(self, soup, selectors: list, attr: str = None) -> Optional[str]:
        """Try multiple CSS selectors and return the first match's text or attribute."""
        for sel in selectors:
            el = soup.select_one(sel)
            if el:
                if attr:
                    val = el.get(attr, '')
                elif el.name == 'meta':
                    val = el.get('content', '')
                elif el.name == 'title':
                    val = el.get_text(strip=True)
                else:
                    val = el.get_text(strip=True)
                if val:
                    return val
        return None

    def _parse_channel_info(self, username: str, html_info: Optional[str],
                            html_msgs: Optional[str]) -> Optional[ChannelInfo]:
        """Parse channel info from fetched HTML pages."""
        title = username
        description = ""
        subscribers = 0
        photo_url = ""
        is_verified = False

        # Parse main page
        if html_info:
            soup = BeautifulSoup(html_info, HTML_PARSER)

            if soup.select_one('.tgme_page_icon_not_found') or 'not found' in html_info.lower()[:500]:
                logger.warning(f"Channel {username} not found")
                return None

            title = self._select_first(soup, self.TITLE_SELECTORS) or username
            description = self._select_first(soup, self.DESCRIPTION_SELECTORS) or ""

            for sel in self.SUBSCRIBERS_SELECTORS:
                el = soup.select_one(sel)
                if el:
                    subscribers = self._parse_subscribers(el.get_text(strip=True))
                    if subscribers > 0:
                        break

            for sel in self.PHOTO_SELECTORS:
                el = soup.select_one(sel)
                if el:
                    photo_url = el.get('src', '') or el.get('content', '')
                    if photo_url:
                        break

            for sel in self.VERIFIED_SELECTORS:
                if soup.select_one(sel):
                    is_verified = True
                    break

        # Enrich from preview page
        if html_msgs:
            soup2 = BeautifulSoup(html_msgs, HTML_PARSER)

            if title == username:
                el = soup2.select_one('.tgme_channel_info_header_title')
                if el:
                    title = el.get_text(strip=True) or title

            if not description:
                el = soup2.select_one('.tgme_channel_info_description')
                if el:
                    description = el.get_text(strip=True)

            if subscribers == 0:
                for sel in ['.tgme_channel_info_counter .counter_value',
                            '.tgme_channel_info_counters']:
                    el = soup2.select_one(sel)
                    if el:
                        s = self._parse_subscribers(el.get_text(strip=True))
                        if s > 0:
                            subscribers = s
                            break

        return ChannelInfo(
            username=username,
            title=title or username,
            description=description,
            subscribers=subscribers,
            photo_url=photo_url,
            is_verified=is_verified,
            scraped_at=datetime.utcnow().isoformat(),
        )

    def _parse_messages(self, username: str, html_msgs: Optional[str]) -> list[dict]:
        """Parse messages from preview page HTML."""
        messages = []
        if not html_msgs:
            return messages

        soup = BeautifulSoup(html_msgs, HTML_PARSER)

        # Try multiple selectors for message elements
        msg_elements = None
        for sel in self.MESSAGE_SELECTORS:
            msg_elements = soup.select(sel)
            if msg_elements:
                break
        if not msg_elements:
            return messages

        # Calculate date cutoff for filtering
        date_cutoff = None
        if self.max_age_days > 0:
            from datetime import timedelta, timezone
            date_cutoff = datetime.now(timezone.utc) - timedelta(days=self.max_age_days)

        for msg_el in msg_elements[-self.max_messages:]:
            msg_data = {}

            msg_link = msg_el.select_one('[data-post]') or msg_el
            if msg_link and msg_link.get('data-post'):
                msg_data['id'] = msg_link['data-post']

            text_el = msg_el.select_one('.tgme_widget_message_text')
            msg_data['text'] = text_el.get_text(strip=True) if text_el else ""
            msg_data['html'] = str(text_el) if text_el else ""

            # Grab all links
            all_links = msg_el.select('a[href]')
            msg_data['html'] += ' ' + ' '.join(a.get('href', '') for a in all_links)

            # Forwarded from
            fwd_username = None
            for fwd_sel in ['.tgme_widget_message_forwarded_from_name',
                            '.tgme_widget_message_forwarded_from a']:
                fwd_el = msg_el.select_one(fwd_sel)
                if fwd_el:
                    fwd_link = fwd_el.get('href', '')
                    fwd_match = re.search(r't\.me/([a-zA-Z_]\w+)', fwd_link)
                    if fwd_match:
                        fwd_username = fwd_match.group(1).lower()
                        break
                    fwd_text = fwd_el.get_text(strip=True)
                    if fwd_text and not fwd_text.startswith('http'):
                        fwd_username = fwd_text.strip('@').lower()
                        break

            if fwd_username:
                msg_data['forwarded_from'] = fwd_username

            date_el = msg_el.select_one('time[datetime]')
            if date_el and date_el.get('datetime'):
                msg_data['date'] = date_el['datetime']

                # Filter by date if max_age_days is set
                if date_cutoff:
                    try:
                        from datetime import timezone
                        msg_date = datetime.fromisoformat(date_el['datetime'].replace('Z', '+00:00'))
                        if msg_date.tzinfo is None:
                            msg_date = msg_date.replace(tzinfo=timezone.utc)
                        if msg_date < date_cutoff:
                            continue  # Skip old message
                    except (ValueError, TypeError):
                        pass  # Keep message if date can't be parsed

            views_el = msg_el.select_one('.tgme_widget_message_views')
            if views_el:
                msg_data['views'] = views_el.get_text(strip=True)

            messages.append(msg_data)

        return messages

    async def find_linked_channels(self, username: str) -> tuple[Optional[ChannelInfo], list[ChannelLink]]:
        """Find all channels linked from a given channel's recent messages.
        Optimized: fetches info + messages in a single parallel request."""

        start_time = time.time()

        # Single parallel fetch for both pages
        html_info, html_msgs = await self._fetch_info_and_messages(username)

        # Parse both from already-fetched HTML (no extra network call)
        info = self._parse_channel_info(username, html_info, html_msgs)
        messages = self._parse_messages(username, html_msgs)

        logger.info(f"[{username}] info={'OK' if info else 'FAIL'}, msgs={len(messages)}")

        # Build link map with deduplication
        link_map: dict[str, ChannelLink] = {}
        seen_msg_refs: set[tuple] = set()  # (message_id, target) pairs to avoid double-counting

        for msg in messages:
            msg_id = msg.get('id', '')

            fwd = msg.get('forwarded_from')
            if fwd and fwd != username:
                pair = (msg_id, fwd)
                if pair not in seen_msg_refs:
                    seen_msg_refs.add(pair)
                    if fwd not in link_map:
                        link_map[fwd] = ChannelLink(source=username, target=fwd)
                    link_map[fwd].count += 1
                    link_map[fwd].link_types.append('forward')
                    date = msg.get('date')
                    if date:
                        if not link_map[fwd].first_seen or date < link_map[fwd].first_seen:
                            link_map[fwd].first_seen = date
                        if not link_map[fwd].last_seen or date > link_map[fwd].last_seen:
                            link_map[fwd].last_seen = date

            combined_text = msg.get('text', '') + ' ' + msg.get('html', '')
            refs = extract_channel_references(combined_text)
            for ref_username, link_type in refs:
                if ref_username != username:
                    pair = (msg_id, ref_username)
                    if pair not in seen_msg_refs:
                        seen_msg_refs.add(pair)
                        if ref_username not in link_map:
                            link_map[ref_username] = ChannelLink(source=username, target=ref_username)
                        link_map[ref_username].count += 1
                        link_map[ref_username].link_types.append(link_type)
                        date = msg.get('date')
                        if date:
                            if not link_map[ref_username].first_seen or date < link_map[ref_username].first_seen:
                                link_map[ref_username].first_seen = date
                            if not link_map[ref_username].last_seen or date > link_map[ref_username].last_seen:
                                link_map[ref_username].last_seen = date

        if info and info.description:
            desc_refs = extract_channel_references(info.description)
            for ref_username, link_type in desc_refs:
                if ref_username != username:
                    if ref_username not in link_map:
                        link_map[ref_username] = ChannelLink(source=username, target=ref_username)
                    link_map[ref_username].link_types.append('description')

        # Filter by allowed link types
        filtered_links = {}
        for target, link in link_map.items():
            # Keep only link_types that are in the allowed set
            allowed_types = [t for t in link.link_types if t in self.link_types]
            if allowed_types:
                link.link_types = allowed_types
                filtered_links[target] = link

        elapsed = time.time() - start_time
        logger.info(f"[{username}] scraped in {elapsed:.1f}s — {len(filtered_links)} links found (filtered from {len(link_map)})")

        # Adaptive delay
        await asyncio.sleep(self.current_delay)

        return info, list(filtered_links.values())


# ────────────────────────────────────────────────────────────
# TelethonScraper — optimized
# ────────────────────────────────────────────────────────────
class TelethonScraper:
    """Scrape Telegram channels via the Telethon API."""

    def __init__(self, api_id: int, api_hash: str, phone: str = None,
                 max_messages: int = 200, delay: float = 0.5, concurrency: int = 1,
                 use_cache: bool = True, cache_ttl: int = 3600,
                 max_age_days: int = 0, link_types: list = None):
        if not api_id or api_id <= 0:
            raise ValueError("api_id must be a positive integer")
        if not api_hash:
            raise ValueError("api_hash must not be empty")

        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.max_messages = max_messages
        self.delay = delay
        self.max_age_days = max_age_days
        self.link_types = link_types or ['forward', 'mention', 'link', 'description']
        self.client = None
        # Entity cache (avoids repeated get_entity calls)
        self._entity_cache: dict = {}

    async def connect(self):
        """Initialize and connect the Telethon client using existing session."""
        from telethon import TelegramClient

        session_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'telegram_mapper_session')
        self.client = TelegramClient(session_path, self.api_id, self.api_hash)

        await self.client.connect()

        if not await self.client.is_user_authorized():
            raise RuntimeError(
                "Session Telethon non autorisée. "
                "Lance d'abord: python auth_telegram.py"
            )
        logger.info("Telethon client connected successfully")

    async def close(self):
        if self.client:
            await self.client.disconnect()

    async def disconnect(self):
        await self.close()

    async def _get_entity(self, username: str):
        """Get entity with caching."""
        if username not in self._entity_cache:
            self._entity_cache[username] = await self.client.get_entity(username)
        return self._entity_cache[username]

    async def get_channel_info(self, username: str) -> Optional[ChannelInfo]:
        """Get channel info via Telethon API."""
        from telethon.tl.functions.channels import GetFullChannelRequest
        try:
            entity = await self._get_entity(username)
            full = await self.client(GetFullChannelRequest(entity))

            return ChannelInfo(
                username=username,
                title=getattr(entity, 'title', username),
                description=getattr(full.full_chat, 'about', '') or '',
                subscribers=getattr(full.full_chat, 'participants_count', 0) or 0,
                is_verified=getattr(entity, 'verified', False),
                scraped_at=datetime.utcnow().isoformat(),
            )
        except Exception as e:
            logger.error(f"Telethon error getting info for {username}: {e}")
            return None

    async def find_linked_channels(self, username: str) -> tuple[Optional[ChannelInfo], list[ChannelLink]]:
        """Find all channels linked from a given channel via Telethon."""
        start_time = time.time()
        info = await self.get_channel_info(username)

        link_map: dict[str, ChannelLink] = {}
        seen_msg_refs: set[tuple] = set()  # Deduplication

        # Calculate date cutoff for filtering
        date_cutoff = None
        if self.max_age_days > 0:
            from datetime import timedelta, timezone
            date_cutoff = datetime.now(timezone.utc) - timedelta(days=self.max_age_days)

        try:
            entity = await self._get_entity(username)
            async for message in self.client.iter_messages(entity, limit=self.max_messages):
                try:
                    # Filter by date — Telethon messages are newest-first, so break on old
                    if date_cutoff and message.date:
                        msg_date = message.date
                        if msg_date.tzinfo is None:
                            from datetime import timezone
                            msg_date = msg_date.replace(tzinfo=timezone.utc)
                        if msg_date < date_cutoff:
                            break  # All remaining messages are older

                    # Check forwarded messages
                    if message.forward:
                        fwd_username = None
                        fwd_chat = message.forward.chat
                        if fwd_chat:
                            fwd_username = getattr(fwd_chat, 'username', None)

                        if not fwd_username and hasattr(message.forward, 'from_id') and message.forward.from_id:
                            try:
                                fwd_entity = await self.client.get_entity(message.forward.from_id)
                                fwd_username = getattr(fwd_entity, 'username', None)
                            except Exception:
                                pass

                        # Fallback: try sender attribute
                        if not fwd_username and hasattr(message.forward, 'sender'):
                            fwd_username = getattr(message.forward.sender, 'username', None)

                        if fwd_username:
                            fwd_username = fwd_username.lower()
                            if fwd_username != username:
                                pair = (message.id, fwd_username)
                                if pair not in seen_msg_refs:
                                    seen_msg_refs.add(pair)
                                    if fwd_username not in link_map:
                                        link_map[fwd_username] = ChannelLink(source=username, target=fwd_username)
                                    link_map[fwd_username].count += 1
                                    link_map[fwd_username].link_types.append('forward')
                                    if message.date:
                                        date_str = message.date.isoformat()
                                        link = link_map[fwd_username]
                                        if not link.first_seen or date_str < link.first_seen:
                                            link.first_seen = date_str
                                        if not link.last_seen or date_str > link.last_seen:
                                            link.last_seen = date_str

                    # Check text content (including media captions)
                    text_content = message.text or message.message or ''
                    if text_content:
                        refs = extract_channel_references(text_content)
                        for ref_username, link_type in refs:
                            if ref_username != username:
                                pair = (message.id, ref_username)
                                if pair not in seen_msg_refs:
                                    seen_msg_refs.add(pair)
                                    if ref_username not in link_map:
                                        link_map[ref_username] = ChannelLink(source=username, target=ref_username)
                                    link_map[ref_username].count += 1
                                    link_map[ref_username].link_types.append(link_type)
                                    if message.date:
                                        date_str = message.date.isoformat()
                                        link = link_map[ref_username]
                                        if not link.first_seen or date_str < link.first_seen:
                                            link.first_seen = date_str
                                        if not link.last_seen or date_str > link.last_seen:
                                            link.last_seen = date_str

                except Exception as e:
                    logger.debug(f"Error processing message {getattr(message, 'id', '?')} in {username}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Telethon error iterating messages for {username}: {e}")

        # Check description
        if info and info.description:
            desc_refs = extract_channel_references(info.description)
            for ref_username, link_type in desc_refs:
                if ref_username != username:
                    if ref_username not in link_map:
                        link_map[ref_username] = ChannelLink(source=username, target=ref_username)
                    link_map[ref_username].link_types.append('description')

        # Filter by allowed link types
        filtered_links = {}
        for target, link in link_map.items():
            allowed_types = [t for t in link.link_types if t in self.link_types]
            if allowed_types:
                link.link_types = allowed_types
                filtered_links[target] = link

        elapsed = time.time() - start_time
        logger.info(f"[{username}] Telethon scraped in {elapsed:.1f}s — {len(filtered_links)} links found (filtered from {len(link_map)})")

        await asyncio.sleep(self.delay)
        return info, list(filtered_links.values())
