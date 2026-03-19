"""
Telegram Network Mapper - Crawler Engine (Optimized)
Orchestrates the scraping with configurable depth and builds the network graph.

Optimizations:
  - Batch parallel scraping (multiple channels at once for web mode)
  - BFS by depth level (scrape all channels at same depth in parallel)
  - Priority queue: channels sorted by subscriber count (popular first)
  - Telethon: sequential (API rate limits) but no unnecessary delays
  - Thread-safe state access
"""

import asyncio
import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Callable
from xml.sax.saxutils import escape, quoteattr

from cache import CrawlCache
from scraper import WebScraper, TelethonScraper, ChannelInfo, ChannelLink, FloodWaitTooLong

logger = logging.getLogger(__name__)


@dataclass
class CrawlState:
    """Tracks the current state of the crawl for real-time updates."""
    status: str = "idle"  # idle, running, paused, completed, error
    current_channel: str = ""
    current_depth: int = 0
    channels_scraped: int = 0
    channels_total: int = 0
    channels_queued: int = 0
    errors: list = field(default_factory=list)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    speed: float = 0.0  # channels per minute
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def to_dict(self):
        with self._lock:
            return {
                "status": self.status,
                "current_channel": self.current_channel,
                "current_depth": self.current_depth,
                "channels_scraped": self.channels_scraped,
                "channels_total": self.channels_total,
                "channels_queued": self.channels_queued,
                "errors": self.errors[-10:],
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "speed": round(self.speed, 1),
            }


class NetworkGraph:
    """In-memory network graph structure (thread-safe)."""

    def __init__(self):
        self.nodes: dict[str, ChannelInfo] = {}
        self.edges: dict[str, ChannelLink] = {}
        self._lock = threading.Lock()

    def add_node(self, info: ChannelInfo):
        with self._lock:
            self.nodes[info.username] = info

    def add_placeholder_node(self, username: str):
        with self._lock:
            if username not in self.nodes:
                self.nodes[username] = ChannelInfo(username=username)

    def add_edge(self, link: ChannelLink):
        key = f"{link.source}->{link.target}"
        with self._lock:
            if key in self.edges:
                existing = self.edges[key]
                existing.count += link.count
                existing.link_types.extend(link.link_types)
                if link.first_seen:
                    if not existing.first_seen or link.first_seen < existing.first_seen:
                        existing.first_seen = link.first_seen
                if link.last_seen:
                    if not existing.last_seen or link.last_seen > existing.last_seen:
                        existing.last_seen = link.last_seen
            else:
                self.edges[key] = link

    def get_node_subscribers(self, username: str) -> int:
        """Get subscriber count for a node (used for priority sorting)."""
        with self._lock:
            node = self.nodes.get(username)
            return node.subscribers if node else 0

    def to_d3_json(self) -> dict:
        with self._lock:
            nodes = []
            for username, info in self.nodes.items():
                nodes.append({
                    "id": username,
                    "title": info.title or username,
                    "subscribers": info.subscribers,
                    "description": info.description[:200] if info.description else "",
                    "photo_url": info.photo_url,
                    "is_verified": info.is_verified,
                    "peer_id": info.peer_id,
                    "scraped": info.scraped_at is not None,
                })

            links = []
            for key, link in self.edges.items():
                links.append({
                    "source": link.source,
                    "target": link.target,
                    "weight": link.count,
                    "types": list(set(link.link_types)),
                    "first_seen": link.first_seen,
                    "last_seen": link.last_seen,
                })

            return {"nodes": nodes, "links": links}

    def to_gexf(self) -> str:
        with self._lock:
            return self._to_gexf_unlocked()

    def _to_gexf_unlocked(self) -> str:
        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<gexf xmlns="http://gexf.net/1.3" version="1.3">',
            '  <meta>',
            f'    <creator>Telegram Network Mapper</creator>',
            f'    <description>Telegram channel network graph</description>',
            f'    <lastmodifieddate>{datetime.utcnow().strftime("%Y-%m-%d")}</lastmodifieddate>',
            '  </meta>',
            '  <graph defaultedgetype="directed" mode="static">',
            '    <attributes class="node" mode="static">',
            '      <attribute id="0" title="title" type="string"/>',
            '      <attribute id="1" title="subscribers" type="integer"/>',
            '      <attribute id="2" title="description" type="string"/>',
            '      <attribute id="3" title="is_verified" type="boolean"/>',
            '      <attribute id="4" title="scraped" type="boolean"/>',
            '      <attribute id="5" title="peer_id" type="long"/>',
            '    </attributes>',
            '    <attributes class="edge" mode="static">',
            '      <attribute id="0" title="weight" type="integer"/>',
            '      <attribute id="1" title="link_types" type="string"/>',
            '    </attributes>',
            '    <nodes>',
        ]

        for username, info in self.nodes.items():
            safe_id = quoteattr(username)
            label = quoteattr(info.title or username)
            desc = quoteattr((info.description or '')[:200])
            lines.append(f'      <node id={safe_id} label={label}>')
            lines.append(f'        <attvalues>')
            lines.append(f'          <attvalue for="0" value={label}/>')
            lines.append(f'          <attvalue for="1" value="{int(info.subscribers or 0)}"/>')
            lines.append(f'          <attvalue for="2" value={desc}/>')
            lines.append(f'          <attvalue for="3" value="{str(info.is_verified).lower()}"/>')
            lines.append(f'          <attvalue for="4" value="{str(info.scraped_at is not None).lower()}"/>')
            lines.append(f'          <attvalue for="5" value="{info.peer_id}"/>')
            lines.append(f'        </attvalues>')
            lines.append(f'      </node>')

        lines.append('    </nodes>')
        lines.append('    <edges>')

        for i, (key, link) in enumerate(self.edges.items()):
            types = escape(','.join(set(link.link_types)))
            src = quoteattr(link.source)
            tgt = quoteattr(link.target)
            lines.append(f'      <edge id="{i}" source={src} target={tgt}>')
            lines.append(f'        <attvalues>')
            lines.append(f'          <attvalue for="0" value="{link.count}"/>')
            lines.append(f'          <attvalue for="1" value="{types}"/>')
            lines.append(f'        </attvalues>')
            lines.append(f'      </edge>')

        lines.append('    </edges>')
        lines.append('  </graph>')
        lines.append('</gexf>')

        return '\n'.join(lines)

    def to_graphml(self) -> str:
        with self._lock:
            return self._to_graphml_unlocked()

    def _to_graphml_unlocked(self) -> str:
        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<graphml xmlns="http://graphml.graphdrawing.org/graphml">',
            '  <key id="title" for="node" attr.name="title" attr.type="string"/>',
            '  <key id="subscribers" for="node" attr.name="subscribers" attr.type="int"/>',
            '  <key id="description" for="node" attr.name="description" attr.type="string"/>',
            '  <key id="peer_id" for="node" attr.name="peer_id" attr.type="long"/>',
            '  <key id="weight" for="edge" attr.name="weight" attr.type="int"/>',
            '  <key id="link_types" for="edge" attr.name="link_types" attr.type="string"/>',
            '  <graph id="telegram_network" edgedefault="directed">',
        ]

        for username, info in self.nodes.items():
            safe_id = quoteattr(username)
            title = escape(info.title or username)
            desc = escape((info.description or '')[:200])
            lines.append(f'    <node id={safe_id}>')
            lines.append(f'      <data key="title">{title}</data>')
            lines.append(f'      <data key="subscribers">{int(info.subscribers or 0)}</data>')
            lines.append(f'      <data key="description">{desc}</data>')
            lines.append(f'      <data key="peer_id">{info.peer_id}</data>')
            lines.append(f'    </node>')

        for key, link in self.edges.items():
            types = escape(','.join(set(link.link_types)))
            src = quoteattr(link.source)
            tgt = quoteattr(link.target)
            lines.append(f'    <edge source={src} target={tgt}>')
            lines.append(f'      <data key="weight">{link.count}</data>')
            lines.append(f'      <data key="link_types">{types}</data>')
            lines.append(f'    </edge>')

        lines.append('  </graph>')
        lines.append('</graphml>')

        return '\n'.join(lines)

    def get_stats(self) -> dict:
        with self._lock:
            in_degree = {}
            out_degree = {}
            for link in self.edges.values():
                out_degree[link.source] = out_degree.get(link.source, 0) + 1
                in_degree[link.target] = in_degree.get(link.target, 0) + 1

            return {
                "total_nodes": len(self.nodes),
                "total_edges": len(self.edges),
                "scraped_nodes": sum(1 for n in self.nodes.values() if n.scraped_at),
                "top_by_subscribers": sorted(
                    [{"username": u, "subscribers": n.subscribers}
                     for u, n in self.nodes.items() if n.subscribers > 0],
                    key=lambda x: x["subscribers"], reverse=True
                )[:10],
                "top_by_in_degree": sorted(
                    [{"username": u, "in_degree": d} for u, d in in_degree.items()],
                    key=lambda x: x["in_degree"], reverse=True
                )[:10],
                "top_by_out_degree": sorted(
                    [{"username": u, "out_degree": d} for u, d in out_degree.items()],
                    key=lambda x: x["out_degree"], reverse=True
                )[:10],
            }


class Crawler:
    """BFS crawler with configurable depth and parallel batch scraping."""

    def __init__(self, method: str = "web", max_messages: int = 100, delay: float = 0.8,
                 concurrency: int = 5, api_id: int = None, api_hash: str = None,
                 phone: str = None, proxies: list = None,
                 max_age_days: int = 0, link_types: list = None,
                 min_subscribers: int = 0, blacklist: list = None,
                 incremental: bool = False):
        self.method = method
        self.concurrency = concurrency
        self.min_subscribers = min_subscribers
        self.blacklist = set(blacklist or [])
        self.incremental = incremental
        self.graph = NetworkGraph()
        self.state = CrawlState()
        self._stop_requested = False
        self._callbacks: list[Callable] = []
        self._start_time = None

        # Persistent cache
        self.cache = CrawlCache()
        self._session_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        if method == "telethon":
            self.scraper = TelethonScraper(
                api_id=api_id, api_hash=api_hash, phone=phone,
                max_messages=max_messages, delay=delay,
                max_age_days=max_age_days, link_types=link_types,
            )
        else:
            self.scraper = WebScraper(
                max_messages=max_messages,
                delay=delay,
                concurrency=concurrency,
                proxies=proxies,
                max_age_days=max_age_days, link_types=link_types,
                cache=self.cache,
            )

    def on_update(self, callback: Callable):
        self._callbacks.append(callback)

    def _notify(self):
        # Update speed
        if self._start_time and self.state.channels_scraped > 0:
            elapsed_min = (time.time() - self._start_time) / 60
            if elapsed_min > 0:
                self.state.speed = self.state.channels_scraped / elapsed_min

        for cb in self._callbacks:
            try:
                cb(self.state, self.graph)
            except Exception:
                pass

    async def stop(self):
        self._stop_requested = True
        if hasattr(self.scraper, 'close'):
            await self.scraper.close()
        elif hasattr(self.scraper, 'disconnect'):
            await self.scraper.disconnect()

    def request_stop(self):
        self._stop_requested = True

    async def _scrape_one(self, username: str) -> tuple[str, Optional[ChannelInfo], list[ChannelLink]]:
        """Scrape a single channel, returning (username, info, links)."""
        try:
            info, links = await self.scraper.find_linked_channels(username)
            return username, info, links
        except FloodWaitTooLong:
            raise  # Let crawler handle fallback
        except Exception as e:
            logger.error(f"Error scraping {username}: {e}")
            return username, None, []

    def _save_to_cache(self, info: Optional[ChannelInfo], links: list[ChannelLink]):
        """Persist channel info and edges to SQLite cache."""
        if info and info.scraped_at:
            self.cache.save_channel_from_info(info, session_id=self._session_id)
        for link in links:
            self.cache.save_edge_from_link(link, session_id=self._session_id)

    def _sort_by_priority(self, channels: list[str]) -> list[str]:
        """Sort channels by subscriber count (popular first) for better network discovery."""
        return sorted(channels, key=lambda c: self.graph.get_node_subscribers(c), reverse=True)

    async def crawl(self, seed_channels: list[str], max_depth: int = 2,
                    max_channels: int = 100) -> NetworkGraph:
        """
        BFS crawl with parallel batch scraping.

        For web mode: scrapes up to `concurrency` channels simultaneously.
        For Telethon: sequential (API rate limits) but with minimal delays.
        """
        self._stop_requested = False
        self._start_time = time.time()
        self.state = CrawlState(
            status="running",
            started_at=datetime.utcnow().isoformat(),
        )

        # Connect Telethon if needed
        if self.method == "telethon":
            try:
                await self.scraper.connect()
                logger.info("Telethon client connected")
            except Exception as e:
                logger.error(f"Telethon connection failed: {e}")
                self.state.status = "error"
                self.state.errors.append(f"Connexion Telethon échouée: {str(e)}")
                self._notify()
                return self.graph

        # Build initial queue grouped by depth
        visited = set()
        depth_queues: dict[int, list[str]] = {0: []}

        # Incremental mode: load cached channels into graph and visited set
        if self.incremental:
            cached_channels = self.cache.get_cached_channels()
            cached_edges = self.cache.get_cached_edges()
            if cached_channels:
                logger.info(f"Incremental mode: loading {len(cached_channels)} cached channels")
                for username, row in cached_channels.items():
                    info = ChannelInfo(
                        username=username,
                        title=row.get('title', ''),
                        description=row.get('description', ''),
                        subscribers=row.get('subscribers', 0),
                        is_verified=bool(row.get('is_verified', 0)),
                        peer_id=row.get('peer_id', 0),
                        scraped_at=row.get('scraped_at', ''),
                    )
                    self.graph.add_node(info)
                    visited.add(username)
                for edge in cached_edges:
                    link = ChannelLink(
                        source=edge['source'],
                        target=edge['target'],
                        count=edge.get('count', 1),
                        link_types=edge.get('link_type', '').split(',') if edge.get('link_type') else [],
                        first_seen=edge.get('first_seen', ''),
                        last_seen=edge.get('last_seen', ''),
                    )
                    self.graph.add_edge(link)
                    self.graph.add_placeholder_node(edge['target'])
                self.state.channels_scraped = len(cached_channels)
                logger.info(f"Incremental: {len(cached_channels)} channels + {len(cached_edges)} edges loaded from cache")

        for channel in seed_channels:
            channel = channel.lower().strip().lstrip('@')
            if 't.me/' in channel:
                channel = channel.split('t.me/')[-1].split('/')[0]
            if channel and channel not in visited:
                depth_queues[0].append(channel)

        self.state.channels_total = len(depth_queues[0]) + self.state.channels_scraped
        self._notify()

        # BFS by depth level
        for depth in range(max_depth + 1):
            if self._stop_requested:
                break

            channels_at_depth = depth_queues.get(depth, [])
            if not channels_at_depth:
                continue

            # Filter already visited and blacklisted
            channels_at_depth = [c for c in channels_at_depth
                                 if c not in visited and c not in self.blacklist]
            if not channels_at_depth:
                continue

            # Sort by priority (popular channels first)
            channels_at_depth = self._sort_by_priority(channels_at_depth)

            # Limit to max_channels (0 = unlimited)
            if max_channels > 0:
                remaining = max_channels - self.state.channels_scraped
                if remaining <= 0:
                    break
                channels_at_depth = channels_at_depth[:remaining]

            self.state.current_depth = depth
            depth_start = time.time()
            logger.info(f"=== Depth {depth}: {len(channels_at_depth)} channels to scrape ===")

            if self.method == "web":
                # PARALLEL: scrape in batches
                batch_size = self.concurrency
                for i in range(0, len(channels_at_depth), batch_size):
                    if self._stop_requested:
                        break

                    batch = channels_at_depth[i:i + batch_size]
                    batch = [c for c in batch if c not in visited]
                    if not batch:
                        continue

                    # Mark as visiting
                    for c in batch:
                        visited.add(c)

                    self.state.current_channel = ', '.join(batch[:3]) + ('...' if len(batch) > 3 else '')
                    self._notify()

                    # Launch all in parallel
                    tasks = [self._scrape_one(username) for username in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Process results
                    for result in results:
                        if isinstance(result, Exception):
                            self.state.errors.append(str(result))
                            continue

                        username, info, links = result

                        # Skip channels below min_subscribers (except seeds at depth 0)
                        if (self.min_subscribers > 0 and info and depth > 0
                                and info.subscribers < self.min_subscribers):
                            logger.info(f"[{username}] skipped: {info.subscribers} subscribers < {self.min_subscribers} min")
                            self.state.channels_scraped += 1
                            continue

                        if info:
                            self.graph.add_node(info)
                        else:
                            self.graph.add_placeholder_node(username)

                        valid_links = []
                        for link in links:
                            # Don't queue blacklisted targets
                            if link.target in self.blacklist:
                                continue
                            self.graph.add_placeholder_node(link.target)
                            self.graph.add_edge(link)
                            valid_links.append(link)

                            if depth + 1 <= max_depth and link.target not in visited:
                                next_depth = depth + 1
                                if next_depth not in depth_queues:
                                    depth_queues[next_depth] = []
                                depth_queues[next_depth].append(link.target)

                        # Persist to SQLite
                        self._save_to_cache(info, valid_links)
                        self.state.channels_scraped += 1

                    self.state.channels_total = len(visited) + sum(
                        len(q) for d, q in depth_queues.items() if d > depth
                    )
                    self.state.channels_queued = sum(
                        len(q) for d, q in depth_queues.items() if d > depth
                    )
                    self._notify()

            else:
                # SEQUENTIAL for Telethon (API rate limits)
                fallback_to_web = False
                for username in channels_at_depth:
                    if self._stop_requested:
                        break
                    if username in visited:
                        continue

                    visited.add(username)
                    self.state.current_channel = username
                    self._notify()

                    try:
                        username, info, links = await self._scrape_one(username)
                    except FloodWaitTooLong as e:
                        wait_msg = f"Rate-limit Telegram: attente de {e.seconds}s requise (~{e.seconds // 3600}h{(e.seconds % 3600) // 60}m). Basculement en mode Web."
                        logger.warning(wait_msg)
                        self.state.errors.append(wait_msg)
                        self._notify()

                        # Close Telethon and switch to Web scraper
                        saved_max_messages = self.scraper.max_messages
                        saved_delay = self.scraper.delay
                        saved_link_types = self.scraper.link_types
                        saved_max_age = getattr(self.scraper, 'max_age_days', 0)
                        if hasattr(self.scraper, 'close'):
                            await self.scraper.close()
                        self.scraper = WebScraper(
                            max_messages=saved_max_messages,
                            delay=saved_delay,
                            link_types=saved_link_types,
                            max_age_days=saved_max_age,
                            cache=self.cache,
                        )
                        self.method = "web"
                        fallback_to_web = True

                        # Remove current channel from visited so it gets re-scraped via Web
                        visited.discard(username)
                        logger.info("Basculement automatique en mode Web — le crawl continue")
                        break  # Break inner loop, will be re-processed in web mode

                    # Skip channels below min_subscribers (except seeds at depth 0)
                    if (self.min_subscribers > 0 and info and depth > 0
                            and info.subscribers < self.min_subscribers):
                        logger.info(f"[{username}] skipped: {info.subscribers} subscribers < {self.min_subscribers} min")
                        self.state.channels_scraped += 1
                        continue

                    if info:
                        self.graph.add_node(info)
                    else:
                        self.graph.add_placeholder_node(username)

                    valid_links = []
                    for link in links:
                        if link.target in self.blacklist:
                            continue
                        self.graph.add_placeholder_node(link.target)
                        self.graph.add_edge(link)
                        valid_links.append(link)

                        if depth + 1 <= max_depth and link.target not in visited:
                            next_depth = depth + 1
                            if next_depth not in depth_queues:
                                depth_queues[next_depth] = []
                            depth_queues[next_depth].append(link.target)

                    # Persist to SQLite
                    self._save_to_cache(info, valid_links)
                    self.state.channels_scraped += 1
                    self.state.channels_total = len(visited) + sum(
                        len(q) for d, q in depth_queues.items() if d > depth
                    )
                    self._notify()

                # If we switched to Web, re-run remaining channels at this depth in web/parallel mode
                if fallback_to_web:
                    remaining = [u for u in channels_at_depth if u not in visited]
                    logger.info(f"Fallback Web: {len(remaining)} chaînes non visitées sur {len(channels_at_depth)} à profondeur {depth}")
                    if remaining:
                        for i in range(0, len(remaining), self.concurrency):
                            if self._stop_requested:
                                break
                            batch = remaining[i:i + self.concurrency]
                            batch = [u for u in batch if u not in visited]
                            if not batch:
                                continue

                            for u in batch:
                                visited.add(u)
                            self.state.current_channel = batch if len(batch) > 1 else batch[0]
                            self._notify()

                            tasks = [self._scrape_one(u) for u in batch]
                            results = await asyncio.gather(*tasks, return_exceptions=True)

                            for result in results:
                                if isinstance(result, Exception):
                                    self.state.errors.append(str(result))
                                    continue
                                uname, info, links = result

                                if (self.min_subscribers > 0 and info and depth > 0
                                        and info.subscribers < self.min_subscribers):
                                    self.state.channels_scraped += 1
                                    continue

                                if info:
                                    self.graph.add_node(info)
                                else:
                                    self.graph.add_placeholder_node(uname)

                                fb_valid_links = []
                                for link in links:
                                    if link.target in self.blacklist:
                                        continue
                                    self.graph.add_placeholder_node(link.target)
                                    self.graph.add_edge(link)
                                    fb_valid_links.append(link)

                                    if depth + 1 <= max_depth and link.target not in visited:
                                        next_depth = depth + 1
                                        if next_depth not in depth_queues:
                                            depth_queues[next_depth] = []
                                        depth_queues[next_depth].append(link.target)

                                # Persist to SQLite
                                self._save_to_cache(info, fb_valid_links)
                                self.state.channels_scraped += 1
                            self.state.channels_total = len(visited) + sum(
                                len(q) for d, q in depth_queues.items() if d > depth
                            )
                            self.state.channels_queued = sum(
                                len(q) for d, q in depth_queues.items() if d > depth
                            )
                            self._notify()

            depth_elapsed = time.time() - depth_start
            logger.info(f"=== Depth {depth} completed in {depth_elapsed:.1f}s ===")

        # Done
        self.state.status = "stopped" if self._stop_requested else "completed"
        self.state.finished_at = datetime.utcnow().isoformat()
        self._notify()

        if hasattr(self.scraper, 'close'):
            await self.scraper.close()

        total_elapsed = time.time() - self._start_time
        logger.info(f"Crawl finished: {self.state.channels_scraped} channels in {total_elapsed:.1f}s")

        return self.graph
