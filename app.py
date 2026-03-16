"""
Telegram Network Mapper - Flask Web Application
Provides API endpoints and serves the web interface.
"""

import asyncio
import json
import logging
import os
import threading
from datetime import datetime

from flask import Flask, render_template, request, jsonify, Response
from flask_cors import CORS

from crawler import Crawler, NetworkGraph

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import config
except ImportError:
    config = None

# Configure logging
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Global state (thread-safe via locks in NetworkGraph and CrawlState)
active_crawler: Crawler = None
crawl_thread: threading.Thread = None
current_graph: NetworkGraph = NetworkGraph()
graph_lock = threading.Lock()


def run_crawl_async(crawler: Crawler, seeds: list, max_depth: int, max_channels: int):
    """Run the async crawl in a new event loop (for threading)."""
    global current_graph
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        graph = loop.run_until_complete(
            crawler.crawl(seeds, max_depth=max_depth, max_channels=max_channels)
        )
        with graph_lock:
            current_graph = graph
    except Exception as e:
        logger.error(f"Crawl error: {e}")
        crawler.state.status = "error"
        crawler.state.errors.append(str(e))
    finally:
        loop.close()


# ──────────────────────────────────────────────
# Routes - Pages
# ──────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


# ──────────────────────────────────────────────
# Routes - API
# ──────────────────────────────────────────────

@app.route('/api/config')
def get_config():
    """Return pre-configured credentials (if any) for the frontend."""
    if config:
        return jsonify({
            "api_id": getattr(config, 'TELEGRAM_API_ID', ''),
            "api_hash": getattr(config, 'TELEGRAM_API_HASH', ''),
            "phone": getattr(config, 'TELEGRAM_PHONE', ''),
        })
    return jsonify({})


@app.route('/api/crawl/start', methods=['POST'])
def start_crawl():
    """Start a new crawl."""
    global active_crawler, crawl_thread, current_graph

    if active_crawler and active_crawler.state.status == "running":
        return jsonify({"error": "A crawl is already running"}), 400

    data = request.json
    seeds = data.get('seeds', [])

    if not seeds:
        return jsonify({"error": "No seed channels provided"}), 400

    # Validate and clamp parameters
    max_depth = max(1, min(5, int(data.get('max_depth', 2))))
    max_channels = max(1, min(500, int(data.get('max_channels', 50))))
    max_messages = max(1, min(1000, int(data.get('max_messages', 100))))
    delay = max(0.0, min(10.0, float(data.get('delay', 1.5))))
    concurrency = max(1, min(20, int(data.get('concurrency', 5))))
    method = data.get('method', 'web')

    if method not in ('web', 'telethon'):
        return jsonify({"error": "method must be 'web' or 'telethon'"}), 400

    # Clean seed names
    clean_seeds = []
    for s in seeds:
        s = s.strip().lstrip('@')
        if 't.me/' in s:
            s = s.split('t.me/')[-1].split('/')[0]
        if s:
            clean_seeds.append(s.lower())

    if not clean_seeds:
        return jsonify({"error": "No valid seed channels after cleaning"}), 400

    # Optional proxy list
    proxies = data.get('proxies', None)

    # Setup crawler
    kwargs = {
        "method": method,
        "max_messages": max_messages,
        "delay": delay,
        "concurrency": concurrency,
        "proxies": proxies,
    }

    if method == "telethon":
        # Use values from request, fallback to config.py
        kwargs["api_id"] = int(data.get('api_id') or (getattr(config, 'TELEGRAM_API_ID', 0) if config else 0))
        kwargs["api_hash"] = data.get('api_hash') or (getattr(config, 'TELEGRAM_API_HASH', '') if config else '')
        kwargs["phone"] = data.get('phone') or (getattr(config, 'TELEGRAM_PHONE', '') if config else '')

        if not kwargs["api_id"] or not kwargs["api_hash"]:
            return jsonify({"error": "Telethon requires api_id and api_hash"}), 400

        # Check that session file exists
        session_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'telegram_mapper_session.session')
        if not os.path.exists(session_file):
            return jsonify({
                "error": "Session Telethon introuvable. Lance d'abord dans le terminal : python auth_telegram.py"
            }), 400

    active_crawler = Crawler(**kwargs)
    with graph_lock:
        current_graph = active_crawler.graph

    # Run in background thread
    crawl_thread = threading.Thread(
        target=run_crawl_async,
        args=(active_crawler, clean_seeds, max_depth, max_channels),
        daemon=True
    )
    crawl_thread.start()

    return jsonify({
        "status": "started",
        "seeds": clean_seeds,
        "max_depth": max_depth,
        "max_channels": max_channels,
    })


@app.route('/api/crawl/stop', methods=['POST'])
def stop_crawl():
    """Stop the current crawl."""
    global active_crawler
    if active_crawler:
        active_crawler.request_stop()
        return jsonify({"status": "stop_requested"})
    return jsonify({"error": "No active crawl"}), 400


@app.route('/api/crawl/status')
def crawl_status():
    """Get the current crawl status."""
    if active_crawler:
        return jsonify(active_crawler.state.to_dict())
    return jsonify({"status": "idle"})


@app.route('/api/graph')
def get_graph():
    """Get the current graph as D3.js JSON."""
    with graph_lock:
        return jsonify(current_graph.to_d3_json())


@app.route('/api/graph/stats')
def get_graph_stats():
    """Get graph statistics."""
    with graph_lock:
        return jsonify(current_graph.get_stats())


@app.route('/api/export/gexf')
def export_gexf():
    """Export graph as GEXF (for Gephi)."""
    with graph_lock:
        gexf = current_graph.to_gexf()
    return Response(
        gexf,
        mimetype='application/xml',
        headers={'Content-Disposition': 'attachment; filename=telegram_network.gexf'}
    )


@app.route('/api/export/graphml')
def export_graphml():
    """Export graph as GraphML."""
    with graph_lock:
        graphml = current_graph.to_graphml()
    return Response(
        graphml,
        mimetype='application/xml',
        headers={'Content-Disposition': 'attachment; filename=telegram_network.graphml'}
    )


@app.route('/api/export/json')
def export_json():
    """Export graph as JSON."""
    with graph_lock:
        data = json.dumps(current_graph.to_d3_json(), indent=2, ensure_ascii=False)
    return Response(
        data,
        mimetype='application/json',
        headers={'Content-Disposition': 'attachment; filename=telegram_network.json'}
    )


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
