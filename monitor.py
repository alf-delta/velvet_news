"""
Media Monitor Pipeline
======================
Monitors 30+ alternative/culture media outlets for fresh articles.
Pipeline: Fetch → Dedup → Classify → Store

Usage:
    python monitor.py              # Run one full cycle
    python monitor.py --report     # Show latest unread articles
    python monitor.py --stats      # Show source statistics
    python monitor.py --export     # Export to JSON

Requirements:
    pip install requests beautifulsoup4 feedparser lxml
    
Optional (for AI classification):
    pip install google-generativeai   # Gemini API
"""

import sqlite3
import hashlib
import json
import re
import time
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from pathlib import Path

# --- Third-party imports ---
import requests
from bs4 import BeautifulSoup

# feedparser is optional — we have a built-in fallback
try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False
    import xml.etree.ElementTree as ET

# ============================================================
# Configuration
# ============================================================

DB_PATH = Path(__file__).parent / "media_monitor.db"
LOG_PATH = Path(__file__).parent / "monitor.log"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 15  # seconds
DELAY_BETWEEN_REQUESTS = 2  # seconds, be polite
FETCH_RETRIES = 2  # retry attempts on network failure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ============================================================
# Data model
# ============================================================

@dataclass
class Article:
    source: str           # e.g. "Dazed", "Metal Injection"
    url: str
    title: str
    date: Optional[str]   # ISO format or None
    topic: str            # classified topic
    format_type: str      # news / interview / essay / review / listicle / feature
    length_est: str       # short / medium / long
    snippet: str          # first ~200 chars of content
    fetched_at: str       # ISO timestamp of when we fetched it


# ============================================================
# Source Registry
# ============================================================
# Each source defines: name, method (rss/html), url, and parser hints.
# RSS sources are preferred — more stable and structured.
# HTML sources use CSS selectors to find article links + titles.

SOURCES = [
    # ── RSS-based sources ──────────────────────────────────────
    {
        "name": "Paper Magazine",
        "method": "rss",
        "url": "https://www.papermag.com/feeds/feed.rss",
        "category_hint": "culture/fashion",
    },
    {
        "name": "Dazed",
        "method": "rss",
        "url": "https://www.dazeddigital.com/rss",
        "category_hint": "culture/fashion",
    },
    {
        "name": "Alternative Press",
        "method": "rss",
        "url": "https://www.altpress.com/feed/",
        "category_hint": "music/alternative",
    },
    {
        "name": "Devolution Magazine",
        "method": "rss",
        "url": "https://devolutionmagazine.co.uk/feed/",
        "category_hint": "music/goth/punk",
    },
    {
        "name": "Gothic Beauty",
        "method": "rss",
        "url": "https://www.gothicbeauty.com/feed/",
        "category_hint": "goth/fashion",
    },
    {
        "name": "Interview Magazine",
        "method": "rss",
        "url": "https://www.interviewmagazine.com/feed",
        "category_hint": "culture/art",
    },
    {
        "name": "Nylon",
        "method": "rss",
        "url": "https://www.nylon.com/feed",
        "category_hint": "fashion/culture",
    },
    {
        "name": "Vestoj",
        "method": "rss",
        "url": "https://vestoj.com/feed/",
        "category_hint": "fashion/theory",
    },
    {
        "name": "Melan Magazine",
        "method": "rss",
        "url": "https://melanmag.com/feed/",
        "category_hint": "culture/black-british",
    },
    {
        "name": "Fashion Grunge",
        "method": "rss",
        "url": "https://fashiongrunge.com/feed/",
        "category_hint": "fashion/music",
    },
    {
        "name": "Metal Injection",
        "method": "rss",
        "url": "https://metalinjection.net/feed",
        "category_hint": "music/metal",
    },
    {
        "name": "Rolling Stone",
        "method": "rss",
        "url": "https://www.rollingstone.com/feed/",
        "category_hint": "music/culture",
        "broad": True,  # covers politics, sports, finance — require relevance
    },
    {
        "name": "Wonderland Magazine",
        "method": "rss",
        "url": "https://www.wonderlandmagazine.com/feed/",
        "category_hint": "fashion/culture",
    },
    {
        "name": "Little White Lies",
        "method": "rss",
        "url": "https://lwlies.com/feed/",
        "category_hint": "film/culture",
    },

    # ── HTML-parsed sources ────────────────────────────────────
    {
        "name": "Sight & Sound (BFI)",
        "method": "html",
        "url": "https://www.bfi.org.uk/sight-and-sound",
        "selector": "a[href*='/sight-and-sound/']",
        "title_attr": None,  # use link text
        "base_url": "https://www.bfi.org.uk",
        "category_hint": "film/criticism",
    },
    {
        "name": "Criterion / The Current",
        "method": "html",
        "url": "https://www.criterion.com/current",
        "selector": "a.current-post-link, article a, .post-title a",
        "base_url": "https://www.criterion.com",
        "category_hint": "film/criticism",
    },
    {
        "name": "V Magazine",
        "method": "html",
        "url": "https://vmagazine.com",
        "selector": "h2 a, h3 a, .post-title a",
        "base_url": "https://vmagazine.com",
        "category_hint": "fashion/culture",
    },
    {
        "name": "Vogue",
        "method": "html",
        "url": "https://www.vogue.com",
        "selector": "a[href*='/article/'], a[href*='/story/']",
        "base_url": "https://www.vogue.com",
        "category_hint": "fashion",
    },
    {
        "name": "i-D",
        "method": "html",
        "url": "https://i-d.co/",
        "selector": "a[href*='/article/']",
        "base_url": "https://i-d.co",
        "category_hint": "fashion/culture",
    },
    {
        "name": "Afrique Noire Magazine",
        "method": "html",
        "url": "https://afriquenoirmagazine.com",
        "selector": "h2 a, h3 a, .entry-title a",
        "base_url": "https://afriquenoirmagazine.com",
        "category_hint": "culture/african-art",
    },
    {
        "name": "Monument Magazine",
        "method": "html",
        "url": "https://monumentmagazine.com",
        "selector": "h2 a, h3 a, article a",
        "base_url": "https://monumentmagazine.com",
        "category_hint": "fashion/culture",
    },
    {
        "name": "Rock Feed",
        "method": "html",
        "url": "https://rockfeed.net",
        "selector": "h2 a, h3 a, .post-title a, article a",
        "base_url": "https://rockfeed.net",
        "category_hint": "music/rock",
    },

    # ── Web sources that are better monitored via Deadline site ──
    {
        "name": "Deadline",
        "method": "rss",
        "url": "https://deadline.com/feed/",
        "category_hint": "entertainment/hollywood",
        "broad": True,  # covers sports, finance, politics — require relevance
    },

    # ── Sources with known issues (kept for retry/fallback) ────
    # Obscura Undead: site down (404)
    # Propaganda Magazine: DNS dead
    # Super.Selected: SSL cert broken
    # DarkestGoth: needs verification
    # Sanctuary Magazine: needs verification
    # Gothic Culture Magazine: Facebook only
    # 1966 Magazine: needs verification
    # The Official Black Magazine: needs verification
    # Boy.Brother.Friend: needs verification
    # NOIR CITY Magazine: PDF quarterly, not monitorable
    # Metal Edge Magazine: Instagram-primary, site is metaledgemag.com
    # Jesea Lee: substack newsletter, not a news site
]


# ============================================================
# Database layer
# ============================================================

def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Initialize SQLite database with articles table."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            url_hash TEXT NOT NULL,
            title TEXT NOT NULL,
            date TEXT,
            topic TEXT DEFAULT '',
            format_type TEXT DEFAULT '',
            length_est TEXT DEFAULT '',
            snippet TEXT DEFAULT '',
            fetched_at TEXT NOT NULL,
            read_flag INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_url_hash ON articles(url_hash)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_source ON articles(source)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_fetched ON articles(fetched_at)
    """)
    conn.commit()
    return conn


def url_exists(conn: sqlite3.Connection, url: str) -> bool:
    """Check if URL already stored (dedup)."""
    h = hashlib.md5(url.encode()).hexdigest()
    row = conn.execute(
        "SELECT 1 FROM articles WHERE url_hash = ?", (h,)
    ).fetchone()
    return row is not None


def store_article(conn: sqlite3.Connection, art: Article) -> bool:
    """Insert article if not duplicate. Returns True if inserted."""
    h = hashlib.md5(art.url.encode()).hexdigest()
    try:
        conn.execute(
            """INSERT INTO articles
               (source, url, url_hash, title, date, topic, format_type,
                length_est, snippet, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                art.source, art.url, h, art.title, art.date,
                art.topic, art.format_type, art.length_est,
                art.snippet, art.fetched_at,
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False  # duplicate URL


# ============================================================
# Fetchers
# ============================================================

def _get_with_retry(session: requests.Session, url: str) -> requests.Response:
    """GET with simple retry on transient failures."""
    last_exc = None
    for attempt in range(FETCH_RETRIES):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_exc = e
            if attempt < FETCH_RETRIES - 1:
                time.sleep(3)
    raise last_exc


def make_session() -> requests.Session:
    """Create a requests session with browser-like headers."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
    })
    return s


def _parse_rss_xml(xml_text: str) -> list[dict]:
    """Fallback RSS parser using stdlib xml.etree (no feedparser needed)."""
    import xml.etree.ElementTree as ET

    articles = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return articles

    # Handle both RSS 2.0 (<item>) and Atom (<entry>) feeds
    # Strip common namespaces
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "dc": "http://purl.org/dc/elements/1.1/",
        "content": "http://purl.org/rss/1.0/modules/content/",
    }

    # Try RSS 2.0 first
    items = root.findall(".//item")
    if not items:
        # Try Atom
        items = root.findall(".//atom:entry", ns)
        if not items:
            items = root.findall(".//{http://www.w3.org/2005/Atom}entry")

    for item in items[:25]:
        # --- URL ---
        link_el = item.find("link")
        url = ""
        if link_el is not None:
            url = (link_el.text or link_el.get("href", "")).strip()
        if not url:
            # Atom style <link href="..."/>
            for l in item.findall("{http://www.w3.org/2005/Atom}link"):
                url = l.get("href", "").strip()
                if url:
                    break
        if not url:
            continue

        # --- Title ---
        title_el = item.find("title")
        if title_el is None:
            title_el = item.find("{http://www.w3.org/2005/Atom}title")
        title = (title_el.text or "").strip() if title_el is not None else ""

        # --- Date ---
        date_str = None
        for tag in ("pubDate", "published", "updated",
                     "{http://purl.org/dc/elements/1.1/}date",
                     "{http://www.w3.org/2005/Atom}published",
                     "{http://www.w3.org/2005/Atom}updated"):
            d_el = item.find(tag)
            if d_el is not None and d_el.text:
                date_str = d_el.text.strip()
                break

        # --- Snippet ---
        snippet = ""
        for tag in ("description",
                     "{http://purl.org/rss/1.0/modules/content/}encoded",
                     "{http://www.w3.org/2005/Atom}summary",
                     "{http://www.w3.org/2005/Atom}content"):
            s_el = item.find(tag)
            if s_el is not None and s_el.text:
                soup = BeautifulSoup(s_el.text, "html.parser")
                snippet = soup.get_text(strip=True)[:300]
                break

        articles.append({
            "url": url,
            "title": title,
            "date": date_str,
            "snippet": snippet,
        })

    return articles


def fetch_rss(session: requests.Session, source: dict) -> list[dict]:
    """Fetch and parse an RSS feed. Uses feedparser if available, else stdlib XML."""
    articles = []
    try:
        resp = _get_with_retry(session, source["url"])

        if HAS_FEEDPARSER:
            feed = feedparser.parse(resp.text)
            for entry in feed.entries[:25]:
                url = entry.get("link", "").strip()
                if not url:
                    continue
                title = entry.get("title", "").strip()
                date_str = None
                for date_field in ("published", "updated", "created"):
                    if hasattr(entry, date_field + "_parsed") and getattr(entry, date_field + "_parsed"):
                        parsed = getattr(entry, date_field + "_parsed")
                        date_str = time.strftime("%Y-%m-%dT%H:%M:%S", parsed)
                        break
                snippet = ""
                if hasattr(entry, "summary"):
                    soup = BeautifulSoup(entry.summary, "html.parser")
                    snippet = soup.get_text(strip=True)[:300]
                elif hasattr(entry, "content"):
                    for c in entry.content:
                        soup = BeautifulSoup(c.get("value", ""), "html.parser")
                        snippet = soup.get_text(strip=True)[:300]
                        break
                articles.append({
                    "url": url, "title": title,
                    "date": date_str, "snippet": snippet,
                })
        else:
            articles = _parse_rss_xml(resp.text)

        log.info(f"RSS [{source['name']}]: fetched {len(articles)} entries")

    except requests.RequestException as e:
        log.warning(f"RSS [{source['name']}]: request failed — {e}")
    except Exception as e:
        log.error(f"RSS [{source['name']}]: parse error — {e}")

    return articles


def fetch_html(session: requests.Session, source: dict) -> list[dict]:
    """Scrape article links from an HTML page. Returns list of {url, title}."""
    articles = []
    try:
        resp = _get_with_retry(session, source["url"])
        soup = BeautifulSoup(resp.text, "lxml")

        base = source.get("base_url", "")
        selector = source.get("selector", "article a")
        seen_urls = set()

        for link in soup.select(selector)[:30]:
            href = link.get("href", "").strip()
            if not href or href == "#" or href.startswith("javascript:"):
                continue

            # Make absolute URL
            if href.startswith("/"):
                href = base + href
            elif not href.startswith("http"):
                continue

            # Skip non-article links (navigation, tags, categories)
            skip_patterns = [
                "/category/", "/tag/", "/page/", "/author/",
                "/search", "/about", "/contact", "/advertise",
                "/subscribe", "/newsletter", "/shop", "/cart",
                "/privacy", "/terms", "/login", "/register",
                "#", "/feed", "/rss",
            ]
            if any(p in href.lower() for p in skip_patterns):
                continue

            # Dedup within this fetch
            if href in seen_urls:
                continue
            seen_urls.add(href)

            title = link.get_text(strip=True)
            if not title or len(title) < 10:
                # Try parent element for title
                parent = link.find_parent(["h1", "h2", "h3", "h4", "article"])
                if parent:
                    title = parent.get_text(strip=True)[:200]

            if title and len(title) >= 10:
                articles.append({
                    "url": href,
                    "title": title[:200],
                    "date": None,
                    "snippet": "",
                })

        log.info(f"HTML [{source['name']}]: found {len(articles)} links")

    except requests.RequestException as e:
        log.warning(f"HTML [{source['name']}]: request failed — {e}")
    except Exception as e:
        log.error(f"HTML [{source['name']}]: parse error — {e}")

    return articles


# ============================================================
# Classifier (rule-based, no API dependency)
# ============================================================

# Topic keywords → topic label mapping
TOPIC_RULES = {
    "fashion": [
        "fashion", "runway", "collection", "designer", "couture",
        "outfit", "style", "wardrobe", "wear", "dress", "denim",
        "streetwear", "menswear", "womenswear", "fw26", "ss26",
        "aw26", "fashion week",
    ],
    "music": [
        "album", "song", "band", "tour", "concert", "vinyl",
        "single", "track", "guitar", "vocalist", "rapper",
        "hip-hop", "hip hop", "metal", "punk", "goth", "rock",
        "electronic", "festival", "grammys", "grammy",
        "perform", "setlist", "ep release", "music video",
    ],
    "film": [
        "film", "movie", "cinema", "film director", "actor", "actress",
        "oscar", "oscars", "screening", "film premiere", "trailer",
        "box office", "theatrical", "netflix", "hulu",
        "streaming", "tv series", "television",
    ],
    "art": [
        "art", "exhibition", "gallery", "museum", "sculpture",
        "painting", "photography", "photographer", "artist",
        "installation", "curator", "biennale",
    ],
    "beauty": [
        "beauty", "makeup", "skincare", "cosmetic", "hair",
        "fragrance", "perfume", "nail", "routine",
    ],
    "culture": [
        "culture", "identity", "community", "subculture",
        "politics", "society", "book", "literary", "essay",
        "interview", "profile", "conversation", "opinion",
    ],
    "entertainment": [
        "celebrity", "hollywood", "red carpet", "award", "gala",
        "reality tv", "casting", "renewal", "cancell", "premiere",
    ],
}

# ============================================================
# Relevance filter
# ============================================================
# Логика: позитивная фильтрация, а не блок-лист.
#
# Источники делятся на два типа:
#   - нишевые (Dazed, Metal Injection, Vestoj…) — берём всё,
#     они по определению про альткультуру.
#   - широкие (Deadline, Rolling Stone) — берём только если
#     статья набирает хотя бы 1 очко по нашим topic-правилам.
#     Спорт, финансы, политика не совпадут ни с одним словом
#     из TOPIC_RULES и будут отброшены автоматически.
#
# Флаг "broad": True проставляется в SOURCES для широких изданий.
# ============================================================

def _kw_match(kw: str, text: str) -> bool:
    """Word-boundary match for single words; substring match for phrases."""
    if ' ' in kw:
        return kw in text
    return bool(re.search(r'\b' + re.escape(kw) + r'\b', text))


def topic_score(title: str, snippet: str) -> int:
    """Сколько culture-keywords нашлось в тексте статьи."""
    text = f"{title} {snippet}".lower()
    return sum(
        1 for keywords in TOPIC_RULES.values()
        for kw in keywords if _kw_match(kw, text)
    )


FORMAT_RULES = {
    "interview": ["interview", "conversation with", "talks to", "speaks to", "in conversation", "q&a", "questionnaire"],
    "review": ["review", "reviewed", "critique", "rating", "stars out of"],
    "listicle": ["best ", "top ", "ranking", "list of", " ways to", "things you", "unmissable"],
    "essay": ["essay", "opinion", "op-ed", "why we", "the case for", "the problem with"],
    "news": ["announces", "announced", "breaking", "exclusive", "confirmed", "launches", "unveils", "reveals", "drops", "releases"],
    "feature": ["feature", "behind the scenes", "inside", "the story of", "the making of", "deep dive", "profile"],
}


def classify_article(title: str, snippet: str, source_hint: str = "") -> tuple[str, str, str]:
    """
    Classify article by topic, format, and estimated length.
    Returns (topic, format_type, length_est).
    source_hint is used only as a tiebreaker, not as primary signal.
    """
    # Classify on content only — source_hint only breaks ties
    content_text = f"{title} {snippet}".lower()
    hint_text    = source_hint.lower()

    # --- Topic classification ---
    topic_scores = {}
    for topic, keywords in TOPIC_RULES.items():
        score = sum(1 for kw in keywords if _kw_match(kw, content_text))
        if score > 0:
            topic_scores[topic] = score

    if topic_scores:
        top_score = max(topic_scores.values())
        # If multiple topics tie, use source_hint as tiebreaker
        if list(topic_scores.values()).count(top_score) > 1:
            for topic in TOPIC_RULES:
                if topic_scores.get(topic) == top_score and topic in hint_text:
                    break
            else:
                topic = max(topic_scores, key=topic_scores.get)
        else:
            topic = max(topic_scores, key=topic_scores.get)
    elif hint_text:
        # No content signal — fall back to hint
        topic = "other"
        for candidate in TOPIC_RULES:
            if candidate in hint_text:
                topic = candidate
                break
    else:
        topic = "other"

    # --- Format classification ---
    format_type = "news"  # default
    for fmt, keywords in FORMAT_RULES.items():
        if any(_kw_match(kw, content_text) for kw in keywords):
            format_type = fmt
            break

    # --- Length estimation ---
    # Heuristic based on format and snippet length
    if format_type in ("essay", "feature", "interview"):
        length_est = "long"
    elif format_type in ("review", "listicle"):
        length_est = "medium"
    else:
        length_est = "short"

    return topic, format_type, length_est


# ============================================================
# Main pipeline
# ============================================================

def run_pipeline():
    """Execute one full monitoring cycle."""
    conn = init_db()
    session = make_session()
    now = datetime.now(timezone.utc).isoformat()

    total_new = 0
    total_checked = 0
    results_by_source = {}

    for source in SOURCES:
        log.info(f"Processing: {source['name']} ({source['method']})")

        # Fetch
        if source["method"] == "rss":
            raw_articles = fetch_rss(session, source)
        elif source["method"] == "html":
            raw_articles = fetch_html(session, source)
        else:
            log.warning(f"Unknown method for {source['name']}")
            continue

        new_count = 0
        for raw in raw_articles:
            total_checked += 1

            # Dedup check
            if url_exists(conn, raw["url"]):
                continue

            # Classify
            topic, fmt, length = classify_article(
                raw["title"],
                raw.get("snippet", ""),
                source.get("category_hint", ""),
            )

            # Relevance filter for broad sources — drop if no culture signal
            if source.get("broad") and topic_score(raw["title"], raw.get("snippet", "")) == 0:
                log.debug(f"  IRRELEVANT (broad source): {raw['title'][:80]}")
                continue

            # Build article
            art = Article(
                source=source["name"],
                url=raw["url"],
                title=raw["title"],
                date=raw.get("date"),
                topic=topic,
                format_type=fmt,
                length_est=length,
                snippet=raw.get("snippet", "")[:300],
                fetched_at=now,
            )

            # Store
            if store_article(conn, art):
                new_count += 1
                total_new += 1
                log.info(f"  NEW: [{topic}/{fmt}] {art.title[:80]}")

        results_by_source[source["name"]] = {
            "checked": len(raw_articles),
            "new": new_count,
        }

        # Polite delay between sources
        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Summary
    log.info("=" * 60)
    log.info(f"CYCLE COMPLETE: checked {total_checked}, new {total_new}")
    for name, stats in results_by_source.items():
        if stats["new"] > 0:
            log.info(f"  {name}: {stats['new']} new / {stats['checked']} checked")
    log.info("=" * 60)

    conn.close()
    return total_new


def show_report(limit: int = 50):
    """Print latest unread articles."""
    conn = init_db()
    rows = conn.execute("""
        SELECT source, title, topic, format_type, length_est, url, date, fetched_at
        FROM articles
        WHERE read_flag = 0
        ORDER BY fetched_at DESC, date DESC
        LIMIT ?
    """, (limit,)).fetchall()

    if not rows:
        print("No unread articles.")
        return

    print(f"\n{'='*80}")
    print(f"  LATEST ARTICLES ({len(rows)} unread)")
    print(f"{'='*80}\n")

    current_source = None
    for source, title, topic, fmt, length, url, date, fetched in rows:
        if source != current_source:
            current_source = source
            print(f"\n── {source} ──")

        date_str = date[:10] if date else "no date"
        tags = f"[{topic}] [{fmt}] [{length}]"
        print(f"  {date_str}  {tags}")
        print(f"  {title[:100]}")
        print(f"  {url}")
        print()

    conn.close()


def show_stats():
    """Print source statistics."""
    conn = init_db()
    rows = conn.execute("""
        SELECT source,
               COUNT(*) as total,
               SUM(CASE WHEN read_flag = 0 THEN 1 ELSE 0 END) as unread,
               MAX(date) as latest_date,
               MAX(fetched_at) as last_fetch
        FROM articles
        GROUP BY source
        ORDER BY total DESC
    """).fetchall()

    print(f"\n{'Source':<30} {'Total':>6} {'Unread':>7} {'Latest':>12} {'Last Fetch':>12}")
    print("-" * 80)
    for name, total, unread, latest, fetched in rows:
        latest_str = latest[:10] if latest else "—"
        fetch_str = fetched[:10] if fetched else "—"
        print(f"{name:<30} {total:>6} {unread:>7} {latest_str:>12} {fetch_str:>12}")

    total_all = sum(r[1] for r in rows)
    unread_all = sum(r[2] for r in rows)
    print("-" * 80)
    print(f"{'TOTAL':<30} {total_all:>6} {unread_all:>7}")

    conn.close()


def export_json(output_path: str = "articles_export.json"):
    """Export all articles to JSON."""
    conn = init_db()
    rows = conn.execute("""
        SELECT source, url, title, date, topic, format_type,
               length_est, snippet, fetched_at
        FROM articles
        ORDER BY fetched_at DESC
    """).fetchall()

    articles = []
    for row in rows:
        articles.append({
            "source": row[0],
            "url": row[1],
            "title": row[2],
            "date": row[3],
            "topic": row[4],
            "format": row[5],
            "length": row[6],
            "snippet": row[7],
            "fetched_at": row[8],
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    print(f"Exported {len(articles)} articles to {output_path}")
    conn.close()


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    import sys

    if "--report" in sys.argv:
        show_report()
    elif "--stats" in sys.argv:
        show_stats()
    elif "--export" in sys.argv:
        export_json()
    else:
        new = run_pipeline()
        print(f"\n✓ Done. {new} new articles found.")
        if new > 0:
            print("Run with --report to see them.")
