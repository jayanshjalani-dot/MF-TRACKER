"""
Macro & Geopolitical Sentiment Engine.

Aggregates financial news headlines from RSS feeds + NewsAPI,
scores each headline with TextBlob polarity, and emits an overall
market sentiment score that can block automated trades.
"""

import os
import time
import hashlib
from dataclasses import dataclass, field
from typing import Optional
import feedparser
import requests
from textblob import TextBlob
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# ---------- Config ----------

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
NEWS_API_URL = "https://newsapi.org/v2/everything"

# Public RSS feeds for Indian financial news
RSS_FEEDS = {
    "moneycontrol_markets": "https://www.moneycontrol.com/rss/marketreports.xml",
    "economic_times_markets": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "livemint_markets": "https://www.livemint.com/rss/markets",
}

# Headline keywords that force extra weight in either direction
HIGH_IMPACT_BEARISH = [
    "war", "conflict", "attack", "explosion", "terror", "nuclear",
    "recession", "crash", "default", "inflation spike", "rate hike",
    "pandemic", "lockdown", "sanctions",
]
HIGH_IMPACT_BULLISH = [
    "ceasefire", "peace deal", "rate cut", "stimulus", "gdp growth",
    "fed pivot", "rbi pause", "surplus", "record high", "breakout",
]

# If |sentiment_score| > this threshold, algo trades are blocked
TRADE_BLOCK_THRESHOLD = 0.65

# Cache duration in seconds — avoid hammering RSS feeds
CACHE_TTL = 300


@dataclass
class Headline:
    source: str
    title: str
    url: str
    published: str
    polarity: float = 0.0
    subjectivity: float = 0.0
    weight: float = 1.0


class SentimentEngine:
    """
    Fetches headlines, scores them, and produces a net sentiment score.

    Score range: -1.0 (extremely bearish) → +1.0 (extremely bullish)
    """

    def __init__(self):
        self._cache: dict = {}
        self._cache_ts: float = 0.0

    # ---------- Public ----------

    def get_sentiment(self, force_refresh: bool = False) -> dict:
        """
        Returns a dict:
        {
            "score": float,        # -1.0 to +1.0
            "label": str,          # "Very Bullish" / "Bullish" / "Neutral" / etc.
            "headlines": list,
            "trade_blocked": bool
        }
        """
        if not force_refresh and time.time() - self._cache_ts < CACHE_TTL:
            return self._cache

        headlines = self._fetch_all_headlines()
        scored = self._score_headlines(headlines)

        if not scored:
            return self._neutral_response()

        net_score = self._weighted_average(scored)
        label = self._label(net_score)
        trade_blocked = abs(net_score) > TRADE_BLOCK_THRESHOLD

        result = {
            "score": round(net_score, 4),
            "label": label,
            "trade_blocked": trade_blocked,
            "headlines": [
                {
                    "source": h.source,
                    "title": h.title,
                    "url": h.url,
                    "published": h.published,
                    "polarity": round(h.polarity, 3),
                    "weight": h.weight,
                }
                for h in scored[:30]  # top 30 headlines in response
            ],
        }

        if trade_blocked:
            logger.warning(
                f"SENTIMENT BLOCK | score={net_score:.3f} ({label}) — "
                f"automated trades suppressed"
            )

        self._cache = result
        self._cache_ts = time.time()
        return result

    # ---------- Fetching ----------

    def _fetch_all_headlines(self) -> list[Headline]:
        headlines: list[Headline] = []
        headlines += self._fetch_rss()
        headlines += self._fetch_newsapi()
        # Deduplicate by title hash
        seen = set()
        unique = []
        for h in headlines:
            key = hashlib.md5(h.title.encode()).hexdigest()
            if key not in seen:
                seen.add(key)
                unique.append(h)
        return unique

    def _fetch_rss(self) -> list[Headline]:
        headlines = []
        for source, url in RSS_FEEDS.items():
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:20]:
                    headlines.append(
                        Headline(
                            source=source,
                            title=entry.get("title", ""),
                            url=entry.get("link", ""),
                            published=entry.get("published", ""),
                        )
                    )
            except Exception as e:
                logger.debug(f"RSS fetch error [{source}]: {e}")
        logger.debug(f"RSS: fetched {len(headlines)} headlines")
        return headlines

    def _fetch_newsapi(self) -> list[Headline]:
        if not NEWS_API_KEY:
            return []
        headlines = []
        try:
            params = {
                "q": "NSE OR BSE OR Nifty OR India stock market OR RBI",
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 30,
                "apiKey": NEWS_API_KEY,
            }
            resp = requests.get(NEWS_API_URL, params=params, timeout=10)
            resp.raise_for_status()
            for article in resp.json().get("articles", []):
                headlines.append(
                    Headline(
                        source=article.get("source", {}).get("name", "NewsAPI"),
                        title=article.get("title", ""),
                        url=article.get("url", ""),
                        published=article.get("publishedAt", ""),
                    )
                )
        except Exception as e:
            logger.debug(f"NewsAPI fetch error: {e}")
        return headlines

    # ---------- Scoring ----------

    def _score_headlines(self, headlines: list[Headline]) -> list[Headline]:
        for h in headlines:
            if not h.title:
                continue
            blob = TextBlob(h.title)
            h.polarity = blob.sentiment.polarity
            h.subjectivity = blob.sentiment.subjectivity
            h.weight = self._compute_weight(h.title, h.subjectivity)
        return [h for h in headlines if h.title]

    def _compute_weight(self, title: str, subjectivity: float) -> float:
        """
        High-impact keywords (war, rate cut, etc.) get a multiplier.
        Highly subjective (opinionated) headlines get downweighted.
        """
        lower = title.lower()
        weight = 1.0

        for kw in HIGH_IMPACT_BEARISH:
            if kw in lower:
                weight = 2.0
                break
        for kw in HIGH_IMPACT_BULLISH:
            if kw in lower:
                weight = max(weight, 2.0)
                break

        # Reduce weight for highly subjective / opinionated content
        if subjectivity > 0.7:
            weight *= 0.6

        return round(weight, 2)

    @staticmethod
    def _weighted_average(headlines: list[Headline]) -> float:
        """Compute the weighted average polarity score."""
        total_weight = sum(h.weight for h in headlines if h.polarity != 0)
        if total_weight == 0:
            return 0.0
        weighted_sum = sum(h.polarity * h.weight for h in headlines)
        return weighted_sum / total_weight

    @staticmethod
    def _label(score: float) -> str:
        if score >= 0.5:
            return "Very Bullish"
        elif score >= 0.2:
            return "Bullish"
        elif score >= -0.2:
            return "Neutral"
        elif score >= -0.5:
            return "Bearish"
        else:
            return "Very Bearish"

    @staticmethod
    def _neutral_response() -> dict:
        return {
            "score": 0.0,
            "label": "Neutral",
            "trade_blocked": False,
            "headlines": [],
        }
