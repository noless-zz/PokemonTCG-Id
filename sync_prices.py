#!/usr/bin/env python3
"""sync_prices.py – Fetch current market prices and store them in card_prices.

Usage
-----
    python sync_prices.py [--limit N] [--set-id SET_ID]

The script calls the pokemontcg.io v2 API (which proxies TCGplayer and
Cardmarket data) page-by-page, extracts all price points for each card, and
upserts them into the `card_prices` table.

Environment variables
---------------------
    POKEMONTCG_API_KEY   – optional; raises the rate limit from ~10 req/s to
                           higher tiers (obtain at dev.pokemontcg.io).
    DB_HOST / DB_USER / DB_PASSWORD / DB_NAME – override default DB config.
    PRICE_SYNC_DELAY     – seconds to pause between API pages (default 0.15).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

import mysql.connector
import requests

# Ensure project root is importable.
sys.path.insert(0, __file__.rsplit("/sync_prices.py", 1)[0])

from api.config import (
    DEFAULT_CURRENCY,
    POKEMONTCG_API_BASE,
    POKEMONTCG_API_KEY,
    PRICE_SYNC_DELAY,
)
from api.db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

# Price conditions we extract from the TCGplayer payload.
TCGPLAYER_CONDITIONS = (
    "normal",
    "holofoil",
    "reverseHolofoil",
    "1stEditionHolofoil",
    "1stEditionNormal",
)
# Cardmarket conditions.
CARDMARKET_CONDITIONS = (
    "averageSellPrice",
    "lowPrice",
    "trendPrice",
    "avg1",
    "avg7",
    "avg30",
)

FETCH_TIMEOUT_SECONDS = 30
FETCH_MAX_RETRIES = 3
FETCH_RETRY_BACKOFF_SECONDS = 2.0


def build_headers() -> dict[str, str]:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if POKEMONTCG_API_KEY:
        headers["X-Api-Key"] = POKEMONTCG_API_KEY
    return headers


def fetch_cards_page(page: int, page_size: int = 250, set_id: str = "") -> dict:
    """Fetch a single page of cards from pokemontcg.io."""
    params: dict = {"page": page, "pageSize": page_size}
    if set_id:
        params["q"] = f"set.id:{set_id}"
    url = f"{POKEMONTCG_API_BASE}/cards"
    for attempt in range(1, FETCH_MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                headers=build_headers(),
                params=params,
                timeout=FETCH_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException:
            if attempt >= FETCH_MAX_RETRIES:
                logger.error(
                    "Fetch page %d failed after %d attempts.",
                    page,
                    FETCH_MAX_RETRIES,
                )
                raise
            delay = FETCH_RETRY_BACKOFF_SECONDS * attempt
            logger.warning(
                "Fetch page %d failed (attempt %d/%d). Retrying in %.1fs.",
                page,
                attempt,
                FETCH_MAX_RETRIES,
                delay,
            )
            time.sleep(delay)


def extract_prices(card: dict) -> list[dict]:
    """Extract all price data points from a single card API response dict."""
    prices: list[dict] = []
    captured_at = datetime.now(timezone.utc)

    # ── TCGplayer ──────────────────────────────────────────────────────────────
    tcg = card.get("tcgplayer", {})
    tcg_prices = tcg.get("prices", {})
    for market in TCGPLAYER_CONDITIONS:
        market_data = tcg_prices.get(market)
        if not market_data:
            continue
        # Prefer market price; fall back to mid.
        for cond_key, cond_label in (
            ("market", "market"),
            ("mid", "mid"),
            ("low", "low"),
            ("high", "high"),
        ):
            val = market_data.get(cond_key)
            if val is not None:
                prices.append(
                    {
                        "source": "tcgplayer",
                        "market": market,
                        "currency": DEFAULT_CURRENCY,
                        "condition": cond_label,
                        "price": float(val),
                        "captured_at": captured_at,
                    }
                )

    # ── Cardmarket ────────────────────────────────────────────────────────────
    cm = card.get("cardmarket", {})
    cm_prices = cm.get("prices", {})
    for cond_key in CARDMARKET_CONDITIONS:
        val = cm_prices.get(cond_key)
        if val is not None:
            prices.append(
                {
                    "source": "cardmarket",
                    "market": "normal",
                    "currency": "EUR",
                    "condition": cond_key,
                    "price": float(val),
                    "captured_at": captured_at,
                }
            )

    return prices


def upsert_prices(cursor, conn, card_id: str, prices: list[dict]) -> int:
    """Insert price rows; returns count of inserted rows."""
    if not prices:
        return 0
    inserted = 0
    for p in prices:
        try:
            cursor.execute(
                    """
                INSERT INTO card_prices
                    (card_id, source, market, currency, `condition`, price, captured_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    card_id,
                    p["source"],
                    p["market"],
                    p["currency"],
                    p["condition"],
                    p["price"],
                    p["captured_at"],
                ),
            )
            inserted += 1
        except mysql.connector.Error as exc:
            logger.debug("Skip price insert for %s: %s", card_id, exc)
    conn.commit()
    return inserted


def sync(limit: int = 0, set_id: str = "") -> None:
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    page = 1
    total_cards = 0
    total_prices = 0

    logger.info("Starting price sync (set_id=%r, limit=%d)…", set_id or "all", limit)

    while True:
        logger.info("Fetching page %d…", page)
        try:
            data = fetch_cards_page(page, set_id=set_id)
        except requests.RequestException as exc:
            logger.error("API error: %s", exc)
            break

        cards = data.get("data", [])
        if not cards:
            logger.info("No more cards on page %d. Done.", page)
            break

        for card in cards:
            card_id = card.get("id", "")
            prices = extract_prices(card)
            inserted = upsert_prices(cursor, conn, card_id, prices)
            total_prices += inserted
            total_cards += 1
            logger.debug("  %s: %d price rows", card_id, inserted)

            if limit and total_cards >= limit:
                break

        total_count = data.get("totalCount", 0)
        logger.info(
            "Page %d done. Cards so far: %d/%d, Price rows: %d",
            page,
            total_cards,
            total_count,
            total_prices,
        )

        if limit and total_cards >= limit:
            break
        if len(cards) < 250:
            break

        page += 1
        time.sleep(PRICE_SYNC_DELAY)

    cursor.close()
    conn.close()
    logger.info(
        "Price sync complete. Cards processed: %d, Price rows inserted: %d",
        total_cards,
        total_prices,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync TCG card prices into MySQL.")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Stop after processing this many cards (0 = all).",
    )
    parser.add_argument(
        "--set-id",
        default="",
        help="Restrict sync to a specific set id, e.g. 'base1'.",
    )
    args = parser.parse_args()
    sync(limit=args.limit, set_id=args.set_id)


if __name__ == "__main__":
    main()
