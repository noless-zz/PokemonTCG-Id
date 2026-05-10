"""Card pricing helpers for the PokemonTCG scanner API.

Queries the local `card_prices` table for pre-fetched price snapshots.
The `sync_prices.py` script (run separately) populates that table.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import mysql.connector

from api.db import get_connection

logger = logging.getLogger(__name__)


def get_prices_for_card(card_id: str) -> list[dict]:
    """Return all stored price snapshots for *card_id*, newest first.

    Each dict contains: source, market, currency, condition, price,
    captured_at (ISO-8601 string).
    """
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT source, market, currency, `condition`, price,
                   captured_at
            FROM card_prices
            WHERE card_id = %s
            ORDER BY captured_at DESC
            LIMIT 50
            """,
            (card_id,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        # Serialise datetime objects for JSON output.
        for row in rows:
            if isinstance(row.get("captured_at"), datetime):
                row["captured_at"] = row["captured_at"].isoformat()
            if row.get("price") is not None:
                row["price"] = float(row["price"])
        return rows
    except mysql.connector.Error as exc:
        logger.warning("Price lookup failed for %s: %s", card_id, exc)
        return []


def get_best_price(card_id: str) -> Optional[dict]:
    """Return the most recently captured market price (any condition/market).

    Preference order: tcgplayer market > cardmarket > anything else.
    Returns None when no price is available.
    """
    rows = get_prices_for_card(card_id)
    if not rows:
        return None

    for source_pref in ("tcgplayer", "cardmarket"):
        for row in rows:
            if row["source"] == source_pref and row.get("price") is not None:
                return row
    # Fall back to whatever is available.
    return next((r for r in rows if r.get("price") is not None), None)
