#!/usr/bin/env python3
"""Export deterministic JSON artifacts for GitHub Pages deployment."""

from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

import mysql.connector


ALLOWED_COUNT_TABLES = {
    "sets",
    "cards_classification",
    "cards_art",
    "preprocessed_images",
    "card_match_index",
    "card_prices",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export card metadata and latest prices as JSON for static hosting."
    )
    parser.add_argument(
        "--output-dir",
        default=os.environ.get("PAGES_DATA_OUTPUT_DIR", "deploy/pages/data"),
        help="Directory where JSON artifacts are written.",
    )
    parser.add_argument("--db-host", default=os.environ.get("DB_HOST", "localhost"))
    parser.add_argument("--db-user", default=os.environ.get("DB_USER", "pokemon"))
    parser.add_argument("--db-password", default=os.environ.get("DB_PASSWORD"))
    parser.add_argument("--db-name", default=os.environ.get("DB_NAME", "pokemon_tcg"))
    parser.add_argument("--db-charset", default=os.environ.get("DB_CHARSET", "utf8mb4"))
    parser.add_argument(
        "--max-cards",
        type=int,
        default=int(os.environ.get("EXPORT_MAX_CARDS", "5000")),
        help="Max cards to export (0 = all).",
    )
    parser.add_argument(
        "--max-prices",
        type=int,
        default=int(os.environ.get("EXPORT_MAX_PRICES", "5000")),
        help="Max latest-price rows to export (0 = all).",
    )
    return parser.parse_args()


def query_count(cur, table: str) -> int:
    if table not in ALLOWED_COUNT_TABLES:
        raise ValueError(f"Unsupported table for count query: {table}")
    # Safe interpolation: table is constrained by ALLOWED_COUNT_TABLES.
    cur.execute(f"SELECT COUNT(*) AS c FROM {table}")
    return int(cur.fetchone()["c"])


def _normalize_for_json(value):
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _normalize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_json(v) for v in value]
    return value


def _normalize_rows(rows: list[dict]) -> list[dict]:
    return [{k: _normalize_for_json(v) for k, v in row.items()} for row in rows]


def main() -> None:
    args = parse_args()
    if not args.db_password:
        raise ValueError("Missing DB password. Set DB_PASSWORD or pass --db-password.")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = mysql.connector.connect(
        host=args.db_host,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        charset=args.db_charset,
    )
    cur = conn.cursor(dictionary=True)

    counts = {
        "sets": query_count(cur, "sets"),
        "cards_classification": query_count(cur, "cards_classification"),
        "cards_art": query_count(cur, "cards_art"),
        "preprocessed_images": query_count(cur, "preprocessed_images"),
        "card_match_index": query_count(cur, "card_match_index"),
        "card_prices": query_count(cur, "card_prices"),
    }

    cards_query = """
        SELECT cc.id, cc.name, cc.number, cc.rarity, cc.supertype,
               cc.set_id, s.name AS set_name, ca.small_image, ca.large_image
        FROM cards_classification cc
        JOIN sets s ON s.id = cc.set_id
        LEFT JOIN cards_art ca ON ca.card_id = cc.id
        ORDER BY cc.id
    """
    if args.max_cards > 0:
        cards_query += " LIMIT %s"
        cur.execute(cards_query, (args.max_cards,))
    else:
        cur.execute(cards_query)
    cards = cur.fetchall()

    latest_prices_query = """
        SELECT p.card_id, p.source, p.market, p.currency, p.`condition`, p.price, p.captured_at
        FROM card_prices p
        JOIN (
            SELECT card_id, MAX(captured_at) AS max_captured_at
            FROM card_prices
            GROUP BY card_id
        ) lp ON lp.card_id = p.card_id AND lp.max_captured_at = p.captured_at
        ORDER BY p.card_id
    """
    if args.max_prices > 0:
        latest_prices_query += " LIMIT %s"
        cur.execute(latest_prices_query, (args.max_prices,))
    else:
        cur.execute(latest_prices_query)
    latest_prices = cur.fetchall()

    cur.close()
    conn.close()

    cards = _normalize_rows(cards)
    latest_prices = _normalize_rows(latest_prices)

    artifact_info = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        "limits": {
            "max_cards": args.max_cards,
            "max_prices": args.max_prices,
        },
    }

    (output_dir / "build-info.json").write_text(
        json.dumps(artifact_info, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "cards.json").write_text(
        json.dumps(cards, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "latest-prices.json").write_text(
        json.dumps(latest_prices, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Export complete: {output_dir}")


if __name__ == "__main__":
    main()
