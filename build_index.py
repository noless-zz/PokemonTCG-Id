#!/usr/bin/env python3
"""build_index.py – Build (or refresh) the card_match_index table.

For every card in `cards_art` that has a local image on disk, compute a
64-bit perceptual hash (pHash) and store it in `card_match_index`.

Usage
-----
    python build_index.py [--limit N] [--force]

Options
-------
    --limit N   Process at most N cards (useful for testing).
    --force     Re-compute hashes even for cards already in the index.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

import imagehash
import mysql.connector
from PIL import Image, UnidentifiedImageError

sys.path.insert(0, __file__.rsplit("/build_index.py", 1)[0])

from api.db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)


def _phash_to_int(ph: imagehash.ImageHash) -> int:
    """Convert imagehash to unsigned 64-bit integer."""
    flat = ph.hash.flatten()
    result = 0
    for bit in flat:
        result = (result << 1) | int(bit)
    return result


def build(limit: int = 0, force: bool = False) -> None:
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # Fetch cards that need indexing.
    if force:
        cursor.execute(
            "SELECT card_id, small_image FROM cards_art WHERE small_image IS NOT NULL"
        )
    else:
        cursor.execute(
            """
            SELECT ca.card_id, ca.small_image
            FROM cards_art ca
            LEFT JOIN card_match_index mi ON mi.card_id = ca.card_id
            WHERE ca.small_image IS NOT NULL
              AND mi.card_id IS NULL
            """
        )
    rows = cursor.fetchall()

    if not rows:
        logger.info("All cards are already indexed. Use --force to rebuild.")
        cursor.close()
        conn.close()
        return

    total = len(rows) if not limit else min(len(rows), limit)
    logger.info("Cards to index: %d", total)

    start = time.time()
    success = 0
    errors = 0

    for i, row in enumerate(rows[:total], start=1):
        card_id = row["card_id"]
        image_path = row["small_image"]

        try:
            with Image.open(image_path) as img:
                img = img.convert("RGB")
                ph = imagehash.phash(img)
                ph_int = _phash_to_int(ph)
                ph_str = str(ph)

            if force:
                cursor.execute(
                    """
                    INSERT INTO card_match_index (card_id, phash_int, phash_str)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        phash_int = VALUES(phash_int),
                        phash_str = VALUES(phash_str),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (card_id, ph_int, ph_str),
                )
            else:
                cursor.execute(
                    """
                    INSERT IGNORE INTO card_match_index (card_id, phash_int, phash_str)
                    VALUES (%s, %s, %s)
                    """,
                    (card_id, ph_int, ph_str),
                )

            conn.commit()
            success += 1

        except (FileNotFoundError, UnidentifiedImageError) as exc:
            logger.warning("Skip %s (%s): %s", card_id, image_path, exc)
            errors += 1
        except (mysql.connector.Error, OSError) as exc:
            logger.error("Error on %s: %s", card_id, exc)
            errors += 1

        # Progress report every 500 cards.
        if i % 500 == 0 or i == total:
            elapsed = time.time() - start
            rate = i / elapsed if elapsed else 0
            eta = (total - i) / rate if rate else 0
            logger.info(
                "Progress: %d/%d (%.0f cards/s, ETA %.0fs)",
                i, total, rate, eta,
            )

    cursor.close()
    conn.close()
    logger.info(
        "Index build complete. Success: %d, Errors: %d", success, errors
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build card phash index.")
    parser.add_argument("--limit", type=int, default=0, help="Max cards to index.")
    parser.add_argument(
        "--force", action="store_true", help="Re-index already-indexed cards."
    )
    args = parser.parse_args()
    build(limit=args.limit, force=args.force)


if __name__ == "__main__":
    main()
