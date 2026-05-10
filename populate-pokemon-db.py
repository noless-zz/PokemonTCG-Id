#!/usr/bin/env python3
"""Populate the PokemonTCG MySQL DB from local pokemon-tcg-data JSON files.

This script is CI-friendly:
- no hardcoded local paths
- no embedded DB credentials
- configurable via CLI flags and environment variables
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import mysql.connector
import requests


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_DATASET_ROOT = (
    REPO_ROOT / "Pokémon Mosaics - Database" / "pokemon-tcg-data-master"
)
DEFAULT_IMAGES_DIR = REPO_ROOT / "images"


@dataclass
class Limits:
    max_sets: int
    max_card_files: int
    max_cards_per_set: int
    max_deck_files: int
    max_decks_per_file: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate pokemon_tcg DB from pokemon-tcg-data JSON files."
    )
    parser.add_argument(
        "--dataset-root",
        default=os.environ.get("POKEMON_DATASET_ROOT", str(DEFAULT_DATASET_ROOT)),
        help="Path to pokemon-tcg-data root (contains sets/en.json, cards/en, decks/en).",
    )
    parser.add_argument(
        "--images-dir",
        default=os.environ.get("POKEMON_IMAGES_DIR", str(DEFAULT_IMAGES_DIR)),
        help="Directory where downloaded set/card images are stored.",
    )
    parser.add_argument(
        "--db-host",
        default=os.environ.get("DB_HOST", "localhost"),
        help="MySQL host.",
    )
    parser.add_argument(
        "--db-user",
        default=os.environ.get("DB_USER", "pokemon"),
        help="MySQL user.",
    )
    parser.add_argument(
        "--db-password",
        default=os.environ.get("DB_PASSWORD"),
        help="MySQL password (defaults to DB_PASSWORD env var).",
    )
    parser.add_argument(
        "--db-name",
        default=os.environ.get("DB_NAME", "pokemon_tcg"),
        help="MySQL database name.",
    )
    parser.add_argument(
        "--db-charset",
        default=os.environ.get("DB_CHARSET", "utf8mb4"),
        help="MySQL charset.",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=float(os.environ.get("DOWNLOAD_TIMEOUT", "30")),
        help="HTTP timeout (seconds) for image downloads.",
    )
    parser.add_argument(
        "--max-sets",
        type=int,
        default=int(os.environ.get("POPULATE_MAX_SETS", "0")),
        help="Max sets to process (0 = all).",
    )
    parser.add_argument(
        "--max-card-files",
        type=int,
        default=int(os.environ.get("POPULATE_MAX_CARD_FILES", "0")),
        help="Max card JSON files to process (0 = all).",
    )
    parser.add_argument(
        "--max-cards-per-set",
        type=int,
        default=int(os.environ.get("POPULATE_MAX_CARDS_PER_SET", "0")),
        help="Max cards processed per set file (0 = all).",
    )
    parser.add_argument(
        "--max-deck-files",
        type=int,
        default=int(os.environ.get("POPULATE_MAX_DECK_FILES", "0")),
        help="Max deck JSON files to process (0 = all).",
    )
    parser.add_argument(
        "--max-decks-per-file",
        type=int,
        default=int(os.environ.get("POPULATE_MAX_DECKS_PER_FILE", "0")),
        help="Max decks processed per deck file (0 = all).",
    )
    return parser.parse_args()


def safe_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, dict)) and not value:
        return None
    return json.dumps(value)


def apply_limit(items: list[Any], n: int) -> list[Any]:
    return items[:n] if n and n > 0 else items


def ensure_exists(path: Path, what: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{what} not found: {path}")


def download_image(
    *,
    url: str,
    filename: str,
    images_dir: Path,
    timeout: float,
) -> str:
    images_dir.mkdir(parents=True, exist_ok=True)
    file_path = images_dir / filename
    if file_path.exists():
        return file_path.resolve().as_posix()

    resp = requests.get(url, stream=True, timeout=timeout)
    resp.raise_for_status()
    with file_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return file_path.resolve().as_posix()


def connect_db(args: argparse.Namespace) -> mysql.connector.MySQLConnection:
    if not args.db_password:
        raise ValueError("Missing DB password. Set DB_PASSWORD or pass --db-password.")
    return mysql.connector.connect(
        host=args.db_host,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        charset=args.db_charset,
    )


def insert_sets(
    cursor,
    conn,
    *,
    dataset_root: Path,
    images_dir: Path,
    timeout: float,
    limits: Limits,
) -> set[str]:
    sets_file = dataset_root / "sets" / "en.json"
    ensure_exists(sets_file, "Sets JSON")
    with sets_file.open("r", encoding="utf-8") as f:
        sets = json.load(f)
    sets = apply_limit(sets, limits.max_sets)
    print(f"Inserting sets ({len(sets)} records)...")
    inserted_set_ids: set[str] = set()

    for i, s in enumerate(sets, start=1):
        symbol_path = None
        logo_path = None
        images = s.get("images", {})
        if "symbol" in images:
            symbol_path = download_image(
                url=images["symbol"],
                filename=f"{s['id']}_symbol.png",
                images_dir=images_dir,
                timeout=timeout,
            )
        if "logo" in images:
            logo_path = download_image(
                url=images["logo"],
                filename=f"{s['id']}_logo.png",
                images_dir=images_dir,
                timeout=timeout,
            )

        cursor.execute(
            """
            INSERT INTO sets
                (id, name, series, printed_total, total, legality, ptcgo_code,
                 release_date, updated_at, symbol_image, logo_image)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                series = VALUES(series),
                printed_total = VALUES(printed_total),
                total = VALUES(total),
                legality = VALUES(legality),
                ptcgo_code = VALUES(ptcgo_code),
                release_date = VALUES(release_date),
                updated_at = VALUES(updated_at),
                symbol_image = VALUES(symbol_image),
                logo_image = VALUES(logo_image)
            """,
            (
                s["id"],
                s["name"],
                s.get("series"),
                s.get("printedTotal"),
                s.get("total"),
                safe_json(s.get("legalities")),
                s.get("ptcgoCode"),
                s.get("releaseDate"),
                s.get("updatedAt"),
                symbol_path,
                logo_path,
            ),
        )
        conn.commit()
        inserted_set_ids.add(s["id"])
        print(f"  [{i}/{len(sets)}] set {s['id']} inserted")

    return inserted_set_ids


def insert_cards(
    cursor,
    conn,
    *,
    dataset_root: Path,
    images_dir: Path,
    timeout: float,
    limits: Limits,
    allowed_set_ids: set[str] | None = None,
) -> None:
    cards_dir = dataset_root / "cards" / "en"
    ensure_exists(cards_dir, "Cards directory")
    card_files = sorted(p for p in cards_dir.iterdir() if p.suffix == ".json")
    card_files = apply_limit(card_files, limits.max_card_files)
    if allowed_set_ids is not None:
        original_count = len(card_files)
        card_files = [p for p in card_files if p.stem in allowed_set_ids]
        skipped_count = original_count - len(card_files)
        if skipped_count:
            print(
                "Skipping card files for sets not inserted in this run "
                f"({skipped_count} files skipped)."
            )
    print(f"Inserting cards from {len(card_files)} set files...")

    for file_idx, card_file in enumerate(card_files, start=1):
        set_id = card_file.stem
        with card_file.open("r", encoding="utf-8") as f:
            cards = json.load(f)
        cards = apply_limit(cards, limits.max_cards_per_set)

        for card_idx, card in enumerate(cards, start=1):
            card_id = card["id"]

            cursor.execute(
                """
                INSERT INTO cards_classification (id, name, supertype, number, rarity, set_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    supertype = VALUES(supertype),
                    number = VALUES(number),
                    rarity = VALUES(rarity),
                    set_id = VALUES(set_id)
                """,
                (
                    card_id,
                    card.get("name"),
                    card.get("supertype"),
                    card.get("number"),
                    card.get("rarity"),
                    set_id,
                ),
            )

            for subtype in card.get("subtypes", []):
                cursor.execute(
                    """
                    INSERT INTO cards_subtypes (card_id, subtype)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE subtype = VALUES(subtype)
                    """,
                    (card_id, subtype),
                )

            for type_ in card.get("types", []):
                cursor.execute(
                    """
                    INSERT INTO cards_types (card_id, type)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE type = VALUES(type)
                    """,
                    (card_id, type_),
                )

            cursor.execute(
                """
                INSERT INTO cards_gameplay (
                    card_id, hp, evolves_from, abilities, attacks,
                    weaknesses, retreat_cost, converted_retreat_cost, legality
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    hp = VALUES(hp),
                    evolves_from = VALUES(evolves_from),
                    abilities = VALUES(abilities),
                    attacks = VALUES(attacks),
                    weaknesses = VALUES(weaknesses),
                    retreat_cost = VALUES(retreat_cost),
                    converted_retreat_cost = VALUES(converted_retreat_cost),
                    legality = VALUES(legality)
                """,
                (
                    card_id,
                    card.get("hp"),
                    card.get("evolvesFrom"),
                    safe_json(card.get("abilities")),
                    safe_json(card.get("attacks")),
                    safe_json(card.get("weaknesses")),
                    safe_json(card.get("retreatCost")),
                    card.get("convertedRetreatCost"),
                    safe_json(card.get("legalities")),
                ),
            )

            cursor.execute(
                """
                INSERT INTO cards_lore (
                    card_id, flavor_text, national_pokedex_numbers, level
                )
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    flavor_text = VALUES(flavor_text),
                    national_pokedex_numbers = VALUES(national_pokedex_numbers),
                    level = VALUES(level)
                """,
                (
                    card_id,
                    card.get("flavorText"),
                    safe_json(card.get("nationalPokedexNumbers")),
                    card.get("level"),
                ),
            )

            images = card.get("images", {})
            small_image = None
            large_image = None
            if "small" in images:
                small_image = download_image(
                    url=images["small"],
                    filename=f"{card_id}_small.png",
                    images_dir=images_dir,
                    timeout=timeout,
                )
            if "large" in images:
                large_image = download_image(
                    url=images["large"],
                    filename=f"{card_id}_large.png",
                    images_dir=images_dir,
                    timeout=timeout,
                )

            cursor.execute(
                """
                INSERT INTO cards_art (card_id, artist, small_image, large_image)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    artist = VALUES(artist),
                    small_image = VALUES(small_image),
                    large_image = VALUES(large_image)
                """,
                (
                    card_id,
                    card.get("artist"),
                    small_image,
                    large_image,
                ),
            )
            conn.commit()

            if card_idx % 100 == 0 or card_idx == len(cards):
                print(
                    f"  [cards] file {file_idx}/{len(card_files)} "
                    f"({set_id}) - {card_idx}/{len(cards)}"
                )


def insert_decks(
    cursor,
    conn,
    *,
    dataset_root: Path,
    limits: Limits,
) -> None:
    decks_dir = dataset_root / "decks" / "en"
    ensure_exists(decks_dir, "Decks directory")
    deck_files = sorted(p for p in decks_dir.iterdir() if p.suffix == ".json")
    deck_files = apply_limit(deck_files, limits.max_deck_files)
    print(f"Inserting decks from {len(deck_files)} set files...")

    for file_idx, deck_file in enumerate(deck_files, start=1):
        with deck_file.open("r", encoding="utf-8") as f:
            decks = json.load(f)
        decks = apply_limit(decks, limits.max_decks_per_file)

        for deck_idx, deck in enumerate(decks, start=1):
            deck_id = deck["id"]
            cursor.execute(
                """
                INSERT INTO decks (id, name, types)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    types = VALUES(types)
                """,
                (
                    deck_id,
                    deck.get("name"),
                    safe_json(deck.get("types")),
                ),
            )

            for card_entry in deck.get("cards", []):
                cursor.execute(
                    """
                    INSERT INTO deck_cards (deck_id, card_id, count)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE count = VALUES(count)
                    """,
                    (
                        deck_id,
                        card_entry.get("id"),
                        card_entry.get("count"),
                    ),
                )
            conn.commit()

            if deck_idx % 50 == 0 or deck_idx == len(decks):
                print(
                    f"  [decks] file {file_idx}/{len(deck_files)} "
                    f"- {deck_idx}/{len(decks)}"
                )


def main() -> None:
    args = parse_args()
    dataset_root = Path(args.dataset_root).expanduser().resolve()
    images_dir = Path(args.images_dir).expanduser().resolve()
    ensure_exists(dataset_root, "Dataset root")

    limits = Limits(
        max_sets=max(0, args.max_sets),
        max_card_files=max(0, args.max_card_files),
        max_cards_per_set=max(0, args.max_cards_per_set),
        max_deck_files=max(0, args.max_deck_files),
        max_decks_per_file=max(0, args.max_decks_per_file),
    )

    print("Starting DB population with configuration:")
    print(f"  dataset_root: {dataset_root}")
    print(f"  images_dir:   {images_dir}")
    print(f"  db:           {args.db_user}@{args.db_host}/{args.db_name}")

    conn = connect_db(args)
    cursor = conn.cursor()
    try:
        inserted_set_ids = insert_sets(
            cursor,
            conn,
            dataset_root=dataset_root,
            images_dir=images_dir,
            timeout=args.request_timeout,
            limits=limits,
        )
        insert_cards(
            cursor,
            conn,
            dataset_root=dataset_root,
            images_dir=images_dir,
            timeout=args.request_timeout,
            limits=limits,
            allowed_set_ids=inserted_set_ids,
        )
        insert_decks(
            cursor,
            conn,
            dataset_root=dataset_root,
            limits=limits,
        )
    finally:
        cursor.close()
        conn.close()

    print("Population complete.")


if __name__ == "__main__":
    main()
