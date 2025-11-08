import os
import json
import mysql.connector
import requests
from pathlib import Path

# ---------- Paths & DB config ----------
JSON_ROOT = "../Databases/Pokemon/pokemon-tcg-data-master"
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "pokemon_tcg",
    "charset": "utf8mb4"
}
IMAGES_DIR = "../Databases/Pokemon/Newbie/images"
Path(IMAGES_DIR).mkdir(exist_ok=True)

# ---------- Error collectors ----------
download_errors = []
db_insert_errors = []

# ---------- Helpers ----------
def connect_db():
    return mysql.connector.connect(**DB_CONFIG)

def download_image(url, filename):
    """
    Used here for set images (symbol/logo). Card art downloads are NOT included at stage 6.
    """
    filepath = os.path.join(IMAGES_DIR, filename)
    filepath = os.path.abspath(filepath).replace("\\", "/")
    if not os.path.exists(filepath):
        print(f"Downloading image from {url}...")
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                print(f"Image saved to {filepath}")
            else:
                error_message = f"Failed to download image from {url}. HTTP status: {response.status_code}"
                print(error_message)
                download_errors.append(error_message)
        except Exception as e:
            error_message = f"Error downloading image from {url}: {str(e)}"
            print(error_message)
            download_errors.append(error_message)
    else:
        print(f"Image already exists at {filepath}")
    return filepath

def get_json_value(value):
    if value is None or (isinstance(value, (list, dict)) and not value):
        return None
    return json.dumps(value)

def insert_data():
    db = connect_db()
    cursor = db.cursor()

    print("Inserting sets...")
    sets_file = os.path.join(JSON_ROOT, "sets/en.json")
    with open(sets_file, "r", encoding="utf-8") as f:
        sets = json.load(f)
        for i, s in enumerate(sets, start=1):
            try:
                cursor.execute("""
                    INSERT INTO sets (id, name, series, printed_total, total, legality, ptcgo_code, release_date, updated_at, symbol_image, logo_image)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE name=VALUES(name), updated_at=VALUES(updated_at)
                """, (
                    s["id"],
                    s["name"],
                    s["series"],
                    s.get("printedTotal"),
                    s.get("total"),
                    get_json_value(s.get("legalities")),
                    s.get("ptcgoCode"),
                    s.get("releaseDate"),
                    s.get("updatedAt"),
                    download_image(s["images"]["symbol"], f"{s['id']}_symbol.png") if "symbol" in s.get("images", {}) else None,
                    download_image(s["images"]["logo"], f"{s['id']}_logo.png") if "logo" in s.get("images", {}) else None
                ))
                db.commit()
            except Exception as e:
                error_message = f"Error inserting set {s.get('id')}: {str(e)}"
                print(error_message)
                db_insert_errors.append(error_message)
            print(f"Processed set {i}/{len(sets)}: {s.get('name')}")

    print("Inserting cards...")
    cards_dir = os.path.join(JSON_ROOT, "cards/en")
    card_files = os.listdir(cards_dir)
    for file_idx, card_file in enumerate(card_files, start=1):
        set_id = os.path.splitext(card_file)[0]
        with open(os.path.join(cards_dir, card_file), "r", encoding="utf-8") as f:
            cards = json.load(f)
            for card_idx, card in enumerate(cards, start=1):
                try:
                    cursor.execute("""
                        INSERT INTO cards_classification (id, name, supertype, number, rarity, set_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE name=VALUES(name)
                    """, (
                        card["id"],
                        card["name"],
                        card.get("supertype"),
                        card.get("number"),
                        card.get("rarity"),
                        set_id
                    ))

                    for subtype in card.get("subtypes", []):
                        cursor.execute("""
                            INSERT INTO cards_subtypes (card_id, subtype)
                            VALUES (%s, %s)
                            ON DUPLICATE KEY UPDATE subtype=VALUES(subtype)
                        """, (card["id"], subtype))

                    for type_ in card.get("types", []):
                        cursor.execute("""
                            INSERT INTO cards_types (card_id, type)
                            VALUES (%s, %s)
                            ON DUPLICATE KEY UPDATE type=VALUES(type)
                        """, (card["id"], type_))


                    cursor.execute("""
                        INSERT INTO cards_gameplay (card_id, hp, evolves_from, abilities, attacks, weaknesses, retreat_cost, converted_retreat_cost, legality)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE hp=VALUES(hp)
                    """, (
                        card["id"],
                        card.get("hp"),
                        card.get("evolvesFrom"),
                        get_json_value(card.get("abilities")),
                        get_json_value(card.get("attacks")),
                        get_json_value(card.get("weaknesses")),
                        get_json_value(card.get("retreatCost")),
                        card.get("convertedRetreatCost"),
                        get_json_value(card.get("legalities"))
                    ))

                    cursor.execute("""
                        INSERT INTO cards_lore (card_id, flavor_text, national_pokedex_numbers, level)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE flavor_text=VALUES(flavor_text)
                    """, (
                        card["id"],
                        card.get("flavorText"),
                        get_json_value(card.get("nationalPokedexNumbers")),
                        card.get("level")
                    ))

                    db.commit()

                except Exception as e:
                    error_message = f"Error inserting card {card.get('id')} from set {set_id}: {str(e)}"
                    print(error_message)
                    db_insert_errors.append(error_message)

            print(f"Processed {card_idx} cards in set {set_id} ({file_idx}/{len(card_files)})")

    print("Inserting decks...")
    decks_dir = os.path.join(JSON_ROOT, "decks/en")
    for file_idx, deck_file in enumerate(os.listdir(decks_dir), start=1):
        set_id = os.path.splitext(deck_file)[0]
        with open(os.path.join(decks_dir, deck_file), "r", encoding="utf-8") as f:
            decks = json.load(f)
            for deck in decks:
                try:
                    cursor.execute("""
                        INSERT INTO decks (id, name, types)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE name=VALUES(name), types=VALUES(types)
                    """, (
                        deck["id"],
                        deck["name"],
                        get_json_value(deck.get("types"))
                    ))
                    for card_entry in deck.get("cards", []):
                        cursor.execute("""
                            INSERT INTO deck_cards (deck_id, card_id, count)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE count=VALUES(count)
                        """, (
                            deck["id"],
                            card_entry["id"],
                            card_entry["count"]
                        ))
                    db.commit()
                except Exception as e:
                    error_message = f"Error inserting deck {deck['id']} from set {set_id}: {str(e)}"
                    print(error_message)
                    db_insert_errors.append(error_message)
                print(f"Processed deck {deck['id']} in set {set_id} ({deck_file}/{len(os.listdir(decks_dir))})")

    cursor.close()
    db.close()

def print_summary():
    if download_errors:
        print("\nDownload Errors:")
        for error in download_errors:
            print(f"- {error}")
    else:
        print("\nNo download errors.")

    if db_insert_errors:
        print("\nDatabase Insertion Errors:")
        for error in db_insert_errors:
            print(f"- {error}")
    else:
        print("\nNo database insertion errors.")

if __name__ == "__main__":
    try:
        insert_data()
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    finally:
        print_summary()


