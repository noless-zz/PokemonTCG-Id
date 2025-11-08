import os
import json
import mysql.connector
import requests
from pathlib import Path

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

download_errors = []
db_insert_errors = []

def connect_db():
    return mysql.connector.connect(**DB_CONFIG)

def download_image(url, filename):
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
                    download_image(s["images"]["symbol"], f"{s['id']}_symbol.png") if "symbol" in s["images"] else None,
                    download_image(s["images"]["logo"], f"{s['id']}_logo.png") if "logo" in s["images"] else None
                ))
                db.commit()
            except Exception as e:
                error_message = f"Error inserting set {s['id']}: {str(e)}"
                print(error_message)
                db_insert_errors.append(error_message)
            print(f"Processed set {i}/{len(sets)}: {s['name']}")
