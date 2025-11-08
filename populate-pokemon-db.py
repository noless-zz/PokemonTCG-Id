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
