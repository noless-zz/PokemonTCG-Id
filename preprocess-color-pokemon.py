import cv2
import time
import json
import mysql.connector
import numpy as np
from mysql.connector import Error

def compute_average_and_median_colors(image_path):
    image = cv2.imread(image_path)
    if image is None:
        raise ValueError(f"Unable to load image: {image_path}")

    avg_color_bgr = np.mean(image, axis=(0, 1))
    avg_color_rgb = {
        "r": round(avg_color_bgr[2], 2),
        "g": round(avg_color_bgr[1], 2),
        "b": round(avg_color_bgr[0], 2)
    }

    median_color_bgr = np.median(image, axis=(0, 1))
    median_color_rgb = {
        "r": round(median_color_bgr[2], 2),
        "g": round(median_color_bgr[1], 2),
        "b": round(median_color_bgr[0], 2)
    }

    image_hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    avg_color_hsv_vec = np.mean(image_hsv, axis=(0, 1))
    avg_color_hsv = {
        "h": round(avg_color_hsv_vec[0] * (360 / 179), 2),
        "s": round(avg_color_hsv_vec[1] / 255, 2),
        "v": round(avg_color_hsv_vec[2] / 255, 2)
    }

    median_color_hsv_vec = np.median(image_hsv, axis=(0, 1))
    median_color_hsv = {
        "h": round(median_color_hsv_vec[0] * (360 / 179), 2),
        "s": round(median_color_hsv_vec[1] / 255, 2),
        "v": round(median_color_hsv_vec[2] / 255, 2)
    }

    image_lab = cv2.cvtColor(image, cv2.COLOR_BGR2LAB)
    avg_color_lab_vec = np.mean(image_lab, axis=(0, 1))
    avg_color_lab = {
        "l": round(avg_color_lab_vec[0] / 2.55, 2),
        "a": round((avg_color_lab_vec[1] - 128) / 128, 2),
        "b": round((avg_color_lab_vec[2] - 128) / 128, 2)
    }

    median_color_lab_vec = np.median(image_lab, axis=(0, 1))
    median_color_lab = {
        "l": round(median_color_lab_vec[0] / 2.55, 2),
        "a": round((median_color_lab_vec[1] - 128) / 128, 2),
        "b": round((median_color_lab_vec[2] - 128) / 128, 2)
    }

    return avg_color_rgb, median_color_rgb, avg_color_hsv, median_color_hsv, avg_color_lab, median_color_lab

def process_images():
    connection = mysql.connector.connect(
        host="localhost",
        user="pokemon",
        password="PokeGen_92xT!4mb7",
        database="pokemon_tcg"
    )
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT c.card_id, c.small_image
        FROM cards_art c
        LEFT JOIN preprocessed_images p ON c.card_id = p.card_id
        WHERE p.card_id IS NULL
    """)
    rows = cursor.fetchall()
    total_images = len(rows)
    print(f"Found {total_images} images to process.")

    start_time = time.time()
    errors = []

    for index, row in enumerate(rows, start=1):
        card_id, image_path = row["card_id"], row["small_image"]
        try:
            avg_color_rgb, median_color_rgb, avg_color_hsv, median_color_hsv, avg_color_lab, median_color_lab = compute_average_and_median_colors(image_path)
            cursor.execute("""
                INSERT INTO preprocessed_images (
                    card_id,
                    average_color_rgb,
                    median_color_rgb,
                    average_color_hsv,
                    median_color_hsv,
                    average_color_lab,
                    median_color_lab
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (card_id, json.dumps(avg_color_rgb), json.dumps(median_color_rgb),
                  json.dumps(avg_color_hsv), json.dumps(median_color_hsv),
                  json.dumps(avg_color_lab), json.dumps(median_color_lab)))
            connection.commit()
        except Exception as e:
            errors.append((card_id, str(e)))

        elapsed_time = time.time() - start_time
        avg_time_per_image = elapsed_time / index
        remaining_time = avg_time_per_image * (total_images - index)
        print(f"Processed {index}/{total_images} images. Estimated time remaining: {remaining_time:.2f}s.", end="\r")

    print("\nProcessing complete.")
    if errors:
        print(f"\nEncountered {len(errors)} errors:")
        for card_id, error in errors:
            print(f"Card ID: {card_id}, Error: {error}")

    cursor.close()
    connection.close()

if __name__ == "__main__":
    process_images()

