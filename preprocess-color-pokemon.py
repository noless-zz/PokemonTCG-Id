#!/usr/bin/env python3
"""Compute and store color statistics for card images in preprocessed_images."""

from __future__ import annotations

import argparse
import json
import os
import time

import cv2
import mysql.connector
import numpy as np


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preprocess card images and store average/median RGB/HSV/LAB values."
    )
    parser.add_argument("--db-host", default=os.environ.get("DB_HOST", "localhost"))
    parser.add_argument("--db-user", default=os.environ.get("DB_USER", "pokemon"))
    parser.add_argument("--db-password", default=os.environ.get("DB_PASSWORD"))
    parser.add_argument("--db-name", default=os.environ.get("DB_NAME", "pokemon_tcg"))
    parser.add_argument("--db-charset", default=os.environ.get("DB_CHARSET", "utf8mb4"))
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.environ.get("PREPROCESS_LIMIT", "0")),
        help="Max images to process (0 = all).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute even if preprocessed_images row exists.",
    )
    return parser.parse_args()


def compute_average_and_median_colors(image_path: str):
    image = cv2.imread(image_path)
    if image is None:
        raise ValueError(f"Unable to load image: {image_path}")

    avg_color_bgr = np.mean(image, axis=(0, 1))
    avg_color_rgb = {
        "r": round(float(avg_color_bgr[2]), 2),
        "g": round(float(avg_color_bgr[1]), 2),
        "b": round(float(avg_color_bgr[0]), 2),
    }

    median_color_bgr = np.median(image, axis=(0, 1))
    median_color_rgb = {
        "r": round(float(median_color_bgr[2]), 2),
        "g": round(float(median_color_bgr[1]), 2),
        "b": round(float(median_color_bgr[0]), 2),
    }

    image_hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    avg_hsv = np.mean(image_hsv, axis=(0, 1))
    median_hsv = np.median(image_hsv, axis=(0, 1))
    avg_color_hsv = {
        "h": round(float(avg_hsv[0] * (360 / 179)), 2),
        "s": round(float(avg_hsv[1] / 255), 2),
        "v": round(float(avg_hsv[2] / 255), 2),
    }
    median_color_hsv = {
        "h": round(float(median_hsv[0] * (360 / 179)), 2),
        "s": round(float(median_hsv[1] / 255), 2),
        "v": round(float(median_hsv[2] / 255), 2),
    }

    image_lab = cv2.cvtColor(image, cv2.COLOR_BGR2LAB)
    avg_lab = np.mean(image_lab, axis=(0, 1))
    median_lab = np.median(image_lab, axis=(0, 1))
    avg_color_lab = {
        "l": round(float(avg_lab[0] / 2.55), 2),
        "a": round(float((avg_lab[1] - 128) / 128), 2),
        "b": round(float((avg_lab[2] - 128) / 128), 2),
    }
    median_color_lab = {
        "l": round(float(median_lab[0] / 2.55), 2),
        "a": round(float((median_lab[1] - 128) / 128), 2),
        "b": round(float((median_lab[2] - 128) / 128), 2),
    }

    return (
        avg_color_rgb,
        median_color_rgb,
        avg_color_hsv,
        median_color_hsv,
        avg_color_lab,
        median_color_lab,
    )


def main() -> None:
    args = parse_args()
    if not args.db_password:
        raise ValueError("Missing DB password. Set DB_PASSWORD or pass --db-password.")

    connection = mysql.connector.connect(
        host=args.db_host,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        charset=args.db_charset,
    )
    cursor = connection.cursor(dictionary=True)

    if args.force:
        query = "SELECT card_id, small_image FROM cards_art WHERE small_image IS NOT NULL"
    else:
        query = """
            SELECT c.card_id, c.small_image
            FROM cards_art c
            LEFT JOIN preprocessed_images p ON c.card_id = p.card_id
            WHERE c.small_image IS NOT NULL
              AND p.card_id IS NULL
        """

    cursor.execute(query)
    rows = cursor.fetchall()
    if args.limit > 0:
        rows = rows[: args.limit]

    total_images = len(rows)
    print(f"Found {total_images} images to process.")

    start_time = time.time()
    errors: list[tuple[str, str]] = []

    for index, row in enumerate(rows, start=1):
        card_id = row["card_id"]
        image_path = row["small_image"]
        try:
            (
                avg_color_rgb,
                median_color_rgb,
                avg_color_hsv,
                median_color_hsv,
                avg_color_lab,
                median_color_lab,
            ) = compute_average_and_median_colors(image_path)

            cursor.execute(
                """
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
                ON DUPLICATE KEY UPDATE
                    average_color_rgb = VALUES(average_color_rgb),
                    median_color_rgb = VALUES(median_color_rgb),
                    average_color_hsv = VALUES(average_color_hsv),
                    median_color_hsv = VALUES(median_color_hsv),
                    average_color_lab = VALUES(average_color_lab),
                    median_color_lab = VALUES(median_color_lab)
                """,
                (
                    card_id,
                    json.dumps(avg_color_rgb),
                    json.dumps(median_color_rgb),
                    json.dumps(avg_color_hsv),
                    json.dumps(median_color_hsv),
                    json.dumps(avg_color_lab),
                    json.dumps(median_color_lab),
                ),
            )
            connection.commit()
        except Exception as exc:
            errors.append((card_id, str(exc)))

        elapsed = time.time() - start_time
        avg_per = elapsed / index
        remaining = avg_per * (total_images - index)
        print(
            f"Processed {index}/{total_images}. ETA: {remaining:.2f}s.",
            end="\r",
        )

    print("\nProcessing complete.")
    if errors:
        print(f"Encountered {len(errors)} errors:")
        for card_id, error in errors:
            print(f"- {card_id}: {error}")

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
