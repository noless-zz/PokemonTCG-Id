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

    # (HSV & Lab will be added in later commits)
    return avg_color_rgb, median_color_rgb
