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


