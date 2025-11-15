import os
import argparse
import cv2
import numpy as np
from PIL import Image
import time
import pickle
import multiprocessing
import sys
import gc
import atexit
import shutil
import tempfile
import json
from mysql.connector import connect
import math
import random
from colormath.color_objects import LabColor
from colormath.color_diff import delta_e_cie2000
import tifffile as tiff

ALGORITHMS = {}

temporary_directory = None

def register_algorithm(name):
    def decorator(function):
        ALGORITHMS[name] = function
        return function
    return decorator

def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("image_folder", help="Path to a folder containing images.", nargs='?')
    parser.add_argument("--image_paths_file", help="Path to a file containing image paths.", nargs='?')
    parser.add_argument("--size", type=str, required=True, help="Target size, e.g., '256x256'.")
    parser.add_argument("--output", type=str, default="output.png", help="Output file name.")
    parser.add_argument("--batch", type=int, default=1000, help="Number of images to process per batch.")
    parser.add_argument("--algorithm", type=str, required=True, choices=ALGORITHMS.keys(), 
                        help="Algorithm to use for processing.")
    parser.add_argument("--config", type=str, help="Additional configuration for the algorithm (key=value pairs).")
    parser.add_argument("--timeout", type=float, default=5.0, help="Timeout to exit the process in minutes.")
    parser.add_argument("--compression", type=str, default="deflate", help="Compression algorithm in case of using the TIFF file format (deflate, lzw, none.")
    return parser.parse_args()

def process_batches(image_files, target_size, batch_size, algorithm, config, temporary_directory, output_path, compression):
    target_width, target_height = target_size
    batch_number = 1
    batch = []
    start_time = time.time()
    total_batches = (len(image_files) + batch_size - 1) // batch_size
    image_count = 0
    temporary_files = []

    print(f"Preprocessing...")
    for image_path in image_files:
        raw_image = load(image_path)
        if raw_image is None:
            print(f"Skipping {image_path}.")
            continue
        preprocessed = resize(raw_image, target_width, target_height)
        batch.append(preprocessed)

        if len(batch) >= batch_size:
            temporary_file_path = os.path.join(temporary_directory, f"batch_{batch_number}.pkl")
            with open(temporary_file_path, "wb") as file:
                pickle.dump(batch, file)
            temporary_files.append(temporary_file_path)

            update_batch_preprocessed(start_time, batch_number, total_batches)
            image_count += len(batch)
            batch_number += 1
            batch = []
            gc.collect()

    if batch:
        temporary_file_path = os.path.join(temporary_directory, f"batch_{batch_number}.pkl")
        with open(temporary_file_path, "wb") as file:
            pickle.dump(batch, file)
        temporary_files.append(temporary_file_path)
        
        update_batch_preprocessed(start_time, batch_number, total_batches)
        image_count += len(batch)
        batch = []
        gc.collect()

    print(f"Processing...")
    process_with_algorithm(algorithm, temporary_files, target_size, config, image_count, batch_size, output_path, compression)

def resize(raw_image, target_width, target_height):
    height, width = raw_image.shape[:2]
    scale_factor = min(target_width / width, target_height / height)
    new_width = int(width * scale_factor)
    new_height = int(height * scale_factor)
    resized = cv2.resize(raw_image, (new_width, new_height), interpolation=cv2.INTER_LANCZOS4)

    framed_image = np.zeros((target_height, target_width, 3), dtype=np.uint8)
    offset_x = (target_width - new_width) // 2
    offset_y = (target_height - new_height) // 2
    framed_image[offset_y:offset_y + new_height, offset_x:offset_x + new_width] = resized
    return framed_image

def apply_algorithm(algorithm, temporary_files, target_size, config, total_images, batch_size):
    target_width, target_height = target_size
    return ALGORITHMS[algorithm](temporary_files, target_width, target_height, config, total_images, batch_size)

def process_with_algorithm(algorithm, temporary_files, target_size, config, total_images, batch_size, output_path, compression):
    results = apply_algorithm(algorithm, temporary_files, target_size, config, total_images, batch_size)
    print(f"Processed {total_images} images.")
    for result in results:
        if result.path:
            output_path = result.path
        save(result.image, output_path, compression)
        print(f"Saved output as {output_path}.")

def update_batch_preprocessed(start_time, batch_number, total_batches):
    update_progress("Batch", "preprocessed", start_time, batch_number, total_batches)

def update_batch_processed(start_time, batch_number, total_batches):
    update_progress("Batch", "processed", start_time, batch_number, total_batches)

def update_pixel_row_processed(start_time, row_number, total_rows):
    update_progress("Pixel Row", "processed", start_time, row_number, total_rows)

def update_pixel_block_processed(start_time, block_number, total_blocks):
    update_progress("Pixel Block", "processed", start_time, block_number, total_blocks)

def update_image_processed(start_time, image_number, total_images):
    update_progress("Image", "processed", start_time, image_number, total_images)

def update_progress(group_label, stage_label, start_time, batch_number, total_batches):
    elapsed_time = time.time() - start_time
    average_group_time = elapsed_time / batch_number
    remaining_groups = total_batches - batch_number
    estimated_time_remaining = average_group_time * remaining_groups
    print(f"{group_label} {batch_number}/{total_batches} {stage_label}. "
          f"Average time per {group_label}: {average_group_time:.2f}s. "
          f"Estimated time remaining: {(estimated_time_remaining / 60):.2f}m.")

def load(image_path):
    try:
        with Image.open(image_path) as image:
            image = image.convert("RGB")
            return np.array(image)
    except Exception as e:
        print(f"Could not load image {image_path}. Error: {e}")
        return None

def save(result, output_path, compression):
    if isinstance(result, np.ndarray):
        _, ext = os.path.splitext(output_path)
        ext = ext.lower()
        if ext == ".tiff":
            tiff.imwrite(output_path, result, compression=compression)
        else:
            result_image = cv2.cvtColor(result, cv2.COLOR_RGB2BGR)
            cv2.imwrite(output_path, result_image)
    else:
        print("Result format not supported for saving.")

def load_image_paths(file_path):
    with open(file_path, 'r') as file:
        image_paths = file.readlines()
    return [path.strip() for path in image_paths]

def create_temporary_directory():
    global temporary_directory
    temporary_directory = tempfile.mkdtemp()
    print(f"Temporary directory {temporary_directory} created.")
    return temporary_directory

def cleanup_temporary_directory():
    global temporary_directory
    if temporary_directory:
        try:
            shutil.rmtree(temporary_directory)
            print(f"Temporary directory {temporary_directory} deleted.")
        except Exception as e:
            print(f"Failed to delete temporary directory {temporary_directory}. Error: {e}")

def parse_config(config_str):
    if not config_str:
        return {}
    config = {}
    for item in config_str.split(","):
        key, value = item.split("=")
        key = key.strip()
        value = value.strip()
        if value.isdigit():
            config[key] = int(value)
        else:
            try:
                config[key] = float(value)
            except ValueError:
                config[key] = value
    return config

def run_with_timeout(target_function, target_args, timeout_minutes):
    print(f"Running with a timeout of {timeout_minutes} minutes.")

    atexit.register(cleanup_temporary_directory)

    process = multiprocessing.Process(target=target_function, args=(target_args, create_temporary_directory()))
    process.start()

    timeout_seconds = timeout_minutes * 60
    process.join(timeout=timeout_seconds)

    if process.is_alive():
        print("Timeout, exiting.")
        process.terminate()
        process.join()
        sys.exit(1)

def patch_asscalar(a):
    return a.item()

setattr(np, "asscalar", patch_asscalar)

class AlgorithmResult:
    def __init__(self, image, path=None):
        self.image = image
        self.path = path

@register_algorithm("all")
def selected_algorithms(temporary_files, target_width, target_height, config, total_images, batch_size):
    output_prefix = config.get("prefix", "")
    to_execute = {
        "average": {"algorithm_name": "average"},
        "median": {"algorithm_name": "median"},
        "average-row": {"algorithm_name": "average_row"},
        "average-row-15": {"algorithm_name": "average_row", "config": {"thickness": 8}},
        "gradient": {"algorithm_name": "average_gradient"},
        "heatmap": {"algorithm_name": "similarity_heatmap"},
        "average-square": {"algorithm_name": "average_square", "config": {"side": 8}},
        "columns-random": {"algorithm_name": "compose_columns", "config": {"thickness": 3}},
        "columns-sorted": {"algorithm_name": "compose_columns", "config": {"thickness": 3, "sorted": "true"}},
        "columns-sorted-reverse": {"algorithm_name": "compose_columns", "config": {"thickness": 3, "sorted": "true", "reverse": "true"}},
    }

    results = []
    for key, value in to_execute.items():
        algorithm_name = value["algorithm_name"]
        if algorithm_name not in ALGORITHMS:
            print(f"Algorithm {algorithm_name} not registered. Skipping.")
            continue

        algorithm_config = value["config"] if "config" in value else {}
        print(f"Processing algorithm: {algorithm_name} with config: {algorithm_config}")
        
        algorithm_result = apply_algorithm(algorithm_name, temporary_files, (target_width, target_height), algorithm_config, total_images, batch_size)

        if len(algorithm_result) != 1:
            print(f"Algorithm {algorithm_name} returned multiple results, expected single instance. Not saving results.")
            continue

        algorithm_path = f"{output_prefix}-{key}.png"
        results.append(AlgorithmResult(algorithm_result[0].image, algorithm_path))

    return results

@register_algorithm("average")
def average_algorithm(temporary_files, target_width, target_height, config, total_images, batch_size):
    accumulator = np.zeros((target_height, target_width, 3), dtype=np.float64)

    batch_number = 1
    start_time = time.time()

    for temporary_file in temporary_files:
        with open(temporary_file, "rb") as file:
            batch = pickle.load(file)

        batch_array = np.array(batch)
        accumulator += np.sum(batch_array, axis=0)
        
        update_batch_processed(start_time, batch_number, len(temporary_files))
        batch_number += 1

    result = (accumulator / total_images).astype(np.uint8)
    return [AlgorithmResult(result)]

@register_algorithm("median")
def median_algorithm(temporary_files, target_width, target_height, config, total_images, batch_size):
    median = np.zeros((target_height, target_width, 3), dtype=np.uint8)

    start_time = time.time()

    for y in range(target_height):
        row_values = np.zeros((total_images, target_width, 3), dtype=np.uint8)

        index = 0
        for temporary_file in temporary_files:
            with open(temporary_file, "rb") as file:
                batch = pickle.load(file)

            for image in batch:
                row_values[index, :] = image[y, :]
                index += 1

        median[y, :] = np.median(row_values, axis=0)
        update_pixel_row_processed(start_time, y+1, target_height)

    return [AlgorithmResult(median)]

@register_algorithm("average_row")
def average_row_algorithm(temporary_files, target_width, target_height, config, total_images, batch_size):
    row_height = int(config.get("thickness", 1))
    accumulator = np.zeros((target_height, target_width, 3), dtype=np.float64)

    batch_number = 1
    start_time = time.time()

    for temporary_file in temporary_files:
        with open(temporary_file, "rb") as file:
            batch = pickle.load(file)

        batch_array = np.array(batch)
        for image in batch_array:
            for y in range(0, target_height, row_height):
                block = image[y:y+row_height, :]
                block_average = np.mean(block, axis=(0, 1))
                accumulator[y:y+row_height, :] += block_average

        update_batch_processed(start_time, batch_number, len(temporary_files))
        batch_number += 1

    result = (accumulator / total_images).astype(np.uint8)
    return [AlgorithmResult(result)]

@register_algorithm("average_square")
def average_square_algorithm(temporary_files, target_width, target_height, config, total_images, batch_size):
    block_size = int(config.get("side", 1))
    accumulator = np.zeros((target_height, target_width, 3), dtype=np.float64)

    batch_number = 1
    start_time = time.time()

    for temporary_file in temporary_files:
        with open(temporary_file, "rb") as file:
            batch = pickle.load(file)

        batch_array = np.array(batch)
        for image in batch_array:
            for y in range(0, target_height, block_size):
                for x in range(0, target_width, block_size):
                    block = image[y:y+block_size, x:x+block_size]
                    block_average = np.mean(block, axis=(0, 1))
                    accumulator[y:y+block_size, x:x+block_size] += block_average

        update_batch_processed(start_time, batch_number, len(temporary_files))
        batch_number += 1

    result = (accumulator / total_images).astype(np.uint8)
    return [AlgorithmResult(result)]

