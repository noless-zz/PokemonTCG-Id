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

@register_algorithm("average_gradient")
def average_gradient_algorithm(temporary_files, target_width, target_height, config, total_images, batch_size):
    gradient_dx_sum = np.zeros((target_height, target_width, 3), dtype=np.float64)
    gradient_dy_sum = np.zeros((target_height, target_width, 3), dtype=np.float64)
    image_count = 0

    batch_number = 1
    start_time = time.time()

    for temporary_file in temporary_files:
        with open(temporary_file, "rb") as file:
            batch = pickle.load(file)

        batch_array = np.array(batch)

        for image in batch_array:
            dx = np.diff(image, axis=1, append=image[:, -1:])
            dy = np.diff(image, axis=0, append=image[-1:, :])

            gradient_dx_sum += dx
            gradient_dy_sum += dy
            image_count += 1

        update_batch_processed(start_time, batch_number, len(temporary_files))
        batch_number += 1

    average_dx = gradient_dx_sum / image_count
    average_dy = gradient_dy_sum / image_count
    result = np.sqrt(np.square(average_dx) + np.square(average_dy)).astype(np.uint8)
    return [AlgorithmResult(result)]

@register_algorithm("similarity_heatmap")
def similarity_heatmap_algorithm(temporary_files, target_width, target_height, config, total_images, batch_size):
    sum_pixels = np.zeros((target_height, target_width, 3), dtype=np.float64)
    sum_squared_pixels = np.zeros((target_height, target_width, 3), dtype=np.float64)

    batch_number = 1
    start_time = time.time()

    for temporary_file in temporary_files:
        with open(temporary_file, "rb") as file:
            batch = pickle.load(file)

        for image in batch:
            sum_pixels += image
            sum_squared_pixels += image.astype(np.float64)**2

        update_batch_processed(start_time, batch_number, len(temporary_files))
        batch_number += 1

    mean_pixels = sum_pixels / total_images
    mean_squared_pixels = sum_squared_pixels / total_images
    variance_map = mean_squared_pixels - mean_pixels**2

    normalized_heatmap = np.zeros_like(variance_map, dtype=np.uint8)
    max_variance = np.max(variance_map)

    if max_variance > 0:
        normalized_heatmap = (variance_map / max_variance * 255).astype(np.uint8)

    grayscale_heatmap = np.mean(normalized_heatmap, axis=2).astype(np.uint8)
    red_channel = grayscale_heatmap
    green_channel = 255 - grayscale_heatmap
    blue_channel = np.zeros_like(grayscale_heatmap)
    result = np.stack([red_channel, green_channel, blue_channel], axis=2)
    return [AlgorithmResult(result)]

@register_algorithm("compose_columns")
def compose_columns_algorithm(temporary_files, target_width, target_height, config, total_images, batch_size):
    column_width = int(config.get("thickness", 1))
    sort = config.get("sorted", "false").lower() == "true"
    reverse = config.get("reverse", "false").lower() == "true"
    required_columns = target_width // column_width

    if total_images < required_columns:
        raise ValueError(
            f"Insufficient images for the composition: {total_images} images provided, "
            f"but {required_columns} are required for column thickness {column_width}."
        )
    elif total_images > required_columns:
        print(
            f"Input contains more images than required for a composition with "
            f"column thickness: {column_width} and total width: {target_width}, "
            f"{total_images - required_columns} out of {total_images} will not be used."
        )

    sorted_images = []
    if sort:
        luminosity_metrics = []
        for temporary_file in temporary_files:
            with open(temporary_file, "rb") as file:
                batch = pickle.load(file)
                for i, image in enumerate(batch):
                    luminosity = np.mean(0.299 * image[:, :, 0] + 0.587 * image[:, :, 1] + 0.114 * image[:, :, 2])
                    luminosity_metrics.append((luminosity, temporary_file, i))
        sorted_images = sorted(luminosity_metrics, key=lambda x: x[0], reverse=reverse)
    else:
        for temporary_file in temporary_files:
            with open(temporary_file, "rb") as file:
                batch = pickle.load(file)
                for i in range(len(batch)):
                    sorted_images.append((None, temporary_file, i))

    result_image = np.zeros((target_height, target_width, 3), dtype=np.uint8)

    start_time = time.time()
    column_start = 0

    for _, temporary_file, index in sorted_images[:required_columns]:
        if column_start >= target_width:
            break

        with open(temporary_file, "rb") as file:
            batch = pickle.load(file)
            image = batch[index]

        column = image[:, column_start:column_start + column_width, :]
        result_image[:, column_start:column_start+column_width, :] = column

        column_start += column_width
        update_image_processed(start_time, index+1, required_columns)

    return [AlgorithmResult(result_image)]

def rotate(image, angle):
    height, width = image.shape[:2]
    new_width = int(np.ceil(abs(width * np.cos(np.radians(angle))) + abs(height * np.sin(np.radians(angle)))))
    new_height = int(np.ceil(abs(height * np.cos(np.radians(angle))) + abs(width * np.sin(np.radians(angle)))))
    canvas = np.zeros((new_height, new_width, 3), dtype=np.uint8)
    center_canvas = (new_width // 2, new_height // 2)
    center_image = (width // 2, height // 2)
    start_y = max(0, center_canvas[1] - center_image[1])
    end_y = min(new_height, start_y + height)
    start_x = max(0, center_canvas[0] - center_image[0])
    end_x = min(new_width, start_x + width)
    image_start_y = max(0, center_image[1] - center_canvas[1])
    image_end_y = image_start_y + (end_y - start_y)
    image_start_x = max(0, center_image[0] - center_canvas[0])
    image_end_x = image_start_x + (end_x - start_x)
    canvas[start_y:end_y, start_x:end_x] = image[image_start_y:image_end_y, image_start_x:image_end_x]
    rotation_matrix = cv2.getRotationMatrix2D(center_canvas, angle, 1.0)
    rotated_image = cv2.warpAffine(canvas, rotation_matrix, (new_width, new_height))
    return rotated_image

@register_algorithm("collage")
def collage_algorithm(temporary_files, target_width, target_height, config, total_images, batch_size):
    reference_image_error_message = "The collage algorithm expects a single reference image as input"
    if len(temporary_files) != 1:
        raise ValueError(reference_image_error_message)
    with open(temporary_files[0], "rb") as file:
        batch = pickle.load(file)
        if len(batch) != 1:
            raise ValueError(reference_image_error_message)
        reference_image = batch[0]

    block_width = int(config.get("width", 50))
    block_height = int(config.get("height", 50))
    repeat = config.get("repeat", "false").lower() == "true"
    candidates = int(config.get("candidates", 500))
    prefilter_max_color_distance = config.get("prefilter", 30.0)
    precise_max_color_distance = config.get("tolerance", 10.0)
    saturation_relevance = config.get("saturation", 0.0)
    energies_config = config.get("energies", "yes").lower()
    energies_allowed = energies_config == "yes"
    only_energies = energies_config == "only"
    random_seed = config.get("seed", None)
    rotation_range = float(config.get("rotation", 15.0))
    displacement_range = int(config.get("displacement", 1))

    connection = connect(
        host="localhost",
        user="root",
        password="",
        database="pokemon_tcg"
    )
    cursor = connection.cursor(dictionary=True)
    
    collage = np.zeros((target_height, target_width, 3), dtype=np.uint8)
    used_images = set()

    block_coordinates = [
        (x, y)
        for y in range(0, target_height, block_height)
        for x in range(0, target_width, block_width)
    ]
    if random_seed is not None:
        random.seed(random_seed)
    random.shuffle(block_coordinates)

    start_time = time.time()
    total_blocks = math.ceil(target_width / block_width) * math.ceil(target_height / block_height)
    block_number = 1

    for x, y in block_coordinates:
        block_end_y = min(y + block_height, target_height)
        block_end_x = min(x + block_width, target_width)
        block = reference_image[y:block_end_y, x:block_end_x]

        median_block_color = np.median(block, axis=(0, 1))
        block_lab = cv2.cvtColor(np.uint8([[median_block_color]]), cv2.COLOR_RGB2LAB)[0][0]
        l = float(round(block_lab[0] / 2.55, 2))
        a = float(round((block_lab[1] - 128) / 128, 2))
        b = float(round((block_lab[2] - 128) / 128, 2))

        query = """
            SELECT
                pi.card_id as card_id,
                ca.small_image as image_path,
                pi.median_color_lab->>'$.l' as lab_l,
                pi.median_color_lab->>'$.a' as lab_a,
                pi.median_color_lab->>'$.b' as lab_b,
                pi.median_color_hsv->>'$.s' as saturation
            FROM preprocessed_images pi
            JOIN cards_art ca ON pi.card_id = ca.card_id
            JOIN cards_classification cc ON cc.id = ca.card_id
        """

        conditions = []
        conditions.append("""
            ABS(median_color_lab->>'$.l' - %s) + 
            ABS(median_color_lab->>'$.a' - %s) + 
            ABS(median_color_lab->>'$.b' - %s) <= %s
        """)
        if only_energies:
            conditions.append("cc.supertype = 'Energy'")
        elif not energies_allowed:
            conditions.append("cc.supertype <> 'Energy'")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += """
            ORDER BY
                ABS(median_color_lab->>'$.l' - %s) + 
                ABS(median_color_lab->>'$.a' - %s) + 
                ABS(median_color_lab->>'$.b' - %s)
            LIMIT %s;
        """

        query_params = [l, a, b, prefilter_max_color_distance, l, a, b, candidates]
        cursor.execute(query, query_params)
        candidate_images = cursor.fetchall()
        precise_candidate_images = []
        candidate_image_distances = []
        adaptive_max_color_distance = precise_max_color_distance
        while len(precise_candidate_images) < 3 and adaptive_max_color_distance <= 1000:
            for candidate_image in candidate_images:
                candidate_lab = LabColor(
                    lab_l=float(candidate_image["lab_l"]) * 100,
                    lab_a=float(candidate_image["lab_a"]) * 128,
                    lab_b=float(candidate_image["lab_b"]) * 128
                )
                block_lab_obj = LabColor(
                    lab_l=l * 100,
                    lab_a=a * 128,
                    lab_b=b * 128
                )
                block_hsv = cv2.cvtColor(np.uint8([[median_block_color]]), cv2.COLOR_RGB2HSV)[0][0]
                block_saturation = block_hsv[1] / 255.0

                distance = max(0, delta_e_cie2000(block_lab_obj, candidate_lab) - float(saturation_relevance) * (float(candidate_image["saturation"]) - block_saturation))
                candidate_image_distances.append((candidate_image["card_id"], distance))

                if distance < adaptive_max_color_distance:
                    precise_candidate_images.append((candidate_image["card_id"], candidate_image["image_path"], distance))
            precise_candidate_images.sort(key=lambda x: x[2])
            adaptive_max_color_distance *= 1.5

        if len(precise_candidate_images) == 0:
            candidate_image_distances.sort(key=lambda x: x[1])
            error_message = f"Failed to find image for block at position ({y}, {x})."
            print(f"{error_message} These are all prefiltered candidates and their CIE2000 distances: {candidate_image_distances}")
            raise ValueError(error_message)

        selected_image = None
        if repeat:
            selected_image = precise_candidate_images[0]
        else:
            for candidate_image in precise_candidate_images:
                card_id, image_path, _ = candidate_image
                if card_id not in used_images:
                    selected_image = candidate_image
                    used_images.add(card_id)
                    break
            if not selected_image:
                selected_image = random.choice(precise_candidate_images)

        _, image_path, _ = selected_image
        image = cv2.imread(image_path)
        if image is None:
            raise FileNotFoundError(f"Failed to load image {image_path}.")

        resized_image = cv2.resize(image, (block_width, block_height))

        angle = random.uniform(-rotation_range, rotation_range)
        rotated_image = rotate(resized_image, angle)

        dx = random.randint(-displacement_range, displacement_range)
        dy = random.randint(-displacement_range, displacement_range)

        insert_x = x + dx
        insert_y = y + dy

        start_x = max(0, insert_x)
        start_y = max(0, insert_y)

        end_x = min(target_width, insert_x + rotated_image.shape[1])
        end_y = min(target_height, insert_y + rotated_image.shape[0])

        crop_x1 = max(0, -insert_x)
        crop_y1 = max(0, -insert_y)
        crop_x2 = crop_x1 + (end_x - start_x)
        crop_y2 = crop_y1 + (end_y - start_y)

        transformed_image = rotated_image[crop_y1:crop_y2, crop_x1:crop_x2]
        collage_section = collage[start_y:end_y, start_x:end_x]

        collage[start_y:end_y, start_x:end_x] = np.where(
            transformed_image > 0,
            transformed_image,
            collage_section
        )

        update_pixel_block_processed(start_time, block_number, total_blocks)
        block_number += 1

    cursor.close()
    connection.close()
    
    collage = cv2.cvtColor(collage, cv2.COLOR_BGR2RGB)
    return [AlgorithmResult(collage)]
