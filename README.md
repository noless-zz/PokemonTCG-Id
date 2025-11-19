# PokemonTCG-Gen
🌟 Pokémon TCG Collage Generator

A high-resolution collage generator using thousands of Pokémon TCG cards.

<p align="center"> <img src="putmybannerhere" width="700"/> </p>

## Table of Contents 📑
- [Overview](#overview-)
- [Project Structure](#project-structure-)
- [Requirements](#requirements-)
- [Installation](#installation-)
- [Dataset Download & Setup](#dataset-download--setup-)
- [Color Preprocessing](#color-preprocessing-)
- [Generating The Collage](#generating-the-collage-)
- [Available Algorithms](#available-algorithms-)
- [Collage Parameters](#collage-parameters-)
- [TIFF Support](#tiff-support-)
- [Images](#images-)
- [Performance Notes](#performance-notes-)
- [Troubleshooting](#troubleshooting-)


## Overview 🧩

This project generates ultra-high-resolution image mosaics using Pokémon TCG card art.
It loads an input image, splits it into blocks, and finds the closest matching Pokémon card for each block using LAB color distance, HSV weighting, saturation awareness, and optional rotation/displacement.

Outputs can reach 7000×7000 or more and are suitable for large prints and posters.

## Project Structure 📁
```bash
PokemonTCG-Gen/
│
├── populate-pokemon-db.py
├── preprocess-color-pokemon-db.py
├── aggregate-pokemon-images.py
│
├── images/
├── input_test/
└── output_test/
```


## Requirements ✅

- Python 3.10+

- MySQL 8+

- OpenCV

- NumPy

- TiffFile

- MySQL Connector

- Pillow

- ColorMath

## Installation 🛠
- Clone
```bash
git clone https://github.com/mmitzy/PokemonTCG-Gen.git
cd PokemonTCG-Gen
```

- Create venv
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Database Download & Setup 📥

Run:

```bash
python populate-pokemon-db.py
```

Creates all tables + downloads all official Pokémon TCG images + fills DB.

## Color Preprocessing 🎨

Run:

```bash
python preprocess-color-pokemon-db.py
```

This computes average/median colors in RGB, HSV, LAB and stores them in MySQL.

## Generating The Collage 🧱

Example (7K resolution):

```bash
python aggregate-pokemon-images.py input_test --algorithm collage --size 7000x7000 --timeout 999 --output output_test/final.png --config "width=20,height=20,candidates=2000,prefilter=30,tolerance=8,saturation=0.4"
```

## Available Algorithms 🧠
-collage: The main Pokémon mosaic engine
-average: Pixel averages
-median: Pixel medians
-average_row: Row collapsing
-average_square: Block averages
-similarity_heatmap: Card similarity map
-compose_columns: Column-stacked composition
-test: Debug mode

## Collage Algorithm Parameters ⚙️

```bash
--config "width=20,height=20,candidates=2000,prefilter=30,tolerance=8,saturation=0.4,rotation=15,displacement=1"
```

Parameter meanings:

-width,height: Tile size
-candidates: SQL candidate depth
-prefilter: LAB quick filter
-tolerance: Max CIEDE2000 distance
-saturation: Weight for color saturation
-rotation: Random angle per tile
-displacement: Pixel jitter
-repeat: Allow card reuse

## TIFF Support 🖼

TIFF is recommended for huge outputs (≥4000×4000):

✔ Supports massive resolutions
✔ Lossless
✔ Photoshop/GIMP compatible
✔ deflate and lzw compression supported

Example:
```bash
--output output_test/final.tiff --compression deflate
```

## Images 📸
1. Fast Test – 512×512
Input  Output
<img src="input_test/example_input.jpg" width="250"/>	<img src="output_test/example_output_512.png" width="250"/>
2. Medium – 2048×2048
<img src="output_test/example_output_2048.png" width="350"/>
3. High – 4096×4096
<img src="output_test/example_output_4096.png" width="450"/>
4. Ultra High – 7000×7000
<img src="output_test/example_output_7000.png" width="550"/>
5. Sky Correction
Before  After
<img src="output_test/sky_bad.png" width="300"/>	<img src="output_test/sky_good.png" width="300"/>

## Performance Notes 🚀

Large collages (7000×7000) can take 8-24 hours

Use --timeout 9999 to avoid automatic termination

Avoid system sleep mode

Increase candidates, prefilter, saturation for better color fidelity

## Troubleshooting 🩹
❗ No candidates found

Increase:
```bash
prefilter=40
candidates=2500
tolerance=10
```

❗ Sky turns red

Use:
```bash
saturation=0.4 or 0.5
prefilter=30+
```

❗ Script stops

Add:
```bash
--timeout 999
```
