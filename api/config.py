"""Central configuration for the PokemonTCG scanner API.

All settings can be overridden via environment variables so the application
works in both local-dev (defaults below) and containerised/CI environments.
"""

import os

# ── Database ──────────────────────────────────────────────────────────────────
DB_CONFIG: dict = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "user": os.environ.get("DB_USER", "pokemon"),
    "password": os.environ["DB_PASSWORD"],   # required – set DB_PASSWORD env var
    "database": os.environ.get("DB_NAME", "pokemon_tcg"),
    "charset": "utf8mb4",
}

# ── Card identifier ───────────────────────────────────────────────────────────
# Hamming distance below which a phash match is considered "good".
PHASH_GOOD_THRESHOLD: int = int(os.environ.get("PHASH_GOOD_THRESHOLD", "12"))
# Top-N candidates returned per scan.
TOP_K_MATCHES: int = int(os.environ.get("TOP_K_MATCHES", "5"))
# Minimum Laplacian variance for a frame to be considered sharp enough.
BLUR_VARIANCE_THRESHOLD: float = float(
    os.environ.get("BLUR_VARIANCE_THRESHOLD", "80.0")
)
# Minimum fraction of the frame area a detected card contour must occupy.
MIN_CARD_AREA_FRACTION: float = float(
    os.environ.get("MIN_CARD_AREA_FRACTION", "0.05")
)
# TCG card aspect ratio (width / height) and tolerance.
CARD_ASPECT_RATIO: float = 2.5 / 3.5   # ≈ 0.714
CARD_ASPECT_TOLERANCE: float = float(
    os.environ.get("CARD_ASPECT_TOLERANCE", "0.20")
)

# ── Pricing ───────────────────────────────────────────────────────────────────
POKEMONTCG_API_KEY: str = os.environ.get("POKEMONTCG_API_KEY", "")
POKEMONTCG_API_BASE: str = "https://api.pokemontcg.io/v2"
# Default currency stored for prices fetched from TCGplayer.
DEFAULT_CURRENCY: str = "USD"
# Rate-limit pause between API requests (seconds).
PRICE_SYNC_DELAY: float = float(os.environ.get("PRICE_SYNC_DELAY", "0.15"))

# ── Auto-scan debounce ────────────────────────────────────────────────────────
AUTO_SCAN_INTERVAL_MS: int = int(os.environ.get("AUTO_SCAN_INTERVAL_MS", "2000"))
