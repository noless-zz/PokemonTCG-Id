"""Card identification pipeline for the PokemonTCG scanner API.

Pipeline
--------
1. Detect the card's rectangular contour in the input frame.
2. Quality-gate the crop (blur / area / aspect-ratio checks).
3. Compute a 64-bit perceptual hash (pHash) of the normalised crop.
4. Compare against every phash in the in-memory index loaded from the DB.
5. Return the top-K matches ranked by ascending Hamming distance.

The in-memory index is loaded once (call `load_index()`) and cached as a
module-level numpy array for O(1) per-comparison throughput.
"""

from __future__ import annotations

import base64
import logging
from io import BytesIO
from typing import Optional

import cv2
import imagehash
import numpy as np
from PIL import Image

from api.config import (
    BLUR_VARIANCE_THRESHOLD,
    CARD_ASPECT_RATIO,
    CARD_ASPECT_TOLERANCE,
    MIN_CARD_AREA_FRACTION,
    PHASH_GOOD_THRESHOLD,
    TOP_K_MATCHES,
)
from api.db import get_connection

logger = logging.getLogger(__name__)

# ── In-memory phash index ─────────────────────────────────────────────────────
# Populated by load_index(); used by find_matches().
_index_card_ids: list[str] = []
_index_phashes: Optional[np.ndarray] = None   # shape (N,), dtype uint64


def load_index() -> int:
    """Load phash index from the DB into module-level cache.

    Returns the number of cards in the index.
    Raises RuntimeError if the index is empty (build_index.py must run first).
    """
    global _index_card_ids, _index_phashes
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT card_id, phash_int FROM card_match_index")
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as exc:
        raise RuntimeError(f"Failed to load phash index from DB: {exc}") from exc

    if not rows:
        raise RuntimeError(
            "card_match_index is empty. Run build_index.py first."
        )

    _index_card_ids = [r["card_id"] for r in rows]
    _index_phashes = np.array(
        [r["phash_int"] for r in rows], dtype=np.uint64
    )
    logger.info("Loaded %d cards into phash index.", len(_index_card_ids))
    return len(_index_card_ids)


def _hamming_distance_all(query_int: int) -> np.ndarray:
    """Return Hamming distance between *query_int* and every indexed phash."""
    q = np.uint64(query_int)
    xor = _index_phashes ^ q
    # Vectorised popcount using bitwise trick.
    dist = np.zeros(len(xor), dtype=np.uint8)
    tmp = xor.copy()
    while np.any(tmp):
        dist += (tmp & np.uint64(1)).astype(np.uint8)
        tmp >>= np.uint64(1)
    return dist


def _phash_to_int(ph: imagehash.ImageHash) -> int:
    """Convert an imagehash.ImageHash to an unsigned 64-bit integer."""
    flat = ph.hash.flatten()
    result = 0
    for bit in flat:
        result = (result << 1) | int(bit)
    return result


# ── Image pre-processing helpers ──────────────────────────────────────────────

def detect_card_crop(frame_bgr: np.ndarray) -> Optional[np.ndarray]:
    """Detect the largest card-like quadrilateral in *frame_bgr*.

    Returns a perspective-corrected BGR crop of the card, or None if no
    suitable contour is found.
    """
    h, w = frame_bgr.shape[:2]
    frame_area = h * w

    gray = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    edges = cv2.Canny(blurred, 50, 150)
    edges = cv2.dilate(edges, None, iterations=1)

    contours, _ = cv2.findContours(
        edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )
    if not contours:
        return None

    # Sort by descending area; evaluate the top candidates.
    contours = sorted(contours, key=cv2.contourArea, reverse=True)[:10]
    best_quad: Optional[np.ndarray] = None
    best_area = 0.0

    for cnt in contours:
        area = cv2.contourArea(cnt)
        if area < frame_area * MIN_CARD_AREA_FRACTION:
            break
        peri = cv2.arcLength(cnt, True)
        approx = cv2.approxPolyDP(cnt, 0.02 * peri, True)
        if len(approx) != 4:
            continue
        pts = approx.reshape(4, 2).astype(np.float32)
        # Check aspect ratio.
        rect_w = float(np.linalg.norm(pts[0] - pts[1]))
        rect_h = float(np.linalg.norm(pts[1] - pts[2]))
        if rect_h == 0:
            continue
        ratio = min(rect_w, rect_h) / max(rect_w, rect_h)
        expected = CARD_ASPECT_RATIO
        if abs(ratio - expected) > CARD_ASPECT_TOLERANCE:
            continue
        if area > best_area:
            best_area = area
            best_quad = pts

    if best_quad is None:
        return None

    # Perspective warp to a standard card size (250×350 px).
    dst = np.array(
        [[0, 0], [250, 0], [250, 350], [0, 350]], dtype=np.float32
    )
    M = cv2.getPerspectiveTransform(_order_points(best_quad), dst)
    warped = cv2.warpPerspective(frame_bgr, M, (250, 350))
    return warped


def _order_points(pts: np.ndarray) -> np.ndarray:
    """Order four points: top-left, top-right, bottom-right, bottom-left."""
    rect = np.zeros((4, 2), dtype=np.float32)
    s = pts.sum(axis=1)
    rect[0] = pts[np.argmin(s)]
    rect[2] = pts[np.argmax(s)]
    diff = np.diff(pts, axis=1)
    rect[1] = pts[np.argmin(diff)]
    rect[3] = pts[np.argmax(diff)]
    return rect


def quality_check(crop_bgr: np.ndarray) -> tuple[bool, str]:
    """Return (passed, reason) for a candidate card crop.

    Checks performed:
    - Sharpness via Laplacian variance.
    """
    gray = cv2.cvtColor(crop_bgr, cv2.COLOR_BGR2GRAY)
    variance = cv2.Laplacian(gray, cv2.CV_64F).var()
    if variance < BLUR_VARIANCE_THRESHOLD:
        return False, f"blurry (var={variance:.1f} < {BLUR_VARIANCE_THRESHOLD})"
    return True, "ok"


def compute_phash_from_bgr(bgr: np.ndarray) -> int:
    """Compute a 64-bit pHash integer from a BGR numpy array."""
    rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
    pil_img = Image.fromarray(rgb)
    ph = imagehash.phash(pil_img)
    return _phash_to_int(ph)


def compute_phash_from_pil(pil_img: Image.Image) -> int:
    """Compute a 64-bit pHash integer from a PIL image."""
    ph = imagehash.phash(pil_img)
    return _phash_to_int(ph)


# ── Matching ──────────────────────────────────────────────────────────────────

def find_matches(
    query_phash_int: int,
    top_k: int = TOP_K_MATCHES,
) -> list[dict]:
    """Find the closest cards to *query_phash_int* in the in-memory index.

    Returns a list of dicts with keys: card_id, hamming_distance, confidence.
    Confidence is 1.0 when distance=0 and 0.0 when distance≥32.
    """
    if _index_phashes is None:
        raise RuntimeError("Index not loaded. Call load_index() first.")

    distances = _hamming_distance_all(query_phash_int)
    order = np.argsort(distances)[:top_k]

    results = []
    for idx in order:
        dist = int(distances[idx])
        # Linear confidence: 1.0 at dist=0, 0.0 at dist=32.
        confidence = max(0.0, 1.0 - dist / 32.0)
        results.append(
            {
                "card_id": _index_card_ids[idx],
                "hamming_distance": dist,
                "confidence": round(confidence, 3),
            }
        )
    return results


# ── High-level entry point ────────────────────────────────────────────────────

def identify_from_bytes(image_bytes: bytes) -> dict:
    """Full identification pipeline from raw image bytes.

    Returns a dict with:
      - matches: list of top-K match dicts (card_id, hamming_distance, confidence)
      - quality_ok: bool
      - quality_reason: str
      - card_detected: bool
    """
    np_arr = np.frombuffer(image_bytes, np.uint8)
    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    if frame is None:
        return {
            "matches": [],
            "quality_ok": False,
            "quality_reason": "failed to decode image",
            "card_detected": False,
        }

    crop = detect_card_crop(frame)
    card_detected = crop is not None
    if crop is None:
        # Fall back to using the whole frame when no card contour found.
        crop = frame

    ok, reason = quality_check(crop)
    phash_int = compute_phash_from_bgr(crop)
    matches = find_matches(phash_int)

    return {
        "matches": matches,
        "quality_ok": ok,
        "quality_reason": reason,
        "card_detected": card_detected,
    }
