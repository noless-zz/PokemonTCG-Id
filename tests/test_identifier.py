"""Unit tests for card identification utilities (no DB / camera required).

Tests cover:
- pHash to int conversion.
- Hamming distance calculation.
- Quality gating.
- Card contour ordering helper.
- find_matches against a synthetic in-memory index.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

# Ensure project root is on the path.
sys.path.insert(0, str(Path(__file__).parent.parent))

import imagehash
from PIL import Image

# Import identifier without triggering DB connection.
from api import identifier as _id_mod


# ── Helpers ───────────────────────────────────────────────────────────────────


def make_solid_pil(r: int, g: int, b: int, size: int = 64) -> Image.Image:
    """Create a solid-colour RGB PIL image."""
    return Image.new("RGB", (size, size), (r, g, b))


def phash_int(pil_img: Image.Image) -> int:
    """Compute phash integer for a PIL image via identifier helpers."""
    return _id_mod.compute_phash_from_pil(pil_img)


# ── phash_to_int ──────────────────────────────────────────────────────────────


class TestPhashToInt:
    def test_zero_hash_is_zero(self):
        ph = imagehash.ImageHash(np.zeros((8, 8), dtype=bool))
        assert _id_mod._phash_to_int(ph) == 0

    def test_all_ones_hash(self):
        ph = imagehash.ImageHash(np.ones((8, 8), dtype=bool))
        expected = (1 << 64) - 1
        assert _id_mod._phash_to_int(ph) == expected

    def test_deterministic(self):
        img = make_solid_pil(128, 64, 32)
        h1 = phash_int(img)
        h2 = phash_int(img)
        assert h1 == h2

    def test_different_images_differ(self):
        h1 = phash_int(make_solid_pil(0, 0, 0))
        h2 = phash_int(make_solid_pil(255, 255, 255))
        assert h1 != h2

    def test_similar_images_close(self):
        """Two very similar images should have small Hamming distance."""
        img_a = make_solid_pil(200, 100, 50)
        img_b = make_solid_pil(205, 105, 55)
        h_a = phash_int(img_a)
        h_b = phash_int(img_b)
        # XOR then popcount.
        diff = bin(h_a ^ h_b).count("1")
        assert diff <= 5, f"Expected similar, got Hamming={diff}"


# ── Hamming distance (vectorised) ─────────────────────────────────────────────


class TestHammingDistance:
    def _setup_index(self, phashes: list[int]) -> None:
        """Inject synthetic index into the module."""
        _id_mod._index_card_ids = [f"card-{i}" for i in range(len(phashes))]
        _id_mod._index_phashes = np.array(phashes, dtype=np.uint64)

    def test_exact_match(self):
        target = phash_int(make_solid_pil(10, 20, 30))
        self._setup_index([0, target, 0xFFFFFFFFFFFFFFFF])
        dists = _id_mod._hamming_distance_all(target)
        assert dists[1] == 0

    def test_ordering(self):
        """Card at distance 0 should rank before card at distance 10."""
        target = 0b1111_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000
        near   = target       # distance 0
        far    = target ^ 0xFFFFFFFF  # distance 32 (lower 32 bits flipped)
        self._setup_index([far, near])
        dists = _id_mod._hamming_distance_all(target)
        assert dists[1] < dists[0]


# ── find_matches ──────────────────────────────────────────────────────────────


class TestFindMatches:
    def _setup_index(self, phashes: list[int]) -> None:
        _id_mod._index_card_ids = [f"card-{i}" for i in range(len(phashes))]
        _id_mod._index_phashes = np.array(phashes, dtype=np.uint64)

    def test_returns_top_k(self):
        self._setup_index([0, 1, 2, 3, 4, 5])
        matches = _id_mod.find_matches(0, top_k=3)
        assert len(matches) == 3

    def test_best_match_first(self):
        target = 42
        self._setup_index([999, target, 12345])
        matches = _id_mod.find_matches(target, top_k=2)
        assert matches[0]["hamming_distance"] == 0
        assert matches[0]["card_id"] == "card-1"

    def test_confidence_perfect_match(self):
        self._setup_index([77])
        matches = _id_mod.find_matches(77, top_k=1)
        assert matches[0]["confidence"] == 1.0

    def test_confidence_zero_at_high_distance(self):
        """Hamming distance >= 32 should give confidence 0."""
        # A target XOR'd with 32 set bits will have Hamming distance 32.
        target = 0
        far = np.uint64((1 << 32) - 1)  # 32 lowest bits set
        self._setup_index([int(far)])
        matches = _id_mod.find_matches(target, top_k=1)
        assert matches[0]["confidence"] == 0.0

    def test_no_index_raises(self):
        _id_mod._index_phashes = None
        with pytest.raises(RuntimeError):
            _id_mod.find_matches(0)
        # Restore for other tests.
        _id_mod._index_card_ids = []
        _id_mod._index_phashes = np.array([], dtype=np.uint64)


# ── Quality check ─────────────────────────────────────────────────────────────


class TestQualityCheck:
    import cv2 as _cv2

    def test_sharp_image_passes(self):
        import cv2
        # A high-frequency checker image is very sharp.
        checker = np.zeros((200, 200, 3), dtype=np.uint8)
        checker[::2, ::2] = 255
        ok, reason = _id_mod.quality_check(checker)
        assert ok, f"Expected pass, got: {reason}"

    def test_blurry_image_fails(self):
        import cv2
        # A heavily blurred solid colour has near-zero Laplacian variance.
        solid = np.full((200, 200, 3), 128, dtype=np.uint8)
        blurred = cv2.GaussianBlur(solid, (51, 51), 0)
        ok, reason = _id_mod.quality_check(blurred)
        assert not ok, "Expected blur detection to fail"


# ── Order points ──────────────────────────────────────────────────────────────


class TestOrderPoints:
    def test_canonical_square(self):
        pts = np.array([[0, 0], [100, 0], [100, 100], [0, 100]], dtype=np.float32)
        ordered = _id_mod._order_points(pts)
        # Top-left should have smallest sum.
        assert tuple(ordered[0]) == (0.0, 0.0)
        # Bottom-right largest sum.
        assert tuple(ordered[2]) == (100.0, 100.0)

    def test_shuffled_points(self):
        pts = np.array(
            [[100, 100], [0, 100], [0, 0], [100, 0]], dtype=np.float32
        )
        ordered = _id_mod._order_points(pts)
        assert tuple(ordered[0]) == (0.0, 0.0)
        assert tuple(ordered[2]) == (100.0, 100.0)
