"""Unit tests for price normalisation and pricer helpers.

These tests exercise logic that does NOT need a live DB; any DB calls are
mocked via monkeypatching.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import pricer after patching the db module so no connection is attempted.
from api import pricer as _pricer_mod


# ── Price extraction from pokemontcg.io payload ───────────────────────────────
# We test the logic embedded in sync_prices.py.

import sync_prices as _sync


class TestExtractPrices:
    def _make_card(self, tcg_prices=None, cm_prices=None) -> dict:
        card: dict = {"id": "test-1"}
        if tcg_prices is not None:
            card["tcgplayer"] = {"prices": tcg_prices}
        if cm_prices is not None:
            card["cardmarket"] = {"prices": cm_prices}
        return card

    def test_empty_card(self):
        prices = _sync.extract_prices({"id": "empty"})
        assert prices == []

    def test_tcgplayer_normal_market(self):
        card = self._make_card(tcg_prices={"normal": {"market": 2.99, "mid": 2.50}})
        prices = _sync.extract_prices(card)
        sources = {p["source"] for p in prices}
        assert "tcgplayer" in sources
        market_rows = [p for p in prices if p["condition"] == "market"]
        assert len(market_rows) == 1
        assert market_rows[0]["price"] == pytest.approx(2.99)

    def test_cardmarket_prices(self):
        card = self._make_card(
            cm_prices={"averageSellPrice": 1.50, "trendPrice": 1.75}
        )
        prices = _sync.extract_prices(card)
        cm_rows = [p for p in prices if p["source"] == "cardmarket"]
        assert len(cm_rows) == 2
        currencies = {p["currency"] for p in cm_rows}
        assert "EUR" in currencies

    def test_holofoil_variant(self):
        card = self._make_card(
            tcg_prices={"holofoil": {"market": 15.00, "low": 12.00, "high": 20.00}}
        )
        prices = _sync.extract_prices(card)
        markets = {p["market"] for p in prices}
        assert "holofoil" in markets

    def test_all_conditions_extracted(self):
        """All four condition keys (market, mid, low, high) should be extracted."""
        card = self._make_card(
            tcg_prices={"normal": {"market": 1.0, "mid": 0.9, "low": 0.5, "high": 2.0}}
        )
        prices = _sync.extract_prices(card)
        conditions = {p["condition"] for p in prices}
        assert conditions == {"market", "mid", "low", "high"}

    def test_captured_at_is_datetime(self):
        card = self._make_card(tcg_prices={"normal": {"market": 1.0}})
        prices = _sync.extract_prices(card)
        assert isinstance(prices[0]["captured_at"], datetime)


# ── get_best_price selection logic ────────────────────────────────────────────


class TestGetBestPrice:
    def _patch_get_prices(self, monkeypatch, rows):
        monkeypatch.setattr(_pricer_mod, "get_prices_for_card", lambda cid: rows)

    def test_tcgplayer_preferred_over_cardmarket(self, monkeypatch):
        rows = [
            {"source": "cardmarket", "market": "normal", "price": 1.0,
             "currency": "EUR", "condition": "trendPrice", "captured_at": "2024-01-01"},
            {"source": "tcgplayer", "market": "normal", "price": 2.0,
             "currency": "USD", "condition": "market", "captured_at": "2024-01-01"},
        ]
        self._patch_get_prices(monkeypatch, rows)
        best = _pricer_mod.get_best_price("test-1")
        assert best["source"] == "tcgplayer"

    def test_cardmarket_fallback(self, monkeypatch):
        rows = [
            {"source": "cardmarket", "market": "normal", "price": 1.50,
             "currency": "EUR", "condition": "trendPrice", "captured_at": "2024-01-01"},
        ]
        self._patch_get_prices(monkeypatch, rows)
        best = _pricer_mod.get_best_price("test-1")
        assert best["source"] == "cardmarket"

    def test_no_prices_returns_none(self, monkeypatch):
        self._patch_get_prices(monkeypatch, [])
        best = _pricer_mod.get_best_price("test-1")
        assert best is None

    def test_skips_none_price(self, monkeypatch):
        rows = [
            {"source": "tcgplayer", "market": "normal", "price": None,
             "currency": "USD", "condition": "market", "captured_at": "2024-01-01"},
            {"source": "cardmarket", "market": "normal", "price": 3.00,
             "currency": "EUR", "condition": "trendPrice", "captured_at": "2024-01-01"},
        ]
        self._patch_get_prices(monkeypatch, rows)
        best = _pricer_mod.get_best_price("test-1")
        # tcgplayer row has None price so should fall through to cardmarket.
        assert best["source"] == "cardmarket"
