"""Unit tests for export_pages_data JSON serialization behavior."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent))

import export_pages_data as _export


class _FakeCursor:
    def __init__(self, cards_rows: list[dict], prices_rows: list[dict]) -> None:
        self._cards_rows = cards_rows
        self._prices_rows = prices_rows
        self._last_query = ""
        self._counts = {
            "sets": 1,
            "cards_classification": 2,
            "cards_art": 2,
            "preprocessed_images": 2,
            "card_match_index": 2,
            "card_prices": 2,
        }

    def execute(self, query: str, _params=None) -> None:
        self._last_query = query

    def fetchone(self) -> dict:
        for table, count in self._counts.items():
            if f"FROM {table}" in self._last_query:
                return {"c": count}
        raise AssertionError(f"Unexpected count query: {self._last_query}")

    def fetchall(self) -> list[dict]:
        if "FROM cards_classification cc" in self._last_query:
            return self._cards_rows
        if "FROM card_prices p" in self._last_query:
            return self._prices_rows
        raise AssertionError(f"Unexpected fetchall query: {self._last_query}")

    def close(self) -> None:
        return None


class _FakeConn:
    def __init__(self, cards_rows: list[dict], prices_rows: list[dict]) -> None:
        self._cursor = _FakeCursor(cards_rows, prices_rows)

    def cursor(self, dictionary=True):
        assert dictionary is True
        return self._cursor

    def close(self) -> None:
        return None


def test_main_serializes_decimal_and_datetime(monkeypatch, tmp_path):
    cards_rows = [
        {
            "id": "xy1-1",
            "name": "Bulbasaur",
            "number": Decimal("1"),
            "rarity": "Common",
            "supertype": "Pokémon",
            "set_id": "xy1",
            "set_name": "XY",
            "small_image": "https://example/small.png",
            "large_image": "https://example/large.png",
        }
    ]
    prices_rows = [
        {
            "card_id": "xy1-1",
            "source": "tcgplayer",
            "market": "normal",
            "currency": "USD",
            "condition": "market",
            "price": Decimal("12.34"),
            "captured_at": datetime(2026, 5, 15, 6, 31, 51, tzinfo=timezone.utc),
        }
    ]

    args = SimpleNamespace(
        output_dir=str(tmp_path / "data"),
        db_host="127.0.0.1",
        db_user="pokemon",
        db_password="pokemon",
        db_name="pokemon_tcg",
        db_charset="utf8mb4",
        max_cards=5000,
        max_prices=5000,
    )

    monkeypatch.setattr(_export, "parse_args", lambda: args)
    monkeypatch.setattr(
        _export.mysql.connector,
        "connect",
        lambda **_kwargs: _FakeConn(cards_rows=cards_rows, prices_rows=prices_rows),
    )

    _export.main()

    output_dir = tmp_path / "data"
    assert (output_dir / "build-info.json").exists()
    assert (output_dir / "cards.json").exists()
    assert (output_dir / "latest-prices.json").exists()

    cards_payload = json.loads((output_dir / "cards.json").read_text(encoding="utf-8"))
    prices_payload = json.loads(
        (output_dir / "latest-prices.json").read_text(encoding="utf-8")
    )

    assert cards_payload[0]["number"] == 1.0
    assert isinstance(cards_payload[0]["number"], float)
    assert prices_payload[0]["price"] == 12.34
    assert isinstance(prices_payload[0]["price"], float)
    assert prices_payload[0]["captured_at"] == "2026-05-15T06:31:51+00:00"
