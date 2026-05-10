"""FastAPI application for the PokemonTCG webcam scanner.

Endpoints
---------
GET  /health              – liveness probe
POST /api/identify        – identify a card from an uploaded image
GET  /api/cards/{card_id} – card metadata + current prices
GET  /                    – serve the single-page scanner UI
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from api.config import TOP_K_MATCHES
from api.db import get_connection
from api.identifier import identify_from_bytes, load_index
from api.pricer import get_best_price, get_prices_for_card

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "static"

# ── Startup / shutdown ────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load the phash index once on startup."""
    try:
        n = load_index()
        logger.info("phash index ready: %d cards", n)
    except RuntimeError as exc:
        logger.warning(
            "Could not load phash index (%s). "
            "/api/identify will return an error until build_index.py is run.",
            exc,
        )
    yield


# ── App factory ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="PokemonTCG Scanner API",
    description=(
        "Identifies Pokémon TCG cards from webcam/phone images and "
        "returns pricing information."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Serve static assets (JS, CSS, icons) from /static/.
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health", tags=["meta"])
def health() -> dict:
    """Liveness probe – always returns 200 when the server is running."""
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def index():
    """Serve the scanner SPA."""
    html_path = STATIC_DIR / "index.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="UI not found")
    return FileResponse(str(html_path))


@app.post("/api/identify", tags=["scanner"])
async def identify(
    request: Request,
    file: UploadFile = File(..., description="JPEG/PNG frame from webcam"),
):
    """Identify a Pokémon TCG card from an uploaded image frame.

    Returns the top-K matching cards with confidence scores and the best
    available price for each match.
    """
    t0 = time.perf_counter()

    image_bytes = await file.read()
    if not image_bytes:
        raise HTTPException(status_code=400, detail="Empty file uploaded")

    result = identify_from_bytes(image_bytes)

    # Enrich matches with card metadata + prices.
    enriched_matches = []
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        for m in result["matches"]:
            card_id = m["card_id"]
            cur.execute(
                """
                SELECT cc.id, cc.name, cc.number, cc.rarity,
                       s.name AS set_name, s.id AS set_id,
                       ca.small_image, ca.large_image
                FROM cards_classification cc
                JOIN sets s ON s.id = cc.set_id
                LEFT JOIN cards_art ca ON ca.card_id = cc.id
                WHERE cc.id = %s
                """,
                (card_id,),
            )
            card = cur.fetchone()
            best_price = get_best_price(card_id)
            enriched_matches.append(
                {
                    **m,
                    "card": card or {},
                    "best_price": best_price,
                }
            )
        cur.close()
        conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("DB enrichment failed: %s", exc)

    # Log scan to scan_history (best-effort; non-fatal).
    _record_scan(
        matched_card_id=(
            enriched_matches[0]["card_id"] if enriched_matches else None
        ),
        confidence=(
            enriched_matches[0]["confidence"] if enriched_matches else None
        ),
        hamming=(
            enriched_matches[0]["hamming_distance"]
            if enriched_matches
            else None
        ),
        user_agent=request.headers.get("user-agent", ""),
    )

    latency_ms = (time.perf_counter() - t0) * 1000
    logger.info(
        "identify: card_detected=%s quality_ok=%s latency=%.1fms",
        result["card_detected"],
        result["quality_ok"],
        latency_ms,
    )

    return JSONResponse(
        {
            "card_detected": result["card_detected"],
            "quality_ok": result["quality_ok"],
            "quality_reason": result["quality_reason"],
            "matches": enriched_matches,
            "latency_ms": round(latency_ms, 1),
        }
    )


@app.get("/api/cards/{card_id}", tags=["cards"])
def card_detail(card_id: str):
    """Return card metadata and all available price snapshots."""
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT cc.id, cc.name, cc.supertype, cc.number, cc.rarity,
                   s.name AS set_name, s.id AS set_id, s.series,
                   ca.artist, ca.small_image, ca.large_image,
                   cg.hp, cg.evolves_from, cg.attacks, cg.abilities
            FROM cards_classification cc
            JOIN sets s ON s.id = cc.set_id
            LEFT JOIN cards_art ca ON ca.card_id = cc.id
            LEFT JOIN cards_gameplay cg ON cg.card_id = cc.id
            WHERE cc.id = %s
            """,
            (card_id,),
        )
        card = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not card:
        raise HTTPException(status_code=404, detail="Card not found")

    prices = get_prices_for_card(card_id)
    return {"card": card, "prices": prices}


# ── Internal helpers ──────────────────────────────────────────────────────────


def _record_scan(
    matched_card_id: str | None,
    confidence: float | None,
    hamming: int | None,
    user_agent: str,
) -> None:
    """Insert a row into scan_history (silently ignores errors)."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO scan_history
                (matched_card_id, confidence, hamming_distance, source_device)
            VALUES (%s, %s, %s, %s)
            """,
            (matched_card_id, confidence, hamming, user_agent[:255]),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:  # noqa: BLE001
        pass
