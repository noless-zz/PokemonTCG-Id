#!/usr/bin/env python3
"""start_server.py – convenience launcher for the PokemonTCG scanner API.

Usage
-----
    python start_server.py [--host HOST] [--port PORT] [--reload]

Defaults: host=0.0.0.0, port=8000.
Using 0.0.0.0 allows phone cameras to connect over the local network.
"""

import argparse
import sys

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the TCG Scanner API server.")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default 8000)")
    parser.add_argument(
        "--reload", action="store_true", help="Enable auto-reload (dev mode)"
    )
    args = parser.parse_args()

    print(f"\n🎴 PokemonTCG Scanner starting on http://{args.host}:{args.port}")
    print("   Open this URL in a browser (desktop or phone on the same network).\n")

    uvicorn.run(
        "api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info",
    )


if __name__ == "__main__":
    main()
