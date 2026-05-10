"""MySQL connection helpers for the PokemonTCG scanner API."""

from __future__ import annotations

import mysql.connector
from mysql.connector import MySQLConnection

from api.config import DB_CONFIG


def get_connection() -> MySQLConnection:
    """Return a fresh MySQL connection using the shared DB_CONFIG."""
    return mysql.connector.connect(**DB_CONFIG)


def get_cursor(connection: MySQLConnection, dictionary: bool = True):
    """Return a cursor from *connection* with optional dict-mode."""
    return connection.cursor(dictionary=dictionary)
