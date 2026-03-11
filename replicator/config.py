from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    db: str


def _parse_int(value: str, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def pg_config_from_env() -> PgConfig:
    return PgConfig(
        host=os.getenv("PG_HOST", "postgres"),
        port=_parse_int(os.getenv("PG_PORT", "5432"), 5432),
        dbname=os.getenv("PG_DB", "shop"),
        user=os.getenv("PG_USER", "admin"),
        password=os.getenv("PG_PASSWORD", "secret"),
    )


def mongo_config_from_env() -> MongoConfig:
    return MongoConfig(
        uri=os.getenv("MONGO_URI", "mongodb://mongodb:27017/"),
        db=os.getenv("MONGO_DB", "replica_db"),
    )

