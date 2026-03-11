from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pymongo import MongoClient

from .config import MongoConfig, PgConfig
from .extract import PostgresExtractor
from .load import MongoLoader, ReplicationResult
from .state import MongoStateStore


@dataclass(frozen=True)
class ReplicatorDeps:
    pg: PgConfig
    mongo: MongoConfig


class Replicator:
    def __init__(self, deps: ReplicatorDeps):
        self._deps = deps

    def run_once(self) -> ReplicationResult:
        started_at = datetime.now(UTC)

        mongo_client = MongoClient(self._deps.mongo.uri)
        mongo_db = mongo_client[self._deps.mongo.db]

        state = MongoStateStore(mongo_db)
        loader = MongoLoader(mongo_db)
        loader.ensure_indexes()

        last_sync = state.load_last_sync()

        extractor = PostgresExtractor(self._deps.pg)
        new_customers = extractor.fetch_new_customers(last_sync=last_sync)
        new_orders = extractor.fetch_new_or_updated_orders(last_sync=last_sync)

        customers_upserted = loader.upsert_customers(new_customers, synced_at=started_at)
        orders_processed = loader.upsert_orders(new_orders, synced_at=started_at)

        state.save_last_sync(started_at)

        return ReplicationResult(
            customers_upserted=customers_upserted,
            orders_processed=orders_processed,
        )

