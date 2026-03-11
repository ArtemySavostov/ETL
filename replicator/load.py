from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from pymongo import UpdateOne

from .extract import CustomerRow, OrderRow


@dataclass(frozen=True)
class ReplicationResult:
    customers_upserted: int
    orders_processed: int


def _to_float(x):
    if isinstance(x, Decimal):
        return float(x)
    return x


class MongoLoader:
    def __init__(self, mongo_db, *, collection_name: str = "customers") -> None:
        self._db = mongo_db
        self._col = mongo_db[collection_name]

    def ensure_indexes(self) -> None:
        self._col.create_index("email", unique=False)
        self._col.create_index("synced_at")

    def upsert_customers(self, customers: list[CustomerRow], *, synced_at: datetime) -> int:
        ops: list[UpdateOne] = []
        for c in customers:
            ops.append(
                UpdateOne(
                    {"_id": c.id},
                    {
                        "$set": {"name": c.name, "email": c.email, "synced_at": synced_at},
                        "$setOnInsert": {"orders": []},
                    },
                    upsert=True,
                )
            )
        if not ops:
            return 0
        self._col.bulk_write(ops, ordered=False)
        return len(ops)

    def upsert_orders(self, orders: list[OrderRow], *, synced_at: datetime) -> int:
        ops: list[UpdateOne] = []
        for o in orders:
            order_doc = {
                "order_id": o.order_id,
                "product": o.product,
                "amount": _to_float(o.amount),
                "status": o.status,
                "placed_at": o.created_at,
                "updated_at": o.updated_at,
            }

            ops.append(
                UpdateOne(
                    {"_id": o.customer_id, "orders.order_id": o.order_id},
                    {
                        "$set": {
                            "name": o.customer_name,
                            "email": o.customer_email,
                            "synced_at": synced_at,
                            "orders.$.product": order_doc["product"],
                            "orders.$.amount": order_doc["amount"],
                            "orders.$.status": order_doc["status"],
                            "orders.$.placed_at": order_doc["placed_at"],
                            "orders.$.updated_at": order_doc["updated_at"],
                        }
                    },
                    upsert=False,
                )
            )

            ops.append(
                UpdateOne(
                    {"_id": o.customer_id, "orders.order_id": {"$ne": o.order_id}},
                    {
                        "$set": {
                            "name": o.customer_name,
                            "email": o.customer_email,
                            "synced_at": synced_at,
                        },
                        "$setOnInsert": {"orders": []},
                        "$push": {"orders": order_doc},
                    },
                    upsert=True,
                )
            )

        if not ops:
            return 0
        self._col.bulk_write(ops, ordered=False)
        return len(orders)

