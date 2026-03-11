from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from pymongo import UpdateOne

from .extract import CustomerRow, OrderProduct, OrderRow


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
        self._col.create_index("deleted_at")
        self._col.create_index("orders.order_id")

    def upsert_customers(self, customers: list[CustomerRow], *, synced_at: datetime) -> int:
        ops: list[UpdateOne] = []
        for c in customers:
            base_set = {"name": c.name, "email": c.email, "synced_at": synced_at}
            if c.deleted_at:
                base_set["deleted_at"] = c.deleted_at

            ops.append(
                UpdateOne(
                    {"_id": c.id},
                    {
                        "$set": base_set,
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
            if o.deleted_at:
                ops.append(
                    UpdateOne(
                        {"_id": o.customer_id},
                        {
                            "$set": {
                                "name": o.customer_name,
                                "email": o.customer_email,
                                "synced_at": synced_at,
                            },
                            "$pull": {"orders": {"order_id": o.order_id}},
                        },
                        upsert=True,
                    )
                )
                continue

            products_docs = [
                {
                    "product_id": p.product_id,
                    "name": p.name,
                    "price": _to_float(p.price),
                    "quantity": p.quantity,
                    "deleted_at": p.deleted_at,
                }
                for p in o.products
                if not p.deleted_at
            ]

            order_doc = {
                "order_id": o.order_id,
                "amount": _to_float(o.amount),
                "status": o.status,
                "placed_at": o.created_at,
                "updated_at": o.updated_at,
                "products": products_docs,
            }

            ops.append(
                UpdateOne(
                    {"_id": o.customer_id},
                    {"$pull": {"orders": {"order_id": o.order_id}}},
                    upsert=False,
                )
            )

            ops.append(
                UpdateOne(
                    {"_id": o.customer_id},
                    {
                        "$set": {
                            "name": o.customer_name,
                            "email": o.customer_email,
                            "synced_at": synced_at,
                        },
                        "$push": {"orders": order_doc},
                    },
                    upsert=True,
                )
            )

        if not ops:
            return 0
        self._col.bulk_write(ops, ordered=False)
        return len(orders)

