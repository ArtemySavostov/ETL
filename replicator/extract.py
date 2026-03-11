from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from .config import PgConfig


@dataclass(frozen=True)
class CustomerRow:
    id: int
    name: str
    email: str
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None


@dataclass(frozen=True)
class OrderRow:
    order_id: int
    customer_id: int
    amount: object
    status: str
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None
    customer_name: str
    customer_email: str
    products: list["OrderProduct"]


@dataclass(frozen=True)
class OrderProduct:
    product_id: int
    name: str
    price: object
    quantity: int
    deleted_at: datetime | None


class PostgresExtractor:
    def __init__(self, cfg: PgConfig):
        self._cfg = cfg

    def _connect(self):
        return psycopg2.connect(
            host=self._cfg.host,
            port=self._cfg.port,
            dbname=self._cfg.dbname,
            user=self._cfg.user,
            password=self._cfg.password,
            cursor_factory=DictCursor,
        )

    def fetch_new_customers(self, *, last_sync: datetime) -> list[CustomerRow]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, email, created_at, updated_at, deleted_at
                    FROM customers
                    WHERE GREATEST(
                        created_at,
                        COALESCE(updated_at, created_at),
                        COALESCE(deleted_at, created_at)
                    ) > %s
                    ORDER BY created_at ASC;
                    """,
                    (last_sync,),
                )
                rows = cur.fetchall()

        return [
            CustomerRow(
                id=int(r["id"]),
                name=r["name"],
                email=r["email"],
                created_at=r["created_at"],
                updated_at=r["updated_at"],
                deleted_at=r["deleted_at"],
            )
            for r in rows
        ]

    def fetch_new_or_updated_orders(self, *, last_sync: datetime) -> list[OrderRow]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        o.id              AS order_id,
                        o.customer_id     AS customer_id,
                        o.amount          AS amount,
                        o.status          AS status,
                        o.created_at      AS created_at,
                        o.updated_at      AS updated_at,
                        o.deleted_at      AS order_deleted_at,
                        c.name            AS customer_name,
                        c.email           AS customer_email,
                        p.id              AS product_id,
                        p.name            AS product_name,
                        p.price           AS product_price,
                        op.quantity       AS quantity,
                        op.deleted_at     AS op_deleted_at,
                        p.deleted_at      AS product_deleted_at
                    FROM orders o
                    JOIN customers c ON c.id = o.customer_id
                    JOIN order_products op ON op.order_id = o.id
                    JOIN products p ON p.id = op.product_id
                    WHERE GREATEST(
                        o.updated_at,
                        COALESCE(o.deleted_at, o.updated_at),
                        COALESCE(op.deleted_at, o.updated_at),
                        COALESCE(p.deleted_at, o.updated_at)
                    ) > %s
                    ORDER BY o.updated_at ASC, o.id ASC;
                    """,
                    (last_sync,),
                )
                rows = cur.fetchall()

        orders_by_id: dict[int, dict] = {}
        for r in rows:
            oid = int(r["order_id"])
            if oid not in orders_by_id:
                orders_by_id[oid] = {
                    "order_id": oid,
                    "customer_id": int(r["customer_id"]),
                    "amount": r["amount"],
                    "status": r["status"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                    "deleted_at": r["order_deleted_at"],
                    "customer_name": r["customer_name"],
                    "customer_email": r["customer_email"],
                    "products": [],
                }

            orders_by_id[oid]["products"].append(
                OrderProduct(
                    product_id=int(r["product_id"]),
                    name=r["product_name"],
                    price=r["product_price"],
                    quantity=int(r["quantity"]),
                    deleted_at=r["op_deleted_at"] or r["product_deleted_at"],
                )
            )

        return [
            OrderRow(
                order_id=v["order_id"],
                customer_id=v["customer_id"],
                amount=v["amount"],
                status=v["status"],
                created_at=v["created_at"],
                updated_at=v["updated_at"],
                deleted_at=v["deleted_at"],
                customer_name=v["customer_name"],
                customer_email=v["customer_email"],
                products=v["products"],
            )
            for v in orders_by_id.values()
        ]

