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


@dataclass(frozen=True)
class OrderRow:
    order_id: int
    customer_id: int
    product: str
    amount: object
    status: str
    created_at: datetime
    updated_at: datetime
    customer_name: str
    customer_email: str


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
                    SELECT id, name, email, created_at
                    FROM customers
                    WHERE created_at > %s
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
                        o.product         AS product,
                        o.amount          AS amount,
                        o.status          AS status,
                        o.created_at      AS created_at,
                        o.updated_at      AS updated_at,
                        c.name            AS customer_name,
                        c.email           AS customer_email
                    FROM orders o
                    JOIN customers c ON c.id = o.customer_id
                    WHERE o.updated_at > %s
                    ORDER BY o.updated_at ASC;
                    """,
                    (last_sync,),
                )
                rows = cur.fetchall()

        return [
            OrderRow(
                order_id=int(r["order_id"]),
                customer_id=int(r["customer_id"]),
                product=r["product"],
                amount=r["amount"],
                status=r["status"],
                created_at=r["created_at"],
                updated_at=r["updated_at"],
                customer_name=r["customer_name"],
                customer_email=r["customer_email"],
            )
            for r in rows
        ]

