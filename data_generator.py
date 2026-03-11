import os
import random

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values


def db_config_from_env():
    return {
        "dbname": os.getenv("PG_DB", "shop"),
        "user": os.getenv("PG_USER", "admin"),
        "password": os.getenv("PG_PASSWORD", "secret"),
        "host": os.getenv("PG_HOST", "postgres"),
        "port": int(os.getenv("PG_PORT", "5432")),
    }


fake = Faker()


def generate_data(num_customers: int = 500_000, batch_size: int = 5_000) -> None:
    """
    Генерирует много данных быстро.
    Каждый customer получает 1..5 orders.
    """
    conn = psycopg2.connect(**db_config_from_env())
    conn.autocommit = False

    customer_rows = []
    order_rows = []

    def flush(cur):
        nonlocal customer_rows, order_rows
        if not customer_rows:
            return

        execute_values(
            cur,
            "INSERT INTO customers (name, email) VALUES %s RETURNING id",
            customer_rows,
            page_size=min(len(customer_rows), 1000),
        )
        customer_ids = [r[0] for r in cur.fetchall()]

        for cid in customer_ids:
            for _ in range(random.randint(1, 5)):
                product = fake.word()
                amount = round(random.uniform(10, 1000), 2)
                status = random.choice(["pending", "completed", "shipped"])
                order_rows.append((cid, product, amount, status))

        execute_values(
            cur,
            "INSERT INTO orders (customer_id, product, amount, status) VALUES %s",
            order_rows,
            page_size=min(len(order_rows), 5000),
        )

        customer_rows = []
        order_rows = []

    try:
        with conn:
            with conn.cursor() as cur:
                for i in range(num_customers):
                    customer_rows.append((fake.name(), f"user_{i}_{fake.uuid4()}@example.com"))
                    if len(customer_rows) >= batch_size:
                        flush(cur)
                flush(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    generate_data(
        int(os.getenv("GEN_CUSTOMERS", "500000")),
        int(os.getenv("GEN_BATCH_SIZE", "5000")),
    )