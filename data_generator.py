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


def _bootstrap_products(cur, *, num_products: int = 1_000) -> list[tuple[int, str, float]]:
    """
    Создаёт набор продуктов и возвращает список (id, name, price).
    """
    products_rows = []
    for _ in range(num_products):
        name = fake.unique.word()
        price = round(random.uniform(5, 2_000), 2)
        products_rows.append((name, price))

    execute_values(
        cur,
        "INSERT INTO products (name, price) VALUES %s RETURNING id, name, price",
        products_rows,
        page_size=min(len(products_rows), 1000),
    )
    return [(int(r[0]), str(r[1]), float(r[2])) for r in cur.fetchall()]


def generate_data(num_customers: int = 500_000, batch_size: int = 5_000) -> None:
    """
    Генерирует много данных быстро.
    Каждый customer получает 1..5 orders.
    Каждый order содержит 1..3 продуктов (many-to-many через order_products).
    """
    conn = psycopg2.connect(**db_config_from_env())
    conn.autocommit = False

    customer_rows = []

    def flush(cur, *, products: list[tuple[int, str, float]]):
        nonlocal customer_rows
        if not customer_rows:
            return

        execute_values(
            cur,
            "INSERT INTO customers (name, email) VALUES %s RETURNING id",
            customer_rows,
            page_size=min(len(customer_rows), 1000),
        )
        customer_ids = [int(r[0]) for r in cur.fetchall()]

        order_rows: list[tuple[int, float, str]] = []
        order_meta: list[tuple[int, list[tuple[int, int]]]] = []

        for cid in customer_ids:
            for _ in range(random.randint(1, 5)):
                chosen_products = random.sample(products, random.randint(1, 3))
                quantity_by_pid: list[tuple[int, int]] = []
                total_amount = 0.0
                for pid, _name, price in chosen_products:
                    qty = random.randint(1, 3)
                    quantity_by_pid.append((pid, qty))
                    total_amount += price * qty

                status = random.choice(["pending", "completed", "shipped"])
                order_rows.append((cid, round(total_amount, 2), status))
                order_meta.append((cid, quantity_by_pid))

        if not order_rows:
            customer_rows = []
            return

        execute_values(
            cur,
            "INSERT INTO orders (customer_id, amount, status) VALUES %s RETURNING id, customer_id",
            order_rows,
            page_size=min(len(order_rows), 5000),
        )
        inserted_orders = cur.fetchall()
        order_products_rows: list[tuple[int, int, int]] = []
        for (order_id, order_customer_id), (_, quantity_by_pid) in zip(
            ((int(r[0]), int(r[1])) for r in inserted_orders),
            order_meta,
        ):
            for pid, qty in quantity_by_pid:
                order_products_rows.append((order_id, pid, qty))

        if order_products_rows:
            execute_values(
                cur,
                "INSERT INTO order_products (order_id, product_id, quantity) VALUES %s",
                order_products_rows,
                page_size=min(len(order_products_rows), 5000),
            )

        customer_rows = []

    try:
        with conn:
            with conn.cursor() as cur:
                products = _bootstrap_products(cur)

                for i in range(num_customers):
                    customer_rows.append((fake.name(), f"user_{i}_{fake.uuid4()}@example.com"))
                    if len(customer_rows) >= batch_size:
                        flush(cur, products=products)
                flush(cur, products=products)
    finally:
        conn.close()


if __name__ == "__main__":
    generate_data(
        int(os.getenv("GEN_CUSTOMERS", "500000")),
        int(os.getenv("GEN_BATCH_SIZE", "5000")),
    )