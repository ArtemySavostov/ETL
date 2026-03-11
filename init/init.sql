-- Таблица покупателей (родительская)
CREATE TABLE customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW(),
    deleted_at  TIMESTAMP
);

-- Таблица заказов (дочерняя)
CREATE TABLE orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INT REFERENCES customers(id) ON DELETE CASCADE,
    amount       NUMERIC(10, 2) NOT NULL,
    status       VARCHAR(50) DEFAULT 'pending',
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW(),
    deleted_at   TIMESTAMP
);

-- Таблица товаров
CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    deleted_at  TIMESTAMP
);

-- Связующая таблица для связи многие-ко-многим orders - products
CREATE TABLE order_products (
    order_id    INT REFERENCES orders(id) ON DELETE CASCADE,
    product_id  INT REFERENCES products(id) ON DELETE CASCADE,
    quantity    INT NOT NULL DEFAULT 1,
    created_at  TIMESTAMP DEFAULT NOW(),
    deleted_at  TIMESTAMP,
    PRIMARY KEY (order_id, product_id)
);

-- Индексы для быстрой выборки новых и удалённых записей
CREATE INDEX idx_orders_updated ON orders(updated_at);
CREATE INDEX idx_orders_deleted ON orders(deleted_at);
CREATE INDEX idx_customers_created ON customers(created_at);
CREATE INDEX idx_customers_updated ON customers(updated_at);
CREATE INDEX idx_customers_deleted ON customers(deleted_at);
CREATE INDEX idx_products_deleted ON products(deleted_at);


INSERT INTO customers (name, email) VALUES
    ('Иван Петров',   'ivan@example.com'),
    ('Мария Сидорова','maria@example.com');

INSERT INTO products (name, price) VALUES
    ('Ноутбук', 75000.00),
    ('Мышь', 1500.00),
    ('Монитор', 35000.00);

INSERT INTO orders (customer_id, amount, status) VALUES
    (1, 76500.00, 'completed'),
    (1, 1500.00,  'pending'),
    (2, 35000.00, 'shipped');

INSERT INTO order_products (order_id, product_id, quantity) VALUES
    (1, 1, 1),
    (1, 2, 1),
    (2, 2, 1),
    (3, 3, 1);

DO $$
BEGIN
    FOR i IN 1..500000 LOOP
        INSERT INTO customers (name, email) VALUES 
            ('Покупатель ' || i, 'purchaser' || i || '@example.com');
    END LOOP;
END $$;