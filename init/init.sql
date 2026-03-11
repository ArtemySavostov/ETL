-- Таблица покупателей (родительская)
CREATE TABLE customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Таблица заказов (дочерняя)
CREATE TABLE orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INT REFERENCES customers(id) ON DELETE CASCADE,
    product      VARCHAR(200) NOT NULL,
    amount       NUMERIC(10, 2) NOT NULL,
    status       VARCHAR(50) DEFAULT 'pending',
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- Индекс для быстрой выборки новых записей
CREATE INDEX idx_orders_updated ON orders(updated_at);
CREATE INDEX idx_customers_created ON customers(created_at);

-- Тестовые данные
INSERT INTO customers (name, email) VALUES
    ('Иван Петров',   'ivan@example.com'),
    ('Мария Сидорова','maria@example.com');

INSERT INTO orders (customer_id, product, amount, status) VALUES
    (1, 'Ноутбук',    75000.00, 'completed'),
    (1, 'Мышь',        1500.00, 'pending'),
    (2, 'Монитор',    35000.00, 'shipped');

DO $$
BEGIN
    FOR i IN 1..500000 LOOP
        INSERT INTO customers (name, email) VALUES 
            ('Покупатель ' || i, 'purchaser' || i || '@example.com');
            
        INSERT INTO orders (customer_id, product, amount, status) VALUES
            (i, 'Товар ' || i, RANDOM() * 100, 'pending');
    END LOOP;
END $$;