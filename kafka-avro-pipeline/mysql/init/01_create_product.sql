CREATE TABLE IF NOT EXISTS product (
    ID INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(12, 2) NOT NULL,
    last_updated DATETIME NOT NULL
);

INSERT INTO product (ID, name, category, price, last_updated) VALUES
(1, 'Phone', 'Category A', 100.00, '2026-09-20 10:00:00'),
(2, 'Laptop', 'Category B', 200.00, '2026-09-20 10:00:00'),
(3, 'Tablet', 'Category A', 300.00, '2026-09-20 10:00:00'),
(4, 'Watch', 'Category C', 150.00, '2026-09-20 10:05:00'),
(5, 'Camera', 'Category B', 250.00, '2026-09-20 10:05:00'),
(6, 'Speaker', 'Category A', 80.00, '2026-09-20 10:05:00')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    category = VALUES(category),
    price = VALUES(price),
    last_updated = VALUES(last_updated);
