CREATE TABLE IF NOT EXISTS payments (
    id          BIGINT PRIMARY KEY,
    customer_id VARCHAR(128) NOT NULL
);
