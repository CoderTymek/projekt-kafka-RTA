CREATE TABLE IF NOT EXISTS crypto_alerts (
    timestamp TIMESTAMP,
    symbol VARCHAR(20),
    current_price DECIMAL(20,8),
    average_price DECIMAL(20,8),
    change_percent DECIMAL(10,2)
);