
CREATE DATABASE IF NOT EXISTS transactions_db;

CREATE VIEW IF NOT EXISTS transactions_db.transactions AS
SELECT
    transaction_time,
    merch,
    cat_id,
    amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    lat,
    lon,
    population_city,
    jobs,
    merchant_lat,
    merchant_lon,
    is_fraud AS target
FROM fraud.labeled_transactions;
