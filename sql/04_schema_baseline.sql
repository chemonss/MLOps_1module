-- Неоптимизированная таблица для сравнения с основной.

CREATE TABLE IF NOT EXISTS fraud.transactions_baseline
(
    transaction_time DateTime,
    merch String,
    cat_id String,
    amount Decimal(18, 2),
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt64,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64
)
ENGINE = MergeTree
ORDER BY tuple();

-- Копируем данные из основной таблицы.
INSERT INTO fraud.transactions_baseline
SELECT
    transaction_time,
    merch,
    toString(cat_id),
    amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    toString(us_state),
    post_code,
    lat,
    lon,
    population_city,
    jobs,
    merchant_lat,
    merchant_lon
FROM fraud.transactions;
