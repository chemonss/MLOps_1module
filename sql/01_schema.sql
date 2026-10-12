CREATE DATABASE IF NOT EXISTS fraud;

-- Persistent table with the original column names.
CREATE TABLE IF NOT EXISTS fraud.transactions
(
    transaction_time DateTime CODEC(Delta, ZSTD(1)),
    merch LowCardinality(String),
    cat_id LowCardinality(String),
    amount Decimal(18, 2) CODEC(ZSTD(3)),
    name_1 String,
    name_2 String,
    gender LowCardinality(String),
    street String,
    one_city LowCardinality(String),
    us_state LowCardinality(String),
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt64,
    jobs LowCardinality(String),
    merchant_lat Float64,
    merchant_lon Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, cat_id, transaction_time)
SETTINGS index_granularity = 8192;

-- Kafka source: the Python csv.DictReader producer sends all values as JSON strings.
CREATE TABLE IF NOT EXISTS fraud.transactions_kafka
(
    transaction_time String,
    merch String,
    cat_id String,
    amount String,
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat String,
    lon String,
    population_city String,
    jobs String,
    merchant_lat String,
    merchant_lon String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'fraud_transactions',
    kafka_group_name = 'clickhouse_fraud',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- Materialize the incoming records into a MergeTree table.
CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.mv_transactions
TO fraud.transactions AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch,
    cat_id,
    toDecimal64(amount, 2) AS amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    toFloat64(lat) AS lat,
    toFloat64(lon) AS lon,
    toUInt64(population_city) AS population_city,
    jobs,
    toFloat64(merchant_lat) AS merchant_lat,
    toFloat64(merchant_lon) AS merchant_lon
FROM fraud.transactions_kafka;

-- Store incremental argMax aggregate states for each US state.
CREATE TABLE IF NOT EXISTS fraud.max_transaction_by_state
(
    us_state LowCardinality(String),
    winner_state AggregateFunction(
        argMax,
        Tuple(String, Decimal(18, 2), DateTime, String),
        Tuple(Decimal(18, 2), DateTime, String, String)
    )
)
ENGINE = AggregatingMergeTree
ORDER BY us_state;

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.mv_max_transaction_by_state
TO fraud.max_transaction_by_state AS
SELECT
    us_state,
    argMaxState(
        tuple(cat_id, toDecimal64(amount, 2), parseDateTimeBestEffort(transaction_time), merch),
        tuple(toDecimal64(amount, 2), parseDateTimeBestEffort(transaction_time), merch, cat_id)
    ) AS winner_state
FROM fraud.transactions_kafka
GROUP BY us_state;
