CREATE TABLE IF NOT EXISTS fraud.labeled_transactions
(
 transaction_time DateTime,
 merch String,
 cat_id String,
 amount Float64,
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
 merchant_lon Float64,
 is_fraud UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (transaction_time, cat_id);

CREATE TABLE IF NOT EXISTS fraud.training_state
(id UInt8, next_month Date)
ENGINE = ReplacingMergeTree
ORDER BY id;

CREATE TABLE IF NOT EXISTS fraud.scoring_results
(transaction_id Nullable(String), score Float64, fraud_flag UInt8,
 model_version String, received_at DateTime DEFAULT now())
ENGINE = MergeTree
ORDER BY (received_at, model_version);

CREATE TABLE IF NOT EXISTS fraud.scoring_kafka
(transaction_id Nullable(String), score Float64, fraud_flag UInt8,
 model_version String)
ENGINE = Kafka
SETTINGS kafka_broker_list='kafka:29092', kafka_topic_list='fraud_scoring',
 kafka_group_name='clickhouse_scoring', kafka_format='JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.mv_scoring
TO fraud.scoring_results AS
SELECT transaction_id, score, fraud_flag, model_version, now() AS received_at
FROM fraud.scoring_kafka;
