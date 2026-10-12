-- Educational unoptimized storage design for comparison.
-- Run manually (not initialized automatically).
CREATE TABLE IF NOT EXISTS fraud.transactions_baseline
(
    trans_num String,
    trans_date_trans_time DateTime,
    state String,
    category String,
    amt Decimal(12, 2),
    is_fraud Nullable(UInt8),
    raw_json String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO fraud.transactions_baseline
SELECT trans_num, trans_date_trans_time, toString(state), toString(category), amt, is_fraud, raw_json
FROM fraud.transactions;
