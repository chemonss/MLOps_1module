CREATE TABLE IF NOT EXISTS fraud.transactions_baseline AS fraud.transactions
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO fraud.transactions_baseline SELECT * FROM fraud.transactions;
