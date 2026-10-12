-- Оптимизированный запрос.
-- Читаем предварительно агрегированные данные,
-- не перебирая все транзакции.

SELECT
    us_state,
    tupleElement(winner, 1) AS cat_id,
    tupleElement(winner, 2) AS max_amount
FROM
(
    SELECT
        us_state,
        argMaxMerge(winner_state) AS winner
    FROM fraud.max_transaction_by_state
    GROUP BY us_state
)
ORDER BY us_state;
