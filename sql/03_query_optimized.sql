-- Read pre-aggregated states rather than scanning every transaction.
SELECT
    state,
    tupleElement(winner, 1) AS category,
    tupleElement(winner, 2) AS max_amount,
    tupleElement(winner, 3) AS trans_num
FROM
(
    SELECT state, argMaxMerge(winner_state) AS winner
    FROM fraud.max_transaction_by_state
    GROUP BY state
)
ORDER BY state;
