SELECT us_state, tupleElement(winner, 1) AS cat_id, tupleElement(winner, 2) AS max_amount
FROM (SELECT us_state, argMax(tuple(toString(cat_id), amount),
tuple(amount, transaction_time, merch, toString(cat_id))) AS winner
FROM fraud.transactions GROUP BY us_state) ORDER BY us_state;
