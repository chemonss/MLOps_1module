WITH source_data AS (
    SELECT *
    FROM {{ source('transactions_db', 'transactions') }}
),

cleaned AS (
    SELECT
        transaction_time,
        toDate(transaction_time) AS transaction_date,
        toHour(transaction_time) AS transaction_hour,
        toDayOfWeek(transaction_time) AS day_of_week,

        trimBoth(merch) AS merch,
        trimBoth(cat_id) AS cat_id,
        toFloat64(amount) AS amount,

        trimBoth(name_1) AS name_1,
        trimBoth(name_2) AS name_2,
        upper(trimBoth(gender)) AS gender,
        upper(trimBoth(us_state)) AS us_state,

        lat,
        lon,
        merchant_lat,
        merchant_lon,

        toUInt8(target) AS target,

        {{ amount_bucket('amount') }} AS amount_bucket

    FROM source_data
)

SELECT *
FROM cleaned
