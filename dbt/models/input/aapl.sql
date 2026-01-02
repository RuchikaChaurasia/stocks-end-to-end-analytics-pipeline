WITH src AS (
    SELECT *
    FROM {{ source('raw', 'market_data') }}
    WHERE SYMBOL = 'AAPL'
)
SELECT *
FROM src
