WITH src AS (
    SELECT *
    FROM {{ source('raw', 'market_data') }}
    WHERE SYMBOL = 'IBM'
)
SELECT *
FROM src
