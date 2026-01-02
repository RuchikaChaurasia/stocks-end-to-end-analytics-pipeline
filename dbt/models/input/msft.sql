WITH src AS (
    SELECT *
    FROM {{ source('raw', 'market_data') }}
    WHERE SYMBOL = 'MSFT'
)
SELECT *
FROM src
