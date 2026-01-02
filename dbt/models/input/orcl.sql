WITH src AS (
    SELECT *
    FROM {{ source('raw', 'market_data') }}
    WHERE SYMBOL = 'ORCL'
)
SELECT *
FROM src
