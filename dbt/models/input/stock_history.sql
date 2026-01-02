-- models/input/stock_history.sql

with source_data as (
    select 
        DATE,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        SYMBOL
    from {{ source('raw', 'market_data') }}
    where SYMBOL in ({{ "'" ~ var('stock_symbols') | join("','") ~ "'" }})
),

filtered as (
    select *
    from source_data
    where date >= dateadd('day', - {{ var('lookback_days') }}, current_date)
)

select * from filtered
