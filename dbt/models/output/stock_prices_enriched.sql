with base as (
    select *
    from {{ ref('stock_history') }}
),

with_returns as (
    select
        date,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        lag(close) over (partition by symbol order by date) as prev_close,
        (close - lag(close) over (partition by symbol order by date)) /
            lag(close) over (partition by symbol order by date) as daily_return
    from base
)

select * from with_returns

