with enriched as (
    select *
    from {{ ref('stock_prices_enriched') }}
)

select
    symbol,
    count(*) as total_days,
    avg(close) as avg_close,
    min(close) as min_close,
    max(close) as max_close,
    stddev(close) as volatility,
    avg(daily_return) as avg_daily_return
from enriched
group by symbol
order by symbol
