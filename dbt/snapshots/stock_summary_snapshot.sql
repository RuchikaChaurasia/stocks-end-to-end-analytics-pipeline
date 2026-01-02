{% snapshot stock_summary_snapshot %}

{{ config(
    target_schema='SNAPSHOT',
    unique_key='symbol',
    strategy='check',
    check_cols=['total_days', 'avg_close', 'volatility', 'avg_daily_return']
) }}

select
    symbol,
    total_days,
    avg_close,
    min_close,
    max_close,
    volatility,
    avg_daily_return
from {{ ref('stock_summary') }}

{% endsnapshot %}

