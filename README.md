# Stocks End-to-End Analytics Pipeline

Airflow, Snowflake, and dbt pipeline built as coursework (SJSU DATA 226). It pulls daily stock prices, loads them into Snowflake, forecasts prices with Snowflake ML, builds analytics tables with dbt, and runs data tests.

## What runs

| DAG | Schedule | What it does |
|---|---|---|
| `yfinance_sql_code_lab1` | daily 01:30 | Pulls prices for the symbols in the Airflow Variable `symbols_json` using the `yfinance` library and loads `RAW.MARKET_DATA` in a transaction (full refresh) |
| `TrainPredict` | daily 02:30 | Trains a Snowflake ML forecast model on `RAW.MARKET_DATA`, forecasts 7 periods with a 95% prediction interval, writes `RAW.MARKET_DATA_FORECAST_LAB1`, and unions history and forecast into `ANALYTICS.MARKET_DATA_LAB1` |
| `BuildELT_dbt` | daily 03:30 | Runs `dbt run`, `dbt test`, `dbt snapshot` |

The three DAGs are spaced by clock time. They are not wired to wait for each other, so a slow load could leave the later DAGs reading yesterday's data.

dbt models: `stock_history` (input, filtered by the `stock_symbols` and `lookback_days` vars), `stock_prices_enriched` (previous close and daily return), `stock_summary` (per-symbol average, min, max, volatility), plus a snapshot of the summary. Tests: `not_null`, `unique`, and `accepted_values` on the symbol.

## Setup
1. `docker compose up -d` starts Airflow (compose file included).
2. In Airflow, create a Snowflake connection named `snowflake_conn`, and Variables `symbols_json` (for example `["AAPL","MSFT","ORCL","IBM"]`) and `lookback_days`.
3. Copy `dbt/profiles.yml.example` to `dbt/profiles.yml`. It reads credentials from environment variables, and the `BuildELT_dbt` DAG sets them from `snowflake_conn`. `dbt/profiles.yml` is gitignored.
4. The database name `USER_DB_LIZARD` is hard-coded in the DAG and in `dbt/models/source.yml` (a course training account). Change it for your own account.

## Known limitations
- The load is a full refresh (delete and reload). It now fails if nothing is extracted, but if only some symbols fail, the table is still replaced with partial data.
- Extract errors are logged and return empty data instead of failing the extract task.
- The symbol list must match in three places: the `symbols_json` Variable, the dbt `stock_symbols` var, and the dbt `accepted_values` test. A symbol outside the test list (for example TSLA) makes `dbt test` fail.
- `TrainPredict` defaults to `["TSLA"]` if the Variable is missing, while the load DAG defaults to `["AAPL"]`.
- Market data comes from the unofficial `yfinance` library. Check the data provider's terms before using it beyond learning.
- Coursework project: no alerting, no incremental loads.

## Security
No credentials are stored in this repo. Do not commit `profiles.yml`, `.env`, or `config/airflow.cfg`.
