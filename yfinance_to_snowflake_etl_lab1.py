from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import yfinance as yf
import pandas as pd
import json

SYMBOLS = Variable.get("symbols_json", default_var='["AAPL"]', deserialize_json=True)
LOOKBACK_DAYS = int(Variable.get("lookback_days", default_var="180"))

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract_stock_data(symbol):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=f"{LOOKBACK_DAYS}d")
        df.reset_index(inplace=True)
        if not df.empty and "Date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["Date"]):
            df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
            return df.to_json(orient="records")
        else:
            print(f"No valid data for {symbol}")
            return json.dumps([])
    except Exception as e:
        print(f"Error extracting {symbol}: {str(e)}")
        return json.dumps([])

@task
def transform_stock_data(json_df, symbol):
    try:
        df = pd.read_json(json_df, convert_dates=False)
        if df.empty:
            print(f"No data to transform for {symbol}")
            return json.dumps([])
        df['Date'] = df['Date'].astype(str)
        df = df.rename(
            columns={
                "Date": "trade_date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "trade_volume",
            }
        )
        df["symbol"] = symbol
        df = df[["symbol", "trade_date", "open", "close", "low", "high", "trade_volume"]]
        print(f"Transformed {len(df)} rows for {symbol}")
        return df.to_json(orient="records")
    except Exception as e:
        print(f"Error transforming {symbol}: {str(e)}")
        return json.dumps([])

@task
def merge_transformed_data(*json_datasets):
    merged = []
    for js in json_datasets:
        merged.extend(json.loads(js))
    print(f"Merged {len(merged)} rows from all symbols")
    return json.dumps(merged)

@task
def load(records_json):
    target_table = "USER_DB_LIZARD.RAW.MARKET_DATA"
    conn = return_snowflake_conn()
    try:
        conn.execute("BEGIN;")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol VARCHAR,
                trade_date DATE,
                open NUMBER,
                close NUMBER,
                low NUMBER,
                high NUMBER,
                trade_volume NUMBER,
                PRIMARY KEY (trade_date, symbol)
            );
        """)
        conn.execute(f"DELETE FROM {target_table}")
        records = json.loads(records_json)
        if not records:
            print("No records to load.")
            conn.execute("COMMIT;")
            return
        df = pd.DataFrame(records)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        insert_sql = f"""
            INSERT INTO {target_table} 
            (SYMBOL, DATE, OPEN, CLOSE, LOW, HIGH, VOLUME)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        data = [
            (
                row["symbol"],
                row["trade_date"].strftime("%Y-%m-%d"),
                row["open"],
                row["close"],
                row["low"],
                row["high"],
                int(row["trade_volume"]),
            )
            for _, row in df.iterrows()
        ]
        conn.executemany(insert_sql, data)
        conn.execute("COMMIT;")
        print(f"Loaded {len(df)} records into {target_table}")
    except Exception as e:
        conn.execute("ROLLBACK;")
        print(f"Error loading data: {str(e)}")
        raise
    finally:
        conn.close()
with DAG(
    dag_id="yfinance_sql_code_lab1",
    start_date=datetime(2025, 10, 1),
    schedule="30 1 * * *",  
    catchup=False,
    tags=["ETL", "Stock", "Snowflake"],
) as dag:
    symbols = SYMBOLS

    extracted = []
    transformed = []
    for symbol in symbols:
        ext = extract_stock_data(symbol)
        trn = transform_stock_data(ext, symbol)
        extracted.append(ext)
        transformed.append(trn)
    merged_json = merge_transformed_data(*transformed)
    load_task = load(merged_json)
    for i in range(len(symbols)):
        extracted[i] >> transformed[i] >> merged_json
    merged_json >> load_task
