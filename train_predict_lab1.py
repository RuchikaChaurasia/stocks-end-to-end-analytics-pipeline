from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


# ---------- Helper: get Snowflake cursor ----------
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()

# ---------- Task 1: Train model ----------
@task
def train(train_input_table: str,
          train_view: str,
          forecast_function_name: str,
          symbols: list):
    """
    1. Create a training view with TRADE_DATE, CLOSE, SYMBOL
    2. Create / replace a Snowflake ML FORECAST model on that view
    3. Show evaluation metrics
    """

    cur = get_snowflake_cursor()

    # Build dynamic SYMBOL filter from Airflow Variable
    if symbols:
        symbols_filter = ",".join([f"'{s}'" for s in symbols])
        where_clause = f"WHERE SYMBOL IN ({symbols_filter})"
    else:
        # If no symbols variable is set, use all symbols
        where_clause = ""

    # View: convert DATE -> TIMESTAMP and rename to TRADE_DATE
    create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS
        SELECT
            TO_TIMESTAMP_NTZ(DATE) AS TRADE_DATE,
            CLOSE,
            SYMBOL
        FROM {train_input_table}
        {where_clause};
    """

    # Snowflake ML FORECAST model
    create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA      => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME  => 'SYMBOL',
            TIMESTAMP_COLNAME => 'TRADE_DATE',
            TARGET_COLNAME  => 'CLOSE',
            CONFIG_OBJECT   => {{ 'ON_ERROR': 'SKIP' }}
        );
    """

    try:
        cur.execute("BEGIN;")
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Optional: show model evaluation metrics
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
        cur.execute("COMMIT;")
        print("Training completed successfully.")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error in train(): {e}")
        raise
    finally:
        cur.close()


# ---------- Task 2: Predict & final table ----------
@task
def predict(forecast_function_name: str,
            forecast_table: str,
            train_input_table: str,
            final_table: str):
    """
    1. Call model!FORECAST() to generate predictions.
    2. Save forecasts into RAW.<forecast_table>.
    3. Union historical + forecast into ANALYTICS.<final_table>.
    """

    cur = get_snowflake_cursor()

    # 2a. Make predictions & store into forecast_table
    make_prediction_sql = f"""
    BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT       => {{ 'prediction_interval': 0.95 }}
        );
        LET x := SQLID;

        CREATE OR REPLACE TABLE {forecast_table} AS
        SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;
    """

    # 2b. Create final table with history + forecasts
    # RAW.MARKET_DATA columns: DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, SYMBOL, CREATED_AT
    # forecast_table columns: SERIES, TS, FORECAST, LOWER_BOUND, UPPER_BOUND
    create_final_table_sql = f"""
    CREATE OR REPLACE TABLE {final_table} AS
    -- historical data
    SELECT
        SYMBOL,
        DATE,
        CLOSE AS ACTUAL,
        NULL  AS FORECAST,
        NULL  AS LOWER_BOUND,
        NULL  AS UPPER_BOUND
    FROM {train_input_table}

    UNION ALL

    -- forecasts
    SELECT
        REPLACE(SERIES, '"', '') AS SYMBOL,
        TS                       AS DATE,
        NULL                     AS ACTUAL,
        FORECAST,
        LOWER_BOUND,
        UPPER_BOUND
    FROM {forecast_table};
    """

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
        print("Prediction & final table creation completed successfully.")
    except Exception as e:
        print(f"Error in predict(): {e}")
        raise
    finally:
        cur.close()


# ---------- DAG Definition ----------
with DAG(
    dag_id="TrainPredict",
    start_date=datetime(2025, 9, 29),
    schedule="30 2 * * *",   # every day at 02:30
    catchup=False,
    tags=["ML", "ETL"],
) as dag:

    # Tables & function names
    train_input_table = "RAW.MARKET_DATA"                 # from ETL DAG
    train_view = "MARKET_DATA_VIEW_LAB1"                  # training view
    forecast_table = "RAW.MARKET_DATA_FORECAST_LAB1"      # table for forecasts
    final_table = "ANALYTICS.MARKET_DATA_LAB1"            # final union table
    forecast_function_name = "RAW.PREDICT_STOCK_PRICE_LAB1"

    # Use the SAME symbols variable as ETL
    symbols = Variable.get(
        "symbols_json",
        default_var='["TSLA"]',
        deserialize_json=True
    )

    train_task = train(train_input_table, train_view, forecast_function_name, symbols)
    predict_task = predict(forecast_function_name, forecast_table, train_input_table, final_table)

    train_task >> predict_task
