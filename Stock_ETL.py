
# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def return_last_90d_price(symbol):
    vantage_api_key = Variable.get("vantage_api_key")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
    r = requests.get(url)
    data = r.json()

    results = []
    today = datetime.today()
    ninety_days_ago = today - timedelta(days=90)

    for d in data["Time Series (Daily)"]:
        date_obj = datetime.strptime(d, "%Y-%m-%d")
        if date_obj >= ninety_days_ago:
            stock_info = data["Time Series (Daily)"][d]
            stock_info["date"] = d
            results.append(stock_info)

    return results



@task
def load_stock_data(con, stock_data):
    target_table = "STOCK_DB.RAW_DATA.STOCK_PRICES"

    try:
        con.execute("BEGIN;")

        con.execute(f'''
        CREATE OR REPLACE TABLE {target_table} (
            date DATE PRIMARY KEY,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            symbol STRING
        );
        ''')

        symbol = 'NVDA'
        today = datetime.today()
        ninety_days_ago = today - timedelta(days=90)

        for record in stock_data:
            if datetime.strptime(record['date'], '%Y-%m-%d') >= ninety_days_ago:
                merge_sql = f"""
                MERGE INTO {target_table} AS target
                USING (SELECT '{record['date']}' AS date, {record['1. open']} AS open, {record['2. high']} AS high, {record['3. low']} AS low, {record['4. close']} AS close, {record['5. volume']} AS volume, '{symbol}' AS symbol) AS source
                ON target.date = source.date
                WHEN MATCHED THEN
                    UPDATE SET target.open = source.open, target.high = source.high, target.low = source.low, target.close = source.close, target.volume = source.volume, target.symbol = source.symbol
                WHEN NOT MATCHED THEN
                    INSERT (date, open, high, low, close, volume, symbol)
                    VALUES (source.date, source.open, source.high, source.low, source.close, source.volume, source.symbol);
                """
                con.execute(merge_sql)

        con.execute("COMMIT;")
        print("Data successfully loaded and committed.")

    except Exception as e:
        con.execute("ROLLBACK;")
        print("Transaction rolled back due to an error:", e)
        raise e


with DAG(
    dag_id = 'StockPrice',
    start_date = datetime(2024,9,21),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    
    cur = return_snowflake_conn()
    data = return_last_90d_price('NVDA')
    load_stock_data(cur, data)
