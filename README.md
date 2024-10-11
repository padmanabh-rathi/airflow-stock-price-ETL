# airflow-stock-price-ETL
This project automates the process of fetching stock price data using the Vantage API and loading it into a Snowflake data warehouse. Apache Airflow orchestrates the entire pipeline, ensuring the data is fetched, processed, and stored in Snowflake at scheduled intervals.

# Steps Followed
Data Fetching: Use Alpha Vantage API to fetch stock prices in JSON/CSV format.

Data Processing: Clean and format the data to fit Snowflake's schema.

Airflow DAG: Set up an Airflow DAG to automate data fetching, processing, and loading.

Data Load: Connect to Snowflake and load the cleaned stock price data into the target table.

Scheduled Runs: DAG scheduled to run at regular intervals (e.g., daily) to keep the data up to date.

# Tech Stack
Apache Airflow: Workflow orchestration.

Alpha Vantage API: Stock price data source.

Snowflake: Data warehouse for storage.

Python: For scripts and API interaction.

GitHub: Version control for code and documentation.

