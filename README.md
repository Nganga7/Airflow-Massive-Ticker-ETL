# Airflow Massive Ticker ETL

An Apache Airflow DAG that retrieves daily stock open/close aggregates from the Massive API and inserts them into PostgreSQL.

## Project files

- [`nganga_massive_dag.py`](./nganga_massive_dag.py) defines the extraction and load tasks and their Airflow schedule.
- [`requirements.txt`](./requirements.txt) lists the Python dependencies used by the DAG.
- `.env` can hold local environment variables. It is excluded from version control; do not commit API keys or database credentials.

## What the DAG does

The DAG is registered as `nganga_massive_tickers`. It runs once a day, starts on April 24, 2026, and does not backfill earlier scheduled runs (`catchup=False`).

1. **Extract:** Requests adjusted daily open/close aggregates for `AAPL`, `GOOGL`, `TSLA`, `NFLX`, and `AMZN` using the Massive REST client. The requested date is the execution machine's current date minus two days.
2. **Load:** Builds a pandas DataFrame, rounds `volume` to two decimal places, creates the configured PostgreSQL table if it does not exist, and inserts the ticker data.

The destination table has `ticker`, `open`, `high`, `low`, `close`, `volume`, and `ticker_date` columns, plus a `load_date` timestamp that defaults to the time of insertion. The script does not define a primary key or deduplicate records, so repeated runs can insert duplicate ticker/date rows.

## Requirements

The dependencies declared in [`requirements.txt`](./requirements.txt) are `massive`, `pandas`, `psycopg2-binary`, `python-dotenv` (`dotenv` in the file), and `airflow.sdk`. The file also lists `os` and `datetime`, which are Python standard-library modules.

The DAG imports its decorators from `airflow.sdk`; run it in a compatible Apache Airflow 3 environment. Install and configure Airflow using the official installation guidance for your Python version and platform, then install this project's listed requirements in the Airflow environment. Airflow's version constraints should be followed when installing packages.

## Configuration

Provide the following environment variables to the Airflow scheduler and workers (or place them in a local `.env` file when running in a context that loads it):

| Variable | Purpose |
| --- | --- |
| `API_KEY` | Massive API key |
| `DB_HOST` | PostgreSQL server hostname |
| `DB_NAME` | PostgreSQL database name |
| `DB_USER` | PostgreSQL username |
| `DB_PASSWORD` | PostgreSQL password |
| `DB_PORT` | PostgreSQL port |
| `TARGET_TABLE` | PostgreSQL table name for ticker rows |

Keep credentials out of source control. For deployed Airflow environments, use the deployment's secret or environment-variable management rather than relying on a developer's local `.env` file.

## Run with Airflow

1. Configure an Airflow 3 environment and install the project dependencies.
2. Set the required environment variables for the Airflow components that execute the DAG.
3. Make `nganga_massive_dag.py` available in the Airflow DAGs folder.
4. Start Airflow and enable `nganga_massive_tickers` in the Airflow UI or CLI.

The PostgreSQL database must be reachable from the Airflow task environment, and the configured user must be able to create the target table and insert rows.
