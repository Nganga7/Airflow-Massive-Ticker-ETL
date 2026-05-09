from massive import RESTClient 
from datetime import datetime,timedelta
import pandas as pd
import psycopg2 
from psycopg2 import sql
from psycopg2 import extras
from airflow.sdk import dag,task
import os 
from dotenv import load_dotenv 

load_dotenv()


#Database Configuration:

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
TARGET_TABLE = os.getenv("TARGET_TABLE")
API_KEY = os.getenv("API_KEY")


@dag(
   dag_id="nganga_massive_tickers",
   start_date=datetime(2026,4,24),
   schedule=timedelta(days=1),
   catchup=False
)

def nganga_massive_tickers():

	#Extract from Massive
	@task()
	def extract_tickers():

		#Extract from Massive
		client = RESTClient(API_KEY)
		symbols = ["AAPL","GOOGL","TSLA","NFLX","AMZN"]
		data_list = []
		yesterday = datetime.today() - timedelta(days=2)
		yesterday_formatted = yesterday.strftime('%Y-%m-%d')

		for ticker in symbols:
			try:
				request = client.get_daily_open_close_agg(ticker,yesterday_formatted,adjusted="true",)
				data_list.append({
					"ticker": ticker,
					"open": request.open,
					"high": request.high,
					"low": request.low,
					"close": request.close,
					"volume": request.volume,
					"ticker_date": yesterday_formatted
				})

			except Exception as e:
				print(f"Error fetching tickers: {e}")

		return data_list

    #Transform and load Tickers
	@task()
	def transform_load_tickers(raw_tickers):

		#Transform data_list to a DataFrame
		data_df = pd.DataFrame(raw_tickers)

		data_df["volume"] = data_df["volume"].round(2)

		#Connect to Postgres 
		conn = psycopg2.connect(
                host = DB_HOST,
                dbname = DB_NAME,
                user = DB_USER,
                password = DB_PASSWORD,
                port = DB_PORT
                )
		
		cur = conn.cursor()

		try:
			print(f"Successfully connected to database: {DB_NAME} with user: {DB_USER}")

		except psycopg2.Error as e:
			print(f"Error in connection: {e}")


		#Create table on Postgres 
		##data_df.columns.to_list

		create_table = sql.SQL("""
		CREATE TABLE IF NOT EXISTS {table} (                       
			ticker VARCHAR(10), 
			open NUMERIC(10,2), 
			high NUMERIC(10,2), 
			low NUMERIC(10,2), 
			close NUMERIC(10,2),
			volume NUMERIC(10,2),
			ticker_date DATE, 
			load_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP); 
		""").format(table = sql.Identifier(TARGET_TABLE))

		try:
			cur.execute(create_table)
			conn.commit()
			print(f"Successfully created table: {TARGET_TABLE} or already exists")

		except psycopg2.Error as e:
			print(f"Error in syntax: {e}")


		#Load data to Postrgres 
		##data_df.columns.to_list

		columns_to_load = ['ticker', 'open', 'high', 'low', 'close', 'volume','ticker_date']
		data_df = data_df[columns_to_load]

		values_to_load = [tuple(row) for row in data_df.values]

		insert_query = sql.SQL("""
					INSERT INTO {table} ({columns})
					VALUES %s
		""").format(table = sql.Identifier(TARGET_TABLE),
					columns = sql.SQL(', ').join(map(sql.Identifier,columns_to_load)))

		try:
			extras.execute_values(cur, insert_query, values_to_load)
			conn.commit()
			print(f"Sucessfully inserted {len(values_to_load)} rows in table: {TARGET_TABLE}")

		except psycopg2.Error as e:
			conn.rollback()
			print(f"Check syntax error: {e}")
			
			
	#Define Task Dependencies
	extracted = extract_tickers()
	
	Loaded = transform_load_tickers(extracted)
	
dag = nganga_massive_tickers()