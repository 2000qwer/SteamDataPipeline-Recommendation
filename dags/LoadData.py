from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
 



def movies_table_data():
    server = r"host.docker.internal\DATA_CENTER"  # Adjust server name
    username = "MichalM"
    password = "test"
    database = "MoviesDB"
    connection_string_data_center = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=yes;"
    
    try:
        print(f"Attempting to connect using: {connection_string_data_center}")
        conn = pyodbc.connect(connection_string_data_center, timeout=15)
        cursor = conn.cursor()
        cursor.execute("SELECT * from [dbo].[IMDB_movies]")  # SQL query to fetch data
        rows = cursor.fetchall()
        


        if rows:
            print(f"Retrieved {len(rows)} rows from IMDB_movies.")
            columns = [column[0] for column in cursor.description]  # Get column names
            df = pd.DataFrame.from_records(rows, columns=columns)   # Convert to DataFrame
            cursor.close()  # Closing the cursor
            conn.close()    # Closing the connection
            return df
        else:
            cursor.close()  # Closing the cursor
            conn.close()    # Closing the connection
            raise ValueError("No data found in the table.") 


    except pyodbc.Error as e:
        raise RuntimeError(f"ODBC Error: {e}")
    except Exception as e:
        raise RuntimeError(f"General Error: {e}")


def LoadDataToPostgresAirflowBD():
    df = movies_table_data()  # Retrieve data from SQL Server
    
    if not df.empty:
        try:
            # Fetching the connection using connection ID
            connection = BaseHook.get_connection('postgres_airflow')

            # Create SQLAlchemy engine using the connection details from Airflow
            engine = create_engine(f'postgresql+psycopg2://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}')

            df.to_sql('movies', engine, if_exists='replace', index=False)  # Load DataFrame to Postgres
            print("Data loaded into Postgres successfully.")
        except Exception as e:
            print(f"Error loading data into Postgres: {e}")
    else:
        print("No data to load into Postgres.")


# Define the DAG with its settings.
with DAG(
    dag_id="Load_movies_table_into_postgress",
    schedule_interval="@once",  # Run once for testing
    default_args={
        "owner": "MichalM",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2024, 8, 20),
    },
    catchup=False
) as dag:

    # Task to test PostgreSQL connection.
    test_postgres_connection_task = PythonOperator(
        task_id="Load_movies_table_postgres",
        python_callable=LoadDataToPostgresAirflowBD

    )