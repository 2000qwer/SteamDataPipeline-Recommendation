from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pyodbc

def test_sql(*args, **kwargs):
    server = r"host.docker.internal\DATA_CENTER"  # Adjust server name
    username = "MichalM"
    password = "test"
    database = "MoviesDB"
    connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=yes;"
    
    try:
        print(f"Attempting to connect using: {connection_string}")
        conn = pyodbc.connect(connection_string, timeout=15)
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 * FROM MoviesDB..IMDB_directors")  # SQL query to fetch one row
        row = cursor.fetchone()
        
        if row:
            print(f"First row from IMDB_directors: {row}")
        else:
            print("No data found in the table.")

        cursor.close()
        conn.close()
    except pyodbc.Error as e:
        print(f"ODBC Error: {e}")
    except Exception as e:
        print(f"General Error: {e}")

# Define the DAG with its settings.
with DAG(
    dag_id="test_sql_server_connection_dag",
    schedule_interval="@once",
    default_args={
        "owner": "MichalM",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2024, 8, 12),
    },
    catchup=False
) as dag:

    # Task to test SQL Server connection.
    test_sql_task = PythonOperator(
        task_id="test_sql_connection",
        python_callable=test_sql
    )