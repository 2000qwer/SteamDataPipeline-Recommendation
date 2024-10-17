from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.dbapi_hook import DbApiHook


# Define a function to test the PostgreSQL connection.
def test_postgres_connection(*args, **kwargs):
    conn_id = kwargs.get('conn_id', 'postgres_default')  # Default Airflow connection ID
    hook = DbApiHook.get_hook(conn_id)
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * from public.ab_user")
        version = cursor.fetchone()
        print(f"PostgreSQL version: {version}")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()



# Define the DAG with its settings.
with DAG(
    dag_id="test_postgres_connection_dag",
    schedule_interval="@once",  # Run once for testing
    default_args={
        "owner": "MichalM",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2024, 8, 12),
    },
    catchup=False
) as dag:

    # Task to test PostgreSQL connection.
    test_postgres_connection_task = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_connection,
        op_kwargs={"conn_id": "postgres_default"}  # Airflow connection ID
    )