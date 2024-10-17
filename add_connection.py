from airflow.models import Connection
from airflow.utils.session import provide_session
from airflow.settings import Session

@provide_session
def add_connection(session=None):
    # Define your connection details
    conn_id = 'my_sql_server_connection'  # The connection ID to be used in Airflow
    conn_type = 'mssql'  # Connection type for Microsoft SQL Server
    host = '127.0.0.1'  # SQL Server hostname or IP address
    schema = 'multi_category_store'  # Database name
    login = 'MichalM'  # Username
    password = '!Everest2021@!'  # Password
    port = 50335  # Port number you found

    # Create a new Connection object
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        schema=schema,
        login=login,
        password=password,
        port=port
    )

    # Add the connection to the session
    session.add(conn)
    session.commit()

if __name__ == "__main__":
    add_connection()