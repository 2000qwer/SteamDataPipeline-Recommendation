import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import pyodbc
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
import urllib
import sqlalchemy.exc

#to cahnge before sending in the github
AWS_ACCESS_KEY_ID = 'test'
AWS_SECRET_ACCESS_KEY = 'test'
BUCKET_NAME = 'test'


#Gry - 
class S3Manager:
    def __init__(self, access_key_id, secret_access_key, bucket_name):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        )
        self.bucket_name = bucket_name

    def upload_data_from_url(self, url, s3_key):
        try:
            # Fetch data from the URL
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Upload data to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=response.content,
                ContentType='application/json'
            )
            print(f"Data from {url} uploaded to {self.bucket_name}/{s3_key}")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {str(e)}")
        except Exception as e:
            print(f"Error uploading data to S3: {str(e)}")
    def upload_data(self, data, s3_key):
        try:
            # Upload the data to S3
            self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=data)
            print(f"Successfully uploaded {s3_key} to S3.")
        except Exception as e:
            print(f"Error uploading data to S3: {str(e)}")
    def load_data_from_bucket(self, s3_key):
        try:
            # Download the JSON file from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            json_data = response['Body'].read().decode('utf-8')

            # Load the JSON data into a Python list
            data = json.loads(json_data)

            return data

        except Exception as e:
            print(f"Error loading data from S3: {str(e)}")
            return None




# Replace these with your actual AWS credentials and bucket name
s3_manager = S3Manager(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME)

# def load_steam_users_to_df():
#     data = s3_manager.load_data_from_bucket('steamUsers.json')
#     df = pd.DataFrame(data)

#     return df.tail()





#Zweryfukowanie na jupyterze więc zgrac dane z s3 do pliku csv pobrac je na jupyter przez df i zweryfikować przesyłanie danych na serwer sql, więc nastpenym elementem bedzie validacja obustronna definowanie typów column



##inserting data into table mssql and check the id if everytginid correct then insert again the data so will be knownu if id is not distincted and inserted with new data in it

def steam_users():
    
    server = r"host.docker.internal\DATA_CENTER"  # Adjust server name
    username = "MichalM"
    password = "test"
    database = "SteamDB"
    driver = "ODBC+Driver+18+for+SQL+Server"
    # connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=yes;"
    
    # data = s3_manager.load_data_from_bucket('steamUsers.json')
    # df = pd.DataFrame(data)

    # Build the connection string for SQLAlchemy
    connection_string = f"mssql://{username}:{urllib.parse.quote_plus(password)}@{server}/{database}?driver={driver}&Encrypt=no&TrustServerCertificate=yes"

    engine = create_engine(connection_string)
    con = engine.connect()


    data = s3_manager.load_data_from_bucket('steamUsers.json')
    df = pd.DataFrame(data)

    #IDS are range in this case, so values from maximum in current data 
    last_steam_id = max(df['steamid'].astype('int64'))


    start_steam_id = last_steam_id + 1 
    number_of_ids = 200
    api_key = "test"

    # Container for all user summary URLs
    users = []

    for i in range(number_of_ids):
        # Generate the URL for each Steam ID
        steam_id = str(start_steam_id + i)
        url = f'https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={api_key}&steamids={steam_id}'
        
        print("row number:", str(i))
        # Fetch data from the URL
        response = requests.get(url)
        if response.status_code == 200:
            # Parse the JSON response and append the player data to the users list
            player_data = response.json().get("response", {}).get("players", [])
            for player in player_data:
                users.append(player)
        else:
            print(f"Failed to fetch data for Steam ID: {steam_id}")
        


    users_json = json.dumps(users, indent=4)
    users_list = json.loads(users_json)
    df_uploaded = pd.DataFrame(users_list)

    # Combine existing and new data
    df_final = pd.concat([df, df_uploaded], ignore_index=True)

    # Optionally upload the combined DataFrame back to S3
    # df_final.to_csv('combined_steam_users.csv', index=False)  # Save as CSV if needed
 

    print("last id in concated file " + str(max(df_final['steamid'].astype('int64'))))

    df_final.to_sql(name='SteamUsers',con = con, index = False, if_exists='replace')
    
    
    
    s3_manager.upload_data(df_final['steamid'].to_json(orient='records'), 'steamUsersIDS.json')


    return s3_manager.upload_data(df_final.to_json(orient='records'), 'steamUsers.json')



def steam_games():
    server = r"host.docker.internal\DATA_CENTER"  # Adjust server name
    username = "MichalM"
    password = "!Everest2021@!"
    database = "SteamDB"
    driver = "ODBC+Driver+18+for+SQL+Server"


    # Build the connection string for SQLAlchemy
    connection_string = f"mssql://{username}:{urllib.parse.quote_plus(password)}@{server}/{database}?driver={driver}&Encrypt=no&TrustServerCertificate=yes"

    engine = create_engine(connection_string)
    con = engine.connect()


    url = "http://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json"
    
    # Fetch the data from the URL
    response = requests.get(url)
    data = response.json()

    # Extract the relevant part of the data
    applist = data.get('applist', {}).get('apps', [])

    # Convert the list of apps into a DataFrame
    df = pd.DataFrame(applist)
    

    df.to_sql(name='SteamGames',con = con, if_exists='replace',index = False)
   

    return s3_manager.upload_data(df.to_json(orient='records'), 'steamAllGames.json')



#testing function for faster checking and loading data
################################################################
def steam_games_description_test():
    server = r"host.docker.internal\DATA_CENTER"  # Adjust server name
    username = "MichalM"
    password = "test"
    database = "SteamDB"
    driver = "ODBC+Driver+18+for+SQL+Server"
   
    steam_game_description_ids = s3_manager.load_data_from_bucket('steamGamesDescriptionIds.json')
    steam_game_description_ids_DF = pd.DataFrame(steam_game_description_ids, columns=['steam_appid'])
    print(steam_game_description_ids_DF)
    # Build the connection string for SQLAlchemy

 
    return s3_manager.upload_data(steam_game_description_ids_DF.to_json(orient='records'), 'steamGamesDescriptionIds.json')
    
################################################################


    

#If non exisitng data then will fill null in dataframe and json and later insert the data without games that had alredy missing values 
def steam_games_description():
    server = r"host.docker.internal\DATA_CENTER"  # Adjust server name
    username = "MichalM"
    password = "test"
    database = "SteamDB"
    driver = "ODBC+Driver+18+for+SQL+Server"
   
     # Select relevant fields to include in the DataFrame
    fields = [
        'name', 'steam_appid', 'required_age', 'is_free', 
        'detailed_description', 'short_description', 
        'supported_languages', 'header_image', 'capsule_image',
        'pc_requirements', 'mac_requirements', 'linux_requirements', 
        'developers', 'publishers', 'price_overview', 'platforms', 
        'metacritic'
    ]






    # using this json data to concet with exisitng created then replace file that is defined in this variable to continue scaling the file in the bucket 
    existing_data = s3_manager.load_data_from_bucket('steamGamesDescription.json')
    
    existing_data = pd.DataFrame(existing_data)

    # Build the connection string for SQLAlchemy
    connection_string = f"mssql://{username}:{urllib.parse.quote_plus(password)}@{server}/{database}?driver={driver}&Encrypt=no&TrustServerCertificate=yes"

    engine = create_engine(connection_string)
    con = engine.connect()



    data = s3_manager.load_data_from_bucket('steamAllGames.json')


    df = pd.DataFrame(data)

    print(df.head())
    df = df[df['name'].str.len() > 0]
    
    
    
    # changing the data so will check in bucket insted of sql server
    steam_game_description_ids = s3_manager.load_data_from_bucket('steamGamesDescriptionIds.json')
    steam_game_description_ids_DF = pd.DataFrame(steam_game_description_ids, columns=['steam_appid'])
    print(steam_game_description_ids_DF)


    # Ensure distinct_ids_df['steam_appid'] is a list or set
    existing_app_ids = set(steam_game_description_ids_DF['steam_appid'])
    
    # Filter df to include only rows where 'appid' is not in existing_app_ids
    df_test = df[~df['appid'].isin(existing_app_ids)]

    #Tommor veryfication fix because not loading the data 
    df_test = df_test['appid'].head(700)
    print(df_test)
    games_data_list = []

    
    # Loop through each appid
    for appid in df_test:
        try:
            # Construct the URL for the current appid
            url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
            
            # Fetch the data from the URL
            response = requests.get(url)
            
            # Check if the response was successful
            if response.status_code == 200:
                data = response.json()

            
                # Make sure the data contains the appid key and the 'data' field
                if data and str(appid) in data and data[str(appid)].get('success', False):
                    game_data = data[str(appid)].get('data', {})

                    # Handle missing or empty fields
                    game_data.setdefault('mac_requirements', 'N/A')
                    game_data.setdefault('linux_requirements', 'N/A')
                    game_data.setdefault('pc_requirements', {})
                    game_data.setdefault('developers', [])
                    game_data.setdefault('publishers', [])
                    game_data.setdefault('price_overview', {})
                    game_data.setdefault('platforms', {})
                    game_data.setdefault('metacritic', {})

                   
                    # Create a dictionary with selected fields
                    processed_data = {field: game_data.get(field, 'N/A') for field in fields}




                    # Serialize dictionaries and lists into JSON strings
                    processed_data['pc_requirements'] = json.dumps(processed_data['pc_requirements'])
                    processed_data['mac_requirements'] = json.dumps(processed_data['mac_requirements'])
                    processed_data['linux_requirements'] = json.dumps(processed_data['linux_requirements'])
                    processed_data['developers'] = json.dumps(processed_data['developers'])
                    processed_data['publishers'] = json.dumps(processed_data['publishers'])
                    processed_data['price_overview'] = json.dumps(processed_data['price_overview'])
                    processed_data['platforms'] = json.dumps(processed_data['platforms'])
                    processed_data['metacritic'] = json.dumps(processed_data['metacritic'])

                    # Normalize nested dictionaries and lists into a flat structure
                    if isinstance(processed_data['price_overview'], dict):
                        for k, v in processed_data['price_overview'].items():
                            processed_data[f'price_overview_{k}'] = v
                        del processed_data['price_overview']

                    if isinstance(processed_data['platforms'], dict):
                        for k, v in processed_data['platforms'].items():
                            processed_data[f'platform_{k}'] = v
                        del processed_data['platforms']

                    # Append the processed data to the list
                    games_data_list.append(processed_data)
                    print("Data added succesfully")
                else:
                    print(f"No data available for appid: {appid} or success: False")
                    
                    # Append the appid with None values for each field
                    null_data = {field: None for field in fields}
                    null_data['steam_appid'] = appid  # Ensure we still store the appid
                    games_data_list.append(null_data)
            else:
                print(f"Failed to fetch data for appid {appid}, status code: {response.status_code}")

                # Append the appid with None values for each field
                null_data = {field: None for field in fields}
                null_data['steam_appid'] = appid  # Ensure we still store the appid
                games_data_list.append(null_data)

        except Exception as e:
            print(f"Error processing appid {appid}: {e}")


    if games_data_list:
        games_df = pd.DataFrame(games_data_list)
        df_final = pd.concat([existing_data, games_df ], ignore_index=True)
        # Store the DataFrame in the SQL database
        games_df.to_sql(name='SteamGamesDescription', con=con, if_exists='append', index=False)


        #Loading the data already inserted in sql server as so in the bucket s3, so leaving the sql distincted selection 
        distincted_games_description_ids = pd.concat([steam_game_description_ids_DF['steam_appid'],games_df['steam_appid'] ], ignore_index=True)
        print(distincted_games_description_ids)
        s3_manager.upload_data(distincted_games_description_ids.to_json(orient='records'), 'steamGamesDescriptionIds.json')
        
        

        return s3_manager.upload_data(df_final.to_json(orient='records'), 'steamGamesDescription.json')
    else:
        return "No valid game data to store."


def test_sql_connection():
    # Connection parameters
    server = r"host.docker.internal\DATA_CENTER"
    username = "MichalMatracz"
    password = "test"
    database = "SteamDB"
    driver = "ODBC+Driver+18+for+SQL+Server"
    try:
        # Build the connection string for SQLAlchemy
        connection_string = (
            f"mssql+pyodbc://{username}:{urllib.parse.quote_plus(password)}@"
            f"{server}/{database}?driver={driver}&Encrypt=no&TrustServerCertificate=yes"
        )

        # Create an SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Try to connect to the server
        with engine.connect() as connection:
            # Execute a simple query to test the connection
            result = connection.execute("SELECT 1;")
            print("Connection successful, query result:", result.fetchone())
        
        return True  # Connection successful
    
    except sqlalchemy.exc.OperationalError as oe:
        print(f"Operational error: {oe}")
        return False  # Connection failed
    
    except sqlalchemy.exc.DatabaseError as de:
        print(f"Database error: {de}")
        return False  # Connection failed
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return False  # Connection failed


# Connection parameters
server = r"host.docker.internal\DATA_CENTER"
username = "MichalMatracz"
password = "Qwer1997@"
database = "test"
driver = "ODBC+Driver+18+for+SQL+Server"











#creating the data insertion in term of users data one game one row with user id 
def steamplayersGames():
    server = r"host.docker.internal\DATA_CENTER"  # Adjust server name
    username = "MichalMatracz"
    password = "Qwer1997@"
    database = "SteamDB"
    driver = "ODBC+Driver+18+for+SQL+Server"
     # Build the connection string for SQLAlchemy
    connection_string = f"mssql://{username}:{urllib.parse.quote_plus(password)}@{server}/{database}?driver={driver}&Encrypt=no&TrustServerCertificate=yes"

    engine = create_engine(connection_string)
    con = engine.connect()

    # Load data from S3 bucket (JSON containing Steam user IDs)
    gamers_ids = s3_manager.load_data_from_bucket('steamUsersIDS.json')

   
    players_games_json = s3_manager.load_data_from_bucket('steamUsersGames.json')


    players_games_df = pd.DataFrame(players_games_json, columns=['steamid','appid','playtime_forever'])
    null_values = players_games_df.isnull().sum()
    print(null_values)


    # Convert the data to a pandas DataFrame
    gamers_ids_df = pd.DataFrame(gamers_ids, columns=['steamid'])


    # Filter out IDs that are already in the distinct_ids_df
    gamers_ids_df_filtered = gamers_ids_df[~gamers_ids_df['steamid'].isin(players_games_df['steamid'].unique())]

    

    api_key = "22AB7B290037490813DF382FD4F94FA4"


    print('this is 250 ids :',gamers_ids_df_filtered['steamid'].head(250))
    # Here you can change number of ids thats help uploding into storage S3 and database

    IDS_new_list = gamers_ids_df_filtered['steamid'].head(250)



    players_games = []



    for steamid in  gamers_ids_df_filtered['steamid'].head(250):
        # Generate the URL for each Steam ID
        url = f'https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key={api_key}&steamid={steamid}&format=json'
        
        # Fetch data from the URL
        response = requests.get(url)
        
        # Check if the response was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json().get("response", {})
            
            # Extract games list
            games_data = data.get("games", [])
            
            # Append each game's data to the usersGames list with the current steamid
            for game in games_data:
                appid = game.get("appid")
                playtime_forever = game.get("playtime_forever")
                players_games.append({
                    "steamid": steamid,  # Append the current user's Steam ID
                    "appid": appid, 
                    "playtime_forever": playtime_forever
                })
        else:
            print(f"Failed to fetch data for Steam ID: {steamid}")
            
        # Create a DataFrame from the rows
    

    usersGames = pd.DataFrame(players_games, columns=["steamid", "appid", "playtime_forever"])
    print(usersGames)


    


    usersGamesConcat = pd.concat([usersGames, players_games_df], ignore_index=True)

    missing_steamids = gamers_ids_df_filtered['steamid'].head(250)[~gamers_ids_df_filtered['steamid'].head(250).isin(usersGamesConcat['steamid'])]

    if not missing_steamids.empty:
        missing_games_df = pd.DataFrame({
            'steamid': missing_steamids,
            'appid': pd.NA,  # Set 'appid' as empty
            'playtime_forever': pd.NA  # Set 'playtime_forever' as empty
        })

    usersGamesConcat = pd.concat([usersGamesConcat, missing_games_df], ignore_index=True)

    #gamers_ids_df_filtered['steamid'].head(30)
    

    users_games_json = usersGamesConcat.to_json(orient='records', indent=4)
    

    print(users_games_json)

    #Zapisanie odrębnych idków gier użytkoników które juz sa w buckecie 
    # Wykorzystanie zgranej tabeli w celu przrównania ich do wszystkich i nie powtasrznai zgrywania juz isntiejacych 

    usersGames.to_sql(name='SteamUsersGames',con = con, index = False, if_exists='append')
    

    return s3_manager.upload_data(users_games_json, 'steamUsersGames.json')
    



def AWS_S3_upload_data():


    #Steam users - Uploading next users into Storage, as so into SQL server 
    #Steam games - reuploading about 230 k rows in one run
    #steam_games_description - Scalling next values of ids, based on the unexistend in the json file
    # steamplayersGames - in exising users description (The file that involve only ids, because of size of file) taking ids non existent in in file , and scalling next values
    # In steamplayersGames we got row by row  id of the user and game id 
    
    functions = [steam_users,steam_games,steam_games_description ,steamplayersGames]



    # Pararell pipiline using all functions
    for func in functions:
        func()  # Execute the function

   

    
with DAG(
    dag_id="test_AWSS3_connection_dag",
    #schedule_interval="@once",  # Run once for testing
    schedule_interval=timedelta(hours=3),  # Run every 2 hours
    default_args={
        "owner": "MichalM",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2024, 8, 12),
    },
    catchup=False
) as dag:
    
    # Task to test PostgreSQL connection.
    test_postgres_connection_task = PythonOperator(
        task_id="test_AWSS3_connection",
        python_callable= AWS_S3_upload_data,

    )