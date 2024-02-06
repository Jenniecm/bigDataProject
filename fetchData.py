import requests
from hdfs import InsecureClient
from datetime import datetime
import json

current_date = datetime.now().strftime("%Y-%m-%d")

def retrieve_data_from_api(api_url, api_key=None):
    headers = {'X-Auth-Token': f'{api_key}'} if api_key else {}
    
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Check for HTTP errors

        data = response.json()
        print(f"Data retrieved succesfully")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving data from API: {e}")
        return None


def save_to_hdfs(data):

    try :
        client = InsecureClient(f"http://localhost:9870", user="hadoop")
        
        with client.write(hdfs_path, overwrite=True) as writer:
            json.dump(data, writer)
        print ("Saved sucesfully to HDFS")
    except Exception as e :
        print(f"An error occurred while saving data to HDFS: {str(e)}")


if __name__ == "__main__":
    # Example API and HDFS configuration
    api_url = "https://api.openweathermap.org/data/2.5/weather?q=limoges,FR&appid=eea045ed57d81cb0b2ad92319810b8c6"
    #"https://api.football-data.org/v4/teams/65/matches?status=FINISHED&limit=1"
    api_key = "4ych92pvgt3tqk52yajw"
    #'92824eba88954af3b13a13e3a1943181' # Set to None if no API key is required
    hdfs_path = f"/user/project/datalake2/raw/{current_date}/weather.json"

    # Retrieve data from API
    data = retrieve_data_from_api(api_url, api_key)

    if data:
        # Save data to HDFS
        save_to_hdfs(data)

def ingest_data() :
    retrieve_data_from_api(api_url= "https://api.football-data.org/v4/teams/65/matches?status=FINISHED&limit=1", api_key= '92824eba88954af3b13a13e3a1943181')
    save_to_hdfs(data)
    