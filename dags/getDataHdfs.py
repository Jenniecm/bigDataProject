import requests
from hdfs import InsecureClient
import json
from datetime import datetime

api_key = '2c5a15136404a6eb67a3b4498cb2b4b8'
current_date = datetime.now().strftime("%Y-%m-%d")

# HDFS path to save the data
hdfs_path = f"/user/project/datalake/raw/{current_date}/tmdb_popular_movies.json"

def fetch_tmdb_data(api_key, page):
    tmdb_url = f"https://api.themoviedb.org/3/movie/popular"
    params = {
        'api_key': api_key,
        'page': page
    }
    
    response = requests.get(tmdb_url, params=params)
    
    if response.status_code == 200:
        return response.json()["results"]
    else:
        print(f"Error fetching data from TMDb API. Status code: {response.status_code}")
        return None

def save_to_hdfs(data):
    client = InsecureClient(f"http://localhost:9870", user="hadoop")
    
    with client.write(hdfs_path, overwrite=True) as writer:
        json.dump(data, writer)

def fetch_and_save_multiple_pages(api_key, start_page, end_page):
    all_data = []
    
    for page in range(start_page, end_page + 1):
        page_data = fetch_tmdb_data(api_key, page)
        
        if page_data:
            all_data.extend(page_data)
        else:
            break

    save_to_hdfs(all_data)
    print(f"Data from pages {start_page} to {end_page} saved to HDFS successfully. Total records: {len(all_data)}")

if __name__ == "__main__":
    # Fetch data for pages 1 to 500
    fetch_and_save_multiple_pages(api_key, start_page=1, end_page=500)

def ingest_all_data(**kwargs):
    print(f"***************************{kwargs}")
    fetch_and_save_multiple_pages(api_key, start_page=1, end_page=4)