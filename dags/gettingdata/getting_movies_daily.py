import sys
import os

# Thêm đường dẫn gốc của dags vào PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import requests
import pandas as pd
import requests
from datetime import datetime

from operations.mongodb import *

cur_date = datetime.now().strftime("%d-%m-%Y")

# Hàm lấy dữ liệu
def get_movie_data(list_movies_name, crawl_date = cur_date):

    # 
    API_KEY = "2d6e1b290dabf74f65b84431677db2b8"
    BASE_URL = "https://api.themoviedb.org/3"
    END_POINT = "/search/movie"

    movies_data = []
    for name in list_movies_name:
        params = {
            'api_key': API_KEY,
            'query': name,
            'language': 'en-US',
            'page': 1
        }

        url = f"{BASE_URL}{END_POINT}"
        response = requests.get(url, params=params)

        if response.status_code == 200:
            movies_data.append({
                "Execution Date"    :   crawl_date,
                "Results"           :   response.json().get('results'),
            })
        else:
            raise TypeError(f"Can't find movie {name}")
    
    return pd.DataFrame(data= movies_data, columns=['Execction Date','Results'])

def loading_movies_data(crawl_date: str):
    with connect_mongodb(username='ndtien',password='ndtien',host='mongodb',port='27017') as client:
        client_op = MongoDB_Operation(client=client)

        # Tìm ra các tên mới trong ngày hôm nay
        new_movies_name = client_op.find_data(db_name="movies_db",collection_name="movies_name",
                                              query={"Execution Date" : crawl_date})['Name'].tolist()
        
        new_movies_data = get_movie_data(new_movies_name,crawl_date=crawl_date)

        return new_movies_data