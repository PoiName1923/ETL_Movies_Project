import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup

from operations.mongodb import *


cur_date = datetime.now().strftime("%d-%m-%Y")

# Hàm lấy dữ liệu 
def getting_movie_name(crawl_date=cur_date):
    movies_name = []
    year = cur_date.split('-')[2]

    url = f"https://www.boxofficemojo.com/year/world/{year}/"
    res = requests.get(url)
    # Kiểm tra nếu xảy ra lỗi request từ web
    if res.status_code == 200:
        print(f"Connected successfully to {year}")
        soup = BeautifulSoup(res.text, 'html.parser')

        # Chỉ định bảng cụ thể để tạo điều kiện cho việc tìm kiếm các tên phim 
        table = soup.find({"table":{"class":"a-bordered a-horizontal-stripes \
                                        a-size-base a-span12 mojo-body-table mojo-table-annotated scrolling-data-table"}})
            
        # Lấy tất cả các hàng trong bảng phim
        rows = table.select('a.a-link-normal')
        for row in rows:
            if '/releasegroup/' in row.get('href', ''):
                movies_name.append({
                    "Execution Date"    :   cur_date,
                    "Name"              :   row.text.strip(),
                    "Year"              :   year
                })
        print(f"Getting Successfully {len(rows)} movies in {year}")
    else:
        raise TypeError(f"Can't find data in {year}")

    return pd.DataFrame(data    =   movies_name,
                        columns =   ['Execution Date', 'Name', 'Year'])

def loading_movies_name(crawl_date : str):
    with connect_mongodb(username='ndtien', password='ndtien',host='mongodb',port='27017') as client:

        client_op = MongoDB_Operation(client=client)

        # Khởi tạo database/collection nếu đây là lần chạy đầu tiên
        client_op.create_database(db_name="movies_db")
        client_op.create_collection(db_name="movies_db",collection_name="movies_name")

        # Lấy dữ liệu từ các ngày trước đó 
        old_movies_name = client_op.find_data(db_name="movies_db",collection_name="movies_name",
                                              query={})
        # Truy xuất dữ liệu trong ngày hôm nay
        new_movies_name = getting_movie_name(crawl_date=crawl_date)
        
        # Dữ liệu mới 
        old_movie_keys = set((doc['Name'], doc['Year']) for doc in old_movies_name)
        unique_movies = new_movies_name[
                                        ~new_movies_name.apply(lambda x: (x['Name'], x['Year']) in old_movie_keys, axis=1)
                                        ]
        
        # Nếu có dữ liệu mới thì thêm vào
        if unique_movies:
            client_op.insert_many(db_name="movies_db",collection_name="movies_name",
                                data=unique_movies)
            print(f"Have {len(unique_movies)} new movie was added in {cur_date}")
        else:
            print("Don't have new movie in this day")