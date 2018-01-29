import requests
import zipfile
import json
from pymongo import MongoClient
import os
import pandas as pd


def add_to_db():
    # downloading zip file
    r = requests.get('http://www.bseindia.com/download/BhavCopy/Equity/EQ250118_CSV.ZIP')
    with open('zero.zip', 'wb') as f:
        for chunk in r.iter_content(chunk_size=100):
            if chunk:
                f.write(chunk)

    # extracting zip file
    c_dir = os.path.dirname(__file__)
    zip_ref = zipfile.ZipFile(c_dir + '\zero.zip', 'r')
    zip_ref.extractall(c_dir)
    zip_ref.close()

    client = MongoClient('mongodb://localhost:27017')

    # here stock_data is the database name
    admin = client.collect.stock_data
    data = pd.read_csv(c_dir+'\EQ250118.csv')
    data_json = json.loads(data.to_json(orient='records'))
    admin.remove()
    admin.insert(data_json)


if __name__ == "__main__":
    add_to_db()
