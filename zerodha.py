import requests
import zipfile
import csv
import json
from pymongo import MongoClient
from bson import json_util


r = requests.get('http://www.bseindia.com/download/BhavCopy/Equity/EQ220118_CSV.ZIP')
with open('zero.zip', 'wb') as f:
    for chunk in r.iter_content(chunk_size=100):
        if chunk:
            f.write(chunk)

zip_ref = zipfile.ZipFile('D:\Projects\web-development\zerodha\zero.zip', 'r')
zip_ref.extractall('D:\Projects\web-development\zerodha')
zip_ref.close()

client = MongoClient('mongodb://localhost:27017')
# here admin is the database name

admin = client.stock_data


# result = admin.col.insert_one(post_data)
field_Name = ("SC_CODE", "SC_NAME", "SC_GROUP", "SC_TYPE", "OPEN", "HIGH", "LOW", "CLOSE", "LAST", "PREVCLOSE", "NO_TRADES", "NO_OF_SHRS", "NET_TURNOV", "TDCLOINDI")

r = 0
with open('D:\Projects\web-development\zerodha\EQ220118.csv', 'r') as csv_file:
    readCsv = csv.DictReader(csv_file, field_Name)
    with open('D:\Projects\web-development\zerodha\Eq.json', 'w') as json_file:
        for row in readCsv:
            if r == 0:
                r = 1
                continue
            else:
                json.dump(row, json_file)
                json_file.write('\n')


with open('D:\Projects\web-development\zerodha\Eq.json', 'r') as json_file:
    for row in json_file:
        data = json_util.loads(row)
        admin.col.insert_one(data)



