import requests
import json
import sqlite3
import datetime
import gspread
import pandas as pd
import html5lib
from oauth2client.service_account import ServiceAccountCredentials

jack = sqlite3.connect("FACTOREY.db")

data = jack.execute("SELECT * FROM NIFTY1MIN").fetchall()

#getting date of today
date = datetime.datetime.now() - datetime.timedelta(days=0)

low = float(jack.execute("SELECT MIN(LOW) FROM NIFTY1MIN WHERE DATE LIKE '"+str(date[:10])+"%'")[0][0])
high = float(jack.execute("SELECT MAX(HIGH) FROM NIFTY1MIN WHERE DATE LIKE '"+str(date[:10])+"%'")[0][0])
close = float(data[len(data)-1][5])

jack.close()

#authorizing access to google sheets
scope = ['https://www.googleapis.com/auth/spreadsheets']
credentials = ServiceAccountCredentials.from_json_keyfile_name('factorey-691075c06a86.json',scope)
gc = gspread.authorize(credentials)

#opening necessary file
wks = gc.open_by_url('https://docs.google.com/spreadsheets/d/1rShYmCRS24wrNrOMAtRHf3IG5HqQs1BrKmgH5brjVrk/edit#gid=0').worksheet("Daily")

#sending values to the cells
wks.update_cell(2,2,close)
wks.update_cell(2,5,low)
wks.update_cell(2,6,high)