import requests
import json
import sqlite3

#get the nifty feed for every minute
url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=^NSEI&interval=1min&outputsize=full&apikey=155JMJD2THUHGJJV"
feed = requests.get(url).text
data = json.loads(feed)

jack = sqlite3.connect("FACTOREY.db")


kk = jack.execute("SELECT MAX(ID) FROM NIFTY1MIN").fetchall()
if str(kk[0])[1:][:-2]!='None':
 uu = int(str(kk[0])[1:][:-2])+1
else:
 uu = 1

data = data[list(data)[1]]

for i in range(0, len(data)):
 z = list(data)[i]
 
 #delete existing entries
 jack.execute("DELETE FROM NIFTY1MIN WHERE DATE='"+list(data)[i]+"'")
 jack.commit()

 #insert values
 jack.execute("INSERT OR IGNORE INTO NIFTY1MIN VALUES(?,?,?,?,?,?,?)",(uu,z,float(data[z]['1. open']),float(data[z]['2. high']),float(data[z]['3. low']),float(data[z]['4. close']),float(data[z]['5. volume'])))
 uu = uu + 1
 jack.commit()

jack.close()

