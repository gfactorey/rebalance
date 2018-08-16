import datetime
import os
import pandas as pd
import gspread
import smtplib
from datetime import date
import openpyxl
import ssl
import shutil
import requests
from nsepy import *
import sqlite3
import random
import pymysql

# accessing the main database
db = sqlite3.connect("/mnt/python_backend/JACK.db")
jack = db.cursor()

host = "o3-caret-production.cz3xo2mjslr0.ap-south-1.rds.amazonaws.com"
port = 3306
username = 'o3caretadmin'
password = 'Dy*tjMC$HBbZ7_vO'

db2 = pymysql.connect(host=host,port=port,user=username,password=password,db = 'o3_backoffice',cursorclass=pymysql.cursors.DictCursor,charset="utf8mb4")
jack2 = db2.cursor()

# gets the holding position everyday
def Snapcash(datermax, clientid2):
 jack = db.cursor()
 
 # teller represents the cash table
 cash = jack.execute("SELECT * FROM TELLER").fetchall()

 # master table with all transactions 
 trades = jack.execute("SELECT * FROM TRADESMAN").fetchall()
 
 def Snapper(dater, clientid2):
  trades = jack.execute("SELECT * FROM TRADESMAN").fetchall()
  db.commit()
  
  #creating a client table for a new client
  try:
   sql="""CREATE TABLE IF NOT EXISTS '"""+str(clientid2)+"""' (ID integer PRIMARY KEY, SECURITY text NOT NULL, QTY float NOT NULL,COST float NOT NULL, VALUE float NOT NULL);"""
   jack.execute(sql)
   db.commit()
  except:
   pass

  #deleting holding statements in the beginning
  jack.execute("DELETE FROM '"+clientid2+"'")
  db.commit()

  indate = dater
  holdings = [[None for x in range(5)] for y in range(1000)] 
  count = 0

  # reading transactions, one after the other
  for y in range(0,len(trades)):
   if str(trades[y][2]) == str(clientid2):
    if indate >= datetime.datetime.strptime(trades[y][1][:10], '%Y-%m-%d'):
     check = 0
     for z in range(0,count):

      # checking if the stock/ MF already exists
      if holdings[z][1] == trades[y][9]:
       check = 1

       # need to fix FIFO/ LIFO here
       if float(trades[y][10]) == 0.0:
        holdings[z][3] = holdings[z][3] - float((float(trades[y][14])/float(holdings[z][2]))*holdings[z][3])
        holdings[z][2] = holdings[z][2] - float(trades[y][14])
       else:
        holdings[z][2] = holdings[z][2] + float(trades[y][10])
        holdings[z][3] = holdings[z][3] + float(trades[y][13])
     
     # new entry if it does not exist
     if check == 0:
      if trades[y][5] == 'EQ' or trades[y][5] == 'MF' :
       holdings[count][1] = trades[y][9]
       holdings[count][2] = -1*float(trades[y][14]) + float(trades[y][10])
       holdings[count][3] = float(trades[y][13])
       count = count + 1

  # removing holdings when they are completely sold
  y = 0
  while y < count:
   if abs(holdings[y][2]) <= 0.01:
    for z in range(y,count):
     holdings[z][1] = holdings[z+1][1]
     holdings[z][2] = holdings[z+1][2]
     holdings[z][3] = holdings[z+1][3]
    holdings[count-1][1] = None
    holdings[count-1][2] = None
    holdings[count-1][3] = None
    count = count - 1
   else:
    y = y + 1
  
  # inserting elements into database of transactions
  wnow=0
  while holdings[wnow][1]!=None:
   jack.execute("INSERT OR IGNORE INTO '"+str(clientid2)+"' VALUES(?,?,?,?,?)",(wnow+1,holdings[wnow][1],holdings[wnow][2],holdings[wnow][3],0))
   db.commit()
   wnow = wnow + 1

 indate = datermax

 rupee = 0

 # estimate cash from money inflows
 for y in range(0,len(cash)):
  if str(cash[y][2]) == str(clientid2):
   if indate >= datetime.datetime.strptime(cash[y][1][:10], '%Y-%m-%d'):
    rupee = rupee + cash[y][4]

 # estimate cash from buy and sell transactions
 for y in range(0,len(trades)):
  if str(trades[y][2]) == str(clientid2):
   if indate >= datetime.datetime.strptime(trades[y][1][:10], '%Y-%m-%d'):
    rupee = rupee + float(trades[y][17]) - float(trades[y][13])
 
 db.commit()
 Snapper(indate, clientid2)
 
 #inserting cash into the last row of the file
 jack.execute("INSERT OR IGNORE INTO '"+str(clientid2)+"' VALUES(?,?,?,?,?)",(99,"CASH",rupee,0,0))
 db.commit()

def reporter():
 
 # start date of NAV calculation as we have prices from this date onwards
 fixed = "2018-06-05"
 
 # get today's date in the current scheme of things\
 check = str(datetime.datetime.now())[:10]
 
 # fixing the start date to run the process
 today = datetime.datetime.strptime(check, '%Y-%m-%d') - datetime.timedelta(days=7)
 
 # fixing the end date to run the process
 date = datetime.datetime.now() - datetime.timedelta(days=1)
 db.commit()

 #selecting the primary key for uploading the report from the data
 kk = jack.execute("SELECT MAX(ID) FROM REPORTDATA")
 kk = kk.fetchall()
 if str(kk[0])[1:][:-2]!='None':
  uu = int(str(kk[0])[1:][:-2])+1
 else:
  uu = 1
 
 navs = [0.0]*1000
 clients = [None]*1000
 
 # list of clients to run the process
 clients = jack.execute("SELECT * FROM CLIENT").fetchall()
 while date >= today:
  for m2 in range(0,len(clients)):
   clientid = str(clients[m2][1])
   db.commit()
   
   # calling the holdings function for a client, date. Will need to pass portfolio sometime later today. 
   Snapcash(today, clientid)

   # getting the client holdings
   scrips = jack.execute("SELECT * FROM '"+clientid+"'").fetchall()

   m = 0
   navs = 0.0
    
   # deleting elements if there is an update for a client on that particular date
   jack.execute("DELETE FROM REPORTDATA WHERE DATE='"+str(today)+"' AND CLIENT='"+str(clientid)+"'")
   db.commit()

   m = 0
   for z in range(0, len(scrips)):
    # getting the prices from the table
    
    # if cash set price as 1
    if scrips[z][1]!='CASH':
     y1 = jack2.execute("SELECT price FROM mf_etf_nav_source WHERE DATE='"+str(today)+"' AND rtaCode='"+scrips[z][1].split(" ")[0]+"'")
     
     # if MF price is not available on that day, take the last price
     try:
      y1 = float(jack2.fetchall()[0]['price'])
     except:
      y3 = jack2.execute("SELECT MAX(DATE) FROM mf_etf_nav_source rtaCode='"+scrips[z][1].split(" ")[0]+"'")
      y3 = jack2.fetchall()[0][0]
      if y3 < str(today):
       y1 = jack2.execute("SELECT price FROM mf_etf_nav_source WHERE DATE='"+str(y3)+"' AND rtaCode='"+scrips[z][1].split(" ")[0]+"'")
       y1 = float(jack2.fetchall()[0]['price'])
      else:
       print("CHECK")
    
     y2 = jack2.execute("SELECT schemeName FROM bse_mf_scheme WHERE rtaCode='"+scrips[z][1].split(" ")[0]+"'")
     y2 = jack2.fetchall()[0]['schemeName']
    else:
     y1 = 1
     y2 = 'CASH'
    
    navs = navs + float(scrips[z][2])*y1
    # performing a check
    if y2!=None and y1!=None:
     m = m + 1
     jack.execute('''INSERT OR IGNORE INTO REPORTDATA VALUES(?,?,?,?,?,?,?)''',(uu,str(today),str(clientid),y2,scrips[z][2],float(scrips[z][2])*y1,scrips[z][3]))
     uu = uu + 1
     db.commit()

   jack.execute("DELETE FROM NAVIGATOR WHERE DATE='"+str(today)+"' AND CLIENT='"+str(clientid)+"'")
   db.commit()
   
   #performing a check
   if m == len(scrips):
    jack.execute('''INSERT OR IGNORE INTO NAVIGATOR VALUES(?,?,?,?)''',(uu,today,str(clientid),navs))
    db.commit()
   else:
    print("CHECK")
  
  print(today)
  today = today + datetime.timedelta(days=1)

reporter()

