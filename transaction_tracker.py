# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:36:10 2019

@author: David
"""

#https://github.com/crazicus/Bitcoin-Transaction-Tracker

import requests
from bs4 import BeautifulSoup
import time
import json
import urllib
import urllib.request
import html5lib

def BTC_crawl(url):
  soup = BeautifulSoup(url, 'html5lib')
  transactions = soup.find("div", {"id": "tx_container"}).findAll("div")

  for transaction in transactions:
    if(transaction.find("table") is not None):
      time_stamp = str(transaction.find("span").contents)
      BTC_val = transaction.find("button").find("span").contents

      print("Transaction on " + time_stamp[2:-11] + " at " + time_stamp[-10:-2] + ": " + str(BTC_val)[2:-2])

      
mybool = True
last_time = time.time()
first_time = last_time
while mybool:
  if((time.time() - last_time) >= 5):
    url = urllib.request.urlopen("https://blockchain.info/unconfirmed-transactions")
    BTC_crawl(url)
    last_time = time.time()
  if((time.time() - first_time) >= 30):
    mybool = False