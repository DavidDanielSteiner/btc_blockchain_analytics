# -*- coding: utf-8 -*-
"""
Created on Wed Oct 23 11:26:08 2019

@author: David
"""

import requests


day = 0
month = 1

while True:
    day += 1    
    if day == 31:
        month += 1
        
    
    params = {
      "source" : "twitter",
      "coin" : "bitcoin",
      "bin_size" : "24H",
      "count_ptr" : 1000,
      "start_ptr" : 0,
      "start_datetime" : "2019-" + month + "-" + day + "T00:00:00Z",
      "end_datetime" : "2019-" + month + "-" + day  + "T00:00:00Z",
    }
    
    r = requests.request("GET", url, params=params)
    aggregated_sentiment = r.json()
print(r.content)



r = requests.request("GET", "http://api-dev.augmento.ai/v0.1/topics")
sentiment_keys = r.json()
print(r.content)



url = "http://api-dev.augmento.ai/v0.1/events/aggregated"

params = {
  "source" : "twitter",
  "coin" : "bitcoin",
  "bin_size" : "24H",
  "count_ptr" : 1000,
  "start_ptr" : 0,
  "start_datetime" : "2017-01-01T00:00:00Z",
  "end_datetime" : "2019-09-01T00:00:00Z",
}

r = requests.request("GET", url, params=params)
aggregated_sentiment = r.json()