import json
import random
import time
from datetime import datetime
import requests, json

from flask import Flask, Response, render_template

app = Flask(__name__)
random.seed()  # Initialize the random number generator


@app.route('/')
def index():
    return render_template('index.html')


#Data API: https://www.bitstamp.net/api/
#Chart Framework: https://www.highcharts.com/stock/demo


@app.route('/chart-data')
def chart_data():
    def get_ticker_data():
        while True:
            URL = 'https://www.bitstamp.net/api/ticker/'
            try:
                r = requests.get(URL)
                priceFloat = float(json.loads(r.text)['last'])
                volume = json.loads(r.text)['volume']
            except requests.ConnectionError:
                print("Error querying Bitstamp API")
            
            json_data = json.dumps(
                {'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'value': priceFloat, 'volume': volume})
            print(volume)
            print(priceFloat)
            return f"data:{json_data}\n\n"
            time.sleep(3)

    return Response(get_ticker_data(), mimetype='text/event-stream')


if __name__ == '__main__':
    app.run()






'''
URL = 'https://www.bitstamp.net/api/ticker/'
r = requests.get(URL)
priceFloat = json.loads(r.text)
'''



