import time

from fastapi import FastAPI
from typing import Optional
# import requests
# import json
import pandas as pd
import datetime as dt
from datetime import datetime

from pytiingo import RESTClient
import requests
import os
from dotenv import load_dotenv
import numpy as np
import json

load_dotenv()

now = datetime.utcnow()

token = os.environ.get("API_KEY")
# symbol = os.environ.get('SYMBOL')
freq = os.environ.get('FREQUENCY')

client = RESTClient(token=f'{token}', output_format='pandas')


# Data Fetching Function
def get_hourly_data(symbol: str, startTime: str, endTime: str):
    url = f'https://api.tiingo.com/iex/{symbol}/prices'
    headers = {
        'Content-Type': 'application/json'
    }
    columns = 'open,high,low,close,volume'
    start_date = str(datetime.strptime(startTime, '%Y-%m-%d'))
    end_date = str(datetime.strptime(endTime, "%Y-%m-%d"))
    req_params = {'startDate': start_date, 'endDate': end_date, 'resampleFreq': freq, 'columns': {columns}, 'token': token}
    data = json.loads(requests.get(url, params=req_params).text)
    # url = f'https://api.tiingo.com/iex/{symbol}/prices?startDate={start_date}&endDate={end_date}&resampleFreq={
    # freq}&columns=open,high,low,close,volume&token={token}' response = requests.get(url, headers=headers) data =
    # pd.read_json(response) symbol = symbol frequency = "1h" token = '1000'

    # data = client.iex.get_prices(ticker=symbol.lower(), startDate=start_date, endDate=end_date, resampleFreq=freq)

    data['date'] = pd.to_datetime(data['date']).astype(np.int64) // 10 ** 9
    # data['symbol'] = symbol

    # Workaround since FastAPI has some issues pending with iteration of Dataframe
    # Issue link: https://github.com/tiangolo/fastapi/issues/1733
    # for i in range(len(data)):
    #     data[i] = list(map(float, data[i]))

    candles = pd.DataFrame(data)  # pd.copy(data)
    candles = candles.iloc[:, 0:5]
    candles.columns = ['date', 'open', 'high', 'low', 'close']
    candles = candles.astype({'date': float, 'open': float, 'close': float, 'high': float, 'low': float,
                              'volume': float})
    candles.datetime = [dt.datetime.fromtimestamp(x / 1000.0) for x in candles.datetime]

    return candles


# FastAPI intialization
app = FastAPI()


# Routes
@app.get("/")
async def root():
    return {"message": "Please add URL+Symbol to get the OHLCV Data"}


@app.get("/{symbol}")
async def get_data(
        symbol: str,
        startTime: Optional[dt.datetime] = dt.datetime(2022, 1, 1),
        endTime: Optional[dt.datetime] = dt.datetime(2022, 2, 1)):
    symbol = symbol.upper()  # + "USDT"
    startTime = str(int(startTime.timestamp() * 1000))
    endTime = str(int(endTime.timestamp() * 1000))

    data = get_hourly_data(symbol, startTime, endTime)
    return data
