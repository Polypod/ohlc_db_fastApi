import main
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

# Database Connection
engine = create_engine("postgresql://ohlcuser:ohlcPassW@localhost/ohlcdata")

# Declarations
symbols = ['QCOM', 'SNAP', 'TSLA']
startTime = str(dt.datetime.strptime((dt.datetime.now() - dt.timedelta(days=30)).strftime('%Y-%m-%d')))
endTime = str(dt.datetime.strptime(dt.datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d'))
# startTime = str(int(dt.datetime.strptime((dt.datetime.now() - dt.timedelta(days=30)).strftime('%Y-%m-%d'), '%Y-%m-%d').timestamp() * 1000))
# endTime = str(
#     int(dt.datetime.strptime(dt.datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d').timestamp() * 1000))
candles = pd.DataFrame()

# Fetching Hourly Data and appending to DataFrame
for symbol in symbols:
    ohlc_data = pd.DataFrame(main.get_hourly_data(symbol, startTime, endTime))
    ohlc_data['symbol'] = symbol
    ohlc_data.set_index('symbol', inplace=True)
    candles = candles.append(ohlc_data)

# Transferring data to PostgreSQL Database
candles.to_sql('ohlc_ts', engine, if_exists='append')
