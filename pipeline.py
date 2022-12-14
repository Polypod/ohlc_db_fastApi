import main
import pandas as pd
import datetime as dt
from dagster import hourly_schedule, pipeline, repository, solid
from sqlalchemy import create_engine


@solid
def store_hourly_data():
    engine = create_engine("postgresql://ohlcuser:ohlcPassW@localhost/ohlcdata")
    symbols = ['QCOM', 'SNAP', 'TSLA']
    startTime = str(dt.datetime.strptime((dt.datetime.now() - dt.timedelta(days=30)).strftime('%Y-%m-%d')))
    endTime = str(dt.datetime.strptime(dt.datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d'))
    # startTime = str(int(dt.datetime.strptime((dt.datetime.now() - dt.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M').timestamp() * 1000))
    # endTime = str(int(dt.datetime.strptime(dt.datetime.now().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M').timestamp() * 1000))
    candles = pd.DataFrame()

    for symbol in symbols:
        ohlc_data = pd.DataFrame(main.get_hourly_data(symbol, startTime, endTime))
        ohlc_data['symbol'] = symbol
        ohlc_data.set_index('symbol', inplace=True)
        candles = candles.append(ohlc_data)

    candles.to_sql('ohlcdata', engine, if_exists='append')


@pipeline
def hourly_fetch_pipeline():
    store_hourly_data()


@hourly_schedule(
    pipeline_name="hourly_fetch_pipeline",
    start_date=dt.datetime(2022, 8, 20, 1),
    execution_time=dt.time(0, 0),
)
def data_fetch_schedule(date):
    pass


@repository
def dara_fetch_repository():
    return [hourly_fetch_pipeline, data_fetch_schedule]
