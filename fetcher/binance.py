import os
import time
import requests
import pandas as pd
import polars as pl
from datetime import datetime, timedelta, timezone
from .util import pl_merge, make_ohlcv_from_timestamp
from .time_util import datetime_to_ms


def binance_get_1st_id(symbol, from_date):
    new_end_date = from_date + timedelta(seconds=60)
    r = requests.get('https://fapi.binance.com/fapi/v1/aggTrades',
                     params={
                         "symbol": symbol,
                         "startTime": datetime_to_ms(from_date),
                         "endTime": datetime_to_ms(new_end_date)
                     })

    if r.status_code != 200:
        print('somethings wrong!', r.status_code)
        print('sleeping for 10s... will retry')
        time.sleep(10)
        binance_get_1st_id(symbol, from_date)

    response = r.json()
    if len(response) > 0:
        return response[0]['a']
    else:
        raise Exception('no trades found')


def binance_fetch_trades(symbol, from_id):
    r = requests.get("https://fapi.binance.com/fapi/v1/aggTrades",
                     params={
                         "symbol": symbol,
                         "limit": 1000,
                         "fromId": from_id
                     })

    if r.status_code != 200:
        print('somethings wrong!', r.status_code)
        print('sleeping for 10s... will retry')
        time.sleep(10)

    return r.json()


def binance_get_trades(start_ymd: str, end_ymd: str = None, symbol: str = 'BTCUSDT', output_dir: str = None,
                       request_interval: float = 0.5):
    """binance 約定履歴

        start_ymd (str): 2022-08-08
        symbol (str, optional): Defaults to 'BTCUSDT'.
        end_ymd (str, optional): 指定しない場合はstartの1日後
        output_dir (str, optional): アウトプット先のフォルダ
        request_interval (float, optional): 待機時間
    """
    if output_dir is None:
        output_dir = f'binance/trades/{symbol}'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if end_ymd is None:
        to_date = datetime.strptime(start_ymd, "%Y-%m-%d") + timedelta(days=1) - timedelta(microseconds=1)
    else:
        to_date = datetime.strptime(end_ymd, "%Y-%m-%d") - timedelta(microseconds=1)
    from_date = datetime.strptime(start_ymd, "%Y-%m-%d")
    from_id = binance_get_1st_id(symbol, from_date)
    current_time = 0
    df = pd.DataFrame()

    while current_time < datetime_to_ms(to_date):
        try:
            trades = binance_fetch_trades(symbol, from_id)

            from_id = trades[-1]['a']
            current_time = trades[-1]['T']

            print("\r"+
                f'fetched {len(trades)} trades from id {from_id} @ {datetime.fromtimestamp(current_time / 1000.0, tz=timezone.utc)}',
                  end="")

            df = pd.concat([df, pd.DataFrame(trades)])

            # don't exceed request limits
            time.sleep(request_interval)
        except Exception as e:
            print(f'{e}\nsomethings wrong....... sleeping for 15s')
            time.sleep(15)

    df.drop_duplicates(subset='a', inplace=True)
    # trim
    df = df[df['T'] <= datetime_to_ms(to_date)]

    df.to_csv(f'{output_dir}/{start_ymd}.csv')

    print(f'\n[Output File] --> {output_dir}/{start_ymd}.csv\nfile created!')


def binance_get_OI(st_date: str, symbol: str = 'BTCUSDT', period: str = '5m', output_dir: str = None) -> None:
    start = time.time()

    if output_dir is None:
        output_dir = f'binance/{symbol}/OI'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    path = f'{output_dir}/{period}.csv'

    if os.path.isfile(path):
        print(f"Found old data --> {path}\nDiff update...\n")
        df_old = pd.read_csv(path, index_col='timestamp', parse_dates=True)
        start_date = int(df_old.index[-1].timestamp() * 1000)
    else:
        df_old = None
        st_date = st_date.replace('/', '-')
        start_date = int(datetime.strptime(st_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)

    print(f'Until  --> {datetime.fromtimestamp(start_date / 1000)}')

    r = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                     params=dict(symbol=symbol,
                                 period=period,
                                 limit=500,
                                 startTime=start_date,
                                 endTime=int(time.time()) * 1000))
    data = r.json()
    last_time = data[0]['timestamp'] - 1
    df = pd.DataFrame(data)

    while last_time >= start_date:
        temp_r = requests.get("https://fapi.binance.com/futures/data/openInterestHist",
                              params=dict(symbol=symbol,
                                          period=period,
                                          limit=500,
                                          startTime=start_date,
                                          endTime=last_time))
        temp_data = temp_r.json()
        try:
            last_time = temp_data[0]['timestamp'] - 1
        except IndexError:
            if os.path.isfile(path):
                print("finish...")
            break
        temp_df = pd.DataFrame(temp_data)
        df = pd.concat([temp_df, df])
        time.sleep(0.2)

    df = df.set_index('timestamp')
    df.index = pd.to_datetime(df.index, unit='ms', utc=True).tz_localize(None)

    if os.path.isfile(path):
        df = pd.concat([df_old, df])
        df = df.drop_duplicates()

    df.to_csv(path)

    print(f'Output --> {path}')
    print(f'elapsed time: {time.time() - start:.2f}sec')


def binance_get_buy_sell_vol(st_date: str, symbol: str = 'BTCUSDT', period: str = '5m',
                             output_dir: str = None) -> None:
    start = time.time()

    if output_dir is None:
        output_dir = f'binance/{symbol}/buy_sell_vol'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    path = f'{output_dir}/{period}.csv'

    if os.path.isfile(path):
        print(f"Found old data --> {path}\nDiff update...\n")
        df_old = pd.read_csv(path, index_col='timestamp', parse_dates=True)
        start_date = int(df_old.index[-1].timestamp() * 1000)
    else:
        df_old = None
        st_date = st_date.replace('/', '-')
        start_date = int(datetime.strptime(st_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)

    print(f'Until  --> {datetime.fromtimestamp(start_date / 1000)}')

    r = requests.get("https://fapi.binance.com/futures/data/takerlongshortRatio",
                     params=dict(symbol=symbol,
                                 period=period,
                                 limit=500,
                                 startTime=start_date,
                                 endTime=int(time.time()) * 1000))
    data = r.json()
    last_time = data[0]['timestamp'] - 1
    df = pd.DataFrame(data)

    while last_time >= start_date:
        temp_r = requests.get("https://fapi.binance.com/futures/data/takerlongshortRatio",
                              params=dict(symbol=symbol,
                                          period=period,
                                          limit=500,
                                          startTime=start_date,
                                          endTime=last_time))
        temp_data = temp_r.json()
        try:
            last_time = temp_data[0]['timestamp'] - 1
        except IndexError:
            if os.path.isfile(path):
                print("finish...")

        temp_df = pd.DataFrame(temp_data)
        df = pd.concat([temp_df, df])
        time.sleep(0.2)

    df = df.set_index('timestamp')
    df.index = pd.to_datetime(df.index, unit='ms', utc=True).tz_localize(None)

    if os.path.isfile(path):
        df = pd.concat([df_old, df])
        df = df.drop_duplicates()

    df.to_csv(path)

    print(f'Output --> {path}')
    print(f'elapsed time: {time.time() - start:.2f}sec')


def binance_make_ohlcv(path: str, time_frame, pl_type: pl.PolarsDataType=pl.Float64) -> pl.DataFrame:
    df = make_ohlcv_from_timestamp(path, "T", "p", "q", "m", True, False, time_frame, pl_type, 1_000)
    start_dt = datetime.combine(df["datetime"][0].date(), datetime.min.time())
    end_dt = datetime.combine(df["datetime"][-1].date(), datetime.min.time()) + timedelta(days=1, seconds=-1)
    dt_range = pl.DataFrame({'datetime': pl.date_range(start_dt, end_dt, time_frame, eager=True)})
    return pl_merge(dt_range, df, "datetime")