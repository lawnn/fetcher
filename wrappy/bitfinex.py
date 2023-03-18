import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta


def bitfinex_get_trades(start_ymd: str, end_ymd: str = None, symbol: str = 'tBTCUSD',
                        output_dir: str = None, progress_info: bool = True, update: bool = True) -> None:
    """
    ※　日本時間の環境に合わせてます.
    時間掛かるので数日分取得する際は注意
    :param progress_info:
    :param start_ymd:
    :param end_ymd:
    :param symbol:
    :param output_dir:
    :param update:
    :return:
    """
    start = time.time()
    df_old = []

    if end_ymd is None:
        end_ymd = datetime.now()
    else:
        end_ymd = end_ymd.replace('/', '-')
        end_ymd = datetime.strptime(end_ymd, '%Y-%m-%d %H:%M:%S') + timedelta(hours=9)
    end_dt = int(end_ymd.timestamp()) * 1000

    if output_dir is None:
        output_dir = f'./csv/trades/bitfinex/{symbol}'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    path = f"{output_dir}/{end_ymd:%Y-%m-%d}.csv"

    if os.path.isfile(path) and update:
        print(f"Found old data --> {path}\nDifference update...\n")
        df_old = pd.read_csv(path, index_col='datetime', parse_dates=True)
        df_old.index = df_old.index
        start_dt = df_old.index[-1].timestamp() + 1
    else:
        start_ymd = start_ymd.replace('/', '-')
        start_ymd = datetime.strptime(start_ymd, '%Y-%m-%d %H:%M:%S') + timedelta(hours=9)
        start_dt = int(start_ymd.timestamp()) * 1000

    if start_dt > end_dt:
        raise ValueError(f'end_ymd{end_ymd} should be after start_ymd{start_ymd}.')

    print(
        f'output dir: {output_dir}  save term: {start_ymd - timedelta(hours=9)} -> {end_ymd - timedelta(hours=9)}')

    r = requests.get(f'https://api-pub.bitfinex.com/v2/trades/{symbol}/hist', params=dict(
        limit=10000, start=start_dt, end=end_dt, sort=-1))
    data = r.json()
    df = pd.DataFrame(data)[::-1]
    last_time = data[-1][1] - 1
    loop_time = 0
    counter = 1
    while last_time >= start_dt:
        start_loop_time = time.time()
        temp_r = requests.get(f'https://api-pub.bitfinex.com/v2/trades/{symbol}/hist', params=dict(
            limit=10000, start=start_dt, end=last_time, sort=-1))
        temp_data = temp_r.json()
        try:
            last_time = temp_data[-1][1] - 1
            if progress_info:
                print(f'process: {datetime.fromtimestamp(int(last_time / 1000))}')
        except IndexError:
            print("completed!!")
            break

        temp_df = pd.DataFrame(temp_data)[::-1]
        df = pd.concat([temp_df, df])
        counter += 1
        loop_time += time.time() - start_loop_time
        if counter % 30 == 0:
            if progress_info:
                print(f'------ waiting {60 - loop_time:.2f}sec ------')
            time.sleep(60 - loop_time)
            loop_time = 0

    df.rename(columns={0: 'ID', 1: 'datetime', 2: 'size', 3: 'price'}, inplace=True)
    df['datetime'] = df['datetime'] / 1000
    df['datetime'] = pd.to_datetime(df['datetime'].astype(float), unit='s', utc=True, infer_datetime_format=True)
    df = df.set_index('datetime')
    df.index = df.index.tz_localize(None)
    if os.path.isfile(path) and update:
        df = pd.concat([df_old, df])
    df.to_csv(path)

    print(f'Output --> {path}')
    print(f'elapsed time: {(time.time() - start) / 60:.2f}min')