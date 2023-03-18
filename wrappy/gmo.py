import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta


def gmo_get_historical(cls, start_ymd: str, end_ymd: str, symbol: str = 'BTC_JPY', interval: str = '1min',
                       output_dir: str = None, request_interval: float = 0.2, progress_info: bool = True) -> None:
    """ example
    gmo_get_historical('2021/09/01', '2021/09/08')
    :param start_ymd: 2021/09/01
    :param end_ymd: 2021/09/08
    :param symbol: BTC, BTC_JPY, ETH_JPY
    :param interval: 1min 5min 10min 15min 30min 1hour 4hour 8hour 12hour 1day 1week 1month
    :param output_dir: csv/hoge/huga/
    :param request_interval: 0
    :param progress_info: False
    :return:
    """
    if output_dir is None:
        output_dir = f'./gmo/{symbol}/ohlcv/{interval}/'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    start_dt = datetime.strptime(start_ymd, '%Y/%m/%d')
    end_dt = datetime.strptime(end_ymd, '%Y/%m/%d')
    if start_dt > end_dt:
        raise ValueError(f'end_ymd{end_ymd} should be after start_ymd{start_ymd}.')

    print(f'output dir: {output_dir}  save term: {start_dt:%Y/%m/%d} -> {end_dt:%Y/%m/%d}')

    cur_dt = start_dt
    total_count = 0
    while cur_dt <= end_dt:
        r = requests.get(f'https://api.coin.z.com/public/v1/klines',
                         params=dict(symbol=symbol, interval=interval, date=cur_dt.strftime('%Y%m%d')))
        data = r.json()
        df = pd.DataFrame(data['data'])
        df.rename(columns={'openTime': 'time'}, inplace=True)
        df = df.set_index('time')
        df.index = pd.to_datetime(df.index, unit='ms', utc=True).tz_localize(None)
        df.to_csv(f'{output_dir}/{cur_dt.strftime("%Y%m%d")}.csv')
        total_count += 1
        if progress_info:
            print(f'Completed output {cur_dt:%Y%m%d}.csv')

        cur_dt += timedelta(days=1)
        if request_interval > 0:
            time.sleep(request_interval)