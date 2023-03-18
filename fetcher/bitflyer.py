import os
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime
from pytz import utc

def bf_get_historical(st_date: str, symbol: str = 'FX_BTC_JPY', period: str = 'm',
                      grouping: int = 1, output_dir: str = None) -> None:
    """ example
    bf_get_historical('2021/09/01')
    :param output_dir: str
    :param st_date: 2021/09/01
    :param symbol: FX_BTC_JPY, BTC_JPY, ETH_JPY
    :param period: m
    :param grouping: 1-30
    :return:
    """
    start = time.time()

    if output_dir is None:
        output_dir = f'csv/bf_ohlcv_{symbol}'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    path = f'{output_dir}_{period}.csv'
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    now = int(datetime.strptime(now_str, "%Y-%m-%d %H:%M").timestamp()) * 1000
    params = {'symbol': symbol, 'period': period, 'type': 'full', 'before': now, 'grouping': grouping}

    if os.path.isfile(path):
        print(f"Found old data --> {path}\nDifference update...\n")
        df_old = pd.read_csv(path, index_col='time', parse_dates=True)
        start_date = int(df_old.index[-1].timestamp() * 1000)
    else:
        df_old = None
        st_date = st_date.replace('/', '-')
        start_date = datetime.strptime(st_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000

    print(f'Until  --> {datetime.fromtimestamp(start_date / 1000)}')

    r = requests.get("https://lightchart.bitflyer.com/api/ohlc", params=params)
    data = r.json()
    last_time = data[-1][0] - params['grouping'] * 1000 * 2

    # while len(data) <= int(needTerm): 必要な期間が必要な時の実用例(100期間のEMAが欲しいなど
    while start_date <= last_time:
        temp_r = requests.get("https://lightchart.bitflyer.com/api/ohlc", params=dict(
            symbol=params['symbol'], period=params['period'], before=last_time, grouping=params['grouping']))
        temp_data = temp_r.json()
        data.extend(temp_data)
        last_time = temp_data[-1][0] - params['grouping'] * 1000 * 2

    df = pd.DataFrame(data, dtype='object')[::-1]
    df = df.drop(columns={6, 7, 8, 9}).rename(
        columns={0: 'time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}).set_index('time').replace(
        {'open': {'': np.nan}, 'high': {'': np.nan}, 'low': {'': np.nan}, 'close': {'': np.nan},
         'volume': {'': np.nan}}).dropna(how='any')
    df.index = pd.to_datetime(df.index, unit='ms', utc=True).tz_localize(None)

    if os.path.isfile(path):
        df = pd.concat([df_old, df])
        df = df.drop_duplicates()

    df.to_csv(path)

    print(f'Output --> {path}')
    print(f'elapsed time: {time.time() - start:.2f}sec')


def bf_get_trades(st_date: str, end_date: str, symbol: str = 'FX_BTC_JPY', output_dir: str = None,
                    request_interval: float = 0.6) -> None:
    """ example
    bf_get_trades('2021/09/01 00:00:00')
    :param output_dir: str
    :param st_date: 2021/09/01 00:00:00     # start date
    :param end_date: 2021/09/10 00:00:00    # end date
    :param symbol: FX_BTC_JPY, BTC_JPY, ETH_JPY etc...
    :param request_interval: 0.6
    """

    def bf_search_id(target_dt, count_id=14000):
        def bf_date_to_dt():
            try:
                return datetime.strptime(_response[0]["exec_date"] + "+0000", "%Y-%m-%dT%H:%M:%S.%f" + "%z")
            except ValueError:
                return datetime.strptime(_response[0]["exec_date"] + "+0000", "%Y-%m-%dT%H:%M:%S" + "%z")
            except KeyError as  e:
                print(_response)
                print(e)

        target_dt = target_dt.replace('/', '-')
        _dt = datetime.strptime(target_dt + "+0000", "%Y-%m-%d %H:%M:%S" + "%z")
        target_date = _dt.astimezone(utc)

        # target_dateと現在時刻のおおまかな時間差
        hours = int((time.time() - target_date.timestamp()) / 3600) + 1

        # 最新約定履歴ID取得
        _params = dict(product_code=symbol, count=500)
        _response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=_params).json()
        _end_id = _response[0]["id"]

        # 二分探索開始ID設定
        _start_id = _end_id - hours * count_id  # 平均count_id件/時間と仮定する

        # start_idの約定履歴を取得
        _params["before"] = _start_id + 1
        _response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=_params).json()

        # start_idの約定日時(exec_date)をdatetime(UTC)変換

        start_date = bf_date_to_dt()

        # target_dateより過去日時になるまでstart_idをずらす
        while start_date > target_date:
            # 1時間分(count_id件)idを差し引いて約定履歴を再取得
            _start_id -= count_id
            _params["before"] = _start_id + 1
            _response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=_params).json()

            # start_idの約定日時(exec_date)をdatetime(UTC)変換
            start_date = bf_date_to_dt()
            time.sleep(request_interval)

        # 検索範囲が500件以下になるまで絞り込み
        while _end_id - _start_id > 500:
            # idの中央値を算出
            mid_id = int((_start_id + _end_id) / 2)

            # 中央値の約定履歴を取得
            _params["before"] = mid_id + 1
            _response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=_params).json()

            # 中央値の約定日時(exec_date)をdatetime(UTC)変換
            mid_date = bf_date_to_dt()

            # mid_dateがtarget_dateの左右どちらかチェック
            if mid_date < target_date:
                # target_dateは中央値よりも右(未来)のため、start_idをずらす
                _start_id = mid_id
            else:
                # target_dateは中央値よりも左(過去)のため、end_idをずらす
                _end_id = mid_id
            time.sleep(request_interval)

        # 絞り込んだend_idまでの約定履歴を取得
        _params["before"] = _end_id + 1
        _response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=_params).json()
        time.sleep(request_interval)

        # 約定履歴リストは新→古順のため、反転する
        execs = _response[::-1]

        # 指定時刻後の最初のIDを検索
        for ex in execs:
            try:
                ex_date = datetime.strptime(ex["exec_date"] + "+0000", "%Y-%m-%dT%H:%M:%S.%f" + "%z")
            except ValueError:
                ex_date = datetime.strptime(ex["exec_date"] + "+0000", "%Y-%m-%dT%H:%M:%S" + "%z")
            if target_date < ex_date:
                # 検索結果出力
                return ex["id"]

    start = time.time()

    if output_dir is None:
        output_dir = f'./bitflyer/{symbol}/trades'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    st_date = st_date.replace('/', '-')
    start_id = bf_search_id(st_date)
    print(f'[start date] {st_date} [target id] {start_id}')

    end_date = end_date.replace('/', '-')
    end_id = bf_search_id(end_date)
    print(f'[end date] {end_date} [target id] {end_id}')

    params = dict(product_code=symbol, count=500)
    params["before"] = end_id
    response = requests.get('https://api.bitflyer.com/v1/getexecutions', params=params).json()
    end_base_id = end_id = response[0]['id']
    progress_time = new_percentage = percentage = 0
    time.sleep(request_interval)

    while start_id <= end_id:
        params["before"] = end_id
        temp_r = requests.get('https://api.bitflyer.com/v1/getexecutions', params=params).json()
        try:
            end_id = temp_r[-1]['id']
        except KeyError:
            print(temp_r)
            break

        if response[-1]["exec_date"][:13] != temp_r[-1]["exec_date"][:13]:
            try:
                new_percentage = (end_base_id - end_id) / (end_base_id - start_id) * 100
            except ZeroDivisionError:
                new_percentage = 0
            progress_time = temp_r[-1]["exec_date"][:19]
            progress_time = progress_time.replace("T", " ")

        if percentage != new_percentage:
            percentage = new_percentage
            try:
                print("\r", f"[Progress] {percentage:.1f}%  " +
                      f"[Remaining time] {(((end_id - start_id) / 500 * request_interval) / 60):.2f}min  " +
                      f"[Current date] {progress_time}  ", end="")
            except ZeroDivisionError:
                pass

        if response[-1]["exec_date"][:10] != temp_r[-1]["exec_date"][:10]:
            response.extend(temp_r)
            df = pd.DataFrame(response)
            df = df[::-1]
            path = f'{output_dir}/{st_date[:10]}_{end_date[:10]}.csv'
            path = path.replace('//', '/')
            print(f'Output --> {path}')
            df.to_csv(path)
        else:
            response.extend(temp_r)

        time.sleep(request_interval)

    df = pd.DataFrame(response)
    df = df[::-1]
    path = f'{output_dir}/{st_date[:10]}_{end_date[:10]}.csv'
    path = path.replace('//', '/')
    df.to_csv(path)
    print(f'Output --> {path}')

    print(f'elapsed time: {(time.time() - start) / 60:.2f}min')