import os
import time
import requests
import numpy as np
import pandas as pd
import  polars as pl
from datetime import datetime, timedelta
from pytz import utc
from .util import make_ohlcv, pl_merge
from . import str_to_datetime


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


def bf_search_id(target_dt, symbol: str = "FX_BTC_JPY", count_id=14000):
    def bf_date_to_dt():
        try:
            return datetime.strptime(response[0]["exec_date"] + "+0000", "%Y-%m-%dT%H:%M:%S.%f%z")
        except ValueError:
            return datetime.strptime(response[0]["exec_date"] + "+0000", "%Y-%m-%dT%H:%M:%S%z")

    target_dt = target_dt.replace('/', '-')
    if len(target_dt) == 10:
        target_date = datetime.strptime(target_dt + " 00:00:00+0000", "%Y-%m-%d %H:%M:%S%z").astimezone(utc)

    elif len(target_dt) == 19:
        target_date = datetime.strptime(target_dt + "+0000", "%Y-%m-%d %H:%M:%S%z").astimezone(utc)
    else:
        raise ValueError

    # target_dateと現在時刻のおおまかな時間差
    hours = int((time.time() - target_date.timestamp()) / 3600) + 1

    # 最新約定履歴ID取得
    params = dict(product_code=symbol, count=500)
    response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=params).json()
    counter = 1
    end_id = response[0]["id"]

    # 二分探索開始ID設定
    start_id = end_id - hours * count_id  # 平均count_id件/時間と仮定する

    # start_idの約定履歴を取得
    params["before"] = start_id + 1
    response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=params).json()
    counter += 1

    # start_idの約定日時(exec_date)をdatetime(UTC)変換
    start_date = bf_date_to_dt()

    # target_dateより過去日時になるまでstart_idをずらす
    while start_date > target_date:
        # 1時間分(count_id件)idを差し引いて約定履歴を再取得
        start_id -= count_id
        params["before"] = start_id + 1
        response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=params).json()
        counter += 1

        # start_idの約定日時(exec_date)をdatetime(UTC)変換
        start_date = bf_date_to_dt()

    # 検索範囲が500件以下になるまで絞り込み
    while end_id - start_id > 500:
        # idの中央値を算出
        mid_id = int((start_id + end_id) / 2)

        # 中央値の約定履歴を取得
        params["before"] = mid_id + 1
        response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=params).json()
        counter += 1

        # 中央値の約定日時(exec_date)をdatetime(UTC)変換
        mid_date = bf_date_to_dt()

        # mid_dateがtarget_dateの左右どちらかチェック
        if mid_date < target_date:
            # target_dateは中央値よりも右(未来)のため、start_idをずらす
            start_id = mid_id
        else:
            # target_dateは中央値よりも左(過去)のため、end_idをずらす
            end_id = mid_id


    # 絞り込んだend_idまでの約定履歴を取得
    params["before"] = end_id + 1
    response = requests.get("https://api.bitflyer.com/v1/getexecutions", params=params).json()
    counter += 1

    # 約定履歴リストは新→古順のため、反転する
    execs = response[::-1]

    # 指定時刻後の最初のIDを検索
    for ex in execs:
        try:
            ex_date = datetime.strptime(ex["exec_date"] + "+0000", "%Y-%m-%dT%H:%M:%S.%f%z")
        except ValueError:
            ex_date = datetime.strptime(ex["exec_date"] + "+0000", "%Y-%m-%dT%H:%M:%S%z")
        if target_date < ex_date:
            # 検索結果出力
            return ex["id"], counter


def bf_get_trades(start_ymd: str, end_ymd: str = None, symbol: str = 'FX_BTC_JPY', output_dir: str = None) -> None:
    """ example
    bf_get_trades('2021/09/01 00:00:00')
    :param output_dir: str
    :param start_ymd: 2021/09/01 00:00:00     # start date
    :param end_ymd: 2021/09/10 00:00:00    # end date
    :param symbol: FX_BTC_JPY, BTC_JPY, ETH_JPY etc...
    """

    # 時間を記録する
    start = cur_time = time.time()

    # 保存先を生成
    if output_dir is None:
        output_dir = f'./bitflyer/{symbol}/trades'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 取得期間
    start_dt = str_to_datetime(start_ymd)
    if end_ymd is None:
        end_dt = start_dt + timedelta(days=1)
        end_ymd = datetime.strftime(end_dt, "%Y-%m-%d")
    else:
        end_dt = str_to_datetime(end_ymd)
    if start_dt > end_dt:
        raise ValueError(f'end_ymd{end_ymd} should be after start_ymd{start_ymd}.')

    # start_ID検索
    start_ymd = start_ymd.replace('/', '-')
    start_id, counter = bf_search_id(start_ymd)
    print(f' [start date] {start_ymd} [target id] {start_id}')

    # end_ID検索
    end_ymd = end_ymd.replace('/', '-')
    end_id, counter1 = bf_search_id(end_ymd)
    print(f' [end date] {end_ymd} [target id] {end_id}')

    # 初期値
    params = dict(product_code=symbol, count=500)

    params["before"] = end_id
    response = requests.get('https://api.bitflyer.com/v1/getexecutions', params=params).json()
    end_base_id = end_id = response[0]['id']
    counter = counter + counter1 + 1
    cur_dt = end_dt

    while start_id <= end_id:
        params["before"] = end_id
        temp_r = requests.get('https://api.bitflyer.com/v1/getexecutions', params=params).json()
        counter += 1
        if "error_message" in temp_r:
            raise Exception("API制限が掛かっています。300秒待機してから再実行してください。")

        # bitflyerのAPI Limitである300秒に500回までのリクエスト数をクリアさせる条件式
        if counter >= 500:
            wait_time = 301 - (time.time() - cur_time)
            time.sleep(wait_time)
            cur_time = time.time()
            counter = 0

        try:
            end_id = temp_r[-1]['id']
        except KeyError:
            print(temp_r)
            pass

        # progress timeの計測
        # str型の中身(2023-01-01T00)なので1時間ごと(13行まで)に変化があったら計算させている。
        if response[-1]["exec_date"][:13] != temp_r[-1]["exec_date"][:13]:
            try:
                percentage = (end_base_id - end_id) / (end_base_id - start_id) * 100
            except ZeroDivisionError:
                percentage = 0
            progress_time = temp_r[-1]["exec_date"][:19]
            progress_time = progress_time.replace("T", " ")

            # 進捗確認,print出力
            try:
                print("\r", f"[Progress] {percentage:.1f}%  " +
                      f"[Remaining time] {(((end_id - start_id) / 500 * 0.6) / 60):.2f}min  " +
                      f"[Current date] {progress_time}  ", end="")
            except ZeroDivisionError:
                pass

        # 日付が変わったらcsv出力する
        if response[-1]["exec_date"][:10] != temp_r[-1]["exec_date"][:10]:
            response.extend(temp_r)
            # exec_dateをdatetime型に変換し1日毎にトリム,行を逆順にする,
            df = (
                pl.DataFrame(response)
                    .with_columns(pl.col("exec_date").str.strptime(pl.Datetime, strict=False))
                    .reverse()
                    .filter((pl.col("exec_date").ge(cur_dt - timedelta(days=1))) & (pl.col("exec_date").lt(cur_dt)))
                )
            cur_dt -= timedelta(days=1)

            path = f'{output_dir}/{cur_dt:%Y-%m-%d}.csv'
            path = path.replace('//', '/')
            df.write_csv(path)
            print(f'[Output File] --> {path}')
            params["before"] = end_id = df.get_column("id")[0]
            response = requests.get('https://api.bitflyer.com/v1/getexecutions', params=params).json()
            counter += 1
        else:
            response.extend(temp_r)

    print(f'elapsed time: {(time.time() - start) / 60:.2f}min')


def bf_trades_to_historical(path: str, price_pl_type: type = pl.Int64, period: str="1s") -> pl.DataFrame:
    return (
        pl.scan_csv(path)
        .with_columns([pl.col("exec_date").str.strptime(pl.Datetime, strict=False).alias("datetime"),
                       pl.col("price").cast(price_pl_type).alias("price"),
                       pl.when(pl.col('side') == 'BUY').then(pl.col('size')).otherwise(0).alias('buy_size'),
                       pl.when(pl.col('side') == 'SELL').then(pl.col('size')).otherwise(0).alias('sell_size')])
        .groupby_dynamic('datetime', every=period)
        .agg([
            pl.col('price').first().alias('open'),
            pl.col('price').max().alias('high'),
            pl.col('price').min().alias('low'),
            pl.col('price').last().alias('close'),
            pl.col('size').sum().alias('volume'),
            pl.col('buy_size').sum().alias('buy_vol'),
            pl.col('sell_size').sum().alias('sell_vol'),
        ])
        ).collect()


def bf_make_ohlcv(path: str, time_frame, pl_type: pl.PolarsDataType=pl.Int64) -> pl.DataFrame:
    df = make_ohlcv(path, "exec_date", "price", "size", "side", "BUY", "SELL", time_frame, pl_type)
    start_dt = datetime.combine(df["datetime"][0].date(), datetime.min.time())
    end_dt = datetime.combine(df["datetime"][-1].date(), datetime.min.time()) + timedelta(days=1, seconds=-1)
    dt_range = pl.DataFrame({'datetime': pl.date_range(start_dt, end_dt, time_frame, eager=True)})
    return pl_merge(dt_range, df, "datetime")