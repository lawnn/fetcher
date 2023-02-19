import os
import time
import requests
import pybotters
import numpy as np
import pandas as pd
import calendar
from datetime import datetime, timedelta
from pytz import utc
from matplotlib import pyplot as plt


class Util:
    @classmethod
    def plot_corrcoef(cls, arr1, arr2, output_dir: str = None, title: str = None, x: str = 'indicator',
                      y: str = 'Return', save_fig: bool = False):
        """
        plot_corrcoef(past_returns, future_returns, output_dir='my_favorite/1', title='comparison', save_fig=True)
        事前にデータ形成しておく
        例は下記のようにすればよい

        :例:pandasでの結合、欠損値の削除,要素数を同じにする方法(推奨される方法)
        a = pd.concat([arr1, arr2], axis=1).dropna(how='any')

        :例:numpyでの結合,欠損値の削除,要素数を同じにする方法(行が合わなくなるので非推奨)
        a = np.vstack([arr1, arr2]) # 縦に結合
        a = a[:, ~np.isnan(a).any(axis=0)]  # 欠損値の削除
        print(a)

        :param x: str
        :param y: str
        :param arr1: ndarray
        :param arr2: ndarray
        :param output_dir: png/comparison
        :param title: EXAMPLE
        :param save_fig: True or False
        :return:
        """

        if output_dir is None:
            output_dir = f'./png/'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        if isinstance(arr1, (pd.DataFrame, pd.Series)):
            arr1 = arr1.to_numpy()
        if isinstance(arr2, (pd.DataFrame, pd.Series)):
            arr2 = arr2.to_numpy()

        correlation = np.corrcoef(arr1, arr2)[1, 0]
        r2 = correlation ** 2

        if title is None:
            title = 'Correlation'

        # 頻出する総和を先に計算
        N = len(arr2)
        # Nxy = np.sum([xi * yi for xi, yi in zip(arr1, arr2)])
        # Nx = np.sum([xi for xi, yi in zip(arr1, arr2)])
        # Ny = np.sum([yi for xi, yi in zip(arr1, arr2)])
        # Nx2 = np.sum([xi * xi for xi, yi in zip(arr1, arr2)])

        # 係数
        # a = (N * Nxy - Nx * Ny) / (N * Nx2 - Nx ** 2)
        # b = (Nx2 * Ny - Nx * Nxy) / (N * Nx2 - Nx ** 2)
        #
        # Yの誤差
        # sigma_y = np.sqrt(1 / (N - 2) * np.sum([(a * xi + b - yi) ** 2 for xi, yi in zip(arr1, arr2)]))
        #
        # 係数の誤差
        # sigma_a = sigma_y * np.sqrt(N / (N * Nx2 - Nx ** 2))
        # sigma_b = sigma_y * np.sqrt(Nx2 / (N * Nx2 - Nx ** 2))

        p, cov, _ = np.polyfit(arr1, arr2, 1, cov=True)
        a = p[0]
        b = p[1]
        sigma_a = np.sqrt(cov[0, 0])
        sigma_b = np.sqrt(cov[1, 1])

        # Yの誤差
        sigma_y = np.sqrt(1 / (N - 2) * np.sum([(a * xi + b - yi) ** 2 for xi, yi in zip(arr1, arr2)]))

        y2 = a * arr1 + b

        fig = plt.figure()
        fig.suptitle(title)
        ax = fig.add_subplot(111)
        ax.scatter(arr1, arr2, c="blue", s=20, edgecolors="blue", alpha=0.3)
        ax.plot(arr1, y2, color='r')
        ax.set_xlabel(f"{x}")
        ax.set_ylabel(f"{y}")
        ax.grid(which="major", axis="x", color="gray", alpha=0.5, linestyle="dotted", linewidth=1)
        ax.grid(which="major", axis="y", color="gray", alpha=0.5, linestyle="dotted", linewidth=1)
        ax.text(1.02, 0.04,
                f"y = {a:.3f} \u00B1 {sigma_a:.3f} x + {b:.3f} \u00B1 {sigma_b:.3f}\nsigma$_y$={sigma_y:.3f}",
                transform=ax.transAxes)
        ax.text(0.83, 0.16, f"IC={correlation:.4f}", transform=ax.transAxes)
        ax.text(0.788, 0.1, f"R**2={r2:.4f}", transform=ax.transAxes)
        ax.text(0.59, 0.04, f"ProportionCorrect={(abs(correlation) + 1) / 2 * 100:.2f}%", transform=ax.transAxes)

        if save_fig:
            plt.savefig(f'{output_dir}/{title}.png')

        plt.show()

    @classmethod
    def np_shift(cls, arr, num=1, fill_value=np.nan):
        result = np.empty_like(arr)
        if num > 0:
            result[:num] = fill_value
            result[num:] = arr[:-num]
        elif num < 0:
            result[num:] = fill_value
            result[:num] = arr[-num:]
        else:
            result[:] = arr
        return result

    @classmethod
    def resample_ohlc(cls, org_df, timeframe):
        df = org_df.resample(f'{timeframe * 60}S').agg(
            {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})
        df['close'] = df['close'].fillna(method='ffill')
        df['open'] = df['open'].fillna(df['close'])
        df['high'] = df['high'].fillna(df['close'])
        df['low'] = df['low'].fillna(df['close'])
        return df

    @classmethod
    def trades_to_historical(cls, df, period: str = '1S'):
        if 'side' in df.columns:
            df['side'] = df['side'].mask(df['side'] == 'Buy', 'buy')
            df['side'] = df['side'].mask(df['side'] == 'BUY', 'buy')
            df['side'] = df['side'].mask(df['side'] == 'OrderSide.BUY', 'buy')
            df['side'] = df['side'].mask(df['side'] == 'Sell', 'sell')
            df['side'] = df['side'].mask(df['side'] == 'SELL', 'sell')
            df['side'] = df['side'].mask(df['side'] == 'OrderSide.SELL', 'sell')

            df["buyVol"] = np.where(df['side'] == 'buy', df['size'], 0)
            df["sellVol"] = np.where(df['side'] == 'sell', df['size'], 0)
            df_ohlcv = pd.concat([df["price"].resample(period).ohlc().ffill(),
                                  df["size"].resample(period).sum(),
                                  df["buyVol"].resample(period).sum(),
                                  df["sellVol"].resample(period).sum()
                                  ], axis=1)
            df_ohlcv.columns = ['open', 'high', 'low', 'close', 'volume', 'buyVol', 'sellVol']
        elif 'm' in df.columns:
            df['T'] = df['T'] / 1000
            df['T'] = pd.to_datetime(df['T'].astype(int), unit='s', utc=True, infer_datetime_format=True)
            df = df.set_index('T')
            df.index = df.index.tz_localize(None)
            df['m'] = df['m'].mask(df['m'] is True, 'buy')
            df['m'] = df['m'].mask(df['m'] is False, 'sell')
            df["buyVol"] = np.where(df['m'] == 'buy', df['q'], 0)
            df["sellVol"] = np.where(df['m'] == 'sell', df['q'], 0)
            df_ohlcv = pd.concat([df["p"].resample(period).ohlc().ffill(),
                                  df["q"].resample(period).sum(),
                                  df["buyVol"].resample(period).sum(),
                                  df["sellVol"].resample(period).sum()
                                  ], axis=1)
            df_ohlcv.columns = ['open', 'high', 'low', 'close', 'volume', 'buyVol', 'sellVol']
        else:
            df_ohlcv = pd.concat([df["price"].resample(period).ohlc().ffill(),
                                  df["size"].resample(period).sum(),
                                  ], axis=1)
            df_ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']
        return df_ohlcv

    @classmethod
    def ftx_get_trades(cls, symbol: str = 'BTC/USD', start_ymd: float = None, end_ymd: float = None,
                       output_dir: str = None):
        """ FTX 約定履歴
        symbol (str): BTC/USD
        start_ymd (float, optional): 2022-08-18
        end_ymd (float, optional): 2022-08-19

        Returns:
            DataFrame
        """

        def trades_ftx(market: str, start: str = None, end=None):
            res = requests.get(f'https://ftx.com/api/markets/{market}/trades',
                               params={'start_time': start, 'end_time': end}).json()
            return pd.DataFrame(res['result']).set_index('time')

        print(f'start: {start_ymd} --> end: {end_ymd}')
        if output_dir is None:
            output_dir = f'csv/FTX/trades/{symbol}'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        start_time = start_ymd
        if isinstance(start_ymd, str):
            start_ymd.replace('/', '-').replace('T', ' ')
            start_time = datetime.strptime(start_ymd, "%Y-%m-%d")
            start_time = cls.datetime_to_timestamp(start_time)

        if isinstance(end_ymd, str):
            end_ymd.replace('/', '-').replace('T', ' ')
            end_ymd = datetime.strptime(end_ymd, "%Y-%m-%d")
            end_ymd = cls.datetime_to_timestamp(end_ymd)

        df = trades_ftx(market=symbol, start=start_time, end=end_ymd)
        current_time = datetime.strptime(df.index[-1], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()

        while current_time > start_time + 3:
            new_df = trades_ftx(market=symbol, start=start_time, end=current_time)
            current_time = datetime.strptime(new_df.index[-1], "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
            df = pd.concat([df, new_df])

            print(new_df.index[0])
            time.sleep(0.1)

        df = df.drop_duplicates()
        df.to_csv(f'{output_dir}/{start_ymd}.csv')

        print(f'{output_dir}/{start_ymd}.csv\nfile created!')

    @classmethod
    def ftx_get_historical(cls, start_ymd: str, end_ymd: str = None, symbol: str = 'BTC-PERP', resolution: int = 60,
                           output_dir: str = None, request_interval: float = 0.035, update: bool = True) -> None:
        df_old = []
        if output_dir is None:
            output_dir = f'./csv/FTX/ohlcv/{resolution}s'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        path = f"{output_dir}/{symbol}.csv"

        if os.path.isfile(path) and update:
            print(f"Found old data --> {path}\nDifference update...\n")
            df_old = pd.read_csv(path, index_col='datetime', parse_dates=True)
            df_old.index = df_old.index
            start_dt = df_old.index[-1].timestamp() + 1
        else:
            start_ymd = start_ymd.replace('/', '-')
            start_dt = datetime.strptime(start_ymd, '%Y-%m-%d %H:%M:%S') + timedelta(hours=9)
            start_dt = int(start_dt.timestamp())

        if end_ymd is None:
            end_ymd = datetime.now()
        else:
            end_ymd = end_ymd.replace('/', '-')
            end_ymd = datetime.strptime(end_ymd, '%Y-%m-%d %H:%M:%S') + timedelta(hours=9)
        end_dt = int(end_ymd.timestamp())

        if start_dt > end_dt:
            raise ValueError(f'end_ymd{end_ymd} should be after start_ymd{start_ymd}.')

        params = dict(resolution=resolution, limit=5000, start_time=start_dt, end_time=end_dt)

        print(f'output dir: {output_dir}  save term: {start_ymd} -> {end_ymd:%Y-%m-%d %H:%M:%S}')

        r = requests.get(f'https://ftx.com/api/markets/{symbol}/candles', params=params)
        data = r.json()
        df = pd.DataFrame(data['result'])
        last_time = int(data['result'][0]['time'] / 1000) - 1
        while last_time >= start_dt:
            time.sleep(request_interval)
            temp_r = requests.get(f'https://ftx.com/api/markets/{symbol}/candles', params=dict(
                resolution=resolution, limit=5000, start_time=start_dt, end_time=last_time))
            temp_data = temp_r.json()
            try:
                last_time = int(temp_data['result'][0]['time'] / 1000) - 1
            except IndexError:
                print("Completed")
                break
            temp_df = pd.DataFrame(temp_data['result'])
            df = pd.concat([temp_df, df])
        df['time'] = df['time'] / 1000
        df.rename(columns={'time': 'datetime'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['datetime'].astype(int), unit='s', utc=True, infer_datetime_format=True)
        df = df.set_index('datetime').reindex(columns=['open', 'high', 'low', 'close', 'volume'])
        df.index = df.index.tz_localize(None)
        if os.path.isfile(path) and update:
            df = pd.concat([df_old, df])
        df.to_csv(path)

    @classmethod
    def bf_get_historical(cls, st_date: str, symbol: str = 'FX_BTC_JPY', period: str = 'm',
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

        r = pybotters.get("https://lightchart.bitflyer.com/api/ohlc", params=params)
        data = r.json()
        last_time = data[-1][0] - params['grouping'] * 1000 * 2

        # while len(data) <= int(needTerm): 必要な期間が必要な時の実用例(100期間のEMAが欲しいなど
        while start_date <= last_time:
            temp_r = pybotters.get("https://lightchart.bitflyer.com/api/ohlc", params=dict(
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

    @classmethod
    def bf_get_trades(cls, st_date: str, end_date: str, symbol: str = 'FX_BTC_JPY', output_dir: str = None,
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

        start_id = bf_search_id(st_date)
        print(f'[start date] {st_date} [target id] {start_id}')

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
                new_percentage = (end_base_id - end_id) / (end_base_id - start_id) * 100
                progress_time = temp_r[-1]["exec_date"][:19]
                progress_time = progress_time.replace("T", " ")

            if percentage != new_percentage:
                percentage = new_percentage
                print("\r", f"Progress: {percentage:.1f}%   Current date: {progress_time}", end="")

            if response[-1]["exec_date"][:10] != temp_r[-1]["exec_date"][:10]:
                response.extend(temp_r)
                df = pd.DataFrame(response)
                df = df[::-1]
                dt = datetime.strptime(response[-1]["exec_date"][:10], '%Y-%m-%d') + timedelta(days=1)
                dt = dt.strftime('%Y-%m-%d')
                path = f'{output_dir}/{dt}.csv'
                path = path.replace('//', '/')
                print(f'   Temp Output --> {path}')
                df.to_csv(path)
            else:
                response.extend(temp_r)

            time.sleep(request_interval)

        df = pd.DataFrame(response)
        df = df[::-1]
        dt = datetime.strptime(response[-1]["exec_date"][:10], '%Y-%m-%d') + timedelta(days=1)
        dt = dt.strftime('%Y-%m-%d')
        path = f'{output_dir}/{dt}-all.csv'
        path = path.replace('//', '/')
        print(f'Output --> {path}')
        df.to_csv(path)

        print(f'elapsed time: {(time.time() - start) / 60:.2f}min')

    @classmethod
    def bitfinex_get_trades(cls, start_ymd: str, end_ymd: str = None, symbol: str = 'tBTCUSD',
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

    @classmethod
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

    @classmethod
    def datetime_to_ms(cls, date):
        return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond / 1000)

    @classmethod
    def datetime_to_timestamp(cls, date):
        return int(calendar.timegm(date.timetuple()) + date.microsecond / 1000)

    @classmethod
    def binance_get_1st_id(cls, symbol, from_date):
        new_end_date = from_date + timedelta(seconds=60)
        r = requests.get('https://fapi.binance.com/fapi/v1/aggTrades',
                         params={
                             "symbol": symbol,
                             "startTime": cls.datetime_to_ms(from_date),
                             "endTime": cls.datetime_to_ms(new_end_date)
                         })

        if r.status_code != 200:
            print('somethings wrong!', r.status_code)
            print('sleeping for 10s... will retry')
            time.sleep(10)
            cls.binance_get_1st_id(symbol, from_date)

        response = r.json()
        if len(response) > 0:
            return response[0]['a']
        else:
            raise Exception('no trades found')

    @classmethod
    def binance_fetch_trades(cls, symbol, from_id):
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

    @classmethod
    def binance_get_trades(cls, start_ymd: str, end_ymd: str = None, symbol: str = 'BTCUSDT', output_dir: str = None,
                           request_interval: float = 0.5):
        """binance 約定履歴

            start_ymd (str): 2022-08-08
            symbol (str, optional): Defaults to 'BTCUSDT'.
            end_ymd (str, optional): 指定しない場合はstartの1日後
            output_dir (str, optional): アウトプット先のフォルダ
            request_interval (float, optional): 待機時間
        """
        if output_dir is None:
            output_dir = f'csv/binance/trades/{symbol}'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        if end_ymd is None:
            to_date = datetime.strptime(start_ymd, "%Y-%m-%d") + timedelta(days=1) - timedelta(microseconds=1)
        else:
            to_date = datetime.strptime(end_ymd, "%Y-%m-%d") - timedelta(microseconds=1)
        from_date = datetime.strptime(start_ymd, "%Y-%m-%d")
        from_id = cls.binance_get_1st_id(symbol, from_date)
        current_time = 0
        df = pd.DataFrame()

        while current_time < cls.datetime_to_ms(to_date):
            try:
                trades = cls.binance_fetch_trades(symbol, from_id)

                from_id = trades[-1]['a']
                current_time = trades[-1]['T']

                print(
                    f'fetched {len(trades)} trades from id {from_id} @ {datetime.utcfromtimestamp(current_time / 1000.0)}')

                df = pd.concat([df, pd.DataFrame(trades)])

                # don't exceed request limits
                time.sleep(request_interval)
            except Exception as e:
                print(f'{e}\nsomethings wrong....... sleeping for 15s')
                time.sleep(15)

        df.drop_duplicates(subset='a', inplace=True)
        # trim
        df = df[df['T'] <= cls.datetime_to_ms(to_date)]

        df.to_csv(f'{output_dir}/{start_ymd}.csv')

        print(f'{output_dir}/{start_ymd}.csv\nfile created!')

    @classmethod
    def binance_get_OI(cls, st_date: str, symbol: str = 'BTCUSDT', period: str = '5m', output_dir: str = None) -> None:
        start = time.time()

        if output_dir is None:
            output_dir = f'csv/binance/OI/{symbol}'
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

    @classmethod
    def binance_get_buy_sell_vol(cls, st_date: str, symbol: str = 'BTCUSDT', period: str = '5m',
                                 output_dir: str = None) -> None:
        start = time.time()

        if output_dir is None:
            output_dir = f'csv/binance/volume/{symbol}'
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
