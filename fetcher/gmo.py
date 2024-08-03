import os
import time
import requests
import polars as pl
from traceback import format_exc
from datetime import datetime, timedelta
from .time_util import str_to_datetime
from .util import make_ohlcv, pl_merge


def gmo_get_historical(start_ymd: str, end_ymd: str = None, symbol: str = 'BTC_JPY', interval: str = '1min',
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
        output_dir = f'./gmo/{symbol}/ohlcv/{interval}'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    start_ymd = start_ymd.replace('/', '-')
    if end_ymd is None:
        end_ymd = start_ymd
    else:
        end_ymd = end_ymd.replace('/', '-')
    start_dt = datetime.strptime(start_ymd, '%Y-%m-%d')
    end_dt = datetime.strptime(end_ymd, '%Y-%m-%d')
    if start_dt > end_dt:
        raise ValueError(f'end_ymd{end_ymd} should be after start_ymd{start_ymd}.')

    print(f'output dir: {output_dir}  save term: {start_dt:%Y/%m/%d} -> {end_dt:%Y/%m/%d}')

    cur_dt = start_dt
    total_count = 0
    while cur_dt <= end_dt:
        r = requests.get(f'https://api.coin.z.com/public/v1/klines',
                         params=dict(symbol=symbol, interval=interval, date=cur_dt.strftime('%Y%m%d'))).json()
        df = (
            pl.DataFrame(r["data"])
            .rename({"openTime": "datetime"})
            .with_columns([
                pl.col("datetime").cast(pl.Int64).map(lambda x: x * 1_000).cast(pl.Datetime(time_unit='us'))
            ])
        )
        df.write_csv(f'{output_dir}/{cur_dt.strftime("%Y-%m-%d")}.csv')
        total_count += 1
        if progress_info:
            print(f'Completed output {cur_dt:%Y%m%d}.csv')

        cur_dt += timedelta(days=1)
        if request_interval > 0:
            time.sleep(request_interval)


def gmo_get_trades(start_ymd: str, end_ymd: str = None, symbol: str = 'BTC_JPY',
                        output_dir: str = None, request_interval: float = 0.01,
                        progress_info: bool = True) -> None:

    try:
        # 出力ディレクトリ設定
        if output_dir is None:
            output_dir = f'./gmo/{symbol}/trades_only/'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 取得期間
        start_dt = str_to_datetime(start_ymd)
        if end_ymd is None:
            end_dt = start_dt
        else:
            end_dt = str_to_datetime(end_ymd)
        if start_dt > end_dt:
            raise ValueError(f'end_ymd{end_ymd} should be after start_ymd{start_ymd}.')

        print(f'output dir: {output_dir}  save term: {start_dt:%Y/%m/%d} -> {end_dt:%Y/%m/%d}')

        # 日別にcsv出力
        cur_dt = start_dt
        total_count = 0
        while cur_dt <= end_dt:
            # csvパス
            csv_path = os.path.join(output_dir, f"{cur_dt:%Y}-{cur_dt:%m}-{cur_dt:%d}.csv")
            # csv存在チェック
            if os.path.isfile(csv_path):
                cur_dt += timedelta(days=1)
                continue

            try:
                df = pl.read_csv(f"https://api.coin.z.com/data/trades/{symbol}/{cur_dt:%Y}/{cur_dt:%m}/{cur_dt:%Y%m%d}_{symbol}.csv.gz")

            except Exception as e:
                print(f"{e}")
                df = None

            if df is None or len(df) < 1:
                print(f"Failed to read the trading file." +
                      f"https://api.coin.z.com/data/trades/{symbol}/{cur_dt:%Y}/{cur_dt:%m}/{cur_dt:%Y%m%d}_{symbol}.csv.gz")
                cur_dt += timedelta(days=1)
                if request_interval > 0:
                    time.sleep(request_interval)
                continue

            df.write_csv(csv_path)
            total_count += 1
            if progress_info:
                print(f'Completed output {csv_path}.csv')

            cur_dt += timedelta(days=1)
            if request_interval > 0:
                time.sleep(request_interval)

        print(f'Total output files: {total_count}')

    except Exception as e:
        print(f'save_daily_ohlcv_from_gmo_trading_gz failed.\n{format_exc()}')
        raise e


def gmo_trades_to_historical(start_ymd: str, end_ymd: str = None, symbol: str = 'BTC_JPY',
                            time_frame: str = '1s', pl_type: pl.DataType = pl.Float64,
                            output_dir: str = None, request_interval: float = 0.01,
                            progress_info: bool = True) -> None:

    try:
        # 出力ディレクトリ設定
        if output_dir is None:
            output_dir = f'./gmo/{symbol}/trades/'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 取得期間
        start_dt = str_to_datetime(start_ymd)

        if end_ymd is None:
            end_dt = start_dt
        else:
            end_dt = str_to_datetime(end_ymd)

        if start_dt > end_dt:
            raise ValueError(f'end_ymd{end_ymd} should be after start_ymd{start_ymd}.')

        print(f'output dir: {output_dir}  save term: {start_dt:%Y/%m/%d} -> {end_dt:%Y/%m/%d}')

        # 日別にcsv出力
        cur_dt = start_dt
        total_count = 0
        while cur_dt <= end_dt:
            # csvパス
            csv_path = os.path.join(output_dir, f"{cur_dt:%Y}-{cur_dt:%m}-{cur_dt:%d}.csv")
            # csv存在チェック
            if os.path.isfile(csv_path):
                cur_dt += timedelta(days=1)
                continue

            try:
                df = make_ohlcv(f"https://api.coin.z.com/data/trades/{symbol}/{cur_dt:%Y}/{cur_dt:%m}/{cur_dt:%Y%m%d}_{symbol}.csv.gz",
                                "timestamp", "price", "size", "side", "BUY", "SELL", time_frame, pl_type)
            except Exception as e:
                print(f"{e}")
                df = None

            if df is None or len(df) < 1:
                print(f"Failed to read the trading file.\n" +
                      f"https://api.coin.z.com/data/trades/{symbol}/{cur_dt:%Y}/{cur_dt:%m}/{cur_dt:%Y%m%d}_{symbol}.csv.gz")
                cur_dt += timedelta(days=1)
                if request_interval > 0:
                    time.sleep(request_interval)
                continue

            df.write_csv(csv_path)
            total_count += 1
            if progress_info:
                print(f'Completed output {csv_path}.csv')

            cur_dt += timedelta(days=1)
            if request_interval > 0:
                time.sleep(request_interval)

        print(f'Total output files: {total_count}')

    except Exception as e:
        print(f'save_daily_ohlcv_from_gmo_trading_gz failed.\n{format_exc()}')
        raise e


def gmo_make_ohlcv(date: str, symbol: str = 'BTC_JPY',
                   time_frame: str = '1s', pl_type: pl.DataType = pl.Float64,):
    """
    example
    print(gmo_make_ohlcv("2023-07-20", pl_type=pl.Int32))
    """
    # 取得期間
    dt = str_to_datetime(date)
    after = dt + timedelta(days=1)

    df1 = make_ohlcv(f"https://api.coin.z.com/data/trades/{symbol}/{dt:%Y}/{dt:%m}/{dt:%Y%m%d}_{symbol}.csv.gz",
                            "timestamp", "price", "size", "side", "BUY", "SELL", time_frame, pl_type)
    df2 = make_ohlcv(f"https://api.coin.z.com/data/trades/{symbol}/{after:%Y}/{after:%m}/{after:%Y%m%d}_{symbol}.csv.gz",
                            "timestamp", "price", "size", "side", "BUY", "SELL", time_frame, pl_type)
    df = (
        pl.concat([df1, df2])
        .lazy()
        .filter((pl.col("datetime").ge(dt)) & (pl.col("datetime").lt(after)))
        .collect()
        )
    start_dt = datetime.combine(df["datetime"][0].date(), datetime.min.time())
    end_dt = datetime.combine(df["datetime"][-1].date(), datetime.min.time()) + timedelta(days=1, seconds=-1)
    dt_range = pl.DataFrame({'datetime': pl.datetime_range(start_dt, end_dt, time_frame, eager=True)})
    return pl_merge(dt_range, df, "datetime")


def gmo_FX_get_historical(start_ymd: str, end_ymd: str = None, symbol: str = 'USD_JPY', interval: str = '1min',
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
        output_dir = f'./gmo_FX/{symbol}/ohlcv/{interval}'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    start_ymd = start_ymd.replace('/', '-')
    if end_ymd is None:
        end_ymd = start_ymd
    else:
        end_ymd = end_ymd.replace('/', '-')
    start_dt = datetime.strptime(start_ymd, '%Y-%m-%d')
    end_dt = datetime.strptime(end_ymd, '%Y-%m-%d')
    if start_dt > end_dt:
        raise ValueError(f'end_ymd{end_ymd} should be after start_ymd{start_ymd}.')

    print(f'output dir: {output_dir}  save term: {start_dt:%Y/%m/%d} -> {end_dt:%Y/%m/%d}')

    cur_dt = start_dt
    total_count = 0
    while cur_dt <= end_dt:
        r = requests.get(f'https://forex-api.coin.z.com/public/v1/klines',
                         params=dict(symbol=symbol, priceType="ASK", interval=interval, date=cur_dt.strftime('%Y%m%d'))).json()
        df = (
            pl.DataFrame(r["data"])
            .rename({"openTime": "datetime"})
            .with_columns([
                pl.col("datetime").cast(pl.Int64).map(lambda x: x * 1_000).cast(pl.Datetime(time_unit='us'))
            ])
        )
        df.write_csv(f'{output_dir}/{cur_dt.strftime("%Y-%m-%d")}.csv')
        total_count += 1
        if progress_info:
            print(f'Completed output {cur_dt:%Y%m%d}.csv')

        cur_dt += timedelta(days=1)
        if request_interval > 0:
            time.sleep(request_interval)