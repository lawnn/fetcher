import os
import time
import requests
import polars as pl
from traceback import format_exc
from datetime import timedelta
from .time_util import str_to_datetime


def bitbank_get_trades(st_date: str, symbol: str = "btc_jpy", output_dir: str = None) -> None:
    dt = str(st_date).replace("/", "-")
    st_date = str(st_date).replace("/", "").replace("-", "").replace(" 00:00:00", "")
    r = requests.get(f"https://public.bitbank.cc/{symbol}/transactions/{st_date}").json()

    if output_dir is None:
        output_dir = f'bitbank/trades/{symbol}'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    pl.DataFrame(r["data"]["transactions"]).write_csv(f"{output_dir}/{dt}.csv")


def bitbank_trades_to_historical(start_ymd: str, end_ymd: str = None, symbol: str = 'btc_jpy',
                            period: str = '1s', price_pl_type: pl.PolarsDataType = pl.Float64,
                            size_pl_type: type = pl.Float64, output_dir: str = None,
                            request_interval: float = 0.01, progress_info: bool = True) -> None:

    try:
        # 出力ディレクトリ設定
        if output_dir is None:
            output_dir = f'./bitbank/{symbol}/trades/'
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
                df = (pl.DataFrame(requests.get(f"https://public.bitbank.cc/{symbol}/transactions/{cur_dt:%Y%m%d}")
                                   .json()["data"]["transactions"])
                .lazy()
                .with_columns([
                (pl.col("executed_at") * 1_000).cast(pl.Datetime(time_unit='us')).alias('datetime'),
                pl.col("price").cast(price_pl_type, strict=False),
                pl.col("amount").cast(size_pl_type, strict=False)
                ])
                .with_columns([
                pl.when(pl.col('side') == 'buy').then(pl.col('amount')).otherwise(0).alias('buy_size'),
                pl.when(pl.col('side') == 'sell').then(pl.col('amount')).otherwise(0).alias('sell_size')
                ])
                .set_sorted("datetime")
                .groupby_dynamic('datetime', every=period)
                .agg([
                pl.col("price").first().alias('open'),
                pl.col("price").max().alias('high'),
                pl.col("price").min().alias('low'),
                pl.col("price").last().alias('close'),
                pl.col("amount").sum().alias('volume'),
                pl.col('buy_size').sum().alias('buy_vol'),
                pl.col('sell_size').sum().alias('sell_vol'),
                ])
                ).collect()
            except Exception as e:
                print(f"{e}")
                df = None

            if df is None or len(df) < 1:
                print(f"Failed to read the trading file.\n")
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
        print(f'save_daily_ohlcv_from_bitbank_trading failed.\n{format_exc()}')
        raise e