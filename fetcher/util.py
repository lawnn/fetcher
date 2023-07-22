import numpy as np
import pandas as pd
import polars as pl
from datetime import datetime, timedelta


def df_list(df: pl.DataFrame, start_date: datetime, interval: int, quantity: int, dt_col: str="") -> list:
    # 日付リストを生成する
    date_list = [start_date + timedelta(days=interval*i)
                for i in range(quantity)
                if start_date + timedelta(days=interval*i) <= df[dt_col].max()]

    # DataFrameリストを生成する
    return [df.filter((pl.col(dt_col).ge(start_date)) & (pl.col(dt_col).lt(end_date)))
            for start_date, end_date in [(date_list[i], date_list[i+1])
                for i in range(0, len(date_list)-1, interval)] + ([ (date_list[-2], date_list[-1]) ]
                    if len(date_list) % interval != 1 else [])]


def pl_merge(left, right, col):
    return (left.join(right ,on=col, how="left")
            .with_columns(pl.col("close").fill_null(strategy="forward").fill_null(strategy="backward"))
            .with_columns([
                pl.col("open").fill_null(pl.col("close")),
                pl.col("high").fill_null(pl.col("close")),
                pl.col("low").fill_null(pl.col("close")),
                pl.col("volume").fill_null(0),
                pl.col("buy_vol").fill_null(0),
                pl.col("sell_vol").fill_null(0),
            ]))


def make_ohlcv(path: str, date_column_name: str, price_column_name: str,
               size_column_name: str, side_column_name: str, buy, sell, time_frame, pl_type: pl.DataType) -> pl.DataFrame:
    return (pl.read_csv(path)
            .lazy()
            .with_columns([pl.col(date_column_name).str.strptime(pl.Datetime, strict=False).alias("datetime"),
                        pl.col(price_column_name).cast(pl_type),
                        pl.when(pl.col(side_column_name) == buy).then(pl.col(size_column_name)).otherwise(0).alias('buy_size'),
                        pl.when(pl.col(side_column_name) == sell).then(pl.col(size_column_name)).otherwise(0).alias('sell_size')])
            .set_sorted("datetime")
            .groupby_dynamic('datetime', every=time_frame)
            .agg([
                pl.col(price_column_name).first().alias('open'),
                pl.col(price_column_name).max().alias('high'),
                pl.col(price_column_name).min().alias('low'),
                pl.col(price_column_name).last().alias('close'),
                pl.col(size_column_name).sum().alias('volume'),
                pl.col('buy_size').sum().alias('buy_vol'),
                pl.col('sell_size').sum().alias('sell_vol')
                ])
            .collect()
            )


def make_ohlcv_from_timestamp(path: str, date_column_name: str, price_column_name: str,
                              size_column_name: str, side_column_name: str, buy, sell,
                              time_frame, pl_type: pl.DataType, timestamp_multiplier: int) -> pl.DataFrame:
    return (pl.read_csv(path)
            .lazy()
            .with_columns([(pl.col(date_column_name) * timestamp_multiplier)
                           .cast(pl.Datetime(time_unit='us')).alias("datetime"),
                           pl.col(price_column_name).cast(pl_type),
                           pl.when(pl.col(side_column_name) == buy)
                           .then(pl.col(size_column_name)).otherwise(0).alias('buy_size'),
                           pl.when(pl.col(side_column_name) == sell)
                           .then(pl.col(size_column_name)).otherwise(0).alias('sell_size')])
            .set_sorted("datetime")
            .groupby_dynamic('datetime', every=time_frame)
            .agg([
                pl.col(price_column_name).first().alias('open'),
                pl.col(price_column_name).max().alias('high'),
                pl.col(price_column_name).min().alias('low'),
                pl.col(price_column_name).last().alias('close'),
                pl.col(size_column_name).sum().alias('volume'),
                pl.col('buy_size').sum().alias('buy_vol'),
                pl.col('sell_size').sum().alias('sell_vol')
            ])
            .collect()
            )


def np_shift(arr, num=1, fill_value=np.nan):
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


def np_stack(x,y):
    """
    ndarrayを結合して欠損値があれば行を削除します
    """
    z = np.column_stack((x,y))
    z = z[~np.isnan(z).any(axis=1)]
    return z[:,0], z[:,1]


def resample_ohlc(org_df, timeframe):
    df = org_df.resample(f'{timeframe * 60}S').agg(
        {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})
    df['close'] = df['close'].fillna(method='ffill')
    df['open'] = df['open'].fillna(df['close'])
    df['high'] = df['high'].fillna(df['close'])
    df['low'] = df['low'].fillna(df['close'])
    return df


def trades_to_historical(df, period: str = '1S'):
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
