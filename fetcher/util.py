import calendar
import numpy as np
import pandas as pd


def datetime_to_ms(date):
    return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond / 1000)


def datetime_to_timestamp(date):
    return int(calendar.timegm(date.timetuple()) + date.microsecond / 1000)


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
