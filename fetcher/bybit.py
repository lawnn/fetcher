import pandas as pd
import polars as pl
from datetime import datetime, timedelta
from .util import pl_merge, make_ohlcv_from_timestamp


def _make_ohlcv_from_timestamp(path: str, date_column_name: str, price_column_name: str,
                              size_column_name: str, side_column_name: str, buy, sell,
                              time_frame, pl_type: pl.DataType, timestamp_multiplier: int) -> pl.DataFrame:
    return (pl.from_pandas(pd.read_csv(path))
            .lazy()
            .with_columns([(pl.col(date_column_name) * timestamp_multiplier)
                                    .cast(pl.Datetime(time_unit='us')).alias("datetime"),
                                    pl.col(price_column_name).cast(pl_type),
                                    pl.when(pl.col(side_column_name) == buy)
                                    .then(pl.col(size_column_name)).otherwise(0).alias('buy_size'),
                                    pl.when(pl.col(side_column_name) == sell)
                                    .then(pl.col(size_column_name)).otherwise(0).alias('sell_size')])
            .set_sorted("datetime")
            .group_by_dynamic('datetime', every=time_frame)
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

def bybit_make_ohlcv(date: str, symbol: str = "BTCUSDT", time_frame: str = "1s",
                     pl_type: pl.DataType=pl.Float64) -> pl.DataFrame:
    """
    example
    import polars as pl
    bybit_make_ohlcv("2023-05-26", "BTCUSDT", "1s", pl.Float64)
    """
    df = _make_ohlcv_from_timestamp(f"https://public.bybit.com/trading/{symbol}/{symbol}{date}.csv.gz",
                                   "timestamp", "price", "size", "side", "Buy", "Sell", time_frame, pl_type, 1_000_000)
    start_dt = datetime.combine(df["datetime"][0].date(), datetime.min.time())
    end_dt = datetime.combine(df["datetime"][-1].date(), datetime.min.time()) + timedelta(days=1, seconds=-1)
    dt_range = pl.DataFrame({'datetime': pl.datetime_range(start_dt, end_dt, time_frame, eager=True)})
    return pl_merge(dt_range, df, "datetime")