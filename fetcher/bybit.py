import polars as pl
from datetime import datetime, timedelta
from .util import pl_merge


def bybit_make_ohlcv(symbol: str, date: str, time_frame, pl_type) -> pl.DataFrame:
    """
    example
    bybit_make_ohlcv("BTCUSDT", "2023-05-26", "1s", pl.Float64)
    """
    df = (pl.read_csv(f"https://public.bybit.com/trading/{symbol}/{symbol}{date}.csv.gz")
            .lazy()
            .with_columns([(pl.col("timestamp") * 1_000_000)
                            .cast(pl.Datetime(time_unit='us')).alias("datetime"),
                            pl.col("price").cast(pl_type),
                            pl.when(pl.col("side") == "Buy")
                            .then(pl.col("size")).otherwise(0).alias('buy_size'),
                            pl.when(pl.col("side") == "Sell")
                            .then(pl.col("size")).otherwise(0).alias('sell_size')])
            .groupby_dynamic('datetime', every=time_frame)
            .agg([
                pl.col("price").first().alias('open'),
                pl.col("price").max().alias('high'),
                pl.col("price").min().alias('low'),
                pl.col("price").last().alias('close'),
                pl.col("size").sum().alias('volume'),
                pl.col('buy_size').sum().alias('buy_vol'),
                pl.col('sell_size').sum().alias('sell_vol'),
            ])
            .collect()
            )
    start_dt = datetime.combine(df["datetime"][0].date(), datetime.min.time())
    end_dt = datetime.combine(df["datetime"][-1].date(), datetime.min.time()) + timedelta(days=1, seconds=-1)
    dt_range = pl.DataFrame({'datetime': pl.date_range(start_dt, end_dt, time_frame, eager=True)})
    return pl_merge(dt_range, df, "datetime")