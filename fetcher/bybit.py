import polars as pl
from datetime import datetime, timedelta
from .util import pl_merge, make_ohlcv_from_timestamp


def bybit_make_ohlcv(symbol: str, date: str, time_frame, pl_type: pl.PolarsDataType=pl.Float64) -> pl.DataFrame:
    """
    example
    bybit_make_ohlcv("BTCUSDT", "2023-05-26", "1s", pl.Float64)
    """
    df = make_ohlcv_from_timestamp(f"https://public.bybit.com/trading/{symbol}/{symbol}{date}.csv.gz",
                                   "timestamp", "price", "size", "side", "Buy", "Sell", time_frame, pl_type, 1_000_000)
    start_dt = datetime.combine(df["datetime"][0].date(), datetime.min.time())
    end_dt = datetime.combine(df["datetime"][-1].date(), datetime.min.time()) + timedelta(days=1, seconds=-1)
    dt_range = pl.DataFrame({'datetime': pl.date_range(start_dt, end_dt, time_frame, eager=True)})
    return pl_merge(dt_range, df, "datetime")