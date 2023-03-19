import os
import requests
import polars as pl


def bitbank_get_trades(st_date: str, symbol: str = "btc_jpy", output_dir: str = None) -> None:
    dt = str(st_date).replace("/", "-")
    st_date = str(st_date).replace("/", "").replace("-", "").replace(" 00:00:00", "")
    r = requests.get(f"https://public.bitbank.cc/{symbol}/transactions/{st_date}").json()

    if output_dir is None:
        output_dir = f'bitbank/trades/{symbol}'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    pl.DataFrame(r["data"]["transactions"]).write_csv(f"{output_dir}/{dt}.csv")


def bitbank_trades_to_historical(path: str, period: str = '1s') -> pl.DataFrame:
    return (
            pl.scan_csv(f"{path}")
                .with_columns(
                (pl.col("executed_at") * 1_000)
                    .cast(pl.Datetime(time_unit='us'))
                    .alias('datetime')
                )
                .groupby_dynamic('datetime', every=period)
                .agg([
                pl.col("price").first().alias('open'),
                pl.col("price").max().alias('high'),
                pl.col("price").min().alias('low'),
                pl.col("price").last().alias('close'),
                pl.col("amount").sum().alias('volume'),
                ])
            ).collect()