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


def bitbank_trades_to_historical(st_date: str, symbol: str = "btc_jpy", period: str = '1s',
                       price_pl_type: type = pl.Int64, size_pl_type: type = pl.Float64,
                       output_dir: str = None) -> None:
    dt = str(st_date).replace("/", "-")
    st_date = str(st_date).replace("/", "").replace("-", "").replace(" 00:00:00", "")
    r = requests.get(f"https://public.bitbank.cc/{symbol}/transactions/{st_date}").json()

    if output_dir is None:
        output_dir = f'bitbank/trades/{symbol}'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df = (pl.DataFrame(r["data"]["transactions"])
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

    df.write_csv(f"{output_dir}/{dt}.csv")