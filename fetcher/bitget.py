import ccxt

def bitget_fetch_trades(symbol, start_date, end_date, limit=1000):
    """
    API 10req/s
    """
    exchange = ccxt.bitget()
    start_time = exchange.parse8601(start_date + 'T00:00:00Z')
    end_time = exchange.parse8601(end_date + 'T00:00:00Z') - 1
    if start_time >= end_time:
        raise ValueError('start_date must be earlier than end_date')
    trades_list = []
    while True:
        trades = exchange.fetch_trades(symbol, limit=limit, params={'startTime': start_time, 'endTime': end_time})
        if len(trades) == 0:
            break
        trades_list += trades
        end_time = trades[0]['timestamp'] - 1
        print("\r", f"{trades[0]['datetime']}", end="")
        if start_time >= end_time:
            break
    return trades_list