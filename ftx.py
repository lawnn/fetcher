import asyncio

import pybotters as pb


class FTX(object):
    def __init__(self, account_name):
        self._ACCOUNT_NAME = account_name
        self._EXCHANGE_URL = {
            'REST': 'https://ftx.com/api',
            'Websocket': 'wss://ftx.com/ws/'
        }
        if self._ACCOUNT_NAME == 'MAIN':
            self._HEADERS_INFORMATION = ''
        else:
            self._HEADERS_INFORMATION = {'FTX-SUBACCOUNT': self._ACCOUNT_NAME}

    async def _requests(self, method=None, path=None, params=None):
        async with pb.Client(
                apis='apis.json', headers=self._HEADERS_INFORMATION, base_url=self._EXCHANGE_URL['REST']
        ) as client:
            if method == 'get':
                resp = await client.get(url=path, params=params)
            elif method == 'post':
                resp = await client.post(url=path, data=params)
            elif method == 'put':
                resp = await client.put(url=path, data=params)
            elif method == 'delete':
                resp = await client.delete(url=path, data=params)
            elif method == 'ws':
                ws = await client.ws_connect(
                    url=self._EXCHANGE_URL['Websocket'],
                    send_json=params,
                    hdlr_json=lambda msg, ws: print(msg),
                )
                await ws
            else:
                raise Exception('No match method')
            if not method == 'ws':
                data = await resp.json()
                return data

    # REST API
    async def subaccounts_balances(self, nick_name=None):
        if nick_name is None:
            nick_name = self._ACCOUNT_NAME
        path = f'/subaccounts/{nick_name}/balances'
        results = await self._requests('get', path)
        return results

    async def transfer_between_subaccounts(self, coin_name, size, to, from_=None):
        """
        送金
        :param coin_name: str
        :param size: int
        :param from_: str
        :param to: str
        :return: dict
        """
        if from_ is None:
            from_ = self._ACCOUNT_NAME
        path = f'/subaccounts/transfer'
        params = {
            'coin': coin_name,
            'size': size,
            'source': from_,
            'destination': to
        }
        results = await self._requests('post', path, params)
        return results

    async def market(self):
        path = f'/markets'
        results = await self._requests('get', path)
        return results

    async def single_market(self, market_name):
        """

        :param market_name: str
        :return:
        """
        path = f'/markets/{market_name}'
        results = await self._requests('get', path)
        return results

    async def orderbook(self, market_name, depth=20):
        """

        :param market_name: str
        :param depth: int # max 100, default 20
        :return: dict
        """
        path = f'/markets/{market_name}/orderbook'
        params = {
            'depth': depth
        }
        results = await self._requests('get', path, params)
        return results

    async def trades(self, market_name, limit=20, start_time=None, end_time=None):
        """

        :param market_name: str
        :param limit: int # max 100, default 20
        :param start_time: int
        :param end_time: int
        :return: dict
        """
        path = f'/markets/{market_name}/trades'
        params = {
            'limit': limit,
            'start_time': start_time,
            'end_time': end_time
        }
        results = await self._requests('get', path, params)
        return results

    async def historical_prices(self, market_name, resolution, limit, start_time=None, end_time=None):
        """
        過去の価格と取引量を取得
        :param market_name: str
        :param resolution: int # 15, 600, 300, 900, 3600, 14400, 86400
        :param limit: str # max 5000
        :param start_time: int
        :param end_time: int
        :return:
        """
        path = f'/markets/{market_name}/candles'
        params = {
            'resolution': resolution,
            'limit': limit,
            'start_time': start_time,
            'end_time': end_time
        }
        results = await self._requests('get', path, params)
        return results

    async def list_all_futures(self):
        path = f'/futures'
        results = await self._requests('get', path)
        return results

    async def get_future(self, future_name):
        path = f'/futures/{future_name}'
        results = await self._requests('get', path)
        return results

    async def future_stats(self, future_name):
        path = f'/futures/{future_name}/stats'
        results = await self._requests('get', path)
        return results

    async def funding_rates(self, start_time=None, end_time=None, future=None):
        """
        資金調達率の取得
        :param start_time: int
        :param end_time: int
        :param future: str
        :return: dict
        """
        path = f'/funding_rates'
        params = {
            'start_time': start_time,
            'end_time': end_time,
            'future': future
        }
        results = await self._requests('get', path, params)
        return results

    async def index_weights(self, index_name):
        path = f'/indexes/{index_name}/weights'
        results = await self._requests('get', path)
        return results

    async def expired_futures(self):
        path = f'/expired_futures'
        results = await self._requests('get', path)
        return results

    async def historical_index(self, index_name, resolution, limit=None, start_time=None, end_time=None):
        """
        過去の価格と取引量を取得
        :param index_name: str
        :param resolution: int # 15, 600, 300, 900, 3600, 14400, 86400
        :param limit: str # max 5000
        :param start_time: int
        :param end_time: int
        :return:
        """
        path = f'/indexes/{index_name}/candles'
        params = {
            'resolution': resolution,
            'limit': limit,
            'start_time': start_time,
            'end_time': end_time
        }
        results = await self._requests('get', path, params)
        return results

    async def account_information(self):
        path = f'/account'
        results = await self._requests('get', path)
        return results

    async def positions(self, show_avg_price=False):
        path = f'/positions'
        params = {
            'showAvgPrice': show_avg_price
        }
        results = await self._requests('get', path, params)
        return results

    async def change_leverage(self, leverage):
        """
        レバレッジの変更
        :param leverage: int # 1, 3, 5, 10, 20, 50, 100
        :return: dict
        """
        path = f'/account/leverage'
        params = {
            'leverage': leverage
        }
        results = await self._requests('post', path, params)
        return results

    async def coins(self):
        path = f'/wallet/coins'
        results = await self._requests('get', path)
        return results

    async def balances(self):
        path = f'/wallet/balances'
        results = await self._requests('get', path)
        return results

    async def balances_of_all_accounts(self):
        path = f'/wallet/all_balances'
        results = await self._requests('get', path)
        return results

    async def deposit_history(self, limit, start_time, end_time):
        path = f'/wallet/deposits'
        params = {
            'limit': limit,
            'start_time': start_time,
            'end_time': end_time
        }
        results = await self._requests('get', path, params)
        return results

    async def withdrawal_history(self, limit, start_time, end_time):
        path = f'/wallet/withdrawal'
        params = {
            'limit': limit,
            'start_time': start_time,
            'end_time': end_time
        }
        results = await self._requests('get', path, params)
        return results

    async def open_orders(self, market):
        """

        :param market: str
        :return: dict
        """
        path = f'/orders'
        params = {
            'market': market
        }
        results = await self._requests('get', path, params)
        return results

    async def order_history(self, market, start_time=None, end_time=None, limit=None):
        """

        :param market: str
        :param start_time: int
        :param end_time: int
        :param limit: str # max 5000
        :return: dict
        """
        path = f'orders/history'
        params = {
            'market': market,
            'start_time': start_time,
            'end_time': end_time,
            'limit': limit
        }
        results = await self._requests('get', path, params)
        return results

    async def open_trigger_orders(self, market=None, type_=None):
        """

        :param market: str
        :param type_: str # stop, trailing_stop, take_profit
        :return: dict
        """

        path = f'/conditional_orders'
        params = {
            'market': market,
            'type': type_
        }
        results = await self._requests('get', path, params)
        return results

    async def trigger_order_triggers(self, conditional_order_id):
        path = f'/conditional_orders/{conditional_order_id}/triggers'
        results = await self._requests('get', path)
        return results

    async def trigger_order_history(
            self, market=None, start_time=None, end_time=None, side=None, type_=None, order_type=None, limit=100
    ):
        """

        :param market: str
        :param start_time: int
        :param end_time: int
        :param side: str # buy, sell
        :param type_: str # stop, trailing_stop, take_profit
        :param order_type: str # market, limit
        :param limit: int # max 100
        :return: dict
        """
        path = f'/conditional_orders/history'
        params = {
            'market': market,
            'start_time': start_time,
            'end_time': end_time,
            'side': side,
            'type': type_,
            'orderType': order_type,
            'limit': limit
        }
        results = await self._requests('get', path, params)
        return results

    async def place_order(
            self, market, side, price, type_, size, reduce_only=False, ioc=False, post_only=False, client_id=None
    ):
        path = f'/orders'
        params = {
            'market': market,  # BTC-PERP
            'side': side,  # buy or sell
            'price': price,  # Send 'null' for market order
            'type': type_,  # limit or market
            'size': size,
            'reduceOnly': reduce_only,
            'ioc': ioc,
            'postOnly': post_only,
            'clientId': client_id
        }
        results = await self._requests('post', path, params)
        return results

    async def place_trigger_order(self, market, side, size, type_, reduce_only=False, retry_unit_filled=False):
        pass

    async def acount_options_info(self):
        path = f'/options/account_info'
        results = await self._requests('get', path)
        return results

    # Websocket API
    async def ws(self, subscribe, channel_name, market_name):
        params = {'op': subscribe, 'channel': channel_name, 'market': market_name},
        await self._requests('ws', params)


async def debug():
    ftx = FTX('BOT_03')
    # get method
    # print(await ftx.trades('ETH-PERP'))
    # print(await ftx.account())
    # print(await ftx.positions())
    # print(await ftx.acount_options_info())
    # print(await ftx.subaccounts_balances())
    # post method
    # print(await ftx.change_leverage(1))
    # subscribe
    await ftx.ws('unsubscribe', 'ticker', 'BTC-PERP')
    # await ftx.ws('subscribe', 'trades', 'BTC-PERP')


if __name__ == '__main__':
    asyncio.run(debug())
