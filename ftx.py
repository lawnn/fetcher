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
                    hdlr_json=self.handler,
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
            'limit': limit
        }
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
        results = await self._requests('get', path, params)
        return results

    async def historical_prices(self, market_name, resolution, limit, start_time=None, end_time=None):
        """
        過去の価格と取引量を取得
        :param market_name: str
        :param resolution: int # 15, 600, 300, 900, 3600, 14400, 86400
        :param limit: int # max 5000
        :param start_time: int
        :param end_time: int
        :return:
        """
        path = f'/markets/{market_name}/candles'
        params = {
            'resolution': resolution,
            'limit': limit
        }
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
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
        params = {}
        if future is not None:
            params['future'] = future
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time

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
        :param limit: inst # max 5000
        :param start_time: int
        :param end_time: int
        :return:
        """
        path = f'/indexes/{index_name}/candles'
        params = {
            'resolution': resolution
        }
        if limit is not None:
            params['limit'] = limit
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
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

    async def deposit_history(self, limit=None, start_time=None, end_time=None):
        path = f'/wallet/deposits'
        params = {}
        if limit is not None:
            params['limit'] = limit
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
        results = await self._requests('get', path, params)
        return results

    async def withdrawal_history(self, limit=None, start_time=None, end_time=None):
        path = f'/wallet/withdrawal'
        params = {}
        if limit is not None:
            params['limit'] = limit
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
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
        :param limit: int # max 5000
        :return: dict
        """
        path = f'orders/history'
        params = {
            'market': market
        }
        if limit is not None:
            params['limit'] = limit
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
        results = await self._requests('get', path, params)
        return results

    async def open_trigger_orders(self, market=None, type_=None):
        """

        :param market: str
        :param type_: str # stop, trailing_stop, take_profit
        :return: dict
        """

        path = f'/conditional_orders'
        params = {}
        if market is not None:
            params['market'] = market
        if type_ is not None:
            params['type'] = type_
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
        params = {}
        if market is not None:
            params['market'] = market
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
        if side is not None:
            params['side'] = side
        if type_ is not None:
            params['type'] = type_
        if order_type is not None:
            params['orderType'] = order_type
        if limit is not None:
            params['limit'] = limit

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
            'postOnly': post_only
        }
        if client_id is not None:
            params['clientId'] = client_id
        results = await self._requests('post', path, params)
        return results

    async def place_trigger_order(self, market, side, size, type_, reduce_only=False, retry_unit_filled=False):
        pass

    async def order_stats(self, order_id):
        path = f'/orders/{order_id}'
        results = await self._requests('get', path)
        return results

    async def order_status_by_client_id(self, client_order_id):
        path = f'/orders/by_client_id/{client_order_id}'
        results = await self._requests('get', path)
        return results

    async def cancel_order(self, order_id):
        path = f'/orders/{order_id}'
        results = await self._requests('delete', path)
        return results

    async def cancel_order_by_client_id(self, client_order_id):
        path = f'/orders/by_client_id/{client_order_id}'
        results = await self._requests('delete', path)
        return results

    async def cancel_open_trigger_order(self, id_):
        path = f'/conditional_orders/{id_}'
        results = await self._requests('delete', path)
        return results

    async def cancel_all_orders(self):
        path = f'/orders'
        results = await self._requests('delete', path)
        return results

    async def request_quote(self, from_coin, to_coin, size):
        """
       通貨の変換
       :param from_coin: str
       :param to_coin:  str
       :param size: float
       :return:  dict
       """
        path = f'/otc/quotes'
        params = {
            'fromCoin': from_coin,
            'toCoin': to_coin,
            'size': size
        }
        results = await self._requests('post', path, params)
        return results

    async def quote_status(self, quote_id, market):
        path = f'/otc/quotes/{quote_id}'
        params = {
            'market': market
        }
        results = await self._requests('get', path, params)
        return results

    async def accept_quote(self, quote_id):
        path = f'/otc/quotes/{quote_id}/accept'
        results = await self._requests('post', path)
        return results

    async def lending_history(self):
        path = f'/spot_margin/history'
        results = await self._requests('get', path)
        return results

    async def borrow_rates(self):
        path = f'/spot_margin/borrow_rates'
        results = await self._requests('get', path)
        return results

    async def lending_rates(self):
        path = f'/spot_margin/lending_rates'
        results = await self._requests('get', path)
        return results

    async def daily_borrowed_amounts(self):
        path = f'/spot_margin/borrow_summary'
        results = await self._requests('get', path)
        return results

    async def market_info(self, market):
        path = f'/spot_margin/market_info'
        params = {
            'market': market
        }
        results = await self._requests('get', path, params)
        return results

    async def my_borrow_history(self):
        path = f'/spot_margin/borrow_history'
        results = await self._requests('get', path)
        return results

    async def my_lending_history(self):
        path = f'/spot_margin/lending_history'
        results = await self._requests('get', path)
        return results

    async def lending_offers(self):
        path = f'/spot_margin/offers'
        results = await self._requests('get', path)
        return results

    async def lending_info(self):
        path = f'/spot_margin/lending_info'
        results = await self._requests('get', path)
        return results

    async def submit_lending_offer(self):
        path = f'/spot_margin/offers'
        results = await self._requests('post', path)
        return results

    async def funding_payments(self, start_time, end_time, future):
        path = f'/funding_payments'
        params = {
            'start_time': start_time,
            'end_time': end_time,
            'future': future
        }
        results = await self._requests('get', path, params)
        return results

    async def list_leverage_tokens(self):
        path = f'/lt/tokens'
        results = await self._requests('get', path)
        return results

    async def token_info(self, token_name):
        path = f'/lt/{token_name}'
        results = await self._requests('get', path)
        return results

    async def leverage_token_balances(self):
        path = f'/lt/balances'
        results = await self._requests('get', path)
        return results

    async def list_leverage_token_creation_requests(self):
        path = f'/lt/creations'
        results = await self._requests('get', path)
        return results

    async def request_leverage_token_creation(self, token_name, size):
        path = f'/lt/{token_name}/create'
        params = {
            'size': size
        }
        results = await self._requests('post', path, params)
        return results

    async def list_leverage_token_redemption_requests(self):
        path = f'/lt/redemptions'
        results = await self._requests('get', path)
        return results

    async def request_leverage_token_redemption(self, token_name, size):
        path = f'/lt/{token_name}/redeem'
        params = {
            'size': size
        }
        results = await self._requests('post', path, params)
        return results

    async def option_open_interest(self):
        path = f'/options/open_interest/BTC'
        results = await self._requests('get', path)
        return results

    async def account_options_info(self):
        path = f'/options/account_info'
        results = await self._requests('get', path)
        return results

    async def option_open_interest_history(self, start_time, end_time, limit):
        """

        :param start_time: int
        :param end_time: int
        :param limit: int # max 200
        :return:
        """
        path = f'options/historical_open_interest/BTC'
        params = {}
        if limit is not None:
            params['limit'] = limit
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
        results = await self._requests('get', path, params)
        return results

    async def option_open_interest_volume(self, start_time, end_time, limit):
        """

        :param start_time: int
        :param end_time: int
        :param limit: int # max 200
        :return:
        """
        path = f'/options/historical_volumes/BTC'
        params = {}
        if limit is not None:
            params['limit'] = limit
        if start_time is not None:
            params['start_time'] = start_time
        if end_time is not None:
            params['end_time'] = end_time
        results = await self._requests('get', path, params)
        return results

    async def stakes(self):
        path = f'/staking/stakes'
        results = await self._requests('get', path)
        return results

    async def get_unstake_request(self):
        path = f'/staking/unstake_requests'
        results = await self._requests('get', path)
        return results

    async def stake_balances(self):
        path = f'/staking/balances'
        results = await self._requests('get', path)
        return results

    async def post_unstake_request(self, coin, size):
        path = f'/staking/unstake_requests'
        params = {
            'coin': coin,
            'size': size
        }
        results = await self._requests('post', path, params)
        return results

    async def cancel_unstake_request(self, request_id):
        path = f'/staking/unstake_requests/{request_id}'
        results = await self._requests('delete', path)
        return results

    async def get_staking_rewards(self):
        path = f'/staking/staking_rewards'
        results = await self._requests('get', path)
        return results

    async def stake_request(self, coin, size):
        path = f'/srm_stakes/stakes'
        params = {
            'coin': coin,
            'size': size
        }
        results = await self._requests('post', path, params)
        return results

    # Websocket API
    async def ws(self, subscribe, channel_name, market_name):
        """

        :param subscribe: str # subscribe, unsubscribe
        :param channel_name: str # orderbook, trades, ticker
        :param market_name: str # e.g.)BTC-PERP
        :return:
        """
        params = {'op': subscribe, 'channel': channel_name, 'market': market_name}
        await self._requests(method='ws', params=params)

    async def handler(self, msg, *ws):
        return msg


# async def debug():
#     ftx = FTX('BOT') # Todo: Your sub-account name
    # get method
    # print(await ftx.trades('ETH-PERP'))
    # print(await ftx.account())
    # print(await ftx.positions())
    # print(await ftx.account_options_info())
    # print(await ftx.subaccounts_balances())
    # post method
    # print(await ftx.change_leverage(1))
    # subscribe
    # await ftx.ws('unsubscribe', 'ticker', 'BTC-PERP')
    # await ftx.ws('subscribe', 'trades', 'BTC-PERP')

# if __name__ == '__main__':
#     asyncio.run(debug())
