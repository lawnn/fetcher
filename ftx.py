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
            else:
                raise Exception('No match method')
            data = await resp.json()
            return data

    # REST API
    async def trades(self, market_name):
        path = f'/markets/{market_name}/trades'
        results = await self._requests('get', path)
        return results

    async def account(self):
        path = f'/account'
        results = await self._requests('get', path)
        return results

    async def positions(self):
        path = f'/positions'
        results = await self._requests('get', path)
        return results

    async def acount_options_info(self):
        path = f'/options/account_info'
        results = await self._requests('get', path)
        return results

    async def subaccounts_balances(self, nick_name=None):
        if nick_name is None:
            nick_name = self._ACCOUNT_NAME
        path = f'/subaccounts/{nick_name}/balances'
        results = await self._requests('get', path)
        return results

    async def change_leverage(self, leverage):
        path = f'/account/leverage'
        params = {
            'leverage': leverage
        }
        results = await self._requests('post', path, params)
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

    # Websocket API
    async def ws(self, subscribe, channel_name, market_name):
        params = {'op': subscribe, 'channel': channel_name, 'market': market_name},
        await self._requests('ws', params)
        await ws  # this await is wait forever (for usage)


async def main():
    # get method
    print(await ftx.trades('ETH-PERP'))
    print(await ftx.account())
    print(await ftx.positions())
    print(await ftx.acount_options_info())
    print(await ftx.subaccounts_balances())
    # post method
    print(await ftx.change_leverage(1))


if __name__ == '__main__':
    ftx = FTX('BOT_03')
    asyncio.run(main())
