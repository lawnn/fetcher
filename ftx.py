# ftx.py

import aiohttp
import asyncio
import async_timeout
import json
from aiohttp import WSMsgType
import traceback
import time
import hmac
import hashlib
from requests import Request


class FTX:
    # 定数
    TIMEOUT = 3600  # タイムアウト
    EXTEND_TOKEN_TIME = 3000  # アクセストークン延長までの時間
    MARKET = 'BTC-PERP'  # 銘柄
    URLS = {'REST': 'https://ftx.com/api',
            'WebSocket': 'wss://ftx.com/ws/',
            }
    PUBLIC_CHANNELS = ['orderbook', 'trades', 'ticker']
    PRIVATE_CHANNELS = ['fills', 'orders']

    # 変数
    api_key = ''
    api_secret = ''

    session = None  # セッション保持
    requests = []  # リクエストパラメータ
    token = ''  # Private Websocket API用トークン

    # ------------------------------------------------ #
    # init
    # ------------------------------------------------ #
    def __init__(self, api_key, api_secret):
        # APIキー・SECRETをセット
        self.api_key = api_key
        self.api_secret = api_secret

    # ------------------------------------------------ #
    # async request for rest api
    # ------------------------------------------------ #

    def set_request(self, method, access_modifiers, target_path, params):
        if access_modifiers == 'public':

            url = ''.join([self.URLS['REST'], target_path])
            if method == 'GET':
                headers = ''
                self.requests.append({'method': method,
                                      'access_modifiers': access_modifiers,
                                      'target_path': target_path, 'url': url,
                                      'params': params, 'headers': {}})

            if method == 'POST':
                headers = {'Content-Type': 'application/json'}
                self.requests.append({'method': method,
                                      'access_modifiers': access_modifiers,
                                      'target_path': target_path, 'url': url,
                                      'params': params, 'headers': headers})

        if access_modifiers == 'private':
            url = ''.join([self.URLS['REST'], target_path])
            path = target_path

            timestamp = int(time.time() * 1000)
            if method == 'GET':
                signature_payload = self.get_payload(timestamp, method, url, params)
                signature = self.get_sign(signature_payload)
                headers = self.set_headers_for_private(timestamp=str(timestamp),
                                                       sign=signature, params=params)

                self.requests.append({'url': url,
                                      'method': method,
                                      'headers': headers,
                                      'params': params,
                                      })

            if method == 'POST':
                post_data = json.dumps(params)

                signature_payload = self.get_payload(timestamp, method, url, params)
                signature = self.get_sign(signature_payload)
                headers = self.set_headers_for_private(timestamp=str(timestamp),
                                                       sign=signature, params=params)

                self.requests.append({'url': url,
                                      'method': method,
                                      'headers': headers,
                                      'params': post_data,
                                      })

            if method == 'PUT':
                post_data = json.dumps(params)

                signature_payload = self.get_payload(timestamp, method, url, params)
                signature = self.get_sign(signature_payload)
                headers = self.set_headers_for_private(timestamp=str(timestamp),
                                                       sign=signature, params=params)
                self.requests.append({'url': url,
                                      'method': method,
                                      'headers': headers,
                                      'params': post_data,
                                      })

            if method == 'DELETE':
                signature_payload = self.get_payload(timestamp, method, url, params)
                signature = self.get_sign(signature_payload)
                headers = self.set_headers_for_private(timestamp=str(timestamp),
                                                       sign=signature, params=params)
                self.requests.append({'url': url,
                                      'method': method,
                                      'headers': headers,
                                      'params': params,
                                      })

    def set_headers_for_private(self, timestamp, sign, params):
        headers = {'FTX-KEY': self.api_key,
                   'FTX-SIGN': sign,
                   'FTX-TS': timestamp,
                   }
        if len(params) > 0:
            headers['Content-Type'] = 'application/json'
        return headers

    def get_payload(self, timestamp, method, url, params):
        request = Request(method, url)
        prepared = request.prepare()

        if len(params) > 0:
            signature_payload = ''.join(
                [str(timestamp), prepared.method, prepared.path_url, '{}'.format(json.dumps(params))]).encode()
        else:
            signature_payload = ''.join([str(timestamp), prepared.method, prepared.path_url]).encode()

        return signature_payload

    def get_sign(self, signature_payload):
        signature = hmac.new(self.api_secret.encode(), signature_payload, hashlib.sha256).hexdigest()

        return signature

    async def fetch(self, request):
        status = 0
        content = []

        async with async_timeout.timeout(self.TIMEOUT):
            try:
                if self.session is None:
                    self.session = await aiohttp.ClientSession().__aenter__()
                if request['method'] is 'GET':
                    async with self.session.get(url=request['url'],
                                                params=request['params'],
                                                headers=request['headers']) as response:
                        status = response.status
                        content = await response.read()
                        if status != 200:
                            # エラーのログ出力など必要な場合
                            pass

                elif request['method'] is 'POST':
                    async with self.session.post(url=request['url'],
                                                 data=request['params'],
                                                 headers=request['headers']) as response:
                        status = response.status
                        content = await response.read()
                        if status != 200:
                            # エラーのログ出力など必要な場合
                            pass

                elif request['method'] is 'PUT':
                    async with self.session.put(url=request['url'],
                                                data=request['params'],
                                                headers=request['headers']) as response:
                        status = response.status
                        content = await response.read()
                        if status != 200:
                            # エラーのログ出力など必要な場合
                            pass

                elif request['method'] is 'DELETE':
                    async with self.session.delete(url=request['url'],
                                                   data=request['params'],
                                                   headers=request['headers']) as response:
                        status = response.status
                        content = await response.read()
                        if status != 200:
                            # エラーのログ出力など必要な場合
                            pass

                if len(content) == 0:
                    result = []

                else:
                    try:
                        result = json.loads(content.decode('utf-8'))
                    except Exception as e:
                        traceback.print_exc()

                return result

            except Exception as e:
                # セッション終了
                if self.session is not None:
                    await self.session.__aexit__(None, None, None)
                    await asyncio.sleep(0)
                    self.session = None

                traceback.print_exc()

    async def send(self):
        promises = [self.fetch(req) for req in self.requests]
        self.requests.clear()
        return await asyncio.gather(*promises)

    # ------------------------------------------------ #
    # REST API(Markets)
    # ------------------------------------------------ #
    # Get markets
    def market(self):
        params = {}
        self.set_request(method='GET', access_modifiers='public',
                         target_path='/markets', params=params)

    # Get single market
    def single_market(self):
        target_path = ''.join(['/markets/', self.MARKET])
        params = {}
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    # Get orderbook
    def orderbooks(self, depth=20):
        target_path = ''.join(['/markets/', self.MARKET, '/orderbook'])
        params = {'depth': depth,
                  }
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    # Get trades
    def trades(self, limit=20, start_time='', end_time=''):
        target_path = ''.join(['/markets/', self.MARKET, '/trades'])
        params = {'limit': limit}

        if len(start_time) > 0:
            params['start_time'] = start_time

        if len(end_time) > 0:
            params['end_time'] = end_time

        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    # Get historical prices
    # また今度

    # ------------------------------------------------ #
    # REST API(Futures)
    # ------------------------------------------------ #
    # List all futures
    def futures(self):
        params = {}
        self.set_request(method='GET', access_modifiers='public',
                         target_path='/futures', params=params)

    # Get future
    def future(self):
        target_path = ''.join(['/futures/', self.MARKET])
        params = {}
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    # Get future stats
    def future_stats(self):
        target_path = ''.join(['/futures/', self.MARKET, '/stats'])
        params = {}
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    # Get funding rates
    def funding_rates(self, start_time='', end_time='', future=''):
        params = {}
        if len(start_time) > 0:
            params['start_time'] = start_time

        if len(end_time) > 0:
            params['end_time'] = end_time

        if len(future) > 0:
            params['future'] = future

        self.set_request(method='GET', access_modifiers='public',
                         target_path='/funding_rates', params=params)

    # Get index weights
    # Note that this only applies to index futures, e.g. ALT/MID/SHIT/EXCH/DRAGON.
    def index_weights(self, index_name):
        target_path = ''.join(['/indexes/', index_name, '/weights'])
        params = {}
        self.set_request(method='GET', access_modifiers='public',
                         target_path=target_path, params=params)

    # Get expired futures
    # Returns the list of all expired futures.
    def expired_futures(self):
        params = {}
        self.set_request(method='GET', access_modifiers='public',
                         target_path='/expired_futures', params=params)

    # Get historical index
    def historical_index(self, resolution='', limit='', start_time='', end_time=''):
        target_path = ''.join(['/indexes/', self.MARKET, '/candles'])
        params = {}

        if len(resolution) > 0:
            params['resolution'] = resolution

        if len(limit) > 0:
            params['limit'] = limit

        if len(start_time) > 0:
            params['start_time'] = start_time

        if len(end_time) > 0:
            params['end_time'] = end_time

        self.set_request(method='GET', access_modifiers='public',
                         target_path='/expired_futures', params=params)

    # ------------------------------------------------ #
    # REST API(Account)
    # ------------------------------------------------ #
    # Get account information
    def account(self):
        target_path = '/account'
        params = {}
        self.set_request(method='GET', access_modifiers='private',
                         target_path=target_path, params=params)

    # Get positions
    def positions(self):
        target_path = '/positions'
        params = {}
        self.set_request(method='GET', access_modifiers='private',
                         target_path=target_path, params=params)

    # Change account leverage
    # また今度

    # ------------------------------------------------ #
    # REST API(Wallet)
    # ------------------------------------------------ #
    # Get coins
    def wallet_coins(self):
        target_path = '/wallet/coins'
        params = {}
        self.set_request(method='GET', access_modifiers='private',
                         target_path=target_path, params=params)

    # Get balances
    def wallet_balances(self):
        target_path = '/wallet/balances'
        params = {}
        self.set_request(method='GET', access_modifiers='private',
                         target_path=target_path, params=params)

    # Get balances of all accounts
    def wallet_all_balances(self):
        target_path = '/wallet/all_balances'
        params = {}
        self.set_request(method='GET', access_modifiers='private',
                         target_path=target_path, params=params)

    # Get deposit address
    # また今度

    # Get deposit history
    # また今度

    # Get withdrawal history
    # また今度

    # Request withdrawal
    # また今度

    # Get airdrops
    # また今度

    # ------------------------------------------------ #
    # REST API(Orders)
    # ------------------------------------------------ #
    # Get open orders
    def open_orders(self):
        target_path = '/orders'
        params = {'market': self.MARKET}
        self.set_request(method='GET', access_modifiers='private',
                         target_path=target_path, params=params)

    # Get order history
    def orders_history(self):
        target_path = '/orders/history'
        params = {'market': self.MARKET}
        self.set_request(method='GET', access_modifiers='private',
                         target_path=target_path, params=params)

    # Get open trigger orders
    def conditional_orders(self):
        target_path = '/conditional_orders'
        params = {'market': self.MARKET}
        self.set_request(method='GET', access_modifiers='private',
                         target_path=target_path, params=params)

    # Get trigger order triggers
    # また今度

    # Get trigger order history
    # また今度

    # Place order
    def place_order(self, side, type, size, price='', reduceOnly=False, ioc=False, postOnly=False, clientId=''):
        target_path = '/orders'
        # price = price if len(str(price)) > 0  else 'null'
        params = {'market': self.MARKET,
                  'side': side,
                  'price': price if len(str(price)) > 0 else None,
                  'type': type,
                  'size': size,
                  'reduceOnly': reduceOnly,
                  'ioc': ioc,
                  'postOnly': postOnly,
                  'clientId': None
                  }

        self.set_request(method='POST', access_modifiers='private',
                         target_path=target_path, params=params)

    # Place trigger order
    # また今度

    # Modify order
    # また今度

    # Modify order by client ID
    # また今度

    # Modify trigger order
    # また今度

    # Get order status
    def order_status(self, order_id):
        target_path = ''.join(['/orders/', order_id])
        params = {}

        self.set_request(method='GET', access_modifiers='private',
                         target_path=target_path, params=params)

    # Get order status by client id
    # また今度

    # Cancel order
    def cancel_order(self, order_id):
        target_path = ''.join(['/orders/', str(order_id)])
        params = {}

        self.set_request(method='DELETE', access_modifiers='private',
                         target_path=target_path, params=params)

    # Cancel order by client id
    # 未確認
    def cancel_order_by_client_id(self, client_order_id):
        target_path = ''.join(['/orders/by_client_id/', client_order_id])
        params = {}

        self.set_request(method='DELETE', access_modifiers='private',
                         target_path=target_path, params=params)

    # Cancel open trigger order
    # また今度

    # Cancel all orders
    # 未確認
    def cancel_all_orders(self):
        target_path = '/orders'
        params = {}

        self.set_request(method='DELETE', access_modifiers='private',
                         target_path=target_path, params=params)

    # ------------------------------------------------ #
    # REST API(Convert)
    # ------------------------------------------------ #

    # ------------------------------------------------ #
    # REST API(Fills)
    # ------------------------------------------------ #

    # ------------------------------------------------ #
    # REST API(Funding Payments)
    # ------------------------------------------------ #

    # ------------------------------------------------ #
    # REST API(Leveraged Tokens)
    # ------------------------------------------------ #

    # ------------------------------------------------ #
    # REST API(Options)
    # ------------------------------------------------ #

    # ------------------------------------------------ #
    # REST API(SRM Staking)
    # ------------------------------------------------ #

    # ------------------------------------------------ #
    # WebSocket
    # ------------------------------------------------ #
    async def ws_run(self, callback):
        # 変数
        end_point_public = self.URLS['WebSocket']

        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    # Public WebSocket
                    async with session.ws_connect(end_point_public,
                                                  receive_timeout=self.TIMEOUT) as client:

                        if len(self.PRIVATE_CHANNELS) > 0 and self.api_key != '' and self.api_secret != '':
                            result = await self.auth(client)
                            await self.subscribe(client, 'private', self.PRIVATE_CHANNELS)

                        if len(self.PUBLIC_CHANNELS) > 0:
                            await self.subscribe(client, 'public', self.PUBLIC_CHANNELS)

                        async for response in client:
                            if response.type != WSMsgType.TEXT:
                                print('response:' + str(response))
                                break
                            elif 'error' in response[1]:
                                print(response[1])
                                break
                            elif 'subscribed' in response[1]:
                                print(response[1])
                            else:
                                data = json.loads(response[1])
                                await self.handler(callback, data)

            except Exception as e:
                print(e)
                print(traceback.format_exc().strip())
                await asyncio.sleep(10)

    # 購読
    async def subscribe(self, client, access_modifiers, channels):
        for channel in channels:
            if access_modifiers == 'private':
                params = {"op": "subscribe", "channel": channel}
            else:
                params = {"op": "subscribe", "channel": channel, "market": self.MARKET}

            await asyncio.wait([client.send_str(json.dumps(params))])
            print('---- %s connect ----' % (channel))

    # 認証
    async def auth(self, client):
        try:
            timestamp = int(time.time() * 1000)
            signature = hmac.new(self.api_secret.encode(), ''.join([str(timestamp), 'websocket_login']).encode(),
                                 hashlib.sha256).hexdigest()

            params = {
                'args': {'key': self.api_key,
                         'sign': signature,
                         'time': timestamp,
                         },
                'op': "login"
            }
            await asyncio.wait([client.send_str(json.dumps(params))])

            result = None

            return result

        except Exception as e:
            print(e)
            print(traceback.format_exc().strip())

    # UTILS
    # コールバック、ハンドラー
    async def handler(self, func, *args):
        return await func(*args)
