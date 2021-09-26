import pybotters
import asyncio
from src.base import BotBase


class FTX(BotBase):
    def __init__(self, config: str, symbol: str):
        super().__init__(config)
        self.symbol = symbol

    async def _requests(self, method: str, url: str, params=None, data=None):
        async with pybotters.Client(apis=self.apis, base_url='https://ftx.com/api') as client:
            r = await client.request(method, url=url, params=params, data=data)
            data = await r.json()
            return data

    async def account(self):
        count = 0
        while True:
            req = await self._requests('GET', '/account')
            if req['success']:
                return req['result']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in account status.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in account status.")

    async def create_order(self, side: str, size: float, _type: str, price=None, stopPrice=None):
        url = None
        data = {
            "market": self.symbol,
            "side": side,
            "type": _type,
            "size": size
        }
        if _type == 'limit':
            url = '/orders'
            data['price'] = float(price)
        elif _type == 'market':
            url = '/orders'
            data['price'] = None
        elif (_type == 'stop') or (_type == 'takeProfit'):
            url = '/conditional_orders'
            data['triggerPrice'] = float(stopPrice)
            if price is not None:
                data['orderPrice'] = float(price)
        elif _type == 'trailingStop':
            url = '/conditional_orders'
            data['trailValue'] = float(price)
        return await self._requests('POST', url, data=data)

    async def market(self, side: str, size: float):
        """
        'id': 88888888888, 'clientId': None, 'market': 'ETH-PERP', 'type': 'market', 'side': 'buy', 'price': None,
        'size': 0.001, 'status': 'new', 'filledSize': 0.0, 'remainingSize': 0.001, 'reduceOnly': False,
        'liquidation': None, 'avgFillPrice': None, 'postOnly': False, 'ioc': True, 'createdAt':
        '2021-09-26T01:07:15.722785+00:00', 'future': 'ETH-PERP' :param side: :param size: :return:
        """
        count = 0
        while True:
            req = await self.create_order(side, size, 'market')
            if req['success']:
                return req['result']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in market order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in market order.")

    async def limit(self, side: str, size: float, price: float):
        """
        :return:
        'id': 89999999999, 'clientId': None, 'market': 'ETH-PERP', 'type': 'limit', 'side': 'buy',
        'price': 100.0, 'size': 0.001, 'status': 'new', 'filledSize': 0.0, 'remainingSize': 0.001,
        'reduceOnly': False, 'liquidation': None, 'avgFillPrice': None, 'postOnly': False, 'ioc': False,
        'createdAt': '2021-09-26T01:01:23.582448+00:00', 'future': 'ETH-PERP'
        """
        count = 0
        while True:
            req = await self.create_order(side, size, 'limit', price)
            if req['success']:
                return req['result']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in limit order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in limit order.")

    async def cancel_order(self, order_id: int):
        """
        :param order_id:
        :return:
        "result": "Order queued for cancelation"
        """
        count = 0
        while True:
            req = await self._requests('DELETE', f'/orders/{order_id}')
            if req['success']:
                return req['result']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in cancel order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in cancel order.")

    async def cancel_all_orders(self):
        """
        :return:
        "result": "Orders queued for cancelation"
        """
        count = 0
        while True:
            req = await self._requests('DELETE', f'/orders', data={"market": self.symbol})
            if req['success']:
                return req['result']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in cancel all orders.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in cancel all orders.")

    async def edit_order(self, order_id: int, size: float, price: float):
        """
        "createdAt": "2019-03-05T11:56:55.728933+00:00", "filledSize": 0, "future": "XRP-PERP", "id": 9596932,
        "market": "XRP-PERP", "price": 0.326525, "remainingSize": 31431, "side": "sell", "size": 31431,
        "status": "open", "type": "limit", "reduceOnly": false, "ioc": false, "postOnly": false, "clientId": null,
        """
        count = 0
        while True:
            req = await self._requests('POST', f'/orders/{order_id}/modify', data={"size": size, "price": price})
            if req['success']:
                return req['result']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in edit orders.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in edit orders.")

    async def historical(self, params: dict):
        """
        "close": 11055.25,
        "high": 11089.0,
        "low": 11043.5,
        "open": 11059.25,
        "startTime": "2019-06-24T17:15:00+00:00",
        "volume": 464193.95725
        :param params: dict
        :return:
        """
        count = 0
        req = await self._requests('GET', f'/markets/{self.symbol}/candles', params=params)
        if req['success']:
            return req['result']
        else:
            count += 1
            await asyncio.sleep(1)
            if count == 60:
                self.statusNotify("API request failed in edit orders.")
                self.statusNotify(str(req))
                raise Exception("API request failed in edit orders.")
