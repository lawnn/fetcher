import pybotters
import asyncio
from wrappy.base import BotBase


class GMO(BotBase):
    def __init__(self, config: str, symbol: str):
        super().__init__(config)
        self.symbol = symbol

    async def _requests(self, method: str, url: str, params=None, data=None):
        async with pybotters.Client(apis=self.apis, base_url='https://api.coin.z.com') as client:
            r = await client.request(method, url=url, params=params, data=data)
            data = await r.json()
            return data

    async def account(self):
        count = 0
        while True:
            req = await self._requests('GET', '/private/v1/account/margin')
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in account status.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in account status.")

    async def replace_order(self, side: str, size: float, _type: str, price: float = None, create_order: bool = None,
                            positionId: int = None):
        data = {
            "symbol": self.symbol,
            "side": side,
            "executionType": _type,
        }

        if create_order:
            url = '/private/v1/order'
            data["size"] = str(size)
        elif not create_order:
            url = '/private/v1/closeOrder'
            data["settlePosition"] = [{"positionId": positionId,
                                       "size": str(size)}]
        else:
            url = '/private/v1/closeBulkOrder'
            data["size"] = str(size)

        if (_type == 'LIMIT') or (_type == 'STOP'):
            data['price'] = str(price)

        return await self._requests('POST', url=url, data=data)

    async def order_market(self, side: str, size: float):
        """
        新規の成行注文
        :param side:
        :param size:
        :return:
                   {"status": 0,
                    "data": "637000", (orderID)
                    "responsetime": "2019-03-19T02:15:06.108Z"}
        """
        count = 0
        while True:
            req = await self.replace_order(side, size, 'MARKET', create_order=True)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in market order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in market order.")

    async def order_limit(self, side: str, size: float, price: float):
        """
        新規の指値注文
        :param side:
        :param size:
        :param price:
        :return:
                   {"status": 0,
                    "data": "637000", (orderID)
                    "responsetime": "2019-03-19T02:15:06.108Z"}
        """
        count = 0
        while True:
            req = await self.replace_order(side, size, 'LIMIT', price=price, create_order=True)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in limit order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in limit order.")

    async def settle_market(self, side: str, size: float, positionId: int):
        """
        成行決済注文
        :param side:
        :param size:
        :param positionId:
        :return:
                    {"status": 0,
                    "data": "637000", (orderID)
                    "responsetime": "2019-03-19T02:15:06.108Z"}
        """
        count = 0
        while True:
            req = await self.replace_order(side, size, 'MARKET', create_order=False, positionId=positionId)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in market order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in market order.")

    async def settle_limit(self, side: str, size: float, price: float, positionId: int):
        """
        指値決済注文
        :param side:
        :param size:
        :param price:
        :param positionId:
        :return:
                    {"status": 0,
                    "data": "637000", (orderID)
                    "responsetime": "2019-03-19T02:15:06.108Z"}
        """
        count = 0
        while True:
            req = await self.replace_order(side, size, 'LIMIT', price=price, create_order=False, positionId=positionId)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in limit order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in limit order.")

    async def collective_settlement_order_market(self, side: str, size: float):
        """
        一括成行決済注文
        :param side:
        :param size:
        :return:
                   {"status": 0,
                    "data": "637000", (orderID)
                    "responsetime": "2019-03-19T02:15:06.108Z"}
        """
        count = 0
        while True:
            req = await self.replace_order(side, size, 'MARKET')
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in market order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in market order.")

    async def collective_settlement_order_limit(self, side: str, size: float, price: float):
        """
        一括指値決済注文
        :param side:
        :param size:
        :param price:
        :return:
                   {"status": 0,
                    "data": "637000", (orderID)
                    "responsetime": "2019-03-19T02:15:06.108Z"}
        """
        count = 0
        while True:
            req = await self.replace_order(side, size, 'LIMIT', price=price)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in limit order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in limit order.")

    async def cancel_order(self, order_id: int):
        """
        注文キャンセル
        :param order_id:
        :return:
        "result": "Order queued for cancelation"
        """
        count = 0
        while True:
            req = await self._requests('POST', '/private/v1/cancelOrder', data={'orderId': order_id})
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in cancel order.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in cancel order.")

    async def cancel_any_orders(self, order_id: int):
        """
        注文の複数キャンセル
        :order_id: [1,2,3]
        :return:
        "result": "Orders queued for cancelation"
        {
          "status": 0,
          "data": {
              "failed": [
                {
                  "message_code": "ERR-5122",
                  "message_string": "The request is invalid due to the status of the specified order.",
                  "orderId": 1
                },
                {
                  "message_code": "ERR-5122",
                  "message_string": "The request is invalid due to the status of the specified order.",
                  "orderId": 2
                }
              ],
              "success": [3,4]
          },
          "responsetime": "2019-03-19T01:07:24.557Z"
        }
        """
        count = 0
        while True:
            req = await self._requests('POST', '/private/v1/cancelOrders', data={'orderId': order_id})
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in cancel all orders.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in cancel all orders.")

    async def cancel_all_orders(self, side: str = None):
        """
        {
          "status": 0,
          "data": [637000,637002],
          "responsetime": "2019-03-19T01:07:24.557Z"
        }
        :param side: BUY SELL 指定時のみ、指定された売買区分の注文を取消対象にします。
        :return:
        """
        data = {
            "symbols": [self.symbol],
            "side": side
        }
        count = 0
        while True:
            req = await self._requests('POST', '/private/v1/cancelBulkOrder', data=data)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in cancel all orders.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in cancel all orders.")

    async def edit_order(self, orderId: int, price: float):
        """
        注文変更
        :param orderId:
        :param price:
        :return:
        """
        count = 0
        while True:
            req = await self._requests('POST', '/private/v1/changeOrder', data={"orderId": orderId, "price": price})
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in edit orders.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in edit orders.")

    async def historical(self, symbol: str, interval: str, date: str):
        params = {"symbol": symbol, 'interval': interval, 'date': date}
        count = 0
        req = await self._requests('GET', f'/public/v1/klines', params=params)
        if req['status'] == 0:
            return req['data']
        else:
            count += 1
            await asyncio.sleep(1)
            if count == 60:
                self.statusNotify("API request failed in historical.")
                self.statusNotify(str(req))
                raise Exception("API request failed in historical.")
