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

    async def account_margin(self):
        """
        余力情報を取得
        :return:
        {
          "status": 0,
          "data": {
            "actualProfitLoss": "5204923",  時価評価総額
            "availableAmount": "5189523",   取引余力
            "margin": "7298",               拘束証拠金
            "marginCallStatus": "NORMAL",   追証ステータス: NORMAL MARGIN_CALL LOSSCUT
            "marginRatio": "345.6",         証拠金維持率
            "profitLoss": "8019"            評価損益
          },
          "responsetime": "2019-03-19T02:15:06.051Z"
        }
        """
        count = 0
        while True:
            req = await self._requests('GET', '/private/v1/account/margin')
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in account margin.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in account margin.")

    async def account_assets(self):
        """
        資産残高を取得
        :return:
        {
          "status": 0,
          "data": [
            {
              "amount": "993982448",      残高
              "available": "993982448",   利用可能金額（残高 - 出金予定額）
              "conversionRate": "1",      円転レート（販売所での売却価格です）
              "symbol": "JPY"             ※取引所（現物取引）の取扱銘柄のみAPIでご注文いただけます。
            },
            {
              "amount": "4.0002",           残高
              "available": "4.0002",        利用可能金額（残高 - 出金予定額）
              "conversionRate": "859614",   円転レート（販売所での売却価格です）
              "symbol": "BTC"               ※取引所（現物取引）の取扱銘柄のみAPIでご注文いただけます。
            }
          ],
          "responsetime": "2019-03-19T02:15:06.055Z"
        }
        """
        count = 0
        while True:
            req = await self._requests('GET', '/private/v1/account/assets')
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in account assets.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in account assets.")

    async def orders(self, orderId):
        """
        注文情報取得
        :return:
        {
          "status": 0,
          "data": {
            "list": [
              {
                "orderId": 223456789,       親注文ID
                "rootOrderId": 223456789,   注文ID
                "symbol": "BTC_JPY",
                "side": "BUY",
                "orderType": "NORMAL",      取引区分: NORMAL LOSSCUT
                "executionType": "LIMIT",   注文タイプ: MARKET LIMIT STOP
                "settleType": "OPEN",       決済区分: OPEN CLOSE
                "size": "0.02",             発注数量
                "executedSize": "0.02",     約定数量
                "price": "1430001",         注文価格 (MARKET注文の場合は"0")
                "losscutPrice": "0",        ロスカットレート (現物取引や未設定の場合は"0")
                "status": "EXECUTED",       注文ステータス: WAITING ORDERED MODIFYING CANCELLING CANCELED EXECUTED EXPIRED
                                            ※逆指値注文の場合はWAITINGが有効
                "timeInForce": "FAS",       執行数量条件: FAK FAS FOK (Post-onlyの場合はSOK)
                "timestamp": "2020-10-14T20:18:59.343Z"
              },
              {
                "rootOrderId": 123456789,
                "orderId": 123456789,
                "symbol": "BTC",
                "side": "BUY",
                "orderType": "NORMAL",
                "executionType": "LIMIT",
                "settleType": "OPEN",
                "size": "1",
                "executedSize": "0",
                "price": "900000",
                "losscutPrice": "0",
                "status": "CANCELED",
                "cancelType": "USER",
                "timeInForce": "FAS",
                "timestamp": "2019-03-19T02:15:06.059Z"
              }
            ]
          },
          "responsetime": "2019-03-19T02:15:06.059Z"
        }
        """
        params = {"orderId": f'{orderId}'}
        count = 0
        while True:
            req = await self._requests('GET', '/private/v1/orders', params=params)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in orders.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in orders.")

    async def active_orders(self, symbol: str, page: int = 1, count: int = 100):
        """
        有効注文一覧
        :return:
        {
          "status": 0,
          "data": {
            "pagination": {
              "currentPage": 1,
              "count": 30
            },
            "list": [
              {
                "rootOrderId": 123456789,   親注文ID
                "orderId": 123456789,       注文ID
                "symbol": "BTC",
                "side": "BUY",
                "orderType": "NORMAL",
                "executionType": "LIMIT",
                "settleType": "OPEN",
                "size": "1",                発注数量
                "executedSize": "0",        約定数量
                "price": "840000",
                "losscutPrice": "0",
                "status": "ORDERED",
                "timeInForce": "FAS",
                "timestamp": "2019-03-19T01:07:24.217Z"
              }
            ]
          },
          "responsetime": "2019-03-19T01:07:24.217Z"
        }
        """
        params = {"symbol": symbol, "page": page, "count": count}
        count = 0
        while True:
            req = await self._requests('GET', '/private/v1/activeOrders', params=params)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in active orders.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in active orders.")

    async def executions(self, executionId):
        """
        約定情報取得
        orderId executionId いずれか一つが必須です。2つ同時には設定できません。
        :return:
        {
          "status": 0,
          "data": {
            "list": [
              {
                "executionId": 92123912,
                "orderId": 223456789,
                "positionId": 1234567,
                "symbol": "BTC_JPY",
                "side": "BUY",
                "settleType": "OPEN",
                "size": "0.02",
                "price": "1900000",
                "lossGain": "0",
                "fee": "223",
                "timestamp": "2020-11-24T21:27:04.764Z"
              },
              {
                "executionId": 72123911,
                "orderId": 123456789,
                "positionId": 1234567,
                "symbol": "BTC",
                "side": "BUY",
                "settleType": "OPEN",
                "size": "0.7361",
                "price": "877404",
                "lossGain": "0",
                "fee": "323",
                "timestamp": "2019-03-19T02:15:06.081Z"
              }
            ]
          },
          "responsetime": "2019-03-19T02:15:06.081Z"
        }
        """
        # params = {"orderId	": orderId}
        params = {"executionId": f'{executionId}'}
        count = 0
        while True:
            req = await self._requests('GET', '/private/v1/executions', params=params)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in executions.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in executions.")

    async def latest_executions(self, symbol: str, page: int = 1, count: int = 100):
        """
        最新の約定一覧
        直近1日分の約定情報を返します。
        :return:
        {
          "status": 0,
          "data": {
            "pagination": {
              "currentPage": 1,
              "count": 30
            },
            "list": [
              {
                "executionId": 72123911,
                "orderId": 123456789,
                "positionId": 1234567,
                "symbol": "BTC",
                "side": "BUY",
                "settleType": "OPEN",
                "size": "0.7361",
                "price": "877404",
                "lossGain": "0",        決済損益
                "fee": "323",           取引手数料
                                        ※Takerの場合はプラスの値、Makerの場合はマイナスの値が返ってきます。
                "timestamp": "2019-03-19T02:15:06.086Z"
              }
            ]
          },
          "responsetime": "2019-03-19T02:15:06.086Z"
        }
        """
        params = {"symbol": symbol, "page": page, "count": count}
        count = 0
        while True:
            req = await self._requests('GET', '/private/v1/latestExecutions', params=params)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in latest executions.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in latest executions.")

    async def open_positions(self, symbol: str, page: int = 1, count: int = 100):
        """
        建玉一覧を取得
        有効建玉一覧を取得します。
        :return:
        {
          "status": 0,
          "data": {
            "pagination": {
              "currentPage": 1,
              "count": 30
            },
            "list": [
              {
                "positionId": 1234567,
                "symbol": "BTC_JPY",
                "side": "BUY",
                "size": "0.22",
                "orderdSize": "0",      発注中数量
                "price": "876045",
                "lossGain": "14",
                "leverage": "4",
                "losscutPrice": "766540",
                "timestamp": "2019-03-19T02:15:06.094Z"
              }
            ]
          },
          "responsetime": "2019-03-19T02:15:06.095Z"
        }
        """
        params = {"symbol": symbol, "page": page, "count": count}
        count = 0
        while True:
            req = await self._requests('GET', '/private/v1/openPositions', params=params)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in open positions.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in open positions.")

    async def position_summary(self, symbol: str):
        """
        建玉サマリーを取得
        指定した銘柄の建玉サマリーを売買区分(買/売)ごとに取得ができます
        symbolパラメータ指定無しの場合は、保有している全銘柄の建玉サマリーを売買区分(買/売)ごとに取得します。
        :return:
        {
          "status": 0,
          "data": {
            "list": [
              {
                "averagePositionRate": "715656",    平均建玉レート
                "positionLossGain": "250675",       評価損益
                "side": "BUY",                      売買区分: BUY SELL
                "sumOrderQuantity": "2",            発注中数量
                "sumPositionQuantity": "11.6999",   建玉数量
                "symbol": "BTC_JPY"
              }
            ]
          },
          "responsetime": "2019-03-19T02:15:06.102Z"
        }
        """
        params = {"symbol": symbol}
        count = 0
        while True:
            req = await self._requests('GET', '/private/v1/positionSummary', params=params)
            if req['status'] == 0:
                return req['data']
            else:
                count += 1
                await asyncio.sleep(1)
                if count == 60:
                    self.statusNotify("API request failed in position summary.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in position summary.")

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
        一括決済注文
        一括決済注文をします。
        :param side:
        :param size:
        :param price:
        :return:
           {"status": 0,
            "data": "637000",       orderID
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
        {
          "status": 0,
          "responsetime": "2019-03-19T01:07:24.557Z"
        }
        """
        count = 0
        while True:
            req = await self._requests('POST', '/private/v1/cancelOrder', data={'orderId': order_id})
            if req['status'] == 0:
                return req['responsetime']
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
                }
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
                    self.statusNotify("API request failed in cancel any orders.")
                    self.statusNotify(str(req))
                    raise Exception("API request failed in cancel any orders.")

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
                return req
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
        {
          "status": 0,
          "responsetime": "2019-03-19T01:07:24.557Z"
        }
        """
        count = 0
        while True:
            req = await self._requests('POST', '/private/v1/changeOrder', data={"orderId": orderId, "price": price})
            if req['status'] == 0:
                return req['responsetime']
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
