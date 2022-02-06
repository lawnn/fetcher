# wrappy
For crypto currency botter.     
## What is it?
仮想通貨のbot作成にかかる手間をこのライブラリに集約するために作りました.      
基本機能はlog出力 discord or LINE通知 指値など取引所への注文です.     
(GMOは口座作っただけで特にテストしてません)
## インストール方法
```
pip install -U git+https://github.com/lawnn/wrappy.git
```
## 事前準備
適当な場所にconfig.jsonファイルを作成する。       
excange_name, bot_name, log_level, discordWebhookは任意で記入      
それ以外は必須です。
```
{
  "exchange_name" : "exchange", # 取引所の名前
  "bot_name" : "bot",   # botの名前
  "log_level" : "DEBUG",    # logレベル
  "line_notify_token" : "", # LINE token
  "discordWebhook" : "",    # discord
  "ftx": ["API_KEY","API_SECRET"],
  "bybit": ["API_KEY", "API_SECRET"],
  "gmocoin": ["API_KEY", "API_SECRET"]
}
```

## 導入方法
```
from wrappy import GMO
bot = GMO('上で作ったjsonファイルがある場所', market_name)
```     
## 基本機能紹介(base.py)
```
# log error出力
log_error('ここに何か書く')
# log warning出力
log_warning('ここに何か書く') 
# log info出力
log_info('ここに何か書く')
# log debug出力
log_debug('ここに何か書く')
# discordかlineに通知
statusNotify('ここに何か書く')
```
## 使用例1(log,Notify出力)     
```buildoutcfg
import asyncio
import os
from wrappy import BotBase


async def main(configPath):
    bot = BotBase(configPath)
    bot.log_error('error')
    bot.log_warning('warning')
    bot.log_debug('debug')
    bot.log_info('info')
    bot.statusNotify('notify test')

if __name__ == '__main__':
    try:
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main('config.json'))
    except KeyboardInterrupt:
        pass
```
## 使用例2(GMOの場合)      
100BTCで0.001lotの買い指値        
config.jsonは同じディレクトリに置いた場合のコード
```buildoutcfg
import asyncio
import os
from wrappy import GMO


async def main(configPath, symbol):
    bot = GMO(configPath, symbol)
    bot.log_info('starting bot...')
    
    # 100BTCで0.001lotの買い指値
    r = await bot.limit('buy', 0.001, 100)
    bot.log_info(r)


if __name__ == '__main__':
    try:
        # windowsで使う場合のおまじない.
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        #　mainの実行
        asyncio.run(main('config.json', 'BTC_JPY'))
    except KeyboardInterrupt:
        pass

```
## 使用例3(csv読み書き)

```buildoutcfg
import asyncio
import os
from datetime import datetime, timezone, timedelta
from wrappy.base import LogBase


class OrderHistory(LogBase):
    """
    注文履歴ログ.
    """

    def __init__(self, full_path):
        super().__init__(full_path, as_new=False, encoding="shift_jis", with_header=True)
        self.columns = {
            "order_no": "オーダーNo.",
            "order_id": "オーダーID",
            "timestamp": "オーダー時刻",
            "order_kind": "オーダー種別",
            "size": "実際にオーダーしたサイズ",
            "price": "実際にオーダーした価格(=ベスト価格+3種類の幅)",
            "current_position": "現在ポジション",
        }


class History:
    def __init__(self):
        self.exchange_name = 'exchange'
        self.bot_name = 'bot'
        # 発注履歴を保存するファイル用のパラメータ.
        self.order_history_file_class = OrderHistory
        self.order_history_dir = os.path.abspath(os.path.abspath(__file__) + "/../log/")
        self.order_history_file_name_base = f"{self.exchange_name}_{self.bot_name}_order_history"
        self.order_history_files = {}
        self.order_history_encoding = "shift_jis"
        self.JST = timezone(timedelta(hours=9), 'JST')
        self.GMT = timezone(timedelta(hours=0), 'GMT')

    def now_jst(self):
        """
        現在時刻をJSTで取得.
        :return: datetime.
        """
        return datetime.now(self.JST)

    def now_jst_str(self, date_format="%Y-%m-%d %H:%M:%S"):
        return self.now_jst().strftime(date_format)

    def now_gmt(self):
        """
        現在時刻をGMTで取得.
        :return: datetime
        """
        return datetime.now(self.GMT)

    def write_order_history(self, order_history):
        """
        発注履歴を出力します.
        :param order_history: ログデータ.
        """
        self.get_or_create_order_history_file().write_row_by_dict(order_history)

    def get_or_create_order_history_file(self):
        """
        現在時刻を元に発注履歴ファイルを取得します.
        ファイルが存在しない場合、新規で作成します.
        :return: 発注履歴ファイル.
        """
        today_str = self.now_jst_str("%y%m%d")
        order_history_file_name = self.order_history_file_name_base + f"_{today_str}.csv"
        full_path = self.order_history_dir + "/" + order_history_file_name
        if today_str not in self.order_history_files:
            self.order_history_files[today_str] = self.order_history_file_class(full_path)
            self.order_history_files[today_str].open()
        return self.order_history_files[today_str]

    def close_order_history_files(self):
        """
        発注履歴ファイルをクローズします.
        """
        for order_history_file in self.order_history_files.values():
            order_history_file.close()


async def main():
    hist = History()
    order_history = {"order_no": 1, "timestamp": 12,
                     "order_kind": 2, "size": 0.3, "price": 5000,
                     "current_position": 5430
                     }
    hist.write_order_history(order_history)


if __name__ == '__main__':
    try:
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

```
## Special thanks   
[Pybotters](https://github.com/MtkN1/pybotters)
