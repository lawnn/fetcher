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
## Special thanks   
[Pybotters](https://github.com/MtkN1/pybotters)
