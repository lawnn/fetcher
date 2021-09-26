# pyrobot
For crypto currency botter.

## Installation
```
pip install -U git+https://github.com/lawnn/pyrobot.git
```
## Setting(config_default.json)
任意のjsonファイルを作成する。
```
{
  "line_notify_token" : "",
  "discordWebhook" : "",
  "ftx": ["API_KEY","API_SECRET"],
  "bybit": ["API_KEY", "API_SECRET"],
  "gmocoin": ["API_KEY", "API_SECRET"]
}
```

## Example1    
```
from pyrobot import FTX
ftx = FTX('上で作ったjsonファイルpath', market_name)
```
## Example2
```
import asyncio
import os
from pyrobot import FTX


async def main(configPath, symbol):
    ftx = FTX(configPath, symbol)
    
    # 100ETHで0.001lotの買い指値
    r = await ftx.limit('buy', 0.001, 100)
    print(r)


if __name__ == '__main__':
    try:
        if os.name != 'posix':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main('config.json', 'ETH-PERP'))
    except KeyboardInterrupt:
        pass

```
## Special thanks   
[Pybotters](https://github.com/MtkN1/pybotters)
