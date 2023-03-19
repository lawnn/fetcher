# fetcher

## インストール方法
```
pip install -U git+https://github.com/lawnn/wrappy.git
```
## example
```commandline
from fetcher.binance import binance_get_OI
binance_get_OI("2023-03-17 00:00:00")
```
## 今後の予定
pandasは全てpolarsにする  
引数に入れるdatetimeなどを統一する