import yfinance as yf
import json, os
from datetime import datetime
import pytz

TZ = pytz.timezone('Asia/Taipei')

US_INDICES = [
    {'symbol': '^GSPC',  'name': 'S&P 500'},
    {'symbol': '^IXIC',  'name': 'NASDAQ'},
    {'symbol': '^DJI',   'name': '道瓊'},
    {'symbol': '^RUT',   'name': '羅素 2000'},
]
US_STOCKS = [
    {'symbol': 'TSM',  'name': '台積電 ADR'},
    {'symbol': 'NVDA', 'name': '輝達 NVDA'},
    {'symbol': 'MU',   'name': '美光 MU'},
    {'symbol': 'TSLA', 'name': '特斯拉 TSLA'},
    {'symbol': 'EWT',  'name': '摩台指 EWT'},
    {'symbol': 'VOO',  'name': 'VOO'},
    {'symbol': 'QQQ',  'name': 'QQQ'},
    {'symbol': 'SCHD', 'name': 'SCHD'},
]

TW_INDICES = [
    {'symbol': '^TWII',   'name': '加權指數'},
    {'symbol': '00631L.TW', 'name': '元大台灣50正2'},
]
TW_STOCKS = [
    {'symbol': '2330.TW', 'name': '台積電'},
    {'symbol': '2454.TW', 'name': '聯發科'},
    {'symbol': '3711.TW', 'name': 'ASE 日月光'},
    {'symbol': '2303.TW', 'name': '聯電'},
    {'symbol': '2382.TW', 'name': '廣達'},
    {'symbol': '2603.TW', 'name': '長榮'},
]
TW_ETF = [
    {'symbol': '0050.TW',   'name': '元大台灣50'},
    {'symbol': '006208.TW', 'name': '富邦台50'},
    {'symbol': '00878.TW',  'name': '國泰永續高股息'},
    {'symbol': '00919.TW',  'name': '群益台灣精選高息'},
    {'symbol': '00929.TW',  'name': '復華台灣科技優息'},
    {'symbol': '00940.TW',  'name': '元大台灣價值高息'},
]

JP_INDICES = [
    {'symbol': '^N225', 'name': '日經 225'},
    {'symbol': 'EWJ',   'name': '日股 ETF (EWJ)'},
]
JP_STOCKS = [
    {'symbol': '8035.T', 'name': '東京威力科創'},
    {'symbol': '6857.T', 'name': 'Advantest'},
    {'symbol': '4063.T', 'name': '信越化學'},
    {'symbol': '6758.T', 'name': 'Sony'},
    {'symbol': '6861.T', 'name': 'Keyence'},
    {'symbol': '4452.T', 'name': '花王'},
]


def fetch_group(items):
    symbols = [x['symbol'] for x in items]
    out = {x['symbol']: {**x, 'price': None, 'change': None, 'pct': None, 'date': None}
           for x in items}
    try:
        raw = yf.download(symbols, period='5d', interval='1d',
                          progress=False, auto_adjust=True, threads=True)
        closes = raw['Close']
        if len(symbols) == 1:
            closes = closes.to_frame(name=symbols[0])

        for sym in symbols:
            try:
                col = closes[sym].dropna()
                if len(col) >= 2:
                    prev, last = float(col.iloc[-2]), float(col.iloc[-1])
                    out[sym].update({
                        'price':  round(last, 2),
                        'change': round(last - prev, 2),
                        'pct':    round((last - prev) / prev * 100, 2),
                        'date':   col.index[-1].strftime('%m/%d'),
                    })
                elif len(col) == 1:
                    out[sym].update({
                        'price': round(float(col.iloc[-1]), 2),
                        'date':  col.index[-1].strftime('%m/%d'),
                    })
            except Exception as e:
                print(f'  {sym}: {e}')
    except Exception as e:
        print(f'download error: {e}')

    return [out[x['symbol']] for x in items]


def save(path, payload):
    os.makedirs('data', exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f'saved {path}')


if __name__ == '__main__':
    now = datetime.now(TZ).strftime('%Y/%m/%d %H:%M')
    print(f'=== fetch_data {now} ===')

    print('--- US ---')
    save('data/us.json', {
        'updated':  now,
        'indices':  fetch_group(US_INDICES),
        'stocks':   fetch_group(US_STOCKS),
    })

    print('--- TW ---')
    save('data/tw.json', {
        'updated':  now,
        'indices':  fetch_group(TW_INDICES),
        'stocks':   fetch_group(TW_STOCKS),
        'etf':      fetch_group(TW_ETF),
    })

    print('--- JP ---')
    save('data/jp.json', {
        'updated':  now,
        'indices':  fetch_group(JP_INDICES),
        'stocks':   fetch_group(JP_STOCKS),
    })

    print('done')
