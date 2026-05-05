import yfinance as yf
import json, os, time
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
US_CLOUD = [
    {'symbol': 'AMZN',  'name': '亞馬遜 AWS'},
    {'symbol': 'MSFT',  'name': '微軟 Azure'},
    {'symbol': 'GOOGL', 'name': 'Alphabet GCP'},
    {'symbol': 'ORCL',  'name': 'Oracle Cloud'},
    {'symbol': 'META',  'name': 'Meta'},
    {'symbol': 'IBM',   'name': 'IBM Cloud'},
    {'symbol': 'AAPL',  'name': 'Apple'},
]

TW_INDICES = [
    {'symbol': '^TWII',     'name': '加權指數'},
    {'symbol': '00631L.TW', 'name': '元大台灣50正2'},
]
TW_STOCKS = [
    {'symbol': '2330.TW', 'name': '台積電'},
    {'symbol': '2454.TW', 'name': '聯發科'},
    {'symbol': '3711.TW', 'name': 'ASE 日月光'},
    {'symbol': '2303.TW', 'name': '聯電'},
    {'symbol': '3008.TW', 'name': '大立光'},
    {'symbol': '2308.TW', 'name': '台達電'},
    {'symbol': '2382.TW', 'name': '廣達'},
    {'symbol': '2603.TW', 'name': '長榮'},
    {'symbol': '2881.TW', 'name': '富邦金'},
    {'symbol': '2882.TW', 'name': '國泰金'},
    {'symbol': '2891.TW', 'name': '中信金'},
    {'symbol': '1802.TW', 'name': '台玻'},
    {'symbol': '6770.TW', 'name': '力積電'},
]
TW_DRONE = [
    {'symbol': '8033.TW', 'name': '雷虎科技'},
    {'symbol': '2634.TW', 'name': '漢翔航空'},
    {'symbol': '4961.TW', 'name': '鼎天國際'},
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


def fetch_market(groups):
    """
    groups: dict of group_name -> list of {symbol, name}
    Downloads all symbols in one batch request (with retry), then splits back into groups.
    Returns dict of group_name -> list of {symbol, name, price, change, pct, date}
    """
    all_items = [(gname, item) for gname, items in groups.items() for item in items]
    symbols = [item['symbol'] for _, item in all_items]

    out = {item['symbol']: {**item, 'price': None, 'change': None, 'pct': None, 'date': None}
           for _, item in all_items}

    closes = None
    for attempt in range(3):
        try:
            raw = yf.download(symbols, period='7d', interval='1d',
                              progress=False, auto_adjust=True)
            closes = raw['Close']
            if len(symbols) == 1:
                closes = closes.to_frame(name=symbols[0])
            break
        except Exception as e:
            print(f'  attempt {attempt + 1} failed: {e}')
            if attempt < 2:
                time.sleep(5)

    if closes is None:
        print('  download failed after retries')
        return {gname: [out[item['symbol']] for item in items]
                for gname, items in groups.items()}

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

    return {gname: [out[item['symbol']] for item in items]
            for gname, items in groups.items()}


def save(path, payload):
    os.makedirs('data', exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f'saved {path}')


if __name__ == '__main__':
    now = datetime.now(TZ).strftime('%Y/%m/%d %H:%M')
    print(f'=== fetch_data {now} ===')

    print('--- US ---')
    us = fetch_market({'indices': US_INDICES, 'stocks': US_STOCKS, 'cloud': US_CLOUD})
    save('data/us.json', {'updated': now, **us})

    time.sleep(2)
    print('--- TW ---')
    tw = fetch_market({'indices': TW_INDICES, 'stocks': TW_STOCKS, 'drone': TW_DRONE, 'etf': TW_ETF})
    save('data/tw.json', {'updated': now, **tw})

    time.sleep(2)
    print('--- JP ---')
    jp = fetch_market({'indices': JP_INDICES, 'stocks': JP_STOCKS})
    save('data/jp.json', {'updated': now, **jp})

    print('done')
