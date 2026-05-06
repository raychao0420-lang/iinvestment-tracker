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


def _parse_col(col):
    """Extract (price, change, pct, date) from a Close series."""
    col = col.dropna()
    if len(col) >= 2:
        prev, last = float(col.iloc[-2]), float(col.iloc[-1])
        return round(last, 2), round(last - prev, 2), round((last - prev) / prev * 100, 2), col.index[-1].strftime('%m/%d')
    if len(col) == 1:
        return round(float(col.iloc[-1]), 2), None, None, col.index[-1].strftime('%m/%d')
    return None, None, None, None


def _fetch_single(sym):
    """Fallback: fetch one symbol via Ticker.history()."""
    for attempt in range(3):
        try:
            h = yf.Ticker(sym).history(period='7d', auto_adjust=True)
            col = h['Close']
            if not col.dropna().empty:
                return col
        except Exception as e:
            print(f'    {sym} fallback attempt {attempt + 1}: {e}')
            if attempt < 2:
                time.sleep(3)
    return None


def fetch_market(groups):
    """
    groups: dict of group_name -> list of {symbol, name}
    First tries a single batch download; failed symbols fall back to individual fetches.
    Returns dict of group_name -> list of {symbol, name, price, change, pct, date}
    """
    all_items = [(gname, item) for gname, items in groups.items() for item in items]
    symbols = [item['symbol'] for _, item in all_items]

    out = {item['symbol']: {**item, 'price': None, 'change': None, 'pct': None, 'date': None}
           for _, item in all_items}

    # --- batch attempt ---
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
            print(f'  batch attempt {attempt + 1} failed: {e}')
            if attempt < 2:
                time.sleep(5)

    failed = []
    if closes is not None:
        for sym in symbols:
            try:
                price, change, pct, date = _parse_col(closes[sym])
                if price is not None:
                    out[sym].update({'price': price, 'change': change, 'pct': pct, 'date': date})
                else:
                    failed.append(sym)
            except Exception as e:
                print(f'  {sym}: {e}')
                failed.append(sym)
    else:
        failed = symbols[:]
        print('  batch failed entirely, falling back to individual fetch')

    # --- individual fallback ---
    if failed:
        print(f'  individual fallback for: {failed}')
        for sym in failed:
            col = _fetch_single(sym)
            if col is not None:
                price, change, pct, date = _parse_col(col)
                if price is not None:
                    out[sym].update({'price': price, 'change': change, 'pct': pct, 'date': date})
                    print(f'    {sym}: recovered via fallback ({date})')
                else:
                    print(f'    {sym}: fallback returned empty data')
            else:
                print(f'    {sym}: fallback also failed')
            time.sleep(1)

    return {gname: [out[item['symbol']] for item in items]
            for gname, items in groups.items()}


def load_existing(path):
    """Load existing JSON as symbol→item dict for merge."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            d = json.load(f)
        result = {}
        for val in d.values():
            if isinstance(val, list):
                for item in val:
                    if 'symbol' in item:
                        result[item['symbol']] = item
        return result
    except Exception:
        return {}


def merge_with_old(new_data, old_lookup):
    """Replace null-price items with cached data so stale fetches don't erase good prices."""
    merged = {}
    for gname, items in new_data.items():
        merged[gname] = []
        for item in items:
            sym = item['symbol']
            if item.get('price') is not None:
                merged[gname].append(item)
            elif sym in old_lookup and old_lookup[sym].get('price') is not None:
                print(f'  {sym}: keeping cached data ({old_lookup[sym].get("date")})')
                merged[gname].append(old_lookup[sym])
            else:
                merged[gname].append(item)
    return merged


def save(path, payload):
    os.makedirs('data', exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f'saved {path}')


if __name__ == '__main__':
    now = datetime.now(TZ).strftime('%Y/%m/%d %H:%M')
    print(f'=== fetch_data {now} ===')

    print('--- US ---')
    old_us = load_existing('data/us.json')
    us = fetch_market({'indices': US_INDICES, 'stocks': US_STOCKS, 'cloud': US_CLOUD})
    us = merge_with_old(us, old_us)
    save('data/us.json', {'updated': now, **us})

    time.sleep(2)
    print('--- TW ---')
    old_tw = load_existing('data/tw.json')
    tw = fetch_market({'indices': TW_INDICES, 'stocks': TW_STOCKS, 'drone': TW_DRONE, 'etf': TW_ETF})
    tw = merge_with_old(tw, old_tw)
    save('data/tw.json', {'updated': now, **tw})

    time.sleep(2)
    print('--- JP ---')
    old_jp = load_existing('data/jp.json')
    jp = fetch_market({'indices': JP_INDICES, 'stocks': JP_STOCKS})
    jp = merge_with_old(jp, old_jp)
    save('data/jp.json', {'updated': now, **jp})

    print('done')
