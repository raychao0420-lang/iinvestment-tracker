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


def fetch_twse_market_amount(target_days=130):
    """Fetch Taiwan market daily 成交金額 (億元) by querying TWSE MI_INDEX day-by-day.
    Walks back through weekdays until target_days trading days are collected.
    Uses table[6] 總計(1~15) row — total market across all categories.
    """
    import requests, pandas as pd, urllib3
    from datetime import date as _date, timedelta

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    records = {}
    today = _date.today()
    offset = 1
    calendar_limit = target_days * 3  # upper bound to avoid infinite loop

    while len(records) < target_days and offset <= calendar_limit:
        d = today - timedelta(days=offset)
        offset += 1
        if d.weekday() >= 5:   # skip weekends
            continue
        date_str = d.strftime('%Y%m%d')
        try:
            r = requests.get(
                'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX',
                params={'date': date_str, 'response': 'json'},
                timeout=12,
                verify=False,
                headers={'User-Agent': 'Mozilla/5.0',
                         'Referer': 'https://www.twse.com.tw/'}
            )
            tables = r.json().get('tables', [])
            if len(tables) > 6:
                rows = tables[6].get('data', [])
                if rows:
                    # last row = 總計(1~15); col[2]=成交金額(元), col[1]=成交股數
                    total_row = rows[-1]
                    amt = int(str(total_row[2]).replace(',', ''))
                    if amt > 5e10:   # sanity: > 500億 to skip empty/holiday dates
                        records[d.isoformat()] = round(amt / 1e8, 1)
        except Exception as e:
            pass
        time.sleep(0.3)

    if not records:
        return None
    s = pd.Series(records)
    s.index = pd.to_datetime(s.index)
    print(f'  TWSE: collected {len(s)} trading days')
    return s.sort_index()


def fetch_tw_analysis():
    """Fetch TWII and 0050 MA/bias analysis; TWII volume from TWSE, 0050 in 張+億元."""
    import pandas as pd

    symbols = ['^TWII', '0050.TW']
    raw = None
    for attempt in range(3):
        try:
            raw = yf.download(symbols, period='1y', interval='1d',
                              progress=False, auto_adjust=True)
            break
        except Exception as e:
            print(f'  tw_analysis attempt {attempt+1}: {e}')
            if attempt < 2:
                time.sleep(5)
    if raw is None:
        return {}

    result = {}
    targets = [
        ('twii',    '^TWII',   '台股大盤'),
        ('etf0050', '0050.TW', '0050 元大台灣50'),
    ]
    for key, sym, name in targets:
        try:
            close = raw['Close'][sym].dropna()
            if len(close) < 20:
                print(f'  {sym}: insufficient data ({len(close)} rows)')
                continue

            price = float(close.iloc[-1])
            date_str = close.index[-1].strftime('%m/%d')

            def sma(n):
                return float(close.rolling(n).mean().iloc[-1]) if len(close) >= n else None
            def bias(mv):
                return round((price - mv) / mv * 100, 2) if mv is not None else None

            ma5, ma20, ma120 = sma(5), sma(20), sma(120)
            result[key] = {
                'name':    name,
                'symbol':  sym,
                'price':   round(price, 2),
                'date':    date_str,
                'ma5':     round(ma5,   2) if ma5   is not None else None,
                'ma20':    round(ma20,  2) if ma20  is not None else None,
                'ma120':   round(ma120, 2) if ma120 is not None else None,
                'bias5':   bias(ma5),
                'bias20':  bias(ma20),
                'bias120': bias(ma120),
                # vol fields filled below
                'vol3': [], 'vol_ma5': None, 'vol_ma20': None, 'vol_ma120': None,
            }

            if sym == '0050.TW':
                # Volume in 股 → 張
                vol_s = raw['Volume'][sym]
                vol_z = (vol_s[vol_s > 0].dropna() / 1000).round()

                def vma(n):
                    return int(vol_z.rolling(n).mean().iloc[-1]) if len(vol_z) >= n else None

                result[key]['vol3']      = [int(v) for v in vol_z.iloc[-3:].tolist()] if len(vol_z) >= 3 else []
                result[key]['vol_ma5']   = vma(5)
                result[key]['vol_ma20']  = vma(20)
                result[key]['vol_ma120'] = vma(120)

                # 億元 = vol_股 × close / 1e8
                vol_shares = vol_s[vol_s > 0].dropna()
                cl_aligned = close.reindex(vol_shares.index)
                vol_amt = (vol_shares * cl_aligned / 1e8).dropna()

                def vma_amt(n):
                    return round(float(vol_amt.rolling(n).mean().iloc[-1]), 1) if len(vol_amt) >= n else None

                result[key]['vol3_amt']      = [round(float(v), 1) for v in vol_amt.iloc[-3:].tolist()] if len(vol_amt) >= 3 else []
                result[key]['vol_ma5_amt']   = vma_amt(5)
                result[key]['vol_ma20_amt']  = vma_amt(20)
                result[key]['vol_ma120_amt'] = vma_amt(120)

            print(f'  {sym}: {price:.2f}, bias5={bias(ma5)}, bias20={bias(ma20)}, bias120={bias(ma120)}')
        except Exception as e:
            print(f'  {sym} analysis error: {e}')

    # TWII volume: TWSE for recent data; calibrate yfinance for 120-day MA
    print('  fetching TWSE market 成交金額...')
    mkt = fetch_twse_market_amount(target_days=130)

    if mkt is not None and len(mkt) >= 3 and 'twii' in result:
        # Align TWSE dates with yfinance volume for calibration
        yf_vol_raw = raw['Volume']['^TWII'].dropna()
        yf_vol_pos = yf_vol_raw[yf_vol_raw > 0]
        # Normalize yfinance index to date-only for matching
        yf_idx = yf_vol_pos.copy()
        yf_idx.index = yf_idx.index.normalize()
        overlap = mkt.index.normalize().intersection(yf_idx.index.normalize())
        print(f'  TWSE/yf overlap: {len(overlap)} days')

        vol_ma120 = None
        if len(overlap) >= 5:
            twse_vals = mkt.loc[overlap]
            yf_vals   = yf_idx.loc[overlap]
            ratio = (twse_vals / yf_vals).median()
            print(f'  calibration ratio (median): {ratio:.1f} 億/yf-unit')
            # Estimate historical 成交金額 from yfinance via ratio
            yf_amt_hist = (yf_vol_pos * ratio).dropna()
            if len(yf_amt_hist) >= 120:
                vol_ma120 = round(float(yf_amt_hist.rolling(120).mean().iloc[-1]), 1)

        def mma(n):
            return round(float(mkt.rolling(n).mean().iloc[-1]), 1) if len(mkt) >= n else None

        result['twii']['vol3']      = [round(float(v), 1) for v in mkt.iloc[-3:].tolist()]
        result['twii']['vol_ma5']   = mma(5)
        result['twii']['vol_ma20']  = mma(20)
        result['twii']['vol_ma120'] = vol_ma120
        print(f'  TWSE vol: last3={result["twii"]["vol3"]}, ma5={mma(5)}, ma120={vol_ma120}億')
    else:
        print('  TWSE market vol: unavailable')

    return result


def fetch_chart_data():
    """Download ~1y OHLCV for ^TWII and 0050.TW; compute MA5/20/60; keep last 126 bars (≈6 months)."""
    import pandas as pd

    symbols = ['^TWII', '0050.TW']
    raw = None
    for attempt in range(3):
        try:
            raw = yf.download(symbols, period='1y', interval='1d',
                              progress=False, auto_adjust=True)
            break
        except Exception as e:
            print(f'  chart attempt {attempt+1}: {e}')
            if attempt < 2:
                time.sleep(5)
    if raw is None:
        return {}

    result = {}
    targets = [('twii', '^TWII'), ('etf0050', '0050.TW')]

    for key, sym in targets:
        try:
            df = pd.DataFrame({
                'open':   raw['Open'][sym],
                'high':   raw['High'][sym],
                'low':    raw['Low'][sym],
                'close':  raw['Close'][sym],
                'volume': raw['Volume'][sym],
            }).dropna()

            # Compute MAs on full 1y data, then trim to last 126 rows (≈6 months)
            ma5  = df['close'].rolling(5).mean()
            ma20 = df['close'].rolling(20).mean()
            ma60 = df['close'].rolling(60).mean()
            df   = df.tail(126)
            ma5  = ma5.reindex(df.index)
            ma20 = ma20.reindex(df.index)
            ma60 = ma60.reindex(df.index)

            # ^TWII yfinance volume is in 張; 0050.TW is in 股 → ÷1000 = 張
            vol_factor = 1 if sym == '^TWII' else 1 / 1000

            candles, volume = [], []
            for ts in df.index:
                row  = df.loc[ts]
                is_up = float(row['close']) >= float(row['open'])
                d_str = ts.strftime('%Y-%m-%d')
                candles.append({
                    'time':  d_str,
                    'open':  round(float(row['open']),  2),
                    'high':  round(float(row['high']),  2),
                    'low':   round(float(row['low']),   2),
                    'close': round(float(row['close']), 2),
                })
                volume.append({
                    'time':  d_str,
                    'value': round(float(row['volume']) * vol_factor),
                    'color': 'rgba(220,38,38,0.55)' if is_up else 'rgba(22,163,74,0.55)',
                })

            def to_line(s):
                return [{'time': ts.strftime('%Y-%m-%d'), 'value': round(float(v), 2)}
                        for ts, v in s.items() if pd.notna(v)]

            result[key] = {
                'symbol':  sym,
                'candles': candles,
                'volume':  volume,
                'ma5':     to_line(ma5),
                'ma20':    to_line(ma20),
                'ma60':    to_line(ma60),
            }
            print(f'  chart {sym}: {len(candles)} candles')
        except Exception as e:
            print(f'  chart {sym} error: {e}')

    return result


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

    time.sleep(2)
    print('--- TW Analysis ---')
    analysis = fetch_tw_analysis()
    if analysis:
        save('data/tw_analysis.json', {'updated': now, **analysis})
    else:
        print('  tw_analysis: no data returned, skipping')

    time.sleep(2)
    print('--- Chart Data ---')
    chart = fetch_chart_data()
    if chart:
        save('data/chart.json', {'updated': now, **chart})
    else:
        print('  chart: no data, skipping')

    print('done')
