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
    {'symbol': 'INTC', 'name': 'Intel'},
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


def _is_newer_date(new_date, old_date):
    """Return True if new_date ('MM/DD') >= old_date. Handles month/year wrap."""
    if not new_date or not old_date:
        return True
    try:
        m1, d1 = int(new_date[:2]), int(new_date[3:5])
        m2, d2 = int(old_date[:2]), int(old_date[3:5])
        if abs(m1 - m2) > 6:   # year boundary (e.g. Dec vs Jan)
            if m1 < m2:
                m1 += 12
            else:
                m2 += 12
        return (m1, d1) >= (m2, d2)
    except Exception:
        return True


def merge_with_old(new_data, old_lookup):
    """Replace null-price items with cached data so stale fetches don't erase good prices.
    Also keeps cached data when new data has an older date—yfinance sometimes returns NaN
    for the most-recent trading day during Yahoo Finance's post-close correction window,
    causing _parse_col to fall back one trading day."""
    merged = {}
    for gname, items in new_data.items():
        merged[gname] = []
        for item in items:
            sym = item['symbol']
            if item.get('price') is not None:
                old = old_lookup.get(sym)
                if (old and old.get('price') is not None and
                        not _is_newer_date(item.get('date'), old.get('date'))):
                    print(f'  {sym}: new date {item.get("date")} < cached {old.get("date")}, keeping cache')
                    merged[gname].append(old)
                else:
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
                    # last row = 總計(1~15); col[1]=成交金額(元), col[2]=成交股數(股)
                    total_row = rows[-1]
                    amt = int(str(total_row[1]).replace(',', ''))
                    if amt > 5e10:   # sanity: > 500億元 to skip empty/holiday dates
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


def fetch_tw_analysis(mkt_series=None):
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
            prev_price = float(close.iloc[-2]) if len(close) >= 2 else None
            change = round(price - prev_price, 2) if prev_price is not None else None
            pct = round((price - prev_price) / prev_price * 100, 2) if prev_price is not None else None

            def sma(n):
                return float(close.rolling(n).mean().iloc[-1]) if len(close) >= n else None
            def bias(mv):
                return round((price - mv) / mv * 100, 2) if mv is not None else None

            ma5, ma20, ma120 = sma(5), sma(20), sma(120)
            result[key] = {
                'name':    name,
                'symbol':  sym,
                'price':   round(price, 2),
                'change':  change,
                'pct':     pct,
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
    if mkt_series is None:
        print('  fetching TWSE market 成交金額...')
        mkt_series = fetch_twse_market_amount(target_days=130)
    mkt = mkt_series

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


def fetch_chart_data(mkt_series=None):
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

            # ^TWII: use TWSE 成交金額(億元) when available; 0050.TW: 股 ÷ 1000 = 張
            use_twse = sym == '^TWII' and mkt_series is not None
            mkt_dict = {}
            if use_twse:
                mkt_dict = {d.strftime('%Y-%m-%d'): v for d, v in mkt_series.items()}
            vol_factor = 1 / 1000  # only used for 0050.TW

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
                if use_twse:
                    vol_val = mkt_dict.get(d_str)
                    if vol_val is not None:
                        volume.append({
                            'time':  d_str,
                            'value': vol_val,  # 億元
                            'color': 'rgba(220,38,38,0.55)' if is_up else 'rgba(22,163,74,0.55)',
                        })
                else:
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
            if use_twse:
                result[key]['vol_unit'] = '億元'
            print(f'  chart {sym}: {len(candles)} candles, {len(volume)} vol bars')
        except Exception as e:
            print(f'  chart {sym} error: {e}')

    return result


def fetch_tw_futures_night():
    """Fetch Taiwan futures night-session closing prices from TAIFEX Open API.
    Uses DailyMarketReportFut with TradingSession='盤後' — works from any IP.
    """
    import requests

    PRODUCTS = [
        {'id': 'TX',  'name': '台指期'},
        {'id': 'MTX', 'name': '小台'},
        {'id': 'TMF', 'name': '微台'},
        {'id': 'TE',  'name': '電子期'},
    ]

    def to_f(v):
        try:
            s = str(v).replace(',', '').replace('%', '').strip()
            return float(s) if s and s not in ('-', '') else None
        except Exception:
            return None

    empty = [{'symbol': p['id'], 'name': p['name'],
              'price': None, 'change': None, 'pct': None, 'contract': ''} for p in PRODUCTS]

    try:
        r = requests.get(
            'https://openapi.taifex.com.tw/v1/DailyMarketReportFut',
            timeout=15,
            headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'},
        )
        r.raise_for_status()
        data = r.json()
        print(f'  Open API: {len(data)} records')
    except Exception as e:
        print(f'  Open API fetch failed: {e}')
        return empty

    # Build lookup: contract_id -> sorted list of night-session records (front-month first)
    night = {}
    for row in data:
        if row.get('TradingSession') == '盤後':
            cid = row.get('Contract', '')
            night.setdefault(cid, []).append(row)
    for cid in night:
        night[cid].sort(key=lambda x: x.get('ContractMonth(Week)', ''))

    result = []
    for p in PRODUCTS:
        rows = night.get(p['id'], [])
        if not rows:
            print(f"  {p['id']}: no 盤後 data")
            result.append({'symbol': p['id'], 'name': p['name'],
                           'price': None, 'change': None, 'pct': None, 'contract': ''})
            continue
        row      = rows[0]
        price    = to_f(row.get('Last') or row.get('SettlementPrice'))
        change   = to_f(row.get('Change'))
        pct      = to_f(row.get('%'))
        contract = row.get('ContractMonth(Week)', '')
        date_raw = row.get('Date', '')
        date_s   = f'{date_raw[4:6]}/{date_raw[6:8]}' if len(date_raw) == 8 else ''
        result.append({
            'symbol':   p['id'],
            'name':     p['name'],
            'price':    price,
            'change':   change,
            'pct':      round(pct, 2) if pct is not None else None,
            'contract': contract,
            'date':     date_s,
        })
        print(f"  {p['id']}: price={price} change={change} date={date_s}")

    return result


def fetch_margin_balance(target_days=90):
    """Fetch total market 融資餘額 (億元) from TWSE MI_MARGN?selectType=MS.
    Loads existing data/margin.json to skip already-fetched dates (incremental).
    """
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    import requests as _req
    from datetime import date as _date, timedelta

    existing = {}
    margin_path = 'data/margin.json'
    if os.path.exists(margin_path):
        try:
            with open(margin_path, 'r', encoding='utf-8') as f:
                for item in json.load(f).get('series', []):
                    existing[item['time']] = item['value']
        except Exception:
            pass

    today      = _date.today()
    offset     = 1
    scanned    = 0
    scan_limit = target_days if len(existing) < 30 else 10

    while scanned < scan_limit and offset <= scan_limit * 3:
        d      = today - timedelta(days=offset)
        offset += 1
        if d.weekday() >= 5:
            continue
        scanned += 1
        d_str = d.isoformat()
        if d_str in existing:
            continue
        try:
            r = _req.get(
                'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN',
                params={'date': d.strftime('%Y%m%d'), 'response': 'json', 'selectType': 'MS'},
                timeout=12, verify=False,
                headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.twse.com.tw/'},
            )
            tables = r.json().get('tables', [])
            if tables and tables[0].get('data'):
                for row in tables[0]['data']:
                    if '融資金額' in str(row[0]):
                        amt = int(str(row[5]).replace(',', ''))
                        if amt > 50_000_000:   # sanity: > 500億 仟元
                            existing[d_str] = round(amt / 100_000, 1)
                        break
        except Exception as e:
            print(f'  margin {d_str}: {e}')
        time.sleep(0.3)

    print(f'  margin: scanned {scanned} trading days, {len(existing)} total records')
    sorted_items = sorted(existing.items())[-target_days:]
    return [{'time': k, 'value': v} for k, v in sorted_items]


def fetch_fundamentals():
    """Fetch valuation & fundamental metrics via yfinance .info (parallel, 5 workers).
    Metrics: trailing P/E, forward P/E, P/B, ROE, dividend yield, gross margin, revenue growth.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import math

    targets = US_STOCKS + US_CLOUD + TW_STOCKS + TW_DRONE + TW_ETF
    symbols = [s['symbol'] for s in targets]

    def _get(sym):
        try:
            info = yf.Ticker(sym).info

            def pf(key, mul=1.0, digits=1):
                v = info.get(key)
                try:
                    f = float(v)
                    if math.isnan(f) or math.isinf(f):
                        return None
                    return round(f * mul, digits)
                except (TypeError, ValueError):
                    return None

            return sym, {
                'pe':         pf('trailingPE'),
                'fpe':        pf('forwardPE'),
                'pb':         pf('priceToBook'),
                'roe':        pf('returnOnEquity', 100),
                'div_yield':  pf('dividendYield', 100),
                'gm':         pf('grossMargins', 100),
                'rev_growth': pf('revenueGrowth', 100),
            }
        except Exception as e:
            print(f'  fund {sym}: {e}')
            return sym, None

    result = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_get, sym): sym for sym in symbols}
        for future in as_completed(futures):
            sym, data = future.result()
            if data:
                result[sym] = data
                print(f'  {sym}: PE={data["pe"]}, ROE={data["roe"]}%, yld={data["div_yield"]}%')

    print(f'  fundamentals: {len(result)}/{len(symbols)} fetched')
    return result


def fetch_signals():
    """Calculate short/medium-term signals for all individual stocks (excl. indices).
    Short:  RSI(14) + MA20 bias  → buy <35/<-8%, sell >65/>+8%
    Medium: RSI(21) + MA60 bias  → buy <35/<-12%, sell >65/>+12%
    Downloads 6 months of daily closes in one batch.
    """
    import pandas as pd

    targets = US_STOCKS + US_CLOUD + TW_STOCKS + TW_DRONE + TW_ETF
    symbols = [s['symbol'] for s in targets]

    print(f'  signals: downloading {len(symbols)} symbols (6mo)...')
    raw = None
    for attempt in range(3):
        try:
            raw = yf.download(symbols, period='6mo', interval='1d',
                              progress=False, auto_adjust=True)
            break
        except Exception as e:
            print(f'  signals attempt {attempt + 1}: {e}')
            if attempt < 2:
                time.sleep(5)
    if raw is None:
        return {}

    closes = raw['Close']
    if len(symbols) == 1:
        closes = closes.to_frame(name=symbols[0])

    def calc_rsi(series, period):
        delta = series.diff()
        gain = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
        rs = gain / loss.where(loss > 0, other=1e-10)
        return 100 - 100 / (1 + rs)

    result = {}
    for sym in symbols:
        try:
            close = closes[sym].dropna()
            if len(close) < 30:
                print(f'  signals {sym}: insufficient data ({len(close)} rows)')
                continue
            price = float(close.iloc[-1])

            rsi14 = round(float(calc_rsi(close, 14).iloc[-1]), 1)
            rsi21 = round(float(calc_rsi(close, 21).iloc[-1]), 1)

            ma20 = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else None
            ma60 = float(close.rolling(60).mean().iloc[-1]) if len(close) >= 60 else None

            bias20 = round((price - ma20) / ma20 * 100, 1) if ma20 else None
            bias60 = round((price - ma60) / ma60 * 100, 1) if ma60 else None

            result[sym] = {
                'rsi14':  rsi14,
                'rsi21':  rsi21,
                'bias20': bias20,
                'bias60': bias60,
            }
        except Exception as e:
            print(f'  signals {sym}: {e}')

    print(f'  signals: computed {len(result)}/{len(symbols)} symbols')
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
    print('--- TWSE Market 成交金額 ---')
    mkt_series = fetch_twse_market_amount(target_days=130)

    time.sleep(1)
    print('--- TW Analysis ---')
    analysis = fetch_tw_analysis(mkt_series=mkt_series)
    if analysis:
        save('data/tw_analysis.json', {'updated': now, **analysis})
    else:
        print('  tw_analysis: no data returned, skipping')

    time.sleep(2)
    print('--- Chart Data ---')
    chart = fetch_chart_data(mkt_series=mkt_series)
    if chart:
        save('data/chart.json', {'updated': now, **chart})
    else:
        print('  chart: no data, skipping')

    time.sleep(1)
    print('--- TW Futures (Night) ---')
    futures = fetch_tw_futures_night()
    if any(f.get('price') is not None for f in futures):
        save('data/tw_futures.json', {'updated': now, 'futures': futures})
    else:
        print('  tw_futures: no live data, skipping save')

    time.sleep(1)
    print('--- 融資餘額 ---')
    margin = fetch_margin_balance()
    if margin:
        save('data/margin.json', {'updated': now, 'series': margin})
    else:
        print('  margin: no data')

    time.sleep(2)
    print('--- Signals ---')
    signals = fetch_signals()
    if signals:
        save('data/signals.json', {'updated': now, 'stocks': signals})
    else:
        print('  signals: no data')

    time.sleep(1)
    print('--- Fundamentals ---')
    fund = fetch_fundamentals()
    if fund:
        save('data/fundamentals.json', {'updated': now, 'stocks': fund})
    else:
        print('  fundamentals: no data')

    print('done')
