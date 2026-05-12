#!/usr/bin/env python3
"""
Taiwan live quote update via TWSE MIS real-time API.
Runs every 5 min during market hours (09:00-13:30 TWN).
Only updates data/tw.json; skips analysis/chart/futures.
"""
import requests, json, os, time, urllib3
from datetime import datetime
import pytz

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TZ = pytz.timezone('Asia/Taipei')

# yfinance symbol  ->  (exchange, TWSE/TPEX code)
SYM_TO_CODE = {
    '^TWII':      ('tse', 't00'),
    '00631L.TW':  ('tse', '00631L'),
    '2330.TW':    ('tse', '2330'),
    '2454.TW':    ('tse', '2454'),
    '3711.TW':    ('tse', '3711'),
    '2303.TW':    ('tse', '2303'),
    '3008.TW':    ('tse', '3008'),
    '2308.TW':    ('tse', '2308'),
    '2382.TW':    ('tse', '2382'),
    '2603.TW':    ('tse', '2603'),
    '2881.TW':    ('tse', '2881'),
    '2882.TW':    ('tse', '2882'),
    '2891.TW':    ('tse', '2891'),
    '1802.TW':    ('tse', '1802'),
    '6770.TW':    ('tse', '6770'),
    '8033.TW':    ('otc', '8033'),
    '2634.TW':    ('tse', '2634'),
    '4961.TW':    ('tse', '4961'),
    '0050.TW':    ('tse', '0050'),
    '006208.TW':  ('tse', '006208'),
    '00878.TW':   ('tse', '00878'),
    '00919.TW':   ('tse', '00919'),
    '00929.TW':   ('tse', '00929'),
    '00940.TW':   ('tse', '00940'),
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer':    'https://mis.twse.com.tw/stock/index.jsp',
    'Accept':     'application/json, */*',
}


def query_mis(market, codes):
    """Batch query TWSE MIS API. Returns dict: code -> {price, change, pct, date}."""
    ex_ch = '|'.join(f'{market}_{c}.tw' for c in codes)
    url   = 'https://mis.twse.com.tw/stock/api/getStockInfo.jsp'
    for attempt in range(3):
        try:
            verify = attempt == 0
            r = requests.get(url,
                params={'ex_ch': ex_ch, 'json': '1', 'delay': '0'},
                headers=HEADERS, timeout=10, verify=verify)
            r.raise_for_status()
            result = {}
            for item in r.json().get('msgArray', []):
                code = item.get('c', '')
                z    = item.get('z', '-')   # 成交價（盤中）
                y    = item.get('y', '-')   # 昨收
                o    = item.get('o', '-')   # 開盤（z 未成交時備用）
                d    = item.get('d', '')    # YYYYMMDD
                price_str = z if z not in ('-', '', None) else o
                if price_str not in ('-', '', None) and y not in ('-', '', None):
                    try:
                        price = float(price_str)
                        prev  = float(y)
                        if prev > 0:
                            chg  = round(price - prev, 2)
                            pct  = round(chg / prev * 100, 2)
                            date = f'{d[4:6]}/{d[6:8]}' if len(d) == 8 else ''
                            result[code] = {'price': price, 'change': chg,
                                            'pct': pct, 'date': date}
                            print(f'  {code}: {price} ({pct:+.2f}%)')
                    except (ValueError, ZeroDivisionError):
                        pass
                else:
                    print(f'  {code}: 尚無成交 (z={z})')
            return result
        except Exception as e:
            print(f'  MIS {market} attempt {attempt+1}: {e}')
            if attempt < 2:
                time.sleep(3)
    return {}


def update_group(items, live):
    """Overwrite price/change/pct/date for each item if live data exists."""
    n = 0
    for item in items:
        sym = item.get('symbol', '')
        mapping = SYM_TO_CODE.get(sym)
        if not mapping:
            continue
        _, code = mapping
        if code in live:
            item.update(live[code])
            n += 1
    return n


if __name__ == '__main__':
    now = datetime.now(TZ)
    total_min = now.hour * 60 + now.minute

    # 非盤中時間直接退出（workflow 仍會 trigger，Python 守門）
    if not (9 * 60 <= total_min <= 13 * 60 + 30):
        print(f'非盤中 ({now.strftime("%H:%M")} TWN)，跳過。')
        exit(0)

    now_str = now.strftime('%Y/%m/%d %H:%M')
    print(f'=== fetch_tw_live {now_str} ===')

    TW_PATH = 'data/tw.json'
    if not os.path.exists(TW_PATH):
        print(f'{TW_PATH} 不存在，請先跑一次主 workflow。')
        exit(1)

    with open(TW_PATH, 'r', encoding='utf-8') as f:
        tw = json.load(f)

    # 分組 codes
    tse_codes = [code for sym, (mkt, code) in SYM_TO_CODE.items() if mkt == 'tse']
    otc_codes = [code for sym, (mkt, code) in SYM_TO_CODE.items() if mkt == 'otc']

    # 查詢即時報價
    live = {}
    live.update(query_mis('tse', tse_codes))
    if otc_codes:
        time.sleep(0.5)
        live.update(query_mis('otc', otc_codes))

    if not live:
        print('沒有取到任何即時報價，保留現有資料。')
        exit(0)

    # 更新各群組
    total = 0
    for group in ('indices', 'stocks', 'drone', 'etf'):
        if isinstance(tw.get(group), list):
            n = update_group(tw[group], live)
            total += n
            print(f'  {group}: {n} 筆更新')

    if total == 0:
        print('沒有任何欄位被更新。')
        exit(0)

    tw['updated'] = now_str
    with open(TW_PATH, 'w', encoding='utf-8') as f:
        json.dump(tw, f, ensure_ascii=False, indent=2)
    print(f'已儲存 {TW_PATH}（{total} 檔更新，{now_str}）')
