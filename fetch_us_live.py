# -*- coding: utf-8 -*-
"""美股夜盤即時更新（盤前 / 盤中 / 盤後）。
抓：美股期貨(ES/NQ/YM/RTY)＋費半代理(SOXX)、現貨指數、以及 us.json 內所有追蹤個股的即時／盤前盤後價。
只在美東時間 04:00–20:00（週一~五）的延伸交易時段動作，對應台灣約 16:00–08:00。
產出 data/us_live.json（獨立檔，不與主 update.yml 的 us.json 衝突）。
資料源：Yahoo v8 chart API（含盤前盤後、免 crumb）。"""
import requests, json, os
from datetime import datetime
import pytz

TZ_TW = pytz.timezone('Asia/Taipei')
TZ_ET = pytz.timezone('America/New_York')
HERE = os.path.dirname(os.path.abspath(__file__))
US_JSON = os.path.join(HERE, 'data', 'us.json')
OUT = os.path.join(HERE, 'data', 'us_live.json')

FUTURES = [
    {'symbol': 'ES=F',  'name': '標普期',      'kind': 'fut'},
    {'symbol': 'NQ=F',  'name': '那斯達克期',  'kind': 'fut'},
    {'symbol': 'YM=F',  'name': '道瓊期',      'kind': 'fut'},
    {'symbol': 'RTY=F', 'name': '羅素2000期',  'kind': 'fut'},
    {'symbol': 'SOXX',  'name': '費半 SOXX',   'kind': 'stk'},  # 費半無流動期貨，用 ETF 盤前後代理
]
INDICES = [
    {'symbol': '^GSPC', 'name': 'S&P 500',    'kind': 'idx'},
    {'symbol': '^IXIC', 'name': 'NASDAQ',     'kind': 'idx'},
    {'symbol': '^DJI',  'name': '道瓊',       'kind': 'idx'},
    {'symbol': '^RUT',  'name': '羅素 2000',  'kind': 'idx'},
    {'symbol': '^SOX',  'name': '費城半導體', 'kind': 'idx'},
]


def load_tracked_stocks():
    """從 us.json 蒐集所有追蹤的美股個股（跟主追蹤清單同步）。"""
    try:
        with open(US_JSON, encoding='utf-8') as f:
            us = json.load(f)
    except Exception:
        return []
    out, seen = [], set()
    for cat in ('stocks', 'cloud', 'gpu', 'cpu', 'optical', 'power', 'etf'):
        for s in us.get(cat, []):
            sym = s.get('symbol')
            if sym and sym not in seen:
                seen.add(sym)
                out.append({'symbol': sym, 'name': s.get('name', sym), 'kind': 'stk'})
    return out


def fetch_one(sym):
    """回傳 (reg_px, prev_close, ext_px)；抓不到回 (None,None,None)。"""
    url = (f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}'
           '?range=1d&interval=1m&includePrePost=true')
    try:
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        res = r.json()['chart']['result'][0]
        meta = res['meta']
        reg_px = meta.get('regularMarketPrice')
        prev = meta.get('chartPreviousClose')
        ext_px = None
        ts = res.get('timestamp')
        closes = (res.get('indicators', {}).get('quote', [{}]) or [{}])[0].get('close')
        if ts and closes:
            for c in closes:
                if c is not None:
                    ext_px = c  # 序列最後一筆非空 = 最新成交（含盤前盤後）
        return reg_px, prev, ext_px
    except Exception as e:
        print(f'  {sym}: {e}')
        return None, None, None


def quote(item, session):
    reg_px, prev, ext_px = fetch_one(item['symbol'])
    if reg_px is None or prev is None:
        return None
    if item['kind'] == 'stk' and session in ('盤前', '盤後'):
        price = ext_px if ext_px is not None else reg_px
        ref = reg_px          # 盤前=昨收；盤後=今收（Yahoo 的 regularMarketPrice 隨時段變動）
    else:                     # 期貨、現貨指數、盤中個股
        price = reg_px
        ref = prev            # 對比前一交易日收盤 / 前結算
    change = round(price - ref, 2)
    pct = round(change / ref * 100, 2) if ref else None
    dec = 0 if price >= 1000 else 2
    return {'symbol': item['symbol'], 'name': item['name'],
            'price': round(price, dec), 'change': round(change, dec), 'pct': pct}


def session_label(et):
    t = et.hour * 60 + et.minute
    if et.weekday() >= 5:
        return None                       # 週末休市
    if 4 * 60 <= t < 9 * 60 + 30:
        return '盤前'
    if 9 * 60 + 30 <= t < 16 * 60:
        return '盤中'
    if 16 * 60 <= t < 20 * 60:
        return '盤後'
    return None                           # ET 20:00–04:00：延伸盤外，休市


def main():
    et = datetime.now(TZ_ET)
    session = session_label(et)
    if not session:
        print(f'非美股延伸交易時段（ET {et.strftime("%a %H:%M")}），跳過。')
        return

    now_tw = datetime.now(TZ_TW)
    print(f'=== fetch_us_live [{session}] ET {et.strftime("%H:%M")} / TW {now_tw.strftime("%H:%M")} ===')

    stocks = load_tracked_stocks()
    result = {
        'updated': now_tw.strftime('%Y/%m/%d %H:%M'),
        'et': et.strftime('%H:%M'),
        'session': session,
        'futures': [q for q in (quote(i, session) for i in FUTURES) if q],
        'indices': [q for q in (quote(i, session) for i in INDICES) if q],
        'stocks':  [q for q in (quote(i, session) for i in stocks) if q],
    }
    total = len(result['futures']) + len(result['indices']) + len(result['stocks'])
    if total == 0:
        print('未取得任何報價，保留現有資料。')
        return
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))
    print(f'已寫入 {OUT}（期貨{len(result["futures"])}／指數{len(result["indices"])}／個股{len(result["stocks"])}）')


if __name__ == '__main__':
    main()
