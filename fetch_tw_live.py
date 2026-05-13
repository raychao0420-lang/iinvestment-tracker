#!/usr/bin/env python3
"""
Taiwan live quote update via yfinance (works from GitHub Actions overseas IPs).
Runs every 5 min during market hours (09:00-13:30 TWN).
Only updates data/tw.json; skips analysis/chart/futures.
"""
import yfinance as yf
import json, os, time
from datetime import datetime
import pytz

TZ = pytz.timezone('Asia/Taipei')

# All yfinance symbols tracked in tw.json (same order doesn't matter)
TW_SYMBOLS = [
    '^TWII',     '00631L.TW',
    '2330.TW',   '2454.TW',  '3711.TW',  '2303.TW',  '3008.TW',
    '2308.TW',   '2382.TW',  '2603.TW',  '2881.TW',  '2882.TW',
    '2891.TW',   '1802.TW',  '6770.TW',  '8033.TW',  '2634.TW',
    '4961.TW',
    '0050.TW',   '006208.TW', '00878.TW', '00919.TW', '00929.TW', '00940.TW',
]


def get_live_prices(symbols, now_date):
    """Fetch current prices via yfinance fast_info. Returns dict: symbol -> {price,change,pct,date}."""
    result = {}
    for sym in symbols:
        for attempt in range(3):
            try:
                fi = yf.Ticker(sym).fast_info
                price = fi.last_price
                prev  = fi.previous_close
                if price and prev and prev > 0:
                    chg = round(price - prev, 2)
                    pct = round(chg / prev * 100, 2)
                    result[sym] = {'price': round(price, 2), 'change': chg,
                                   'pct': pct, 'date': now_date}
                    print(f'  {sym}: {price:.2f} ({pct:+.2f}%)')
                else:
                    print(f'  {sym}: 尚無成交 (price={price}, prev={prev})')
                break
            except Exception as e:
                print(f'  {sym} attempt {attempt+1}: {e}')
                if attempt < 2:
                    time.sleep(2)
    return result


def update_group(items, live):
    n = 0
    for item in items:
        sym = item.get('symbol', '')
        if sym in live:
            item.update(live[sym])
            n += 1
    return n


if __name__ == '__main__':
    now = datetime.now(TZ)
    total_min = now.hour * 60 + now.minute

    # 非盤中時間直接退出（workflow 仍會 trigger，Python 守門）
    if not (9 * 60 <= total_min <= 13 * 60 + 30):
        print(f'非盤中 ({now.strftime("%H:%M")} TWN)，跳過。')
        exit(0)

    now_str  = now.strftime('%Y/%m/%d %H:%M')
    now_date = now.strftime('%m/%d')
    print(f'=== fetch_tw_live {now_str} ===')

    TW_PATH = 'data/tw.json'
    if not os.path.exists(TW_PATH):
        print(f'{TW_PATH} 不存在，請先跑一次主 workflow。')
        exit(1)

    with open(TW_PATH, 'r', encoding='utf-8') as f:
        tw = json.load(f)

    live = get_live_prices(TW_SYMBOLS, now_date)

    if not live:
        print('沒有取到任何即時報價，保留現有資料。')
        exit(0)

    total = 0
    for group in ('indices', 'stocks', 'drone', 'etf'):
        if isinstance(tw.get(group), list):
            n = update_group(tw[group], live)
            total += n
            print(f'  {group}: {n} 筆更新')

    if total == 0:
        print('沒有任何欄位被更新。')
        exit(0)

    # Recalculate ETF premium using updated price and stored nav
    for item in tw.get('etf', []):
        nav   = item.get('nav')
        price = item.get('price')
        if nav and price and nav > 0:
            item['premium'] = round((price - nav) / nav * 100, 2)

    tw['updated'] = now_str
    with open(TW_PATH, 'w', encoding='utf-8') as f:
        json.dump(tw, f, ensure_ascii=False, indent=2)
    print(f'已儲存 {TW_PATH}（{total} 筆更新，{now_str}）')
