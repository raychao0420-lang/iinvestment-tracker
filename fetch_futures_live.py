#!/usr/bin/env python3
"""
Taiwan futures night session live update via TAIFEX MIS API.
Runs every 5 minutes during night session (15:00-05:00 TWN, Mon-Fri).
Updates data/tw_futures.json only.
"""
import requests, json, os
from datetime import datetime
import pytz

TZ = pytz.timezone('Asia/Taipei')

MONTH_MAP = {
    'A': '01', 'B': '02', 'C': '03', 'D': '04',
    'E': '05', 'F': '06', 'G': '07', 'H': '08',
    'I': '09', 'J': '10', 'K': '11', 'L': '12',
}

PRODUCTS = [
    {'symbol': 'TX',  'name': '台指期'},
    {'symbol': 'MTX', 'name': '小台'},
    {'symbol': 'TMF', 'name': '微台'},
    {'symbol': 'TE',  'name': '電子期'},
]


def parse_contract(symbol_id):
    """TXFE6-M → '202605'"""
    try:
        base = symbol_id.split('-')[0]  # e.g. TXFE6
        month_char = base[-2]
        year_digit = base[-1]
        month = MONTH_MAP.get(month_char.upper(), '??')
        return f'202{year_digit}{month}'
    except Exception:
        return ''


def fetch_night_quotes():
    result = {}
    for p in PRODUCTS:
        try:
            r = requests.post(
                'https://mis.taifex.com.tw/futures/api/getQuoteList',
                json={
                    'MarketType': '1',
                    'CommodityID': p['symbol'],
                    'ContractDate': '',
                    'RowCount': 10,
                    'PageNo': 1,
                    'SortColumn': '',
                    'AscDesc': 'A',
                },
                headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Referer': 'https://mis.taifex.com.tw/futures/zh',
                },
                timeout=10,
            )
            quotes = r.json().get('RtData', {}).get('QuoteList', [])
            # Keep only outright night-session contracts (has -M, no spread '/')
            contracts = [
                q for q in quotes
                if '-M' in q.get('SymbolID', '')
                and '/' not in q.get('SymbolID', '')
                and q.get('CLastPrice')
            ]
            if not contracts:
                print(f'  {p["symbol"]}: 無夜盤資料')
                continue
            # Front month = highest volume
            contracts.sort(key=lambda x: int(x.get('CTotalVolume') or 0), reverse=True)
            front = contracts[0]

            price = float(front['CLastPrice'])
            ref   = float(front['CRefPrice']) if front.get('CRefPrice') else None
            change = round(price - ref, 0) if ref else None
            pct    = round(change / ref * 100, 2) if ref and ref > 0 else None

            result[p['symbol']] = {
                'price':    price,
                'change':   change,
                'pct':      pct,
                'contract': parse_contract(front.get('SymbolID', '')),
            }
            print(f'  {p["symbol"]}: {price:.0f} ({change:+.0f} / {pct:+.2f}%)')
        except Exception as e:
            print(f'  {p["symbol"]}: {e}')
    return result


if __name__ == '__main__':
    now = datetime.now(TZ)
    h   = now.hour

    if not (h >= 15 or h < 5):
        print(f'非夜盤 ({now.strftime("%H:%M")} TWN)，跳過。')
        exit(0)

    now_str  = now.strftime('%Y/%m/%d %H:%M')
    now_date = now.strftime('%m/%d')
    print(f'=== fetch_futures_live {now_str} ===')

    FUTURES_PATH = 'data/tw_futures.json'
    if not os.path.exists(FUTURES_PATH):
        print(f'{FUTURES_PATH} 不存在，請先跑主 workflow。')
        exit(1)

    with open(FUTURES_PATH, 'r', encoding='utf-8') as f:
        existing = json.load(f)

    live = fetch_night_quotes()
    if not live:
        print('未取得任何夜盤報價，保留現有資料。')
        exit(0)

    updated = 0
    for item in existing.get('futures', []):
        sym = item.get('symbol')
        if sym in live:
            d = live[sym]
            item['price']  = d['price']
            item['change'] = d['change']
            item['pct']    = d['pct']
            item['date']   = now_date
            if d['contract']:
                item['contract'] = d['contract']
            updated += 1

    if updated == 0:
        print('無任何欄位被更新。')
        exit(0)

    existing['updated'] = now_str
    with open(FUTURES_PATH, 'w', encoding='utf-8') as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    print(f'已儲存 {FUTURES_PATH}（{updated} 筆更新，{now_str}）')
