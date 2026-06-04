#!/usr/bin/env python3
"""
Taiwan futures live update.
- Day session   09:00-13:35 TWN: TAIFEX MIS API, MarketType 0, regular contracts (no -M)
- Night session 15:00-05:00 TWN: TAIFEX MIS API, MarketType 1, night contracts (with -M)
- Post-close    05:00-08:00 TWN: TAIFEX OpenAPI fallback (MIS drops -M contracts after close)
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
    """TXFE6-M → '202605'  or  TXFE6 → '202605'"""
    try:
        base = symbol_id.split('-')[0]  # e.g. TXFE6
        month_char = base[-2]
        year_digit = base[-1]
        month = MONTH_MAP.get(month_char.upper(), '??')
        return f'202{year_digit}{month}'
    except Exception:
        return ''


def fetch_quotes(is_night: bool):
    market_type = '1' if is_night else '0'
    session_label = '夜盤' if is_night else '日盤'
    result = {}
    for p in PRODUCTS:
        try:
            r = requests.post(
                'https://mis.taifex.com.tw/futures/api/getQuoteList',
                json={
                    'MarketType': market_type,
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

            if is_night:
                # Night session: contracts marked with -M, no spreads
                contracts = [
                    q for q in quotes
                    if '-M' in q.get('SymbolID', '')
                    and '/' not in q.get('SymbolID', '')
                    and q.get('CLastPrice')
                ]
            else:
                # Day session: regular contracts (no -M), no spreads
                contracts = [
                    q for q in quotes
                    if '-M' not in q.get('SymbolID', '')
                    and '/' not in q.get('SymbolID', '')
                    and q.get('CLastPrice')
                ]

            if not contracts:
                print(f'  {p["symbol"]}: 無{session_label}資料')
                continue

            # Front month = highest volume
            contracts.sort(key=lambda x: int(x.get('CTotalVolume') or 0), reverse=True)
            front = contracts[0]

            price  = float(front['CLastPrice'])
            ref    = float(front['CRefPrice']) if front.get('CRefPrice') else None
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


def fetch_quotes_openapi():
    """Post-close fallback using TAIFEX OpenAPI.
    Change is calculated vs same-day regular-session settlement (not vs previous night).
    Returns dict keyed by symbol: {price, change, pct, contract, date}.
    """
    PRODUCTS_MAP = {'TX': '台指期', 'MTX': '小台', 'TMF': '微台', 'TE': '電子期'}

    def to_f(v):
        try:
            return float(str(v).replace(',', ''))
        except Exception:
            return None

    try:
        r = requests.get(
            'https://openapi.taifex.com.tw/v1/DailyMarketReportFut',
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f'  OpenAPI fetch error: {e}')
        return {}

    night, day = {}, {}
    for row in data:
        session = row.get('TradingSession', '')
        cid = row.get('Contract', '')
        if session == '盤後':
            night.setdefault(cid, []).append(row)
        elif session == '一般':
            day.setdefault(cid, []).append(row)

    for cid in night:
        night[cid].sort(key=lambda x: x.get('ContractMonth(Week)', ''))
    for cid in day:
        day[cid].sort(key=lambda x: x.get('ContractMonth(Week)', ''))

    day_settlement = {cid: to_f(rows[0].get('SettlementPrice'))
                      for cid, rows in day.items()
                      if to_f(rows[0].get('SettlementPrice')) is not None}

    result = {}
    for sym in PRODUCTS_MAP:
        rows = night.get(sym)
        if not rows:
            print(f'  {sym}: 無OpenAPI夜盤資料')
            continue
        row = rows[0]
        price = to_f(row.get('Last')) or to_f(row.get('SettlementPrice'))
        if price is None:
            continue

        ref = day_settlement.get(sym)
        if ref:
            change = round(price - ref, 0)
            pct = round(change / ref * 100, 2)
        else:
            change = to_f(row.get('Change'))
            pct = round(change / (price - change) * 100, 2) if change and price != change else None

        date_raw = row.get('Date', '')
        date_s = f'{date_raw[4:6]}/{date_raw[6:8]}' if len(date_raw) == 8 else ''

        result[sym] = {
            'price':    price,
            'change':   change,
            'pct':      pct,
            'contract': row.get('ContractMonth(Week)', ''),
            'date':     date_s,
        }
        print(f'  {sym}(OpenAPI): {price:.0f} ({change:+.0f} / {pct:+.2f}%)')

    return result


if __name__ == '__main__':
    now = datetime.now(TZ)
    h, m = now.hour, now.minute
    total_min = h * 60 + m

    is_day        = 9 * 60 <= total_min <= 13 * 60 + 35  # 09:00-13:35
    is_night_live = h >= 15 or h < 5                      # 15:00-04:59 夜盤進行中
    is_post_close = 5 <= h < 8                            # 05:00-07:59 收盤後，MIS 已無 -M 合約

    if not (is_day or is_night_live or is_post_close):
        print(f'非交易時段 ({now.strftime("%H:%M")} TWN)，跳過。')
        exit(0)

    now_str  = now.strftime('%Y/%m/%d %H:%M')
    now_date = now.strftime('%m/%d')

    FUTURES_PATH = 'data/tw_futures.json'
    if not os.path.exists(FUTURES_PATH):
        print(f'{FUTURES_PATH} 不存在，請先跑主 workflow。')
        exit(1)

    with open(FUTURES_PATH, 'r', encoding='utf-8') as f:
        existing = json.load(f)

    if is_post_close:
        print(f'=== fetch_futures_live [夜盤收盤後/OpenAPI] {now_str} ===')
        live = fetch_quotes_openapi()
        if live:
            cached_date = (existing.get('futures') or [{}])[0].get('date', '')
            api_date = next((d['date'] for d in live.values() if d.get('date')), '')
            if cached_date and api_date and api_date < cached_date:
                print(f'  post-close: OpenAPI date {api_date} < cached {cached_date}，保留現有資料。')
                live = {}
    elif is_day:
        print(f'=== fetch_futures_live [日盤] {now_str} ===')
        live = fetch_quotes(is_night=False)
    else:
        print(f'=== fetch_futures_live [夜盤] {now_str} ===')
        live = fetch_quotes(is_night=True)

    if not live:
        print('未取得任何報價，保留現有資料。')
        exit(0)

    updated = 0
    for item in existing.get('futures', []):
        sym = item.get('symbol')
        if sym in live:
            d = live[sym]
            item['price']  = d['price']
            item['change'] = d['change']
            item['pct']    = d['pct']
            item['date']   = d.get('date') or now_date
            if d.get('contract'):
                item['contract'] = d['contract']
            updated += 1

    if updated == 0:
        print('無任何欄位被更新。')
        exit(0)

    existing['updated'] = now_str
    with open(FUTURES_PATH, 'w', encoding='utf-8') as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    print(f'已儲存 {FUTURES_PATH}（{updated} 筆更新，{now_str}）')
