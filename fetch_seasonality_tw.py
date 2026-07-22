# -*- coding: utf-8 -*-
"""台股月度季節性，作法比照 fetch_seasonality.py（美股），共用其統計函式。
資料來源：
  - 加權指數 / 台灣50 → yfinance 月線（同美股）
  - 櫃買指數 → FinMind『櫃買報酬指數』（TPEx，含息；官方公開報表來源，不擋境外 IP，
    yfinance 的 ^TWOII 抓不到）。報酬指數對季節性反而更乾淨——不會有除權息蒸發點數失真。
產出 data/seasonality_tw.json；事件註記在 data/seasonality_tw_events.json（人工維護，不覆蓋）。
抓不到資料的指數會自動略過。"""
import json, os, time
from datetime import datetime
import requests
from fetch_seasonality import month_returns, summarize, decade_buckets, TZ, START_YEAR

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'data', 'seasonality_tw.json')

INDICES = [
    {'key': 'twii',  'symbol': '^TWII',   'name': '加權指數',       'src': 'yf'},
    {'key': 'twoii', 'symbol': 'TPEx',    'name': '櫃買指數(報酬)', 'src': 'finmind'},
    {'key': 'tw50',  'symbol': '0050.TW', 'name': '台灣50',        'src': 'yf'},
]


def finmind_month_returns(data_id):
    """FinMind 報酬指數日線 → 各月最後交易日收盤 → 月報酬 {year: {month: pct}}。
    網路錯誤重試 3 次後才放行例外（寧可讓 job 失敗通知，也不假成功）。"""
    url = 'https://api.finmindtrade.com/api/v4/data'
    params = {'dataset': 'TaiwanStockTotalReturnIndex', 'data_id': data_id,
              'start_date': f'{START_YEAR}-01-01'}
    tok = os.environ.get('FINMIND_TOKEN')
    if tok:
        params['token'] = tok
    rows = None
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            rows = r.json().get('data', [])
            break
        except Exception as e:
            if attempt == 2:
                raise
            print(f'  FinMind 第 {attempt+1} 次失敗（{e}），重試…')
            time.sleep(5)
    if not rows:
        return {}
    # 依日期排序，每個 (年,月) 最後寫入者＝該月最後交易日收盤
    month_close = {}
    for row in sorted(rows, key=lambda x: x['date']):
        y, m = int(row['date'][:4]), int(row['date'][5:7])
        month_close[(y, m)] = row['price']
    out, prev = {}, None
    for (y, m) in sorted(month_close):
        price = month_close[(y, m)]
        if prev is not None and prev > 0:
            pct = round((price / prev - 1) * 100, 2)
            if abs(pct) <= 50:   # 同 month_returns：濾掉分割/資料錯誤的假暴衝
                out.setdefault(y, {})[m] = pct
        prev = price
    return out


def main():
    result = {'updated': datetime.now(TZ).strftime('%Y-%m-%d %H:%M'), 'indices': {}}
    for idx in INDICES:
        print(f'抓取 {idx["name"]} ({idx["symbol"]}) ...')
        matrix = finmind_month_returns(idx['symbol']) if idx['src'] == 'finmind' else month_returns(idx['symbol'])
        if not matrix:
            print(f'  ⚠ {idx["symbol"]} 無資料，略過')
            continue
        result['indices'][idx['key']] = {
            'name': idx['name'],
            'symbol': idx['symbol'],
            'matrix': {str(y): matrix[y] for y in sorted(matrix)},
            'monthly': summarize(matrix),
            'decades': decade_buckets(matrix),
        }
        yrs = sorted(matrix)
        print(f'  {idx["name"]}: {yrs[0]}~{yrs[-1]}，{len(matrix)} 年')
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))
    print(f'已寫入 {OUT}')


if __name__ == '__main__':
    main()
