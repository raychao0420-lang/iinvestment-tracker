# -*- coding: utf-8 -*-
"""台股月度季節性，作法完全比照 fetch_seasonality.py（美股），共用其統計函式。
產出 data/seasonality_tw.json；事件註記在 data/seasonality_tw_events.json（人工維護，不覆蓋）。
抓不到資料的指數會自動略過。歷史資料變動極少，建議每月或手動執行一次。"""
import json, os
from datetime import datetime
from fetch_seasonality import month_returns, summarize, decade_buckets, TZ

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'data', 'seasonality_tw.json')

INDICES = [
    {'key': 'twii',  'symbol': '^TWII',   'name': '加權指數'},
    {'key': 'twoii', 'symbol': '^TWOII',  'name': '櫃買指數'},
    {'key': 'tw50',  'symbol': '0050.TW', 'name': '台灣50'},
]


def main():
    result = {'updated': datetime.now(TZ).strftime('%Y-%m-%d %H:%M'), 'indices': {}}
    for idx in INDICES:
        print(f'抓取 {idx["name"]} ({idx["symbol"]}) ...')
        matrix = month_returns(idx['symbol'])
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
