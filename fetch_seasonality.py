# -*- coding: utf-8 -*-
"""抓取美股主要指數近 40 年月度漲跌，算出季節性統計矩陣。
產出 data/seasonality.json：月度報酬矩陣、各月平均/勝率、逐十年鈍化對照。
事件註記另存於 data/seasonality_events.json（人工維護，不覆蓋）。
歷史資料變動極少，建議每月或手動執行一次即可。"""
import yfinance as yf
import json, os
from datetime import datetime
import pytz

TZ = pytz.timezone('Asia/Taipei')
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'data', 'seasonality.json')

INDICES = [
    {'key': 'sp500',  'symbol': '^GSPC', 'name': 'S&P 500'},
    {'key': 'nasdaq', 'symbol': '^IXIC', 'name': 'NASDAQ'},
    {'key': 'dow',    'symbol': '^DJI',  'name': '道瓊'},
]
START_YEAR = datetime.now().year - 40  # 近 40 年


def month_returns(symbol):
    """回傳 {year: {month: pct}}，月報酬 = 當月收盤相對前月收盤漲跌%。"""
    df = yf.Ticker(symbol).history(start=f'{START_YEAR}-01-01', interval='1mo', auto_adjust=True)
    if df.empty:
        return {}
    closes = df['Close'].dropna()
    out = {}
    prev = None
    for ts, price in closes.items():
        if prev is not None and prev > 0:
            pct = round((price / prev - 1) * 100, 2)
            out.setdefault(ts.year, {})[ts.month] = pct
        prev = price
    return out


def summarize(matrix):
    """各月平均報酬、勝率(正報酬年數比例)、中位數。"""
    stats = {}
    for m in range(1, 13):
        vals = [matrix[y][m] for y in matrix if m in matrix[y]]
        if not vals:
            continue
        wins = sum(1 for v in vals if v > 0)
        stats[m] = {
            'avg': round(sum(vals) / len(vals), 2),
            'win': round(wins / len(vals) * 100, 1),
            'best': round(max(vals), 2),
            'worst': round(min(vals), 2),
            'n': len(vals),
        }
    return stats


def decade_buckets(matrix):
    """逐十年各月平均，用來觀察季節效應是否鈍化。"""
    years = sorted(matrix.keys())
    if not years:
        return []
    lo, hi = years[0], years[-1]
    buckets = []
    start = lo
    while start <= hi:
        end = start + 9
        # 尾端不足 5 年的殘桶併入本桶，避免出現單年分組
        if hi - end < 5:
            end = hi
        yrs = [y for y in years if start <= y <= end]
        if yrs:
            sub = {y: matrix[y] for y in yrs}
            buckets.append({'label': f'{start}-{end}', 'stats': summarize(sub)})
        start = end + 1
    return buckets


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
        yrs = sorted(matrix.keys())
        print(f'  {idx["name"]}: {yrs[0]}~{yrs[-1]}，{len(matrix)} 年')
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))
    print(f'已寫入 {OUT}')


if __name__ == '__main__':
    main()
