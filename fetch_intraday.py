"""Lightweight intraday updater — only fetches tw.json.
Run by intraday.yml every 5 min during TW market hours.
Does NOT touch US/JP/KR, charts, margin, NAV, or fundamentals.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fetch_data import (
    TZ, TW_INDICES, TW_STOCKS, TW_DRONE, TW_ETF,
    fetch_market, merge_with_old, load_existing, save,
)
from datetime import datetime

now = datetime.now(TZ).strftime('%Y/%m/%d %H:%M')
print(f'=== intraday {now} ===')

old_tw = load_existing('data/tw.json')
tw = fetch_market({'indices': TW_INDICES, 'stocks': TW_STOCKS, 'drone': TW_DRONE, 'etf': TW_ETF})
tw = merge_with_old(tw, old_tw)

# Carry forward nav/premium from cache (ETF NAV is not available intraday)
for item in tw.get('etf', []):
    old = old_tw.get(item['symbol'], {})
    for field in ('nav', 'premium'):
        if item.get(field) is None and old.get(field) is not None:
            item[field] = old[field]

save('data/tw.json', {'updated': now, **tw})
print('Done.')
