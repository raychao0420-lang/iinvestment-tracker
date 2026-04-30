import os, time
from flask import Flask, render_template, jsonify
import yfinance as yf
from datetime import datetime
import pytz

app = Flask(__name__)

TZ = pytz.timezone('Asia/Taipei')

# ── 股票清單 ─────────────────────────────────────────────────────────────────

US_INDICES = [
    {'symbol': '^GSPC',  'name': 'S&P 500'},
    {'symbol': '^IXIC',  'name': 'NASDAQ'},
    {'symbol': '^DJI',   'name': '道瓊'},
    {'symbol': '^RUT',   'name': '羅素 2000'},
]
US_STOCKS = [
    {'symbol': 'TSM',  'name': '台積電 ADR'},
    {'symbol': 'NVDA', 'name': '輝達 NVDA'},
    {'symbol': 'MU',   'name': '美光 MU'},
    {'symbol': 'TSLA', 'name': '特斯拉 TSLA'},
    {'symbol': 'EWT',  'name': '摩台指 EWT'},
    {'symbol': 'VOO',  'name': 'VOO'},
    {'symbol': 'QQQ',  'name': 'QQQ'},
    {'symbol': 'SCHD', 'name': 'SCHD'},
]

TW_INDICES = [
    {'symbol': '^TWII',    'name': '加權指數'},
    {'symbol': '^TWO',     'name': '櫃買指數'},
]
TW_STOCKS = [
    {'symbol': '2330.TW',   'name': '台積電'},
    {'symbol': '2454.TW',   'name': '聯發科'},
    {'symbol': '3711.TW',   'name': 'ASE 日月光'},
    {'symbol': '2303.TW',   'name': '聯電'},
    {'symbol': '2382.TW',   'name': '廣達'},
    {'symbol': '2603.TW',   'name': '長榮'},
]
TW_ETF = [
    {'symbol': '0050.TW',    'name': '元大台灣50'},
    {'symbol': '006208.TW',  'name': '富邦台50'},
    {'symbol': '00878.TW',   'name': '國泰永續高股息'},
    {'symbol': '00919.TW',   'name': '群益台灣精選高息'},
    {'symbol': '00929.TW',   'name': '復華台灣科技優息'},
    {'symbol': '00940.TW',   'name': '元大台灣價值高息'},
]

JP_INDICES = [
    {'symbol': '^N225',  'name': '日經 225'},
    {'symbol': '^TOPIX', 'name': 'TOPIX'},
]
JP_STOCKS = [
    {'symbol': '8035.T', 'name': '東京威力科創'},
    {'symbol': '6857.T', 'name': 'Advantest'},
    {'symbol': '4063.T', 'name': '信越化學'},
    {'symbol': '6758.T', 'name': 'Sony'},
    {'symbol': '6861.T', 'name': 'Keyence'},
    {'symbol': '4452.T', 'name': '花王'},
]

# ── 快取 ─────────────────────────────────────────────────────────────────────

_cache = {}
CACHE_TTL = 300  # 5 分鐘

def _fetch(symbols):
    key = ','.join(sorted(symbols))
    now = time.time()
    if key in _cache and now - _cache[key]['ts'] < CACHE_TTL:
        return _cache[key]['data']

    result = {}
    try:
        tickers = yf.download(
            symbols, period='5d', interval='1d',
            progress=False, auto_adjust=True, threads=True
        )
        closes = tickers['Close'] if len(symbols) > 1 else tickers[['Close']]
        closes.columns = [symbols[0]] if len(symbols) == 1 else closes.columns.tolist()

        for sym in symbols:
            try:
                col = closes[sym].dropna()
                if len(col) >= 2:
                    prev, last = float(col.iloc[-2]), float(col.iloc[-1])
                    chg = last - prev
                    pct = chg / prev * 100
                    result[sym] = {
                        'price': round(last, 2),
                        'change': round(chg, 2),
                        'pct': round(pct, 2),
                        'date': col.index[-1].strftime('%m/%d'),
                    }
                elif len(col) == 1:
                    last = float(col.iloc[-1])
                    result[sym] = {
                        'price': round(last, 2),
                        'change': None, 'pct': None,
                        'date': col.index[-1].strftime('%m/%d'),
                    }
                else:
                    result[sym] = None
            except Exception:
                result[sym] = None
    except Exception:
        for sym in symbols:
            result[sym] = None

    _cache[key] = {'ts': now, 'data': result}
    return result


def build_group(items):
    symbols = [x['symbol'] for x in items]
    data = _fetch(symbols)
    out = []
    for item in items:
        d = data.get(item['symbol'])
        out.append({**item, **(d or {'price': None, 'change': None, 'pct': None, 'date': None})})
    return out


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    now_tw = datetime.now(TZ)
    return render_template('index.html', now=now_tw.strftime('%Y/%m/%d %H:%M'))


@app.route('/api/us')
def api_us():
    return jsonify({
        'indices': build_group(US_INDICES),
        'stocks':  build_group(US_STOCKS),
    })

@app.route('/api/tw')
def api_tw():
    return jsonify({
        'indices': build_group(TW_INDICES),
        'stocks':  build_group(TW_STOCKS),
        'etf':     build_group(TW_ETF),
    })

@app.route('/api/jp')
def api_jp():
    return jsonify({
        'indices': build_group(JP_INDICES),
        'stocks':  build_group(JP_STOCKS),
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=False)
