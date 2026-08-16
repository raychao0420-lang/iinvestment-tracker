# -*- coding: utf-8 -*-
"""財經新聞彙整（國內外＋產業媒體）→ data/news.json

每天四班（台灣時間 07:30／12:30／14:30／21:30），各配半點備援班次；
班次由腳本自己 gate，同一班重複跑會直接跳過（GitHub 排程常漏跑，多排幾班拉高命中率）。

設計要點：
- 只存標題／連結／來源／時間，**不存全文、不存圖片**，一律外連原站（著作權）。
- 全部走公開 RSS / JSON，**零金鑰**，符合本 repo「前端不打包 token」原則。
- 單一來源掛掉不影響整批（各自 try/except），抓到 0 則就跳過並保留既有資料。
- 去重：標題正規化後比對，重複的合併並累計 hot（幾家同報）＝重要性訊號。
  實測中央社供稿給經濟日報，兩家頭條常常是同一則，不去重版面會很難看。
- 標的連動：比對 us/tw/jp/kr.json 的追蹤清單，標記 sym，前端做「我追蹤的」分類。
"""
import argparse
import hashlib
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import pytz
import requests

if hasattr(sys.stdout, 'reconfigure'):          # Windows 主控台是 cp950，中文標題會炸
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

TZ_TW = pytz.timezone('Asia/Taipei')
HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data')
OUT = os.path.join(DATA, 'news.json')

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/124.0 Safari/537.36'}
NS = {'rss10': 'http://purl.org/rss/1.0/',
      'atom': 'http://www.w3.org/2005/Atom',
      'dc': 'http://purl.org/dc/elements/1.1/'}
TIMEOUT = 20
SLOTS = ('07:30', '12:30', '14:30', '21:30')
KEEP_DAYS = 3            # news.json 只留三天（stock_charts.json 已經 12MB，別再養一隻）
MAX_ITEMS = 600
MAX_PER_SOURCE = 40      # 單一來源上限，避免一家洗版（ChosunBiz 一次就能吐 100 則）

# (顯示名, 網址, kind=xml|cnyes, 分類 tw/us/jp/kr/ind, lang=zh/en/ko, 綜合?)
# ⚠️ 最後那個布林＝「綜合新聞來源」。日韓找不到能用的財經專屬 feed
#    （Nikkei 分類 feed 全 404、Japan Times 財經版 403、Korea Herald 分類 feed 空），
#    只好收綜合 feed 再過財經相關性檢查，否則會混進「印度人重新考慮留學」「日本貓咖啡」。
SOURCES = [
    ('中央社 財經',    'https://feeds.feedburner.com/rsscna/finance', 'xml', 'tw', 'zh', False),
    ('經濟日報',      'https://money.udn.com/rssfeed/news/1001/5591?ch=money', 'xml', 'tw', 'zh', False),
    ('自由財經',      'https://news.ltn.com.tw/rss/business.xml', 'xml', 'tw', 'zh', False),
    ('鉅亨網',       'https://api.cnyes.com/media/api/v1/newslist/category/headline?limit=30',
     'cnyes', 'tw', 'zh', False),
    ('CNBC',        'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147',
     'xml', 'us', 'en', False),
    ('MarketWatch', 'https://feeds.content.dowjones.io/public/rss/mw_topstories', 'xml', 'us', 'en', False),
    ('聯準會',       'https://www.federalreserve.gov/feeds/press_all.xml', 'xml', 'us', 'en', False),
    ('Nikkei Asia', 'https://asia.nikkei.com/rss/feed/nar', 'xml', 'jp', 'en', True),
    ('Japan Times', 'https://www.japantimes.co.jp/feed/', 'xml', 'jp', 'en', True),
    ('Korea Herald', 'https://www.koreaherald.com/rss/newsAll', 'xml', 'kr', 'en', True),
    ('KED Global',  'https://www.kedglobal.com/rss/news.xml', 'xml', 'kr', 'en', True),
    ('ChosunBiz 證券',
     'https://biz.chosun.com/arc/outboundfeeds/rss/category/stock/?outputType=xml', 'xml', 'kr', 'ko', False),
    ('科技新報',      'https://technews.tw/feed/', 'xml', 'ind', 'zh', False),
    ('DigiTimes',   'https://www.digitimes.com/rss/daily.xml', 'xml', 'ind', 'en', False),
    ("Tom's Hardware", 'https://www.tomshardware.com/feeds/all', 'xml', 'ind', 'en', False),
    ('EE Times',    'https://www.eetimes.com/feed/', 'xml', 'ind', 'en', False),
]

# 綜合來源的財經相關性關鍵字（中／英／韓）。寧可漏收也不要讓貓咖啡進來。
FINANCE_KW = [
    '股', '匯', '債', '央行', '升息', '降息', '財報', '獲利', '營收', '晶片', '半導體', '記憶體',
    '經濟', '通膨', '關稅', '併購', '投資', '市場', '基金', '房市', '油價', '黃金', '景氣',
    'stock', 'share', 'market', 'index', 'nikkei', 'kospi', 'fed', 'inflation', 'tariff',
    'chip', 'semiconductor', 'earnings', 'revenue', 'profit', 'ipo', 'bank', 'economy',
    'economic', 'trade', 'export', 'investment', 'currency', 'yen', 'won', 'bond', 'merger',
    'samsung', 'hynix', 'toyota', 'softbank', 'sony',
    '주가', '증시', '코스피', '반도체', '삼성', '경제', '실적', '투자',
]

# Google News RSS：全球可用，台灣媒體萬一被境外 IP 擋掉時的保底，也負責日韓的中文報導
GNEWS_URL = ('https://news.google.com/rss/search?q={q}+when:1d'
             '&hl=zh-TW&gl=TW&ceid=TW:zh-Hant')
GNEWS_TOPICS = [
    ('台股 OR 台積電', 'tw'), ('美股 OR 那斯達克', 'us'),
    ('日股 OR 日經', 'jp'), ('韓股 OR 三星電子', 'kr'),
    ('半導體 OR 記憶體', 'ind'), ('聯準會 OR 關稅', 'us'),
]


# ── 解析 ────────────────────────────────────────────────────────────────
def parse_any(content):
    """RSS 2.0 / RSS 1.0(RDF) / Atom 三種通吃。

    ⚠️ Nikkei Asia 是 RSS 1.0，item 在 purl.org/rss/1.0 命名空間下，
    用一般 './/item' 會抓到 0 筆（探路時踩過）。
    """
    root = ET.fromstring(content)
    for path, kind in (('.//item', 'rss2'), ('.//rss10:item', 'rss1'), ('.//atom:entry', 'atom')):
        items = root.findall(path, NS)
        if items:
            return items, kind
    return [], None


def _text(it, kind, names):
    pre = {'rss2': '', 'rss1': 'rss10:', 'atom': 'atom:'}[kind]
    for n in names:
        v = it.findtext(pre + n, None, NS) or it.findtext('dc:' + n, None, NS)
        if v and v.strip():
            return v.strip()
    return ''


def _link(it, kind):
    if kind == 'atom':
        el = it.find('atom:link', NS)
        if el is not None and el.get('href'):
            return el.get('href')
    return _text(it, kind, ['link', 'guid'])


def to_iso(raw, fallback):
    """RFC822 / ISO 都吃。回傳 (iso, 是否為真實發布時間)。

    ⚠️ Nikkei、KED 沒有日期欄位 → 退回抓取時間，並標記 nd=1。
    這種項目的時間語意是「首次收錄」而非「發布」，前端要顯示成「剛收錄」而不是假的精確時間。
    去重時保留最早的 p，所以同一則在後續班次不會一直被推回最上面。
    """
    if raw:
        for fn in (lambda s: parsedate_to_datetime(s),
                   lambda s: datetime.fromisoformat(s.replace('Z', '+00:00'))):
            try:
                return fn(raw).astimezone(timezone.utc).isoformat(timespec='seconds'), True
            except Exception:
                pass
    return fallback, False


# ── 各來源抓取 ──────────────────────────────────────────────────────────
def is_finance(title):
    low = title.lower()          # lower() 不影響中日韓字元，中英韓關鍵字可以一起比
    return any(k in low for k in FINANCE_KW)


def fetch_xml(url, source, cat, lang, now_iso):
    r = requests.get(url, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    items, kind = parse_any(r.content)
    out = []
    for it in items:
        title, link = _text(it, kind, ['title']), _link(it, kind)
        if not title or not link:
            continue
        p, dated = to_iso(_text(it, kind, ['pubDate', 'date', 'updated', 'published']), now_iso)
        row = {'t': title, 'u': link, 's': source, 'c': cat, 'lang': lang, 'p': p}
        if not dated:
            row['nd'] = 1
        out.append(row)
    return out


def fetch_cnyes(url, source, cat, lang, now_iso):
    r = requests.get(url, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    out = []
    for it in (r.json().get('items') or {}).get('data') or []:
        title, nid = it.get('title'), it.get('newsId')
        if not title or not nid:
            continue
        ts = it.get('publishAt')
        p = (datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec='seconds')
             if isinstance(ts, (int, float)) else now_iso)
        out.append({'t': title, 'u': 'https://news.cnyes.com/news/id/%s' % nid,
                    's': source, 'c': cat, 'lang': lang, 'p': p})
    return out


def fetch_gnews(query, cat, now_iso):
    """Google News：標題結尾的『 - 來源』要剝掉，來源名改讀 <source> 元素。"""
    r = requests.get(GNEWS_URL.format(q=requests.utils.quote(query)), headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    items, kind = parse_any(r.content)
    out = []
    for it in items:
        title, link = _text(it, kind, ['title']), _link(it, kind)
        if not title or not link:
            continue
        src_el = it.find('source')
        source = (src_el.text or '').strip() if src_el is not None and src_el.text else 'Google 新聞'
        if source and title.endswith(' - ' + source):
            title = title[:-(len(source) + 3)].strip()
        p, _dated = to_iso(_text(it, kind, ['pubDate']), now_iso)
        out.append({'t': title, 'u': link, 's': source, 'c': cat, 'lang': 'zh', 'p': p})
    return out


# ── 去重與標的連動 ──────────────────────────────────────────────────────
_PUNCT = re.compile(r'[\s，,。.、！!？?：:；;「」『』（）()\[\]【】…—\-～~"\'|｜]+')


def norm_title(t):
    """正規化：全形轉半形、去標點空白、小寫。去重與 id 都靠它。"""
    t = ''.join(chr(ord(c) - 0xFEE0) if 0xFF01 <= ord(c) <= 0xFF5E else c for c in t)
    return _PUNCT.sub('', t).lower()


def make_id(t):
    return hashlib.sha1(norm_title(t).encode('utf-8')).hexdigest()[:10]


def load_aliases():
    """從追蹤清單建「別名 → 代號」表，用來把新聞標題標上 sym。"""
    aliases = {}
    for mk in ('tw', 'us', 'jp', 'kr'):
        path = os.path.join(DATA, '%s.json' % mk)
        try:
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue
        for cat, arr in data.items():
            if not isinstance(arr, list):
                continue
            for s in arr:
                sym, name = s.get('symbol'), s.get('name')
                if not sym or not name:
                    continue
                code = sym.split('.')[0].lstrip('^')
                disp = code
                for a in (name, code):
                    a = re.sub(r'\s*(ADR|Group|Inc\.?|Corp\.?)$', '', a).strip()
                    # 太短的英文代號（如 MU）容易誤判，只收 3 字以上；中文名 2 字以上即可
                    if len(a) >= (2 if re.search(r'[一-鿿]', a) else 3):
                        aliases.setdefault(a, disp)
    return aliases


def tag_symbols(title, aliases):
    """中文名直接子字串比對；英文須前後為非英數，避免 INTC 命中 INTCX 之類。"""
    hits = []
    low = title.lower()
    for alias, sym in aliases.items():
        if re.search(r'[一-鿿]', alias):
            ok = alias in title
        else:
            ok = re.search(r'(?<![a-z0-9])%s(?![a-z0-9])' % re.escape(alias.lower()), low) is not None
        if ok and sym not in hits:
            hits.append(sym)
    return hits


def merge(items, aliases):
    """去重＋累計 hot＋標記 sym。同一則以最早發布時間為準。"""
    bucket = {}
    for it in items:
        key = norm_title(it['t'])
        if not key:
            continue
        cur = bucket.get(key)
        if cur is None:
            it['id'] = make_id(it['t'])
            it['hot'] = 1
            it['sym'] = tag_symbols(it['t'], aliases)
            bucket[key] = it
        else:
            cur['hot'] += 1
            if it['p'] < cur['p']:          # 保留最早發布時間
                cur['p'] = it['p']
            if cur['lang'] != 'zh' and it['lang'] == 'zh':   # 有中文版本就用中文的
                cur.update({'t': it['t'], 'u': it['u'], 's': it['s'], 'lang': 'zh'})
    return list(bucket.values())


# ── 班次判定 ────────────────────────────────────────────────────────────
def current_slot(now_tw):
    """回傳此刻所屬班次（班次起算後 60 分鐘內都算），不在任何班次回 None。"""
    mins = now_tw.hour * 60 + now_tw.minute
    for s in SLOTS:
        h, m = map(int, s.split(':'))
        if h * 60 + m <= mins < h * 60 + m + 60:
            return s
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--force', action='store_true', help='忽略班次判定，直接抓（手動觸發用）')
    args = ap.parse_args()

    now_tw = datetime.now(TZ_TW)
    today = now_tw.strftime('%Y-%m-%d')
    now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')

    try:
        with open(OUT, encoding='utf-8') as f:
            old = json.load(f)
    except Exception:
        old = {'items': [], 'runs': [], 'date': today}

    if old.get('date') != today:            # 跨日，班次紀錄歸零
        old['runs'], old['date'] = [], today

    slot = current_slot(now_tw)
    if not args.force:
        if not slot:
            print('非更新班次（台灣時間 %s），跳過。' % now_tw.strftime('%H:%M'))
            return 0
        if slot in old.get('runs', []):
            print('本班 %s 已跑過，跳過（備援班次不重複抓）。' % slot)
            return 0

    print('=== fetch_news TW %s slot=%s ===' % (now_tw.strftime('%m/%d %H:%M'), slot or 'force'))
    fresh, failed = [], []
    for name, url, kind, cat, lang, general in SOURCES:
        try:
            got = fetch_cnyes(url, name, cat, lang, now_iso) if kind == 'cnyes' \
                else fetch_xml(url, name, cat, lang, now_iso)
            raw = len(got)
            if general:                       # 綜合來源：只留財經相關
                got = [g for g in got if is_finance(g['t'])]
            got = got[:MAX_PER_SOURCE]        # 單一來源上限，避免洗版
            note = '' if len(got) == raw else '（原 %d，篩後留 %d）' % (raw, len(got))
            print('  %-14s %3d 則%s' % (name, len(got), note))
            fresh += got
        except Exception as e:
            print('  %-14s ✗ %s: %s' % (name, type(e).__name__, str(e)[:60]))
            failed.append(name)

    for query, cat in GNEWS_TOPICS:
        try:
            got = fetch_gnews(query, cat, now_iso)[:MAX_PER_SOURCE]   # 每題最多 100 則，要夾
            print('  GN %-11s %3d 則' % (query[:11], len(got)))
            fresh += got
        except Exception as e:
            print('  GN %-11s ✗ %s' % (query[:11], type(e).__name__))
            failed.append('GN/' + query)

    if not fresh:
        print('未取得任何新聞，保留現有資料。')
        return 0

    aliases = load_aliases()
    merged = merge(fresh + old.get('items', []), aliases)

    cutoff = (datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)).isoformat(timespec='seconds')
    merged = [m for m in merged if m['p'] >= cutoff]
    merged.sort(key=lambda m: m['p'], reverse=True)
    merged = merged[:MAX_ITEMS]

    runs = old.get('runs', [])
    if slot and slot not in runs:
        runs.append(slot)
    result = {'updated': now_tw.strftime('%Y/%m/%d %H:%M'), 'date': today,
              'runs': sorted(runs), 'failed': failed, 'items': merged}
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))

    tagged = sum(1 for m in merged if m['sym'])
    dup = sum(1 for m in merged if m['hot'] > 1)
    print('已寫入 %s：%d 則（本次新抓 %d、去重後多家同報 %d、標到追蹤標的 %d）%s'
          % (OUT, len(merged), len(fresh), dup, tagged,
             '，失敗來源：' + '、'.join(failed) if failed else ''))
    return 0


if __name__ == '__main__':
    sys.exit(main())
