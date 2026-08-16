# -*- coding: utf-8 -*-
"""財經新聞來源探路（一次性診斷工具，不寫任何資料檔）。

用途：新聞頁動工前，確認每個候選來源在 **GitHub Actions 的美國 IP** 底下抓不抓得到。
本機（台灣 IP）測過全部可用，但本 repo 歷史上 TWSE / TAIFEX / MOPS 都是「台灣通、境外擋」，
所以台灣媒體那幾家一定要在 Actions 上實跑過才能定案。

跑法：Actions → Probe News Sources → Run workflow（workflow_dispatch，不排程）。
輸出：每個來源一行 OK/失敗 ＋ 則數 ＋ 首則標題，最後一段彙總。
"""
import json
import sys
import xml.etree.ElementTree as ET

import requests

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/124.0 Safari/537.36'}
NS = {'rss10': 'http://purl.org/rss/1.0/',
      'atom': 'http://www.w3.org/2005/Atom',
      'dc': 'http://purl.org/dc/elements/1.1/'}
TIMEOUT = 20

# (區塊, 顯示名, 網址, 型別)  型別：xml=RSS/Atom、cnyes=鉅亨網 JSON
FEEDS = [
    ('TW',  '中央社 財經',      'https://feeds.feedburner.com/rsscna/finance', 'xml'),
    ('TW',  '經濟日報 產經',    'https://money.udn.com/rssfeed/news/1001/5591?ch=money', 'xml'),
    ('TW',  '自由財經',         'https://news.ltn.com.tw/rss/business.xml', 'xml'),
    ('TW',  '鉅亨網 JSON',      'https://api.cnyes.com/media/api/v1/newslist/category/headline?limit=30', 'cnyes'),
    ('US',  'CNBC 財經',        'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147', 'xml'),
    ('US',  'MarketWatch',      'https://feeds.content.dowjones.io/public/rss/mw_topstories', 'xml'),
    ('US',  'Fed 新聞稿',       'https://www.federalreserve.gov/feeds/press_all.xml', 'xml'),
    ('US',  'Yahoo Fin (TSM)',  'https://feeds.finance.yahoo.com/rss/2.0/headline?s=TSM&region=US&lang=en-US', 'xml'),
    ('JP',  'Nikkei Asia',      'https://asia.nikkei.com/rss/feed/nar', 'xml'),
    ('JP',  'Japan Times',      'https://www.japantimes.co.jp/feed/', 'xml'),
    ('KR',  'Korea Herald',     'https://www.koreaherald.com/rss/newsAll', 'xml'),
    ('KR',  'KED Global',       'https://www.kedglobal.com/rss/news.xml', 'xml'),
    ('KR',  'ChosunBiz',        'https://biz.chosun.com/arc/outboundfeeds/rss/?outputType=xml', 'xml'),
    ('IND', '科技新報',         'https://technews.tw/feed/', 'xml'),
    ('IND', 'DigiTimes (EN)',   'https://www.digitimes.com/rss/daily.xml', 'xml'),
    ('IND', "Tom's Hardware",   'https://www.tomshardware.com/feeds/all', 'xml'),
    ('IND', 'EE Times',         'https://www.eetimes.com/feed/', 'xml'),
]

# Google News RSS：全球可用，當台灣媒體被境外擋掉時的保底來源
GNEWS = 'https://news.google.com/rss/search?q={q}+when:1d&hl={hl}&gl={gl}&ceid={gl}:{lang}'
GQUERIES = [
    ('GN', '台股',     '台股',            'zh-TW', 'TW', 'zh-Hant'),
    ('GN', '美股',     '美股',            'zh-TW', 'TW', 'zh-Hant'),
    ('GN', '半導體',   '半導體 OR 記憶體', 'zh-TW', 'TW', 'zh-Hant'),
    ('GN', '日股(中文)', '日股 OR 日經',   'zh-TW', 'TW', 'zh-Hant'),
    ('GN', '韓股(中文)', '韓股 OR 三星電子', 'zh-TW', 'TW', 'zh-Hant'),
    ('GN', '個股:台積電', '台積電',        'zh-TW', 'TW', 'zh-Hant'),
]


def parse_any(content):
    """RSS 2.0 / RSS 1.0(RDF) / Atom 三種通吃。

    ⚠️ Nikkei Asia 是 RSS 1.0(RDF)，item 在 purl.org/rss/1.0 命名空間下，
    用一般 './/item' 會抓到 0 筆（本機探路踩過）。
    """
    root = ET.fromstring(content)
    for path, kind in (('.//item', 'rss2'), ('.//rss10:item', 'rss1'), ('.//atom:entry', 'atom')):
        items = root.findall(path, NS)
        if items:
            return items, kind
    return [], None


def field(it, kind, names):
    pre = {'rss2': '', 'rss1': 'rss10:', 'atom': 'atom:'}[kind]
    for n in names:
        v = it.findtext(pre + n, None, NS) or it.findtext('dc:' + n, None, NS)
        if v and v.strip():
            return v.strip()
    return ''


def probe_xml(url):
    r = requests.get(url, headers=UA, timeout=TIMEOUT)
    if r.status_code != 200:
        return None, 'HTTP %s' % r.status_code, ''
    items, kind = parse_any(r.content)
    if not items:
        return None, '200 但解析不到 item', ''
    it = items[0]
    title = field(it, kind, ['title'])
    date = field(it, kind, ['pubDate', 'date', 'updated', 'published'])
    note = kind + ('' if date else ' 無日期欄位')
    return len(items), note, title


def probe_cnyes(url):
    r = requests.get(url, headers=UA, timeout=TIMEOUT)
    if r.status_code != 200:
        return None, 'HTTP %s' % r.status_code, ''
    data = r.json()
    items = (data.get('items') or {}).get('data') or []
    if not items:
        return None, '200 但無 items.data', ''
    return len(items), 'json', (items[0].get('title') or '')


def run(rows):
    ok, bad = [], []
    for block, name, url, kind in rows:
        label = '%-4s %-16s' % (block, name)
        try:
            n, note, title = (probe_cnyes if kind == 'cnyes' else probe_xml)(url)
            if n is None:
                print('%s ✗ %s' % (label, note))
                bad.append('%s/%s (%s)' % (block, name, note))
            else:
                print('%s ✓ %3d 則 [%s] %s' % (label, n, note, title[:46]))
                ok.append('%s/%s' % (block, name))
        except Exception as e:
            print('%s ✗ %s: %s' % (label, type(e).__name__, str(e)[:70]))
            bad.append('%s/%s (%s)' % (block, name, type(e).__name__))
    return ok, bad


def main():
    print('=== 直連來源 ===')
    ok1, bad1 = run(FEEDS)

    print()
    print('=== Google News RSS（保底來源）===')
    gfeeds = [(b, n, GNEWS.format(q=requests.utils.quote(q), hl=hl, gl=gl, lang=lang), 'xml')
              for b, n, q, hl, gl, lang in GQUERIES]
    ok2, bad2 = run(gfeeds)

    ok, bad = ok1 + ok2, bad1 + bad2
    print()
    print('=== 彙總 ===')
    print('可用 %d / 全部 %d' % (len(ok), len(ok) + len(bad)))
    if bad:
        print('失敗清單：')
        for b in bad:
            print('  - %s' % b)
    else:
        print('全部可用。')
    # 探路是診斷用途，失敗不讓 job 變紅（要看的是這張表）
    return 0


if __name__ == '__main__':
    sys.exit(main())
