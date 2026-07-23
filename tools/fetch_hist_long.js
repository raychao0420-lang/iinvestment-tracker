// 產生 data/hist_long.json：各標的「上市至今」日線（清理分割與假跳動），供前端完整歷史回測。
// 用法：node tools/fetch_hist_long.js
// 抓法：Yahoo v8 chart API（range=max 會被降頻成月線，故用 period1/period2 強制日線）。
const fs = require('fs'), path = require('path');

// market: 'lev'=2倍槓桿ETF(±20%限制)、'tw'=台股/一般ETF(±10%)、'us'=美股(無漲跌幅限制)
const TICKERS = [
  { sym: '00631L.TW', name: '元大台灣50正2', market: 'lev' },
  { sym: '00675L.TW', name: '富邦加權正2',   market: 'lev' },
  { sym: '00663L.TW', name: '國泰加權正2',   market: 'lev' },
  { sym: '0050.TW',   name: '元大台灣50',    market: 'tw' },
  { sym: '006208.TW', name: '富邦台50',      market: 'tw' },
  { sym: '00878.TW',  name: '國泰永續高股息', market: 'tw' },
  { sym: '2330.TW',   name: '台積電',        market: 'tw' },
  { sym: '2454.TW',   name: '聯發科',        market: 'tw' },
  { sym: '2317.TW',   name: '鴻海',          market: 'tw' },
  { sym: '2308.TW',   name: '台達電',        market: 'tw' },
  { sym: 'TSM',       name: '台積電ADR',     market: 'us' },
  { sym: 'NVDA',      name: 'NVIDIA',        market: 'us' },
  { sym: 'AAPL',      name: 'Apple',         market: 'us' },
];

// 各市場的「不可能單日漲跌」門檻（ratio 下/上界）；超過者視為假資料壓平
const LIMIT = { lev: [0.79, 1.30], tw: [0.87, 1.15], us: [0.50, 2.00] };

async function fetchOne(t) {
  const p1 = Math.floor(new Date('2010-01-01').getTime() / 1000), p2 = Math.floor(Date.now() / 1000);
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(t.sym)}?period1=${p1}&period2=${p2}&interval=1d`;
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  if (!r.ok) throw new Error('HTTP ' + r.status);
  const res = (await r.json()).chart.result[0];
  const ts = res.timestamp, q = res.indicators.quote[0];
  const adj = res.indicators.adjclose ? res.indicators.adjclose[0].adjclose : null;
  const d = x => new Date(x * 1000).toISOString().slice(0, 10);
  // 1) 以 adjclose/close 還原（處理有記錄的分割/配息）
  let bars = [];
  for (let i = 0; i < ts.length; i++) {
    if (q.close[i] == null || q.open[i] == null) continue;
    const f = (adj && adj[i] != null && q.close[i]) ? adj[i] / q.close[i] : 1;
    bars.push({ time: d(ts[i]), open: q.open[i] * f, high: q.high[i] * f, low: q.low[i] * f, close: q.close[i] * f, volume: q.volume[i] || 0 });
  }
  // 2) 未記錄的分割斷層（ratio<0.5 或 >2）：跨斷層回補還原（保留全部歷史）
  let seams = 0;
  for (let i = 1; i < bars.length; i++) {
    const g = bars[i].close / bars[i - 1].close;
    if (g < 0.5 || g > 2) {   // 視為分割：把斷層前所有 bar 乘以 g，消除跳空
      for (let k = 0; k < i; k++) { bars[k].open *= g; bars[k].high *= g; bars[k].low *= g; bars[k].close *= g; }
      seams++;
    }
  }
  // 3) 壓平超過該市場漲跌幅限制的假跳動
  const [lo, hi] = LIMIT[t.market]; let flat = 0;
  for (let i = 1; i < bars.length; i++) {
    const rr = bars[i].close / bars[i - 1].close;
    if (rr < lo || rr > hi) { const p = bars[i - 1].close; bars[i] = { time: bars[i].time, open: p, high: p, low: p, close: p, volume: 0 }; flat++; }
  }
  // 4) 壓縮：short keys + 四捨五入
  const rnd = v => Math.round(v * 100) / 100;
  const candles = bars.map(b => ({ t: b.time, o: rnd(b.open), h: rnd(b.high), l: rnd(b.low), c: rnd(b.close), v: Math.round(b.volume) }));
  return { name: t.name, market: t.market, seams, flat, candles };
}

(async () => {
  const out = { updated: new Date().toISOString().slice(0, 16).replace('T', ' ') + ' UTC', stocks: {} };
  for (const t of TICKERS) {
    try {
      const r = await fetchOne(t);
      out.stocks[t.sym] = r;
      console.log(`${t.sym.padEnd(11)} ${r.candles.length}根 ${r.candles[0].t}~${r.candles[r.candles.length - 1].t} 壓平${r.flat} 分割還原${r.seams}`);
    } catch (e) { console.log(`${t.sym} 失敗: ${e.message}`); }
    await new Promise(s => setTimeout(s, 400));
  }
  const p = path.join(__dirname, '..', 'data', 'hist_long.json');
  fs.writeFileSync(p, JSON.stringify(out));
  console.log('寫入', p, (fs.statSync(p).size / 1024).toFixed(0) + 'KB');
})();
