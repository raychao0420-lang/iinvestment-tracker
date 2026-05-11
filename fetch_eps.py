#!/usr/bin/env python3
"""
Fetch EPS → update data/eps.json.
Taiwan: FinMind TaiwanStockFinancialStatements (quarterly cumulative, Dec-31 = full year)
US/JP:  yfinance income_stmt (Basic/Diluted EPS row, annual)

Requires: pip install requests yfinance
Set FINMIND_TOKEN env var (free at finmindtrade.com, optional but higher rate limit)
"""
import requests, json, os, time
import yfinance as yf
from datetime import datetime

TOKEN = os.environ.get("FINMIND_TOKEN", "")
URL   = "https://api.finmindtrade.com/api/v4/data"
PATH  = "data/eps.json"
TODAY = datetime.now()
CY    = TODAY.year


def fm_get(dataset, data_id, start="2021-01-01"):
    params = {"dataset": dataset, "data_id": data_id,
              "start_date": start, "token": TOKEN}
    for attempt in range(3):
        try:
            r = requests.get(URL, params=params, timeout=15)
            r.raise_for_status()
            d = r.json()
            if d.get("status") == 200:
                return d.get("data", [])
            print(f"    FinMind [{data_id}]: {d.get('msg', 'error')}")
            return []
        except Exception as e:
            print(f"    [{data_id}] attempt {attempt+1}: {e}")
            if attempt < 2:
                time.sleep(2)
    return []


def find_eps_rows(rows, candidates=("EPS", "BasicEPS", "DilutedEPS", "EpsDiluted", "eps")):
    """Try known EPS type names; return sorted rows."""
    for etype in candidates:
        matched = sorted(
            [r for r in rows if r.get("type") == etype and r.get("value") is not None],
            key=lambda r: r["date"]
        )
        if matched:
            return matched, etype
    return [], None


def parse_eps(rows, use_dec31_for_annual=True):
    """
    Returns (annual_dict, pub_tuple).
    annual_dict: {year: eps}
      - use_dec31_for_annual=True  → take Dec-31 cumulative (TW style)
      - use_dec31_for_annual=False → take last report per year (US/JP annual reports)
    pub_tuple: (date, eps) most recent current-year report, or None
    """
    eps_rows, etype = find_eps_rows(rows)
    if not eps_rows:
        types_seen = sorted({r.get("type") for r in rows if r.get("type")})
        if types_seen:
            print(f"    找不到 EPS 欄位，現有 type: {types_seen[:10]}")
        return {}, None, None

    annual, pub = {}, None
    if use_dec31_for_annual:
        # Taiwan: FinMind stores individual quarterly EPS (not cumulative)
        # Must sum Q1+Q2+Q3+Q4 to get annual; only include complete years (4 quarters)
        by_year = {}
        for row in eps_rows:
            year = int(row["date"][:4])
            by_year.setdefault(year, []).append((row["date"], round(float(row["value"]), 2)))

        for year, entries in by_year.items():
            if len(entries) == 4:
                annual[year] = round(sum(v for _, v in entries), 2)

        if CY in by_year:
            cy_entries = sorted(by_year[CY], key=lambda x: x[0])
            last_date = cy_entries[-1][0]
            cy_sum    = round(sum(v for _, v in cy_entries), 2)
            pub = (last_date, cy_sum)
    else:
        # US/JP: yfinance returns one row per fiscal year end date
        by_year = {}
        for row in eps_rows:
            year = int(row["date"][:4])
            by_year[year] = round(float(row["value"]), 2)
        annual = by_year
        if CY in by_year:
            pub = (f"{CY}-12-31", by_year[CY])

    return annual, pub, etype


def quarter_label(date_str):
    m = int(date_str[5:7])
    if   m <= 3:  return "Q1 ✓"
    elif m <= 6:  return "H1 ✓"
    elif m <= 9:  return "前三季 ✓"
    else:         return "全年 ✓"


def apply_to_item(item, annual, pub_data):
    n = 0
    for year, key in [(2022,"y22"),(2023,"y23"),(2024,"y24"),(2025,"y25")]:
        if year in annual and item.get(key) != annual[year]:
            item[key] = annual[year]
            n += 1
    if pub_data:
        date, val = pub_data
        if item.get("pub") != val:
            item["pub"]      = val
            item["pub_note"] = quarter_label(date)
            n += 1
    return n


if __name__ == "__main__":
    now_str = TODAY.strftime("%Y/%m/%d")
    print(f"=== fetch_eps {now_str} ===")
    if not TOKEN:
        print("提示：未設定 FINMIND_TOKEN，使用匿名限速（每天600次）\n")

    with open(PATH, "r", encoding="utf-8") as f:
        eps = json.load(f)

    total = 0

    # ── 台股 ───────────────────────────────────────────────────────
    print("[台股]")
    for item in eps["tw"]:
        sym = item["symbol"]
        print(f"  {sym} {item['name']}")
        rows = fm_get("TaiwanStockFinancialStatements", sym)
        if not rows:
            print("    → 無資料")
            time.sleep(0.4)
            continue
        annual, pub_data, etype = parse_eps(rows, use_dec31_for_annual=True)
        if not annual:
            time.sleep(0.4)
            continue
        n = apply_to_item(item, annual, pub_data)
        print(f"    → {n} 欄更新 (type={etype})" if n else "    → 無變動")
        total += n
        time.sleep(0.4)

    # ── 美股 / 日股：yfinance income_stmt ─────────────────────────
    # TSM: yfinance returns TWD (not USD), skip and keep manual values
    YF_SKIP_US = {"TSM"}

    def fetch_yf_eps(ticker_sym):
        """Return {year: eps} from yfinance annual income statement."""
        try:
            t = yf.Ticker(ticker_sym)
            stmt = t.income_stmt   # columns = fiscal year-end dates, rows = line items
            if stmt is None or stmt.empty:
                return {}
            for row_name in ("Diluted EPS", "Basic EPS"):
                if row_name in stmt.index:
                    row = stmt.loc[row_name].dropna()
                    return {int(str(col)[:4]): round(float(val), 2)
                            for col, val in row.items()}
            print(f"    找不到 EPS 行，有: {list(stmt.index[:8])}")
            return {}
        except Exception as e:
            print(f"    yfinance error: {e}")
            return {}

    print("\n[美股]")
    for item in eps["us"]:
        sym = item["symbol"]
        print(f"  {sym} {item['name']}")
        if sym in YF_SKIP_US:
            print("    → 跳過（手動維護）")
            continue
        annual = fetch_yf_eps(sym)
        if not annual:
            print("    → 無資料，保留現有")
            continue
        n = apply_to_item(item, annual, None)
        print(f"    → {n} 欄更新" if n else "    → 無變動")
        total += n
        time.sleep(0.3)

    print("\n[日股]")
    for item in eps["jp"]:
        sym = item["symbol"]
        yf_sym = f"{sym}.T"   # Tokyo Stock Exchange suffix
        print(f"  {sym} {item['name']} ({yf_sym})")
        annual = fetch_yf_eps(yf_sym)
        if not annual:
            print("    → 無資料，保留現有")
            continue
        n = apply_to_item(item, annual, None)
        print(f"    → {n} 欄更新" if n else "    → 無變動")
        total += n
        time.sleep(0.3)

    eps["updated"] = now_str
    with open(PATH, "w", encoding="utf-8") as f:
        json.dump(eps, f, ensure_ascii=False, indent=2)
    print(f"\n完成，共更新 {total} 個欄位，已存 {PATH}")
