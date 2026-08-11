import csv
import os
import re
import sys
import time
import hashlib
import logging
from pathlib import Path
from typing import List, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

URL = "https://live.euronext.com/en/products/equities/company-news"

TOPIC_SIGNAL_MAP = {
    "mandatory notification of trade primary insiders": "INSIDER_TRADE",
    "major shareholding notifications": "FLAGGING",
    "flagging": "FLAGGING",
    "acquisition or disposal of the issuer's own shares": "BUYBACK",
    "acquisition or disposal of the issuer’s own shares": "BUYBACK",  # curly apostrophe variant seen on-site
}

FIELDNAMES = [
    "Unique_ID","Date filed","Trade date","Ticker","Company","Insider name","Role",
    "Transaction","Shares","Price","Value","Ownership after",
    "Source link","Market","Topic","Signal_type","Title"
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,no;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("insider_tracker")

# -----------------------------
# HTTP session with retries
# -----------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s

SESSION = make_session()

# -----------------------------
# Helpers
# -----------------------------

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def norm_topic(s: str) -> str:
    return clean(s).lower()

def load_osebx(path: str) -> set:
    with open(path, newline="", encoding="utf-8") as f:
        return {row[0].strip().upper() for row in csv.reader(f) if row and row[0].strip()}

def norm_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("/"):
        return "https://live.euronext.com" + href
    return href

def cached_get_text(url: str, cache_dir="data/cache_html", sleep_s: float = 0.3) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    key = hashlib.md5(url.encode("utf-8")).hexdigest()
    path = Path(cache_dir) / f"{key}.html"
    if path.exists():
        return path.read_text(encoding="utf-8", errors="ignore")

    time.sleep(sleep_s)
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    text = r.text
    path.write_text(text, encoding="utf-8")
    return text

def download_file(url: str, out_path: str, sleep_s: float = 0.3) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if os.path.exists(out_path):
        return
    time.sleep(sleep_s)
    r = SESSION.get(url, timeout=60)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)

def try_pdf_to_text(pdf_path: str) -> str:
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return ""
    parts = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                t = page.extract_text() or ""
                t = t.strip()
                if t:
                    parts.append(f"\n--- PAGE {i} ---\n{t}\n")
    except Exception:
        return ""
    return clean("\n".join(parts))

def _to_intish(x: str) -> str:
    if x is None:
        return ""
    x = x.replace(" ", "").replace(",", "")
    return re.sub(r"[^\d]", "", x)

def _to_decimalish(x: str) -> str:
    if not x:
        return ""
    s = x.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(",", "")
    else:
        s = s.replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    return s

# -----------------------------
# Euronext detail page parsing
# -----------------------------

def parse_release_meta_and_text(release_url: str) -> Dict:
    html = cached_get_text(release_url)
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    date_filed = ""
    m = re.search(r"\b\d{1,2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}\s+CET\b", page_text)
    if m:
        date_filed = m.group(0)

    issuer = ""
    m = re.search(r"###\s*Issuer\s*\n([^\n]+)", page_text)
    if m:
        issuer = clean(m.group(1))

    symbol = ""
    m = re.search(r"###\s*Symbol\s*\n([A-Z0-9]{1,15})\b", page_text)
    if m:
        symbol = clean(m.group(1))

    market = ""
    m = re.search(r"###\s*Market\s*\n([^\n]+)", page_text)
    if m:
        market = clean(m.group(1))

    pdf_urls = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if ".pdf" in href.lower():
            pdf_urls.append(norm_url(href))

    body_text = page_text
    if issuer:
        idx = body_text.find(issuer)
        if idx != -1:
            body_text = body_text[idx + len(issuer):]

    stop_markers = ["More information:", "SOURCE", "### Source", "### Provider", "PROVIDER"]
    stops = [body_text.find(mk) for mk in stop_markers if body_text.find(mk) != -1]
    if stops:
        body_text = body_text[:min(stops)]

    body_text = clean(body_text)

    return {
        "date_filed": date_filed,
        "issuer": issuer,
        "symbol": symbol,
        "market": market,
        "body_text": body_text,
        "pdf_urls": list(dict.fromkeys(pdf_urls)),
    }

# -----------------------------
# PDF parsing (MAR Article 19 template)
# -----------------------------

def _txn_from_nature(nature: str) -> str:
    n = (nature or "").strip().lower()
    if "acquisition" in n or "purchase" in n or "buy" in n:
        return "BUY"
    if "disposal" in n or "sale" in n or "sell" in n:
        return "SELL"
    return ""

def parse_mar_pdf_text(pdf_text: str) -> List[Dict]:
    t = pdf_text or ""
    if not t.strip():
        return []

    person_header = r"Details of the person discharging managerial responsibilities/person closely associated"
    if re.search(person_header, t, re.IGNORECASE):
        blocks = re.split(rf"(?={person_header})", t, flags=re.IGNORECASE)
    else:
        blocks = re.split(
            r"(?=NOTIFICATION OF TRANSACTIONS PURSUANT TO THE MARKET ABUSE REGULATION ARTICLE 19)",
            t, flags=re.IGNORECASE
        )

    results: List[Dict] = []
    for block in blocks:
        block = block.strip()
        if not block:
            continue

        m_name = re.search(r"\bName\s+([A-ZÆØÅ][A-Za-zÆØÅæøå\-\.\s]+)", block, re.IGNORECASE)
        name = clean(m_name.group(1)) if m_name else ""

        m_role = re.search(r"\bPosition/status\s+([A-Za-z0-9\-/\s]+)", block, re.IGNORECASE)
        role = clean(m_role.group(1)) if m_role else ""

        m_issuer = re.search(
            r"Details of the issuer.*?\bName\s+([A-Za-z0-9 .,&\-]+)",
            block, re.IGNORECASE | re.DOTALL
        )
        issuer = clean(m_issuer.group(1)) if m_issuer else ""

        if re.search(r"Details of the transaction", block, re.IGNORECASE):
            txn_parts = re.split(r"Details of the transaction", block, flags=re.IGNORECASE)
            txn_chunks = [("Details of the transaction " + p).strip() for p in txn_parts[1:] if p.strip()]
            if not txn_chunks:
                txn_chunks = [block]
        else:
            txn_chunks = [block]

        for ch in txn_chunks:
            m_nat = re.search(r"\bNature of the transaction\s+([A-Za-z ]+)", ch, re.IGNORECASE)
            nature = clean(m_nat.group(1)) if m_nat else ""
            txn = _txn_from_nature(nature)

            m_dt = re.search(r"\bDate of the transaction\s+(\d{4}-\d{2}-\d{2})", ch, re.IGNORECASE)
            trade_date = clean(m_dt.group(1)) if m_dt else ""

            m_pv = re.search(
                r"\bPrice(?:\(s\))?\s*:\s*(?P<ccy>[A-Z]{3})\s*(?P<price>[\d\.,\s]+)"
                r"(?:\s*,?\s*volume(?:\(s\))?\s*:\s*(?P<vol>[\d\.,\s]+))?",
                ch, re.IGNORECASE
            )
            ccy, price, vol = "", "", ""
            if m_pv:
                ccy = (m_pv.group("ccy") or "").upper()
                price = _to_decimalish(m_pv.group("price") or "")
                vol = _to_intish(m_pv.group("vol") or "")

            if not vol:
                m_vol = re.search(r"\bAggregated information:\s*Volume\s+([\d\.,\s]+)", ch, re.IGNORECASE)
                if m_vol:
                    vol = _to_intish(m_vol.group(1))
                else:
                    m_vol2 = re.search(r"\bVolume\s+([\d\.,\s]+)", ch, re.IGNORECASE)
                    if m_vol2 and not re.search(r"Volume weighted average price", ch, re.IGNORECASE):
                        vol = vol or _to_intish(m_vol2.group(1))

            if not price:
                m_vwap = re.search(r"\bVolume weighted average price\s+([\d\.,\s]+)", ch, re.IGNORECASE)
                if m_vwap:
                    price = _to_decimalish(m_vwap.group(1))
                    if not ccy and re.search(r"\bNOK\b", ch):
                        ccy = "NOK"

            m_total = re.search(r"\bTotal price\s+(?P<ccy>[A-Z]{3})\s*(?P<tot>[\d\.,\s]+)", ch, re.IGNORECASE)
            total_val = ""
            if m_total:
                total_val = f"{m_total.group('ccy').upper()} {_to_decimalish(m_total.group('tot'))}"

            if any([name, role, txn, vol, price, trade_date, total_val]):
                results.append({
                    "Insider name": name,
                    "Role": role,
                    "Transaction": txn,
                    "Shares": vol,
                    "Price": (f"{ccy} {price}".strip() if ccy and price else (ccy.strip() if ccy else "")),
                    "Value": total_val,
                    "Ownership after": "",
                    "Trade date": trade_date,
                    "_issuer_from_pdf": issuer,
                })

    uniq, seen = [], set()
    for r in results:
        key = (r.get("Insider name",""), r.get("Role",""), r.get("Transaction",""),
               r.get("Shares",""), r.get("Price",""), r.get("Trade date",""), r.get("Value",""))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    return uniq

# -----------------------------
# HTML press release parsing (backup)
# -----------------------------

def extract_trades_from_text(text: str) -> List[Dict]:
    t = clean(text)
    trades: List[Dict] = []

    pat = re.compile(
        r"(?P<name>[A-ZÆØÅ][A-Za-zÆØÅæøå\-\.\s]+?),\s*"
        r"(?P<role>[^.]{3,200}?)\s*,?\s*"
        r"(?:has\s+on\s+(?P<tradedate>\d{1,2}\s+[A-Za-z]+\s+\d{4})\s+)?"
        r"(?P<verb>bought|purchased|acquired|sold)\s+"
        r"(?P<shares>[\d\.,\s]+)\s*shares\b"
        r"(?:.*?\bshare\s+price\s+(?:of\s+)?(?P<ccy>[A-Z]{3})\s*(?P<price>[\d]+(?:[.,]\d+)?))?",
        re.IGNORECASE
    )
    for m in pat.finditer(t):
        verb = m.group("verb").lower()
        txn = "BUY" if verb in ("bought", "purchased", "acquired") else "SELL"
        ccy = (m.group("ccy") or "").upper()
        price = _to_decimalish(m.group("price") or "")
        price_str = f"{ccy} {price}".strip() if ccy and price else ""
        trades.append({
            "Insider name": clean(m.group("name")),
            "Role": clean(m.group("role")),
            "Transaction": txn,
            "Shares": _to_intish(m.group("shares")),
            "Price": price_str,
            "Value": "",
            "Ownership after": "",
            "Trade date": clean(m.group("tradedate") or ""),
        })
    if trades:
        return trades

    m_total = re.search(
        r"\btotal\s+of\s+(?P<shares>[\d\.,\s]+)\s*shares\b.*?\b"
        r"(?:average\s+price\s+per\s+share\s+of|average\s+price\s+per\s+share\s+was|at\s+an\s+average\s+price\s+per\s+share\s+of)\s*"
        r"(?P<ccy>[A-Z]{3})\s*(?P<price>[\d]+(?:[.,]\d+)?)",
        t, re.IGNORECASE
    )
    if m_total:
        trades.append({
            "Insider name": "(Aggregate – primary insiders)",
            "Role": "", "Transaction": "BUY",
            "Shares": _to_intish(m_total.group("shares")),
            "Price": f"{m_total.group('ccy').upper()} {_to_decimalish(m_total.group('price'))}",
            "Value": "", "Ownership after": "", "Trade date": "",
        })
    return trades

def extract_trades_prefer_pdf_then_fallback(meta: Dict) -> List[Dict]:
    pdf_urls = meta.get("pdf_urls", []) or []
    for pdf_url in pdf_urls[:3]:
        try:
            pdf_path = f"data/cache_pdf/{hashlib.md5(pdf_url.encode('utf-8')).hexdigest()}.pdf"
            download_file(pdf_url, pdf_path)
            pdf_text = try_pdf_to_text(pdf_path)
            if not pdf_text:
                continue
            trades = parse_mar_pdf_text(pdf_text)
            if trades:
                return trades
        except Exception as e:
            log.warning(f"PDF parse failed for {pdf_url}: {e}")
            continue
    return extract_trades_from_text(meta.get("body_text", ""))

# -----------------------------
# Pagination
# -----------------------------

def find_next_page_url(soup: BeautifulSoup) -> str:
    a = soup.find("a", attrs={"rel": "next"}, href=True)
    if a:
        return norm_url(a["href"])

    candidates = ("next", "›", ">", "→", "»")
    for a in soup.select("a[href]"):
        txt = clean(a.get_text(" ", strip=True)).lower()
        aria = (a.get("aria-label") or "").lower()
        title = (a.get("title") or "").lower()
        if txt in candidates or any(c in aria for c in ("next",)) or any(c in title for c in ("next",)):
            href = a.get("href", "")
            if href and href != "#":
                return norm_url(href)
    return ""

def fetch_listing_pages(max_pages: int = 5):
    url = URL
    for page_num in range(1, max_pages + 1):
        r = SESSION.get(url, timeout=30)
        log.info(f"Listing page {page_num}: GET {url} -> {r.status_code}, {len(r.text)} bytes")
        r.raise_for_status()

        # DIAGNOSTIC: log a snippet of the raw response so we can see
        # whether we're getting the real listing HTML or a bot-challenge page.
        log.info(f"Response snippet: {r.text[:500]!r}")

        soup = BeautifulSoup(r.text, "html.parser")

        row_count = len(soup.find_all("tr"))
        log.info(f"Listing page {page_num}: found {row_count} <tr> elements")

        yield soup

        nxt = find_next_page_url(soup)
        if not nxt or nxt == url:
            log.info(f"No further pagination link found after page {page_num}, stopping.")
            break
        url = nxt

# -----------------------------
# Listing scrape
# -----------------------------

def guess_ticker(text: str) -> str:
    m = re.search(r"\(([A-Z0-9]{2,10})\)", text or "")
    if m:
        return m.group(1)
    m = re.search(r"\b[A-Z0-9]{2,10}\b", text or "")
    return m.group(0) if m else ""

def main():
    osebx = load_osebx("data/osebx_tickers.csv") if os.path.exists("data/osebx_tickers.csv") else set()

    candidate_releases = []
    seen_links = set()
    total_trs_seen = 0

    try:
        for soup in fetch_listing_pages(max_pages=5):
            trs = soup.find_all("tr")
            total_trs_seen += len(trs)
            for tr in trs:
                tds = tr.find_all("td")
                if len(tds) < 5:
                    continue

                cells = [clean(td.get_text(" ", strip=True)) for td in tds]
                released, company, title, topic = cells[0], cells[1], cells[2], cells[4]

                signal_type = TOPIC_SIGNAL_MAP.get(norm_topic(topic))
                if not signal_type:
                    continue

                a = tr.find("a", href=True)
                if not a:
                    continue
                link = norm_url(a["href"])
                if link in seen_links:
                    continue
                seen_links.add(link)

                candidate_releases.append({
                    "released": released, "company": company, "title": title,
                    "topic": topic, "signal_type": signal_type,
                    "ticker_guess": guess_ticker(company), "link": link,
                })
    except requests.exceptions.RequestException as e:
        log.error(f"FATAL: could not fetch listing pages: {e}")
        sys.exit(1)

    log.info(f"Total <tr> elements across all pages: {total_trs_seen}")
    log.info(f"Candidate releases matching TOPIC_SIGNAL_MAP: {len(candidate_releases)}")

    if total_trs_seen == 0:
        log.error(
            "FATAL: 0 <tr> elements found on the listing page. "
            "Euronext likely changed the page markup or is blocking this request "
            "(check the Response snippet log line above for a bot-challenge page). "
            "View-source the listing URL and compare to the parser's assumptions."
        )
        sys.exit(1)

    if len(candidate_releases) == 0:
        log.error(
            "FATAL: table rows were found, but none matched TOPIC_SIGNAL_MAP. "
            "The Topic column text likely changed — compare actual topic strings "
            "in the log/cache to TOPIC_SIGNAL_MAP's keys."
        )
        sys.exit(1)

    out_path = "data/insider_trades.csv"
    existing_ids = set()
    existing_rows = []
    if os.path.exists(out_path):
        with open(out_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for rr in reader:
                existing_rows.append(rr)
                if rr.get("Unique_ID"):
                    existing_ids.add(rr["Unique_ID"])

    new_rows = []
    for rel in candidate_releases:
        link = rel["link"]
        if any(uid.startswith(link + "|") for uid in existing_ids):
            continue

        try:
            meta = parse_release_meta_and_text(link)
        except Exception as e:
            log.warning(f"Failed to parse release {link}: {e}")
            meta = {"body_text": "", "pdf_urls": []}

        ticker = (meta.get("symbol") or rel["ticker_guess"] or "").upper()
        company_name = meta.get("issuer") or rel["company"]
        market = meta.get("market") or "EURONEXT/OSLO"
        date_filed = meta.get("date_filed") or rel["released"]

        if osebx and ticker and ticker not in osebx:
            continue

        trades = extract_trades_prefer_pdf_then_fallback(meta)

        if trades:
            pdf_issuer = trades[0].get("_issuer_from_pdf")
            if pdf_issuer:
                company_name = pdf_issuer or company_name

            for i, t in enumerate(trades, start=1):
                uid = f"{link}|{i}"
                if uid in existing_ids:
                    continue
                new_rows.append({
                    "Unique_ID": uid, "Date filed": date_filed,
                    "Trade date": t.get("Trade date", ""), "Ticker": ticker,
                    "Company": company_name, "Insider name": t.get("Insider name", ""),
                    "Role": t.get("Role", ""), "Transaction": t.get("Transaction", ""),
                    "Shares": t.get("Shares", ""), "Price": t.get("Price", ""),
                    "Value": t.get("Value", ""), "Ownership after": t.get("Ownership after", ""),
                    "Source link": link, "Market": market, "Topic": rel["topic"],
                    "Signal_type": rel["signal_type"], "Title": rel["title"],
                })
        else:
            uid = f"{link}|0"
            if uid not in existing_ids:
                new_rows.append({
                    "Unique_ID": uid, "Date filed": date_filed, "Trade date": "",
                    "Ticker": ticker, "Company": company_name, "Insider name": "",
                    "Role": "", "Transaction": "", "Shares": "", "Price": "",
                    "Value": "", "Ownership after": "", "Source link": link,
                    "Market": market, "Topic": rel["topic"],
                    "Signal_type": rel["signal_type"], "Title": rel["title"],
                })

    all_rows = existing_rows + new_rows

    os.makedirs("data", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(all_rows)

    log.info(f"Added {len(new_rows)} new rows. Total rows: {len(all_rows)}")

if __name__ == "__main__":
    main()
