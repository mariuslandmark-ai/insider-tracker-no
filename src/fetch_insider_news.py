import csv
import os
import re
import time
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

URL = "https://live.euronext.com/en/products/equities/company-news"

TOPIC_SIGNAL_MAP = {
    "Mandatory notification of trade primary insiders": "INSIDER_TRADE",
    "Major shareholding notifications": "FLAGGING",
    "Flagging": "FLAGGING",
    "Acquisition or disposal of the issuer’s own shares": "BUYBACK",
}

FIELDNAMES = [
    "Unique_ID","Date filed","Trade date","Ticker","Company","Insider name","Role",
    "Transaction","Shares","Price","Value","Ownership after",
    "Source link","Market","Topic","Signal_type","Title"
]

HEADERS = {"User-Agent": "Mozilla/5.0"}

# -----------------------------
# Helpers
# -----------------------------

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def load_osebx(path: str) -> set[str]:
    with open(path, newline="", encoding="utf-8") as f:
        return {row[0].strip().upper() for row in csv.reader(f) if row and row[0].strip()}

def norm_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("/"):
        return "https://live.euronext.com" + href
    return href

def cached_get_text(url: str, cache_dir="data/cache_html", sleep_s: float = 0.25) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    key = hashlib.md5(url.encode("utf-8")).hexdigest()
    path = Path(cache_dir) / f"{key}.html"
    if path.exists():
        return path.read_text(encoding="utf-8", errors="ignore")

    time.sleep(sleep_s)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    text = r.text
    path.write_text(text, encoding="utf-8")
    return text

def download_file(url: str, out_path: str, sleep_s: float = 0.25) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if os.path.exists(out_path):
        return
    time.sleep(sleep_s)
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)

def try_pdf_to_text(pdf_path: str) -> str:
    """
    Requires pdfplumber in requirements.txt to work reliably.
    If PDF is scanned image, this often returns empty.
    """
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return ""

    parts = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t.strip():
                    parts.append(t)
    except Exception:
        return ""
    return clean("\n".join(parts))

def _to_intish(x: str) -> str:
    if x is None:
        return ""
    # keep only digits
    x = x.replace(" ", "")
    x = x.replace(",", "")  # 4,859 -> 4859
    x = re.sub(r"[^\d]", "", x)
    return x

def _to_decimalish(x: str) -> str:
    """
    Keeps decimals for prices, tolerates comma thousands and dot decimals.
    Examples:
      "288.845" -> "288.845"
      "288,845" -> "288.845"
      "1,403,497.855" -> "1403497.855"
      "1 403 497,855" -> "1403497.855"
    """
    if not x:
        return ""
    s = x.replace(" ", "")
    # If both comma and dot exist, assume comma is thousands separator
    if "," in s and "." in s:
        s = s.replace(",", "")
    else:
        s = s.replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    # collapse multiple dots (rare)
    if s.count(".") > 1:
        # keep last dot as decimal separator
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    return s

# -----------------------------
# Euronext detail page parsing
# -----------------------------

def parse_release_meta_and_text(release_url: str) -> Dict:
    """
    Parses metadata + extracts a cleaned announcement body_text (reduced nav/footer noise).
    """
    html = cached_get_text(release_url)
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    # Date filed like: "05 Feb 2026 14:48 CET"
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

    # PDF attachments
    pdf_urls = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if ".pdf" in href.lower():
            pdf_urls.append(norm_url(href))

    # --- Clean body extraction ---
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
    """
    Parses the common MAR Article 19 notification template (like your screenshot).
    Extracts:
      - Name
      - Position/status
      - Nature of the transaction
      - Price + volume
      - Date of the transaction
      - Total price (if present)
    Handles multiple instruments/transactions if repeated blocks exist (best-effort).
    """
    t = pdf_text

    # Try to split into repeating "Details of the transaction" sections if present
    # If not present, treat whole doc as one.
    chunks = []
    if re.search(r"Details of the transaction", t, re.IGNORECASE):
        # split but keep some context
        parts = re.split(r"Details of the transaction", t, flags=re.IGNORECASE)
        # first part is header; subsequent parts are sections
        for p in parts[1:]:
            chunks.append("Details of the transaction " + p)
    else:
        chunks = [t]

    results: List[Dict] = []

    # Global fields (often appear once)
    name = ""
    role = ""
    issuer = ""
    m = re.search(r"\bName\s+([A-ZÆØÅ][^\n]+)", t)
    if m:
        # This can match issuer name too; we refine below per template markers
        pass

    # Better: locate the "Details of the person..." block
    m_name = re.search(r"Details of the person.*?\bName\s+([A-ZÆØÅ][A-Za-zÆØÅæøå\-\.\s]+)", t, re.IGNORECASE)
    if m_name:
        name = clean(m_name.group(1))

    m_role = re.search(r"\bPosition/status\s+([A-Za-z0-9\-\/\s]+)", t, re.IGNORECASE)
    if m_role:
        role = clean(m_role.group(1))

    m_issuer = re.search(r"Details of the issuer.*?\bName\s+([A-Za-z0-9 .,&\-]+)", t, re.IGNORECASE)
    if m_issuer:
        issuer = clean(m_issuer.group(1))

    for ch in chunks:
        # Nature
        m_nat = re.search(r"\bNature of the transaction\s+([A-Za-z ]+)", ch, re.IGNORECASE)
        nature = clean(m_nat.group(1)) if m_nat else ""

        # Price + volume line (common)
        # e.g. "Price: NOK 288.845, volume: 4,859"
        m_pv = re.search(
            r"\bPrice\s*:\s*(?P<ccy>[A-Z]{3})\s*(?P<price>[\d\.,\s]+)\s*,?\s*volume\s*:\s*(?P<vol>[\d\.,\s]+)",
            ch,
            re.IGNORECASE
        )
        ccy = ""
        price = ""
        vol = ""
        if m_pv:
            ccy = m_pv.group("ccy").upper()
            price = _to_decimalish(m_pv.group("price"))
            vol = _to_intish(m_pv.group("vol"))

        # Date of transaction
        m_dt = re.search(r"\bDate of the transaction\s+(\d{4}-\d{2}-\d{2})", ch, re.IGNORECASE)
        trade_date = clean(m_dt.group(1)) if m_dt else ""

        # Total price
        m_total = re.search(r"\bTotal price\s+(?P<ccy>[A-Z]{3})\s*(?P<tot>[\d\.,\s]+)", ch, re.IGNORECASE)
        total_val = ""
        if m_total:
            total_val = f"{m_total.group('ccy').upper()} {_to_decimalish(m_total.group('tot'))}"

        # Fallback: volume weighted average price (sometimes appears instead of Price:)
        if not price:
            m_vwap = re.search(r"\bVolume weighted average price\s+([\d\.,\s]+)", ch, re.IGNORECASE)
            if m_vwap:
                price = _to_decimalish(m_vwap.group(1))
                # currency often NOK in these
                if not ccy:
                    m_ccy = re.search(r"\bNOK\b", ch)
                    ccy = "NOK" if m_ccy else ccy

        txn = _txn_from_nature(nature)

        # Only add if we have something useful
        if any([name, vol, price, trade_date, txn]):
            results.append({
                "Insider name": name,
                "Role": role,
                "Transaction": txn,
                "Shares": vol,
                "Price": f"{ccy} {price}".strip() if ccy and price else (f"{ccy}".strip() if ccy else ""),
                "Value": total_val,
                "Ownership after": "",
                "Trade date": trade_date,
                "_issuer_from_pdf": issuer,
            })

    # Deduplicate identical rows (sometimes chunks repeat)
    uniq = []
    seen = set()
    for r in results:
        key = (r.get("Insider name",""), r.get("Role",""), r.get("Transaction",""), r.get("Shares",""),
               r.get("Price",""), r.get("Trade date",""), r.get("Value",""))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    return uniq

# -----------------------------
# HTML press release parsing (backup)
# -----------------------------

def extract_trades_from_text(text: str) -> List[Dict]:
    """
    Backup parser for announcements written as free text on the Euronext page.
    """
    t = clean(text)
    trades: List[Dict] = []

    # Pattern: "Name, role ..., has on <date> bought/sold <shares> shares ... at a share price of NOK <price>"
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

    # Aggregate fallback (total shares + avg price)
    m_total = re.search(
        r"\btotal\s+of\s+(?P<shares>[\d\.,\s]+)\s*shares\b.*?\b"
        r"(?:average\s+price\s+per\s+share\s+of|average\s+price\s+per\s+share\s+was|at\s+an\s+average\s+price\s+per\s+share\s+of)\s*"
        r"(?P<ccy>[A-Z]{3})\s*(?P<price>[\d]+(?:[.,]\d+)?)",
        t,
        re.IGNORECASE
    )
    if m_total:
        shares = _to_intish(m_total.group("shares"))
        ccy = m_total.group("ccy").upper()
        price = _to_decimalish(m_total.group("price"))
        trades.append({
            "Insider name": "(Aggregate – primary insiders)",
            "Role": "",
            "Transaction": "BUY",
            "Shares": shares,
            "Price": f"{ccy} {price}",
            "Value": "",
            "Ownership after": "",
            "Trade date": "",
        })
        return trades

    return []

def extract_trades_prefer_pdf_then_fallback(meta: Dict) -> List[Dict]:
    """
    Prefer parsing the attached PDF(s) first (generic MAR template => low misread risk).
    If no PDF parse yields trades, fall back to HTML body_text.
    """
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
        except Exception:
            continue

    # Backup: HTML body
    return extract_trades_from_text(meta.get("body_text", ""))

# -----------------------------
# Pagination: skim first N pages
# -----------------------------

def find_next_page_url(soup: BeautifulSoup) -> str:
    a = soup.find("a", attrs={"rel": "next"}, href=True)
    if a:
        return norm_url(a["href"])

    for a in soup.select("a[href]"):
        txt = clean(a.get_text(" ", strip=True)).lower()
        aria = (a.get("aria-label") or "").lower()
        if txt in ("next", "›", ">", "→") or "next" in aria:
            return norm_url(a.get("href", ""))

    return ""

def fetch_listing_pages(max_pages: int = 5):
    url = URL
    for _ in range(max_pages):
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        yield soup
        nxt = find_next_page_url(soup)
        if not nxt or nxt == url:
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

    for soup in fetch_listing_pages(max_pages=5):
        trs = soup.find_all("tr")
        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue

            cells = [clean(td.get_text(" ", strip=True)) for td in tds]
            released = cells[0]
            company = cells[1]
            title = cells[2]
            topic = cells[4]

            signal_type = TOPIC_SIGNAL_MAP.get(topic)
            if not signal_type:
                continue

            a = tr.find("a", href=True)
            if not a:
                continue
            link = norm_url(a["href"])

            if link in seen_links:
                continue
            seen_links.add(link)

            ticker_guess = guess_ticker(company)

            candidate_releases.append({
                "released": released,
                "company": company,
                "title": title,
                "topic": topic,
                "signal_type": signal_type,
                "ticker_guess": ticker_guess,
                "link": link,
            })

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

        # Skip if we already have rows for this release link
        if any(uid.startswith(link + "|") for uid in existing_ids):
            continue

        try:
            meta = parse_release_meta_and_text(link)
        except Exception:
            meta = {"body_text": "", "pdf_urls": []}

        ticker = (meta.get("symbol") or rel["ticker_guess"] or "").upper()
        company_name = meta.get("issuer") or rel["company"]
        market = meta.get("market") or "EURONEXT/OSLO"
        date_filed = meta.get("date_filed") or rel["released"]

        if osebx and ticker and ticker not in osebx:
            continue

        trades = extract_trades_prefer_pdf_then_fallback(meta)

        if trades:
            # If PDF contained issuer name, prefer it
            if trades[0].get("_issuer_from_pdf"):
                company_name = trades[0].get("_issuer_from_pdf") or company_name

            for i, t in enumerate(trades, start=1):
                uid = f"{link}|{i}"
                row = {
                    "Unique_ID": uid,
                    "Date filed": date_filed,
                    "Trade date": t.get("Trade date", ""),
                    "Ticker": ticker,
                    "Company": company_name,
                    "Insider name": t.get("Insider name", ""),
                    "Role": t.get("Role", ""),
                    "Transaction": t.get("Transaction", ""),
                    "Shares": t.get("Shares", ""),
                    "Price": t.get("Price", ""),
                    "Value": t.get("Value", ""),
                    "Ownership after": t.get("Ownership after", ""),
                    "Source link": link,
                    "Market": market,
                    "Topic": rel["topic"],
                    "Signal_type": rel["signal_type"],
                    "Title": rel["title"],
                }
                if uid not in existing_ids:
                    new_rows.append(row)
        else:
            # Keep a signal row even if parsing failed
            uid = f"{link}|0"
            row = {
                "Unique_ID": uid,
                "Date filed": date_filed,
                "Trade date": "",
                "Ticker": ticker,
                "Company": company_name,
                "Insider name": "",
                "Role": "",
                "Transaction": "",
                "Shares": "",
                "Price": "",
                "Value": "",
                "Ownership after": "",
                "Source link": link,
                "Market": market,
                "Topic": rel["topic"],
                "Signal_type": rel["signal_type"],
                "Title": rel["title"],
            }
            if uid not in existing_ids:
                new_rows.append(row)

    all_rows = existing_rows + new_rows

    os.makedirs("data", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(all_rows)

    print(f"Candidate releases across first pages: {len(candidate_releases)}")
    print(f"Added {len(new_rows)} new rows. Total rows: {len(all_rows)}")

    # Tip in logs (helps when pdfplumber is missing)
    if new_rows and any(r.get("Insider name","").startswith("(Aggregate") for r in new_rows):
        print("Note: Some rows were aggregated. If you want person-level detail, ensure PDFs are parsed (pdfplumber).")

if __name__ == "__main__":
    main()
