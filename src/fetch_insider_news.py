import csv
import os
import re
import time
import hashlib
from pathlib import Path
from typing import List, Dict

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
    Optional: requires pdfplumber.
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
    return clean(" ".join(parts))

def _to_intish(x: str) -> str:
    if x is None:
        return ""
    x = x.replace(" ", "").replace(",", "")
    x = re.sub(r"[^\d]", "", x)
    return x

# -----------------------------
# Euronext detail page parsing
# -----------------------------

def parse_release_meta_and_text(release_url: str) -> Dict:
    html = cached_get_text(release_url)
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    # Date filed like: "30 Jan 2026 10:00 CET"
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

    return {
        "date_filed": date_filed,
        "issuer": issuer,
        "symbol": symbol,
        "market": market,
        "body_text": page_text,   # <-- “press release directly in the search engine”
        "pdf_urls": list(dict.fromkeys(pdf_urls)),
    }

# -----------------------------
# Trade extraction (regex)
# -----------------------------

def extract_trades_from_text(text: str) -> List[Dict]:
    """
    Extract multiple people lines.
    Handles: acquired / purchased / bought / sold
    Works with and without "New holding is ..."
    """
    t = clean(text)

    # Common price line (often one price for the program day)
    common_price = ""
    m_price = re.search(
        r"\bprice\s+for\s+the\s+shares\s+was\s+(?P<ccy>[A-Z]{3})\s*(?P<p>[\d]+(?:[.,]\d+)?)",
        t,
        re.IGNORECASE
    )
    if m_price:
        common_price = f"{m_price.group('ccy').upper()} {m_price.group('p').replace(',', '.')}"

    trades: List[Dict] = []

    # Pattern A: with "New holding is X shares"
    pat_with_holding = re.compile(
        r"(?P<name>[A-ZÆØÅ][A-Za-zÆØÅæøå\-\.\s]+?),\s*"
        r"(?P<role>[^,]{3,140}?),\s*.*?\b"
        r"(?P<verb>acquired|purchased|bought|sold)\b\s*"
        r"(?P<shares>[\d\.,\s]+)\s*shares\b.*?\b"
        r"New\s+holding\s+is\s*(?P<own>[\d\.,\s]+)\s*shares\b",
        re.IGNORECASE
    )

    for m in pat_with_holding.finditer(t):
        verb = m.group("verb").lower()
        txn = "BUY" if verb in ("acquired", "purchased", "bought") else "SELL"
        trades.append({
            "Insider name": clean(m.group("name")),
            "Role": clean(m.group("role")),
            "Transaction": txn,
            "Shares": _to_intish(m.group("shares")),
            "Price": common_price,
            "Ownership after": _to_intish(m.group("own")),
        })

    if trades:
        return trades

    # Pattern B: without "New holding" (still capture name/role/verb/shares)
    pat_basic = re.compile(
        r"(?P<name>[A-ZÆØÅ][A-Za-zÆØÅæøå\-\.\s]+?),\s*"
        r"(?P<role>[^,]{3,140}?),\s*.*?\b"
        r"(?P<verb>acquired|purchased|bought|sold)\b\s*"
        r"(?P<shares>[\d\.,\s]+)\s*shares\b",
        re.IGNORECASE
    )

    for m in pat_basic.finditer(t):
        verb = m.group("verb").lower()
        txn = "BUY" if verb in ("acquired", "purchased", "bought") else "SELL"
        trades.append({
            "Insider name": clean(m.group("name")),
            "Role": clean(m.group("role")),
            "Transaction": txn,
            "Shares": _to_intish(m.group("shares")),
            "Price": common_price,
            "Ownership after": "",
        })

    return trades

def extract_trades_with_pdf_fallback_only_if_needed(meta: Dict) -> List[Dict]:
    """
    1) Try extracting from the press release text on Euronext page.
    2) ONLY if we found 0 trades: try PDFs.
    """
    # 1) Press release text (Euronext page)
    trades = extract_trades_from_text(meta.get("body_text", ""))
    if trades:
        return trades

    # 2) PDF fallback
    pdf_urls = meta.get("pdf_urls", []) or []
    for pdf_url in pdf_urls[:3]:  # cap
        try:
            pdf_path = f"data/cache_pdf/{hashlib.md5(pdf_url.encode('utf-8')).hexdigest()}.pdf"
            download_file(pdf_url, pdf_path)
            pdf_text = try_pdf_to_text(pdf_path)
            if not pdf_text:
                continue
            trades = extract_trades_from_text(pdf_text)
            if trades:
                return trades
        except Exception:
            continue

    return []

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

    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    trs = soup.find_all("tr")

    candidate_releases = []
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

        trades = extract_trades_with_pdf_fallback_only_if_needed(meta)

        if trades:
            for i, t in enumerate(trades, start=1):
                uid = f"{link}|{i}"
                row = {
                    "Unique_ID": uid,
                    "Date filed": date_filed,
                    "Trade date": "",
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
            # Keep a signal row even if we couldn't parse trade details
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

    print(f"Candidate releases on page: {len(candidate_releases)}")
    print(f"Added {len(new_rows)} new rows. Total rows: {len(all_rows)}")

if __name__ == "__main__":
    main()
