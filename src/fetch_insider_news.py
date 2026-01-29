import csv
import os
import re
import requests
from bs4 import BeautifulSoup

URL = "https://live.euronext.com/en/products/equities/company-news"

# Kun disse fire topic-ene
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

def load_osebx(path: str) -> set[str]:
    with open(path, newline="", encoding="utf-8") as f:
        return {row[0].strip().upper() for row in csv.reader(f) if row and row[0].strip()}

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def make_unique_id(company: str, released: str, title: str, link: str) -> str:
    return f"{company}|{released}|{title[:80]}|{link}"

def guess_ticker(text: str) -> str:
    m = re.search(r"\(([A-Z0-9]{2,10})\)", text or "")
    if m:
        return m.group(1)
    m = re.search(r"\b[A-Z0-9]{2,10}\b", text or "")
    return m.group(0) if m else ""

def main():
    osebx = load_osebx("data/osebx_tickers.csv") if os.path.exists("data/osebx_tickers.csv") else set()

    r = requests.get(URL, headers={"User-Agent":"Mozilla/5.0"}, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # Finn alle tabellrader på siden
    trs = soup.find_all("tr")

    rows = []
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

        ticker = guess_ticker(company)

        # hvis du har osebx-liste, filtrer på den. ellers: ta alt.
        if osebx and ticker and ticker not in osebx:
            continue

        a = tr.find("a", href=True)
        if not a:
            continue

        link = a["href"]
        if link.startswith("/"):
            link = "https://live.euronext.com" + link

        rows.append({
            "Unique_ID": make_unique_id(company, released, title, link),
            "Date filed": released,
            "Trade date": "",
            "Ticker": ticker,
            "Company": company,
            "Insider name": "",
            "Role": "",
            "Transaction": "",
            "Shares": "",
            "Price": "",
            "Value": "",
            "Ownership after": "",
            "Source link": link,
            "Market": "EURONEXT/OSLO",
            "Topic": topic,
            "Signal_type": signal_type,
            "Title": title,
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

    new_rows = [rr for rr in rows if rr["Unique_ID"] not in existing_ids]
    all_rows = existing_rows + new_rows

    os.makedirs("data", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(all_rows)

    print(f"Matched rows on page: {len(rows)}")
    print(f"Added {len(new_rows)} new rows. Total rows: {len(all_rows)}")

if __name__ == "__main__":
    main()
