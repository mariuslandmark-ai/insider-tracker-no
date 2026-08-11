import csv
import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict

import requests

API_URL = "https://api3.oslo.oslobors.no/v1/newsreader/list"

# Category IDs discovered from the NewsWeb API response:
# 1102 = MANAGERS' TRANSACTION (mandatory notification of trade, primary insiders)
# 1006 = MAJOR SHAREHOLDINGS NOTIFICATION (flagging)
# 1007 = ACQUISITION OR DISPOSAL OF AN ISSUER'S OWN SHARES (buyback)
CATEGORY_SIGNAL_MAP = {
    "1102": "INSIDER_TRADE",
    "1006": "FLAGGING",
    "1007": "BUYBACK",
}

FIELDNAMES = [
    "Unique_ID", "Date filed", "Ticker", "Company", "Issuer_ID",
    "Category_no", "Category_en", "Signal_type", "Title",
    "Market", "Num_attachments", "News_ID", "Message_URL"
]

HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,nb;q=0.8",
    "content-type": "application/json",
    "origin": "https://newsweb.oslobors.no",
    "referer": "https://newsweb.oslobors.no/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("insider_tracker")


def fetch_messages_for_category(category_id: str, from_date: str, to_date: str) -> List[Dict]:
    """
    POSTs to the NewsWeb API with an empty body (all filtering is done via
    query params). Returns the list of message dicts from data.messages.
    """
    params = {
        "category": category_id,
        "issuer": "",
        "fromDate": from_date,
        "toDate": to_date,
        "market": "XOSL",   # Oslo Børs only
        "messageTitle": "",
    }

    r = requests.post(API_URL, headers=HEADERS, params=params, data="", timeout=30)
    log.info(
        f"Category {category_id}: POST -> {r.status_code}, "
        f"{len(r.content)} bytes, url={r.url}"
    )
    r.raise_for_status()

    payload = r.json()
    header = payload.get("header", {})
    if header.get("result.val") != 0:
        log.warning(f"Category {category_id}: API returned non-OK result: {header}")
        return []

    messages = payload.get("data", {}).get("messages", [])
    overflow = payload.get("data", {}).get("overflow", False)
    log.info(f"Category {category_id}: {len(messages)} messages returned (overflow={overflow})")
    return messages


def main():
    # Look back 3 days by default — the hourly cron means we rarely need more,
    # but this gives a buffer against a missed run or API hiccup.
    to_date = datetime.now(timezone.utc).date()
    from_date = to_date - timedelta(days=3)

    from_date_str = from_date.isoformat()
    to_date_str = to_date.isoformat()

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
    total_fetched = 0

    for category_id, signal_type in CATEGORY_SIGNAL_MAP.items():
        try:
            messages = fetch_messages_for_category(category_id, from_date_str, to_date_str)
        except requests.exceptions.RequestException as e:
            log.error(f"FATAL: request failed for category {category_id}: {e}")
            sys.exit(1)

        total_fetched += len(messages)

        for m in messages:
            uid = str(m.get("messageId") or m.get("id"))
            if not uid or uid in existing_ids:
                continue

            cats = m.get("category", [])
            cat_no = cats[0].get("category_no", "") if cats else ""
            cat_en = cats[0].get("category_en", "") if cats else ""

            new_rows.append({
                "Unique_ID": uid,
                "Date filed": m.get("publishedTime", ""),
                "Ticker": m.get("issuerSign", ""),
                "Company": m.get("issuerName", ""),
                "Issuer_ID": m.get("issuerId", ""),
                "Category_no": cat_no,
                "Category_en": cat_en,
                "Signal_type": signal_type,
                "Title": m.get("title", ""),
                "Market": ",".join(m.get("markets", [])),
                "Num_attachments": m.get("numbAttachments", 0),
                "News_ID": m.get("newsId", ""),
                "Message_URL": f"https://newsweb.oslobors.no/message/{m.get('messageId', '')}",
            })
            existing_ids.add(uid)

    log.info(f"Total messages fetched across all categories: {total_fetched}")
    log.info(f"New rows to add: {len(new_rows)}")

    # Fail loudly if the API contract changed (e.g. field names renamed)
    if total_fetched > 0 and len(new_rows) == 0 and len(existing_rows) == 0:
        log.warning("Fetched messages but produced zero rows — check field mapping.")

    all_rows = existing_rows + new_rows

    os.makedirs("data", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(all_rows)

    log.info(f"Added {len(new_rows)} new rows. Total rows: {len(all_rows)}")


if __name__ == "__main__":
    main()
