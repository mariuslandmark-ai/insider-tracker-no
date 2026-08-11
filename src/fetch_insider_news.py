import csv
import os
import re
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict

import requests

LIST_URL = "https://api3.oslo.oslobors.no/v1/newsreader/list"
MESSAGE_URL = "https://api3.oslo.oslobors.no/v1/newsreader/message"

CATEGORY_SIGNAL_MAP = {
    "1102": "INSIDER_TRADE",   # MANAGERS' TRANSACTION
    "1006": "FLAGGING",        # MAJOR SHAREHOLDINGS NOTIFICATION
    "1007": "BUYBACK",         # ACQUISITION OR DISPOSAL OF AN ISSUER'S OWN SHARES
}

FIELDNAMES = [
    "Unique_ID", "Date filed", "Ticker", "Company", "Issuer_ID",
    "Category_no", "Category_en", "Signal_type", "Title",
    "Insider name", "Role", "Transaction", "Shares",
    "New_holding_shares", "New_holding_pct", "Price_NOK",
    "Market", "Num_attachments", "News_ID", "Message_URL",
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


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def fetch_messages_for_category(category_id: str, from_date: str, to_date: str) -> List[Dict]:
    params = {
        "category": category_id, "issuer": "", "fromDate": from_date,
        "toDate": to_date, "market": "XOSL", "messageTitle": "",
    }
    r = requests.post(LIST_URL, headers=HEADERS, params=params, data="", timeout=30)
    log.info(f"Category {category_id}: POST -> {r.status_code}, {len(r.content)} bytes")
    r.raise_for_status()
    payload = r.json()
    if payload.get("header", {}).get("result.val") != 0:
        log.warning(f"Category {category_id}: non-OK result: {payload.get('header')}")
        return []
    messages = payload.get("data", {}).get("messages", [])
    log.info(f"Category {category_id}: {len(messages)} messages returned")
    return messages


def fetch_message_body(message_id: int, sleep_s: float = 0.3) -> str:
    time.sleep(sleep_s)
    params = {"messageId": str(message_id)}
    r = requests.post(MESSAGE_URL, headers=HEADERS, params=params, data="", timeout=30)
    r.raise_for_status()
    payload = r.json()
    if payload.get("header", {}).get("result.val") != 0:
        log.warning(f"Message {message_id}: non-OK result: {payload.get('header')}")
        return ""
    return payload.get("data", {}).get("message", {}).get("body", "") or ""


# -----------------------------
# Free-text transaction parsing
# -----------------------------
# Four observed phrasings so far:
#   A. "NAME, ROLE, today VERB N shares [...]. New holding is M shares, P% of the outstanding shares." (StrongPoint)
#   B. "NAME, ROLE... has on DATE VERB N shares... at a price of NOK X per share. [...] holds M shares." (OKEA)
#   C. "NAME, ROLE, will have N share options cancelled. [...] will hold M shares [or 'no shares']." (Elkem — not a market trade)
#   D. "---- NAME - N shares" bullet list, RSU/PSU vesting (TGS — not a market trade)
# Some announcements (e.g. Nordic Semiconductor) reference a prior announcement
# and contain no extractable per-person data in the body at all — these fall
# through to the |0 metadata-only row, which is correct, not a parsing bug.

_PERSON_TODAY_PAT = re.compile(
    r"(?P<name>[A-ZÆØÅ][\wÆØÅæøå .\-]+?),\s*"
    r"(?P<role>[^.,]{3,120}?),?\s*"
    r"today\s+(?P<verb>acquired|purchased|bought|sold|disposed of)\s+"
    r"(?P<shares>[\d,\.\s]+)\s*shares\b"
    r"(?:.*?New holding is\s+(?P<newholding>[\d,\.\s]+)\s*shares,\s*"
    r"(?P<pct>[\d\.]+)%\s*of the outstanding shares)?",
    re.IGNORECASE | re.DOTALL,
)

_COMPANY_TODAY_PAT = re.compile(
    r"(?P<issuer>[A-ZÆØÅ][\w &.\-]+?)\s+has\s+today\s+"
    r"(?P<verb>acquired|purchased|bought|sold|disposed of)\s+"
    r"(?P<shares>[\d,\.\s]+)\s*shares\b"
    r"(?:.*?New holding is\s+(?P<newholding>[\d,\.\s]+)\s*shares,\s*"
    r"(?P<pct>[\d\.]+)%\s*of the outstanding shares)?",
    re.IGNORECASE | re.DOTALL,
)

_PERSON_HAS_ON_PAT = re.compile(
    r"(?P<name>[A-ZÆØÅ][\wÆØÅæøå .\-]+?),\s*"
    r"(?P<role>[^.,]{3,150}?),?\s*"
    r"has on\s+\d{1,2}\s+\w+\s+\d{4}\s+"
    r"(?P<verb>acquired|purchased|bought|sold|disposed of)\s+"
    r"(?P<shares>[\d,\.\s]+)\s*shares\b.*?"
    r"at a price of\s+NOK\s+(?P<price>[\d\.,]+)\s*per share\."
    r".*?holds\s+(?P<newholding>[\d,\.\s]+)\s*shares",
    re.IGNORECASE | re.DOTALL,
)

_OPTION_CANCEL_PAT = re.compile(
    r"(?P<name>[A-ZÆØÅ][\wÆØÅæøå .\-]+?),\s*"
    r"(?P<role>[^.,]{3,120}?),?\s*"
    r"will have\s+(?P<options>[\d,\.\s]+)\s*share options cancelled\."
    r".*?will hold\s+(?P<newholding>[\d,\.\s]+|no)\s*shares",
    re.IGNORECASE | re.DOTALL,
)

_VESTING_BULLET_PAT = re.compile(
    r"----\s*(?P<name>[A-ZÆØÅ][\wÆØÅæøå .\-]+?)\s*-\s*(?P<shares>[\d,\.\s]+)\s*shares",
    re.IGNORECASE,
)

_PRICE_PAT = re.compile(r"price for the shares was\s+NOK\s+([\d\.,]+)", re.IGNORECASE)


def _verb_to_txn(verb: str) -> str:
    v = (verb or "").lower()
    if v in ("acquired", "purchased", "bought"):
        return "BUY"
    if v in ("sold", "disposed of"):
        return "SELL"
    return ""


def _to_int(x: str) -> str:
    return re.sub(r"[^\d]", "", x or "")


def parse_transactions_from_body(body: str) -> List[Dict]:
    if not body:
        return []

    results = []
    matched_spans = []

    def overlaps(span):
        return any(s <= span[0] < e or s < span[1] <= e for s, e in matched_spans)

    global_price_match = _PRICE_PAT.search(body)
    global_price = global_price_match.group(1) if global_price_match else ""

    # Pattern A: "today acquired/sold ... New holding is ... %" (person)
    for m in _PERSON_TODAY_PAT.finditer(body):
        if overlaps(m.span()):
            continue
        matched_spans.append(m.span())
        results.append({
            "Insider name": clean(m.group("name")),
            "Role": clean(m.group("role")),
            "Transaction": _verb_to_txn(m.group("verb")),
            "Shares": _to_int(m.group("shares")),
            "New_holding_shares": _to_int(m.group("newholding") or ""),
            "New_holding_pct": clean(m.group("pct") or ""),
            "Price_NOK": global_price,
        })

    # Pattern A2: "today acquired/sold ... New holding is ... %" (issuer/own-account)
    for m in _COMPANY_TODAY_PAT.finditer(body):
        if overlaps(m.span()):
            continue
        matched_spans.append(m.span())
        results.append({
            "Insider name": clean(m.group("issuer")) + " (issuer)",
            "Role": "Issuer (own-account transaction)",
            "Transaction": _verb_to_txn(m.group("verb")),
            "Shares": _to_int(m.group("shares")),
            "New_holding_shares": _to_int(m.group("newholding") or ""),
            "New_holding_pct": clean(m.group("pct") or ""),
            "Price_NOK": global_price,
        })

    # Pattern B: "has on DATE sold ... at a price of NOK X per share. ... holds Y shares"
    for m in _PERSON_HAS_ON_PAT.finditer(body):
        if overlaps(m.span()):
            continue
        matched_spans.append(m.span())
        results.append({
            "Insider name": clean(m.group("name")),
            "Role": clean(m.group("role")),
            "Transaction": _verb_to_txn(m.group("verb")),
            "Shares": _to_int(m.group("shares")),
            "New_holding_shares": _to_int(m.group("newholding") or ""),
            "New_holding_pct": "",
            "Price_NOK": clean(m.group("price") or ""),
        })

    # Pattern C: option cancellations — NOT a market trade, tagged distinctly
    for m in _OPTION_CANCEL_PAT.finditer(body):
        if overlaps(m.span()):
            continue
        matched_spans.append(m.span())
        newholding = m.group("newholding") or ""
        results.append({
            "Insider name": clean(m.group("name")),
            "Role": clean(m.group("role")),
            "Transaction": "OPTIONS_CANCELLED",
            "Shares": _to_int(m.group("options")),
            "New_holding_shares": "0" if "no" in newholding.lower() else _to_int(newholding),
            "New_holding_pct": "",
            "Price_NOK": "",
        })

    # Pattern D: RSU/PSU vesting bullet list — NOT a market trade
    for m in _VESTING_BULLET_PAT.finditer(body):
        if overlaps(m.span()):
            continue
        matched_spans.append(m.span())
        results.append({
            "Insider name": clean(m.group("name")),
            "Role": "",
            "Transaction": "VESTING",
            "Shares": _to_int(m.group("shares")),
            "New_holding_shares": "",
            "New_holding_pct": "",
            "Price_NOK": "",
        })

    return results


def main():
    to_date = datetime.now(timezone.utc).date()
    from_date = to_date - timedelta(days=3)
    from_date_str, to_date_str = from_date.isoformat(), to_date.isoformat()

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
            log.error(f"FATAL: list request failed for category {category_id}: {e}")
            sys.exit(1)

        total_fetched += len(messages)

        for m in messages:
            base_id = str(m.get("messageId") or m.get("id"))
            if not base_id:
                continue

            cats = m.get("category", [])
            cat_no = cats[0].get("category_no", "") if cats else ""
            cat_en = cats[0].get("category_en", "") if cats else ""

            base_fields = {
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
                "Message_URL": f"https://newsweb.oslobors.no/message/{base_id}",
            }

            if signal_type == "INSIDER_TRADE":
                if any(uid == f"{base_id}|0" or uid.startswith(base_id + "|") for uid in existing_ids):
                    continue

                try:
                    body = fetch_message_body(int(base_id))
                except requests.exceptions.RequestException as e:
                    log.warning(f"Message {base_id}: detail fetch failed: {e}")
                    body = ""

                txns = parse_transactions_from_body(body)

                if txns:
                    for i, t in enumerate(txns, start=1):
                        uid = f"{base_id}|{i}"
                        if uid in existing_ids:
                            continue
                        row = {**base_fields, **t, "Unique_ID": uid}
                        new_rows.append(row)
                        existing_ids.add(uid)
                else:
                    uid = f"{base_id}|0"
                    row = {**base_fields, "Unique_ID": uid}
                    new_rows.append(row)
                    existing_ids.add(uid)
            else:
                if base_id in existing_ids:
                    continue
                row = {**base_fields, "Unique_ID": base_id}
                new_rows.append(row)
                existing_ids.add(base_id)

    log.info(f"Total messages fetched across all categories: {total_fetched}")
    log.info(f"New rows to add: {len(new_rows)}")

    all_rows = existing_rows + new_rows

    os.makedirs("data", exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for row in all_rows:
            w.writerow({k: row.get(k, "") for k in FIELDNAMES})

    log.info(f"Added {len(new_rows)} new rows. Total rows: {len(all_rows)}")


if __name__ == "__main__":
    main()
