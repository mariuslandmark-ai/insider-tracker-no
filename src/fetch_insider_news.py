import csv
import json
import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict

import requests
from openai import OpenAI

LIST_URL = "https://api3.oslo.oslobors.no/v1/newsreader/list"
MESSAGE_URL = "https://api3.oslo.oslobors.no/v1/newsreader/message"

CATEGORY_SIGNAL_MAP = {
    "1102": "INSIDER_TRADE",
    "1006": "FLAGGING",
    "1007": "BUYBACK",
}

PARSE_BODY_FOR = {"INSIDER_TRADE", "FLAGGING"}

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

openai_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

EXTRACTION_PROMPT = """You are extracting structured transaction data from a Norwegian/English stock exchange announcement (Oslo Børs / NewsWeb). The announcement may describe one or more transactions by different people or entities, in English or Norwegian, in varying phrasing.

For EACH distinct transaction described (a person or entity buying, selling, or having options cancelled, or shares vesting), extract:
- name: the person or entity's full name
- role: their stated role/title (empty string if not stated, or "Issuer" for the company's own-account transactions)
- transaction: one of "BUY", "SELL", "OPTIONS_CANCELLED", "VESTING", or "OTHER" if unclear
- shares: number of shares in this transaction (integer, no commas/spaces)
- new_holding_shares: their resulting total holding after the transaction, if stated (integer, or null)
- new_holding_pct: their resulting ownership percentage, if stated (number, or null)
- price_nok: price per share in NOK, if stated (number, or null)

Only extract transactions that are actually described with at least a name and an action. Do not invent data. If the text describes no specific transaction (e.g. it only references a prior announcement), return an empty list.

Respond with ONLY a JSON array of objects with exactly these keys: name, role, transaction, shares, new_holding_shares, new_holding_pct, price_nok. No other text, no markdown formatting, just the raw JSON array."""


def clean(s: str) -> str:
    return " ".join((s or "").split())


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


def extract_transactions_via_llm(body: str, message_id: str) -> List[Dict]:
    if not body.strip():
        return []
    try:
        response = openai_client.chat.completions.create(
            model="gpt-5.6-luna",
            temperature=0,
            messages=[
                {"role": "system", "content": EXTRACTION_PROMPT},
                {"role": "user", "content": body},
            ],
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.lower().startswith("json"):
                raw = raw[4:].strip()
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            log.warning(f"Message {message_id}: LLM returned non-list JSON, skipping")
            return []
        return parsed
    except json.JSONDecodeError as e:
        log.warning(f"Message {message_id}: LLM returned invalid JSON: {e}")
        return []
    except Exception as e:
        log.warning(f"Message {message_id}: LLM extraction failed: {e}")
        return []


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
    llm_calls = 0

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

            if signal_type in PARSE_BODY_FOR:
                if any(uid == f"{base_id}|0" or uid.startswith(base_id + "|") for uid in existing_ids):
                    continue

                try:
                    body = fetch_message_body(int(base_id))
                except requests.exceptions.RequestException as e:
                    log.warning(f"Message {base_id}: detail fetch failed: {e}")
                    body = ""

                txns = extract_transactions_via_llm(body, base_id)
                llm_calls += 1

                if txns:
                    for i, t in enumerate(txns, start=1):
                        uid = f"{base_id}|{i}"
                        if uid in existing_ids:
                            continue
                        row = {
                            **base_fields,
                            "Unique_ID": uid,
                            "Insider name": clean(t.get("name", "")),
                            "Role": clean(t.get("role", "")),
                            "Transaction": clean(t.get("transaction", "")).upper(),
                            "Shares": t.get("shares") or "",
                            "New_holding_shares": t.get("new_holding_shares") or "",
                            "New_holding_pct": t.get("new_holding_pct") or "",
                            "Price_NOK": t.get("price_nok") or "",
                        }
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
    log.info(f"LLM extraction calls made: {llm_calls}")
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
