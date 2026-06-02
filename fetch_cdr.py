import requests
import csv
import json
import sys
import time
import random
from datetime import datetime

# ================================================================
# CONFIG
# ================================================================
API_KEY  = "KK01b6bcdbcad7fdfced420ada0186393b"
USERNAME = "qht_regrow"
URL      = "https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails"

TIMEOUT        = (10, 120)   # (connect, read) seconds
TIME_BUDGET_S  = 600         # total wall-clock budget to keep retrying (10 min)
BACKOFF_BASE_S = 15          # base wait between cycles
BACKOFF_CAP_S  = 90          # max wait between cycles
METHODS        = ("GET", "POST")   # try GET-with-body first, then POST as fallback
RETRYABLE      = {429, 500, 502, 503, 504}


# ================================================================
# ONE HTTP TRY (returns: ("ok", records) | ("fatal", None) | ("retry", None))
# ================================================================
def try_once(method, headers, payload):
    try:
        response = requests.request(
            method=method,
            url=URL,
            headers=headers,
            data=payload,
            timeout=TIMEOUT,
        )
        print(f"      [{method}] Status : {response.status_code}")

        if response.status_code == 200:
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"      ❌ JSON decode error: {e}")
                print(f"      Raw: {response.text[:200]}")
                return ("retry", None)   # a 504 HTML body lands here too -> keep trying

            if "details" not in data:
                keys = list(data.keys()) if isinstance(data, dict) else type(data)
                print(f"      ❌ 'details' key missing. Keys: {keys}")
                print(f"      Response: {str(data)[:200]}")
                return ("fatal", None)   # valid JSON but wrong shape -> retrying won't help

            records = data["details"]
            print(f"      ✅ Got {len(records)} records.")
            return ("ok", records)

        elif response.status_code == 401:
            print(f"      ❌ 401 Unauthorised — check API key / username.")
            return ("fatal", None)

        elif response.status_code in RETRYABLE:
            print(f"      ⚠️  {response.status_code} (Ozonetel-side) — will retry.")
            return ("retry", None)

        else:
            print(f"      ❌ Error {response.status_code}: {response.text[:200]}")
            return ("retry", None)

    except requests.exceptions.Timeout:
        print(f"      ⏱️  [{method}] read timeout (>{TIMEOUT[1]}s) — will retry.")
        return ("retry", None)
    except requests.exceptions.RequestException as e:
        print(f"      ❌ [{method}] request error: {e} — will retry.")
        return ("retry", None)


# ================================================================
# FETCH (full-day window, GET->POST fallback, patient retry budget)
# ================================================================
def fetch_cdr_data():
    now       = datetime.now()
    from_date = now.strftime("%Y-%m-%d 00:00:00")
    to_date   = now.strftime("%Y-%m-%d 23:59:59")

    print(f"\n📞 Fetching CDR data")
    print(f"   From   : {from_date}")
    print(f"   To     : {to_date}")
    print(f"   User   : {USERNAME}")
    print(f"   Budget : up to {TIME_BUDGET_S}s of retries")

    headers = {
        "apiKey":       API_KEY,
        "Content-Type": "application/json",
        "accept":       "application/json",
    }
    payload = json.dumps({
        "fromDate": from_date,
        "toDate":   to_date,
        "userName": USERNAME,
    }, allow_nan=True)

    deadline = time.time() + TIME_BUDGET_S
    cycle = 0

    while True:
        cycle += 1
        print(f"\n🔄 Cycle {cycle}...")

        for method in METHODS:
            status, records = try_once(method, headers, payload)
            if status == "ok":
                return records
            if status == "fatal":
                return None
            # "retry" -> fall through and try the next method / next cycle

        remaining = deadline - time.time()
        if remaining <= 0:
            print(f"\n   ❌ Retry budget exhausted ({TIME_BUDGET_S}s). Ozonetel API is")
            print(f"      not responding — this is server-side, not a script bug.")
            print(f"      Re-run once their API recovers.")
            return None

        wait = min(BACKOFF_CAP_S, BACKOFF_BASE_S * (2 ** (cycle - 1)))
        wait = min(wait + random.uniform(0, 5), remaining)   # jitter, never overshoot budget
        print(f"   ⏳ Both methods failed this cycle. Waiting {wait:.0f}s "
              f"({remaining:.0f}s budget left)...")
        time.sleep(wait)


# ================================================================
# DEDUP  (inbound only)
# ================================================================
def dedup_inbound(records):
    if not records:
        return records

    if "CallID" not in records[0] or "Type" not in records[0]:
        print(f"⚠️  Skipping dedup — columns missing.")
        return records

    inbound = [r for r in records if str(r.get("Type", "")).lower() == "inbound"]
    other   = [r for r in records if str(r.get("Type", "")).lower() != "inbound"]

    seen, deduped = set(), []
    for r in inbound:
        cid = r.get("CallID")
        if cid not in seen:
            seen.add(cid)
            deduped.append(r)

    print(f"   Inbound : {len(inbound)} → {len(deduped)} after dedup")
    print(f"   Other   : {len(other)}")
    return deduped + other


# ================================================================
# SAVE — full JSON in cell A2 (GSheet importCDRFromGitHub() compatible)
# ================================================================
def save_to_csv(records, filename="cdr_data_master.csv"):
    if not records:
        print("⚠️  No records to save.")
        return None

    try:
        json_string = json.dumps(records, ensure_ascii=False)
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["data"])          # Row 1 — header (rows[0][0])
            writer.writerow([json_string])     # Row 2 — JSON  (rows[1][0])

        print(f"\n💾 Saved '{filename}'")
        print(f"   Records : {len(records)}")
        print(f"   Format  : JSON string in cell A2 (GSheet-compatible)")
        return filename

    except Exception as e:
        print(f"❌ Error saving file: {e}")
        import traceback
        traceback.print_exc()
        return None


# ================================================================
# MAIN
# ================================================================
def main():
    print("=" * 60)
    print("🚀 CDR Data Fetch — Starting")
    print("=" * 60)

    records = fetch_cdr_data()

    if records is None:
        print("=" * 60)
        print("❌ Process failed — no data received (see reason above).")
        print("=" * 60)
        sys.exit(1)

    print(f"\n🔁 Deduplicating inbound calls...")
    print(f"   Total before : {len(records)}")
    records = dedup_inbound(records)
    print(f"   Total after  : {len(records)}")

    result = save_to_csv(records)
    print("=" * 60)
    if result:
        print(f"✅ Done — output: {result}")
        print("=" * 60)
        sys.exit(0)
    else:
        print("❌ Save failed.")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
