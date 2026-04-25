import requests
import csv
import json
import os
import sys
import time
from datetime import datetime

# ================================================================
# CONFIG  ← hardcoded exactly like your working GSheet script
# ================================================================
API_KEY  = "KK01b6bcdbcad7fdfced420ada0186393b"
USERNAME = "qht_regrow"
URL      = "https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails"

MAX_RETRIES  = 3
RETRY_BASE_S = 15   # 15s → 30s → 60s backoff on 429


# ================================================================
# FETCH  ← identical request pattern to your working GSheet script
# ================================================================
def fetch_cdr_data():
    now       = datetime.now()
    from_date = now.strftime("%Y-%m-%d 00:00:00")
    to_date   = now.strftime("%Y-%m-%d 23:59:59")

    print(f"\n📞 Fetching CDR data")
    print(f"   From : {from_date}")
    print(f"   To   : {to_date}")
    print(f"   User : {USERNAME}")

    # ── Exact same headers as your working script ──
    headers = {
        "apiKey":       API_KEY,
        "Content-Type": "application/json",
        "accept":       "application/json",
    }

    # ── Exact same payload as your working script ──
    payload_dict = {
        "fromDate": from_date,
        "toDate":   to_date,
        "userName": USERNAME,
    }
    payload = json.dumps(payload_dict, allow_nan=True)

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n🔄 Attempt {attempt}/{MAX_RETRIES}...")
        try:
            # ── Exact same request call as your working script ──
            response = requests.request(
                method="GET",
                url=URL,
                headers=headers,
                data=payload,
                timeout=30,
            )
            print(f"   Status : {response.status_code}")

            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    print(f"   ❌ JSON decode error: {e}")
                    print(f"   Raw response: {response.text[:300]}")
                    return None

                # ── Exact same response key as your working script ──
                if "details" not in data:
                    print(f"   ❌ 'details' key missing in response.")
                    print(f"   Keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                    print(f"   Response: {str(data)[:300]}")
                    return None

                records = data["details"]
                print(f"   ✅ Got {len(records)} records.")
                return records

            elif response.status_code == 429:
                wait = RETRY_BASE_S * (2 ** (attempt - 1))
                if attempt < MAX_RETRIES:
                    print(f"   ⏳ Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                else:
                    print("   ❌ Rate limited — max retries exhausted.")
                    return None

            elif response.status_code == 401:
                print("   ❌ 401 Unauthorised — API key or username is wrong.")
                print(f"   API_KEY used : {API_KEY[:10]}...")
                print(f"   USERNAME used: {USERNAME}")
                return None

            else:
                print(f"   ❌ Error {response.status_code}: {response.text[:300]}")
                return None

        except requests.exceptions.Timeout:
            print(f"   ⏱️  Timeout on attempt {attempt}.")
            if attempt < MAX_RETRIES:
                time.sleep(10)
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request error: {e}")
            return None

    return None


# ================================================================
# DEDUP  ← same logic as your working GSheet script
# ================================================================
def dedup_inbound(records):
    if not records:
        return records

    if "CallID" not in records[0] or "Type" not in records[0]:
        print(f"⚠️  Skipping dedup — columns missing. Found: {list(records[0].keys())}")
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
# SAVE TO CSV  ← same string-conversion logic as your working script
# ================================================================
def save_to_csv(records, filename="cdr_data_master.csv"):
    if not records:
        print("⚠️  No records to save.")
        return None

    try:
        cleaned = []
        for row in records:
            clean_row = {}
            for k, v in row.items():
                if v is None:
                    clean_row[k] = ""
                elif isinstance(v, float) and str(v) == "nan":
                    clean_row[k] = ""
                else:
                    clean_row[k] = str(v)
            cleaned.append(clean_row)

        file_existed = os.path.isfile(filename)
        fieldnames   = list(cleaned[0].keys())

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned)

        action = "Refreshed" if file_existed else "Created"
        print(f"\n💾 {action} '{filename}'  ({len(cleaned)} rows, {len(fieldnames)} columns)")
        return filename

    except Exception as e:
        print(f"❌ Error saving CSV: {e}")
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
        print("❌ Process failed — no data received.")
        print("=" * 60)
        sys.exit(1)

    print(f"\n🔁 Deduplicating inbound calls...")
    print(f"   Total before dedup : {len(records)}")
    records = dedup_inbound(records)
    print(f"   Total after dedup  : {len(records)}")

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
