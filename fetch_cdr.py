import requests
import csv
import json
import os
import sys
import time
from datetime import datetime

# ================================================================
# CONFIG — read from environment (set via GitHub Actions Secrets)
# ================================================================
API_KEY  = os.environ.get("OZONETEL_API_KEY",  "KK01b6bcdbcad7fdfced420ada0186393b")
USERNAME = os.environ.get("OZONETEL_USERNAME", "qht_regrow")
URL      = "https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails"

MAX_RETRIES  = 3
RETRY_BASE_S = 15   # 15s → 30s → 60s on 429


# ================================================================
# FETCH CDR
# ================================================================
def fetch_cdr_data():
    """
    Fetch CDR data using the CORRECT auth method confirmed by the
    working Google Sheets script:
      - Method  : GET
      - Auth    : apiKey sent as a REQUEST HEADER (not Bearer token)
      - Payload : JSON sent as raw body string via data= (not json= or params=)
      - Response: records are under data["details"]
    """
    now       = datetime.now()
    from_date = now.strftime("%Y-%m-%d 00:00:00")
    to_date   = now.strftime("%Y-%m-%d 23:59:59")

    print(f"\n📞 Fetching CDR data")
    print(f"   From : {from_date}")
    print(f"   To   : {to_date}")
    print(f"   User : {USERNAME}")

    headers = {
        "apiKey":       API_KEY,        # ← correct auth: header, not Bearer token
        "Content-Type": "application/json",
        "accept":       "application/json",
    }

    payload_dict = {
        "fromDate": from_date,
        "toDate":   to_date,
        "userName": USERNAME,
    }
    # Must be sent as raw string body (data=), not json= or params=
    payload = json.dumps(payload_dict)

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n🔄 Attempt {attempt}/{MAX_RETRIES}...")
        try:
            response = requests.request(
                method="GET",
                url=URL,
                headers=headers,
                data=payload,       # ← raw JSON body, confirmed working
                timeout=30,
            )
            print(f"   Status : {response.status_code}")

            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    print(f"   ❌ JSON decode error: {e}")
                    print(f"   Raw: {response.text[:300]}")
                    return None

                # Response structure: {"details": [...], ...}
                if "details" not in data:
                    print(f"   ❌ Unexpected response — 'details' key missing.")
                    print(f"   Keys received: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                    print(f"   Response: {str(data)[:300]}")
                    return None

                records = data["details"]
                print(f"   ✅ Fetched {len(records)} records.")
                return records

            elif response.status_code == 429:
                wait = RETRY_BASE_S * (2 ** (attempt - 1))
                if attempt < MAX_RETRIES:
                    print(f"   ⏳ Rate limited (429). Waiting {wait}s before retry...")
                    time.sleep(wait)
                else:
                    print("   ❌ Rate limited — max retries exhausted.")
                    return None

            elif response.status_code == 401:
                print("   ❌ 401 Unauthorised — check OZONETEL_API_KEY and OZONETEL_USERNAME.")
                return None

            elif response.status_code == 405:
                print("   ❌ 405 Method Not Allowed — check API URL.")
                return None

            else:
                print(f"   ❌ Unexpected error {response.status_code}: {response.text[:300]}")
                return None

        except requests.exceptions.Timeout:
            print(f"   ⏱️  Timeout on attempt {attempt}.")
            if attempt < MAX_RETRIES:
                time.sleep(10)
        except requests.exceptions.ConnectionError as e:
            print(f"   🔌 Connection error: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request error: {e}")
            return None

    print("❌ All retry attempts exhausted.")
    return None


# ================================================================
# DEDUP  (inbound calls only, preserve all other types as-is)
# ================================================================
def dedup_inbound(records):
    """
    Mirror the dedup logic from the working GSheet script:
    - Remove duplicate CallID only for inbound calls
    - Leave all other call types untouched
    """
    if not records:
        return records

    has_call_id = "CallID" in records[0]
    has_type    = "Type"   in records[0]

    if not (has_call_id and has_type):
        print(f"⚠️  Skipping dedup — required columns missing. Found: {list(records[0].keys())}")
        return records

    inbound  = [r for r in records if str(r.get("Type", "")).lower() == "inbound"]
    other    = [r for r in records if str(r.get("Type", "")).lower() != "inbound"]

    # Dedup inbound by CallID (keep first occurrence)
    seen     = set()
    deduped  = []
    for r in inbound:
        cid = r.get("CallID")
        if cid not in seen:
            seen.add(cid)
            deduped.append(r)

    print(f"   Inbound  : {len(inbound)} → {len(deduped)} after dedup")
    print(f"   Other    : {len(other)}")

    return deduped + other


# ================================================================
# SAVE TO CSV
# ================================================================
def save_to_csv(records, filename="cdr_data_master.csv"):
    if not records:
        print("⚠️  No records to save.")
        return None

    try:
        # Normalise all values to strings (mirrors GSheet script)
        cleaned = []
        for row in records:
            clean_row = {}
            for k, v in row.items():
                if v is None:
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

    # 1. Fetch
    records = fetch_cdr_data()
    if records is None:
        print("=" * 60)
        print("❌ Process failed — no data received from API.")
        print("=" * 60)
        sys.exit(1)

    # 2. Dedup inbound calls
    print(f"\n🔁 Deduplicating inbound calls...")
    print(f"   Total before dedup : {len(records)}")
    records = dedup_inbound(records)
    print(f"   Total after dedup  : {len(records)}")

    # 3. Save
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
