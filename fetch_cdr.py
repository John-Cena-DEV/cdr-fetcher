import requests
import csv
import json
import os
import sys
import time
import pandas as pd
from datetime import datetime

# ================================================================
# CONFIG
# ================================================================
API_KEY  = "KK01b6bcdbcad7fdfced420ada0186393b"
USERNAME = "qht_regrow"
URL      = "https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails"

MAX_RETRIES     = 3
RETRY_BASE_S    = 15
RETRYABLE_CODES = {429, 500, 502, 503, 504}


# ================================================================
# FETCH  — uses exact same request style as the working script
#          (GET + data=payload + allow_nan), plus retries
# ================================================================
def fetch_cdr_data():
    now       = datetime.now()
    from_date = now.strftime("%Y-%m-%d 00:00:00")
    to_date   = now.strftime("%Y-%m-%d 23:59:59")

    print(f"\n📞 Fetching CDR data")
    print(f"   From : {from_date}")
    print(f"   To   : {to_date}")
    print(f"   User : {USERNAME}")

    payload = json.dumps(
        {"fromDate": from_date, "toDate": to_date, "userName": USERNAME},
        allow_nan=True          # ← kept exactly as in working script
    )
    headers = {
        "apiKey":       API_KEY,
        "Content-Type": "application/json",
        "accept":       "application/json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n🔄 Attempt {attempt}/{MAX_RETRIES}...")
        try:
            # ── same call style as the working script ──
            response = requests.request(
                method="GET",
                url=URL,
                headers=headers,
                data=payload,
                timeout=60,     # ← increased from 30 s
            )
            print(f"   Status : {response.status_code}")

            # ── 200 OK ──
            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    print(f"   ❌ JSON decode error: {e}")
                    print(f"   Raw: {response.text[:300]}")
                    return None

                if "details" not in data:
                    print(f"   ❌ 'details' key missing.")
                    print(f"   Keys    : {list(data.keys()) if isinstance(data, dict) else type(data)}")
                    print(f"   Response: {str(data)[:300]}")
                    return None

                records = data["details"]
                print(f"   ✅ Got {len(records)} records.")
                return records

            # ── 401 – wrong credentials, no point retrying ──
            elif response.status_code == 401:
                print("   ❌ 401 Unauthorised — check API_KEY / USERNAME.")
                return None

            # ── transient errors → retry with back-off ──
            elif response.status_code in RETRYABLE_CODES:
                wait = RETRY_BASE_S * (2 ** (attempt - 1))   # 15 s → 30 s → 60 s
                if attempt < MAX_RETRIES:
                    print(f"   ⏳ {response.status_code} — retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"   ❌ {response.status_code} — max retries exhausted.")
                    return None

            # ── anything else ──
            else:
                print(f"   ❌ Error {response.status_code}: {response.text[:300]}")
                return None

        except requests.exceptions.Timeout:
            wait = RETRY_BASE_S * (2 ** (attempt - 1))
            print(f"   ⏱️  Timeout on attempt {attempt}.")
            if attempt < MAX_RETRIES:
                print(f"   Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print("   ❌ Timeout — max retries exhausted.")
                return None

        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request error: {e}")
            return None

    return None


# ================================================================
# DEDUP  — pandas-based logic from the working script
#          (drop_duplicates on CallID for inbound only)
# ================================================================
def dedup_inbound(records):
    if not records:
        return records

    df = pd.DataFrame(records)

    if "CallID" not in df.columns or "Type" not in df.columns:
        print("⚠️  Skipping dedup — CallID/Type columns missing.")
        print(f"   Available columns: {list(df.columns)}")
        return records

    inbound_df = df[df["Type"].str.lower() == "inbound"]
    other_df   = df[df["Type"].str.lower() != "inbound"]

    inbound_deduped = inbound_df.drop_duplicates(subset=["CallID"], keep="first")

    print(f"   Inbound : {len(inbound_df)} → {len(inbound_deduped)} after dedup")
    print(f"   Other   : {len(other_df)}")

    merged = pd.concat([inbound_deduped, other_df], ignore_index=True)

    # ── clean exactly as the working script does ──
    merged = merged.fillna("")
    merged = merged.astype(str)

    return merged.to_dict(orient="records")


# ================================================================
# SAVE  — same CSV layout as original script 1
#         Row 1 : header "data"
#         Row 2 : entire JSON array as a single string  (GSheet-compatible)
# ================================================================
def save_to_csv(records, filename="cdr_data_master.csv"):
    if not records:
        print("⚠️  No records to save.")
        return None

    try:
        json_string = json.dumps(records, ensure_ascii=False)

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["data"])        # Row 1 — header  (rows[0][0])
            writer.writerow([json_string])   # Row 2 — JSON    (rows[1][0])

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

    # 1. Fetch
    records = fetch_cdr_data()
    if records is None:
        print("=" * 60)
        print("❌ Process failed — no data received.")
        print("=" * 60)
        sys.exit(1)

    # 2. Dedup
    print(f"\n🔁 Deduplicating inbound calls...")
    print(f"   Total before : {len(records)}")
    records = dedup_inbound(records)
    print(f"   Total after  : {len(records)}")

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
