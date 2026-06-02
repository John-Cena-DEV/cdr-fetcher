import requests
import json
import csv
import os
import sys
import time
import pandas as pd
from datetime import datetime

# ================================================================
# CONFIG
# ================================================================
API_KEY      = "KK01b6bcdbcad7fdfced420ada0186393b"
USERNAME     = "qht_regrow"
URL          = "https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails"
CSV_FILENAME = "cdr_data_master.csv"

MAX_RETRIES  = 3
RETRY_BASE_S = 15


# ================================================================
# FETCH (with retry — from working script)
# ================================================================
def fetch_cdr_data():
    now       = datetime.now()
    from_date = now.strftime("%Y-%m-%d 00:00:00")
    to_date   = now.strftime("%Y-%m-%d 23:59:59")

    print(f"\n📞 Fetching CDR data")
    print(f"   From : {from_date}")
    print(f"   To   : {to_date}")

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

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n🔄 Attempt {attempt}/{MAX_RETRIES}...")
        try:
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
                    return None

                if "details" not in data:
                    print(f"   ❌ 'details' key missing. Keys: {list(data.keys())}")
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
                print("   ❌ 401 Unauthorised — check API key / username.")
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
# DEDUP INBOUND (pandas approach — from working script)
# ================================================================
def dedup_inbound(records):
    df = pd.DataFrame(records)
    print(f"   Total before dedup : {len(df)}")

    if "CallID" in df.columns and "Type" in df.columns:
        inbound_df = df[df["Type"].str.lower() == "inbound"]
        inbound_df = inbound_df.drop_duplicates(subset=["CallID"], keep="first")
        other_df   = df[df["Type"].str.lower() != "inbound"]
        df = pd.concat([inbound_df, other_df], ignore_index=True)
        print(f"   Inbound deduped    : {len(inbound_df)}")
        print(f"   Other (as-is)      : {len(other_df)}")
    else:
        print("   ⚠️  CallID/Type columns missing — skipping dedup")

    df = df.fillna("")
    df = df.astype(str)
    print(f"   Total after dedup  : {len(df)}")

    return df.to_dict(orient="records")


# ================================================================
# SAVE JSON CSV FOR GITHUB
#   Row 1 : "data"
#   Row 2 : entire JSON array as string in cell A2
# ================================================================
def save_json_csv(records):
    print(f"\n💾 Saving → '{CSV_FILENAME}'")
    try:
        json_string = json.dumps(records, ensure_ascii=False)

        with open(CSV_FILENAME, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["data"])
            writer.writerow([json_string])

        print(f"   ✅ Saved — {len(records)} records as JSON in A2")
        return True
    except Exception as e:
        print(f"   ❌ Save failed: {e}")
        return False


# ================================================================
# MAIN
# ================================================================
def main():
    print("=" * 60)
    print("🚀 CDR Fetch → Dedup → GitHub JSON CSV")
    print("=" * 60)

    records = fetch_cdr_data()
    if records is None:
        print("\n❌ No data received. Exiting.")
        sys.exit(1)

    print(f"\n🔁 Deduplicating inbound calls...")
    clean_records = dedup_inbound(records)

    success = save_json_csv(clean_records)

    print("\n" + "=" * 60)
    if success:
        print(f"✅ Done — '{CSV_FILENAME}' ready for GitHub commit")
    else:
        print("❌ Failed.")
    print("=" * 60)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
