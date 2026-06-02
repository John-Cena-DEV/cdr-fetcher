mport requests
import csv
import json
import sys
import time
from datetime import datetime
 
# ================================================================
# CONFIG
# ================================================================
API_KEY  = "KK01b6bcdbcad7fdfced420ada0186393b"
USERNAME = "qht_regrow"
URL      = "https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails"
 
# Mirror the WORKING sheet script: single full-day request, generous patience.
# A few light retries only to ride out short 504 blips — no chunking.
MAX_RETRIES  = 3
RETRY_BASE_S = 10           # 10s -> 20s -> 40s backoff
TIMEOUT      = (10, 180)    # (connect, read) — read kept long like the working script
RETRYABLE    = {429, 500, 502, 503, 504}
 
 
# ================================================================
# FETCH (single full-day request, same as the working script)
# ================================================================
def fetch_cdr_data():
    now       = datetime.now()
    from_date = now.strftime("%Y-%m-%d 00:00:00")
    to_date   = now.strftime("%Y-%m-%d 23:59:59")
 
    print(f"\n📞 Fetching CDR data")
    print(f"   From : {from_date}")
    print(f"   To   : {to_date}")
    print(f"   User : {USERNAME}")
 
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
                timeout=TIMEOUT,
            )
            print(f"   Status : {response.status_code}")
 
            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    print(f"   ❌ JSON decode error: {e}")
                    print(f"   Raw: {response.text[:300]}")
                    return None
 
                if "details" not in data:
                    keys = list(data.keys()) if isinstance(data, dict) else type(data)
                    print(f"   ❌ 'details' key missing. Keys: {keys}")
                    print(f"   Response: {str(data)[:300]}")
                    return None
 
                records = data["details"]
                print(f"   ✅ Got {len(records)} records.")
                return records
 
            elif response.status_code == 401:
                print(f"   ❌ 401 Unauthorised — check API key / username.")
                return None
 
            elif response.status_code in RETRYABLE:
                wait = RETRY_BASE_S * (2 ** (attempt - 1))
                label = "Rate limited" if response.status_code == 429 else f"Server {response.status_code}"
                if attempt < MAX_RETRIES:
                    print(f"   ⏳ {label} (likely Ozonetel-side). Waiting {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"   ❌ {label} — exhausted retries. This is an Ozonetel API issue,")
                    print(f"      not a script bug. Re-run when their API recovers.")
                    return None
 
            else:
                print(f"   ❌ Error {response.status_code}: {response.text[:300]}")
                return None
 
        except requests.exceptions.Timeout:
            wait = RETRY_BASE_S * (2 ** (attempt - 1))
            print(f"   ⏱️  Client read timeout (>{TIMEOUT[1]}s) on attempt {attempt}.")
            if attempt < MAX_RETRIES:
                print(f"   ⏳ Waiting {wait}s...")
                time.sleep(wait)
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request error: {e}")
            return None
 
    return None
 
 
# ================================================================
# DEDUP  (inbound only — same logic as the working script)
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
