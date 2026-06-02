import requests
import csv
import json
import sys
import time
from datetime import datetime, timedelta

# ================================================================
# CONFIG
# ================================================================
API_KEY  = "KK01b6bcdbcad7fdfced420ada0186393b"
USERNAME = "qht_regrow"
URL      = "https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails"

MAX_RETRIES  = 4
RETRY_BASE_S = 15
TIMEOUT      = (10, 120)   # (connect, read) seconds — read raised to survive slow responses

# Split the day into N-hour windows to reduce backend load per request.
# 24 = single full-day request (old behaviour). 6 = four requests/day (recommended).
CHUNK_HOURS  = 6

# HTTP codes that are worth retrying (transient / server-side)
RETRYABLE = {429, 500, 502, 503, 504}


# ================================================================
# FETCH A SINGLE TIME WINDOW (with retry on 5xx / 429 / timeout)
# ================================================================
def fetch_window(from_date, to_date):
    """
    Returns: list of records on success.
    Raises:  RuntimeError on auth failure (401) — fatal, no point retrying.
             TimeoutError after exhausting retries — caller decides what to do.
    """
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

    print(f"\n📞 Window {from_date}  →  {to_date}")

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"   🔄 Attempt {attempt}/{MAX_RETRIES}...")
        try:
            response = requests.request(
                method="GET",
                url=URL,
                headers=headers,
                data=payload,
                timeout=TIMEOUT,
            )
            print(f"      Status : {response.status_code}")

            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    print(f"      ❌ JSON decode error: {e}")
                    print(f"      Raw: {response.text[:300]}")
                    raise TimeoutError("Bad JSON in 200 response")

                if "details" not in data:
                    keys = list(data.keys()) if isinstance(data, dict) else type(data)
                    print(f"      ❌ 'details' key missing. Keys: {keys}")
                    raise TimeoutError("'details' key missing")

                records = data["details"]
                print(f"      ✅ Got {len(records)} records.")
                return records

            elif response.status_code == 401:
                print(f"      ❌ 401 Unauthorised — API key or username wrong.")
                raise RuntimeError("401 Unauthorised — fatal, check API_KEY / USERNAME")

            elif response.status_code in RETRYABLE:
                # 504, 502, 503, 500, 429 — all transient, back off and retry
                wait = RETRY_BASE_S * (2 ** (attempt - 1))
                label = "Rate limited" if response.status_code == 429 else f"Server {response.status_code}"
                if attempt < MAX_RETRIES:
                    print(f"      ⏳ {label}. Waiting {wait}s before retry...")
                    time.sleep(wait)
                else:
                    print(f"      ❌ {label} — max retries exhausted.")
                    raise TimeoutError(f"HTTP {response.status_code} after {MAX_RETRIES} attempts")

            else:
                print(f"      ❌ Error {response.status_code}: {response.text[:300]}")
                raise TimeoutError(f"Unexpected HTTP {response.status_code}")

        except requests.exceptions.Timeout:
            wait = RETRY_BASE_S * (2 ** (attempt - 1))
            print(f"      ⏱️  Client timeout on attempt {attempt}.")
            if attempt < MAX_RETRIES:
                print(f"      ⏳ Waiting {wait}s before retry...")
                time.sleep(wait)
            else:
                raise TimeoutError("Client timeout after max retries")

        except requests.exceptions.RequestException as e:
            wait = RETRY_BASE_S * (2 ** (attempt - 1))
            print(f"      ❌ Request error: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(wait)
            else:
                raise TimeoutError(f"Request error after max retries: {e}")

    raise TimeoutError("Exhausted retries")


# ================================================================
# FETCH FULL DAY (chunked into CHUNK_HOURS windows)
# ================================================================
def fetch_cdr_data():
    now = datetime.now()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end   = now.replace(hour=23, minute=59, second=59, microsecond=0)

    print(f"\n🗓️  Day : {day_start:%Y-%m-%d}")
    print(f"   User : {USERNAME}")
    print(f"   Chunk: {CHUNK_HOURS}h windows")

    # Build the list of (from, to) windows
    windows = []
    cursor = day_start
    while cursor <= day_end:
        w_start = cursor
        w_end   = min(cursor + timedelta(hours=CHUNK_HOURS) - timedelta(seconds=1), day_end)
        windows.append((w_start.strftime("%Y-%m-%d %H:%M:%S"),
                        w_end.strftime("%Y-%m-%d %H:%M:%S")))
        cursor = w_end + timedelta(seconds=1)

    all_records = []
    for from_date, to_date in windows:
        # Any window failing after retries aborts the run — partial CDR data
        # would corrupt the daily report, so we fail loudly instead.
        records = fetch_window(from_date, to_date)
        all_records.extend(records)

    print(f"\n   ✅ Total records across all windows: {len(all_records)}")
    return all_records


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

    try:
        records = fetch_cdr_data()
    except RuntimeError as e:          # auth failure — fatal
        print("=" * 60)
        print(f"❌ Fatal: {e}")
        print("=" * 60)
        sys.exit(2)
    except TimeoutError as e:          # a window failed after all retries
        print("=" * 60)
        print(f"❌ Process failed — a window could not be fetched: {e}")
        print("   (Failing loudly to avoid saving partial CDR data.)")
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
