import requests
import csv
from datetime import datetime, timedelta
import json
import sys
import os
import time


# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
API_URL  = 'https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails'
API_KEY  = 'KK01b6bcdbcad7fdfced420ada0186393b'
USERNAME = 'qht_regrow'

MAX_RETRIES   = 3       # how many times to retry on 429
RETRY_BASE_S  = 10      # base wait in seconds (doubles each attempt: 10 → 20 → 40)


# ──────────────────────────────────────────────
# FETCH
# ──────────────────────────────────────────────
def fetch_cdr_data():
    """Fetch CDR data from Ozonetel API (GET-only, with retry on 429)."""

    today     = datetime.now()
    from_date = today.strftime('%Y-%m-%d 00:00:00')
    to_date   = today.strftime('%Y-%m-%d 23:59:59')

    print(f"📞 Fetching CDR data")
    print(f"   From : {from_date}")
    print(f"   To   : {to_date}")
    print(f"   User : {USERNAME}")

    headers = {
        'accept':       'application/json',
        'Content-Type': 'application/json',
        'apiKey':       API_KEY,
    }

    # Use query-string params (correct for GET endpoints)
    params = {
        'fromDate': from_date,
        'toDate':   to_date,
        'userName': USERNAME,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n🔄 Attempt {attempt}/{MAX_RETRIES} — GET {API_URL}")
        try:
            response = requests.get(API_URL, headers=headers, params=params, timeout=30)
            print(f"   Status : {response.status_code}")

            if response.status_code == 200:
                try:
                    data = response.json()
                    print("✅ Data received successfully!")

                    if isinstance(data, list):
                        print(f"   Records : {len(data)}")
                    elif isinstance(data, dict):
                        print(f"   Keys    : {list(data.keys())}")

                    return data

                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error: {e}")
                    print(f"   Raw response: {response.text[:300]}")
                    return None

            elif response.status_code == 429:
                wait = RETRY_BASE_S * (2 ** (attempt - 1))   # 10 → 20 → 40
                if attempt < MAX_RETRIES:
                    print(f"⏳ Rate limited (429). Waiting {wait}s before retry...")
                    time.sleep(wait)
                else:
                    print("❌ Rate limited (429). Max retries reached.")
                    return None

            elif response.status_code == 401:
                print("❌ Unauthorised (401). Check your API key / username.")
                return None

            elif response.status_code == 404:
                print("❌ Not found (404). Check the API URL.")
                return None

            else:
                print(f"❌ Unexpected status {response.status_code}")
                print(f"   Response: {response.text[:300]}")
                return None

        except requests.exceptions.Timeout:
            print(f"⏱️  Request timed out (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(5)
        except requests.exceptions.ConnectionError as e:
            print(f"🔌 Connection error: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
            return None

    print("❌ All retry attempts exhausted.")
    return None


# ──────────────────────────────────────────────
# SAVE
# ──────────────────────────────────────────────
def save_to_csv(data, filename='cdr_data_master.csv'):
    """Save API response to a CSV file (overwrites each run)."""

    if not data:
        print("⚠️  No data to save.")
        return None

    try:
        # Unwrap common envelope keys
        if isinstance(data, dict):
            for key in ('data', 'records', 'results', 'calls'):
                if key in data and isinstance(data[key], list):
                    print(f"   Unwrapping data from key: '{key}'")
                    data = data[key]
                    break

        if isinstance(data, list):
            if len(data) == 0:
                print("⚠️  API returned an empty list — nothing to save.")
                return None

            if not isinstance(data[0], dict):
                print(f"⚠️  Unexpected list item type: {type(data[0])}")
                return _save_json_fallback(data)

            keys = data[0].keys()
            file_existed = os.path.isfile(filename)

            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)

            action = "Refreshed" if file_existed else "Created"
            print(f"💾 {action} {filename}  ({len(data)} records)")
            return filename

        elif isinstance(data, dict):
            # Single-row dict
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data.keys())
                writer.writeheader()
                writer.writerow(data)
            print(f"💾 Saved single-row dict to {filename}")
            return filename

        else:
            return _save_json_fallback(data)

    except Exception as e:
        print(f"❌ Error saving CSV: {e}")
        import traceback
        traceback.print_exc()
        return None


def _save_json_fallback(data):
    """Fallback: save raw data as JSON when format is unexpected."""
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    fname = f'cdr_data_{ts}.json'
    with open(fname, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"⚠️  Unknown format — saved as {fname}")
    print(f"   Preview: {str(data)[:200]}")
    return fname


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    print("=" * 60)
    print("🚀 CDR Data Fetch — Starting")
    print("=" * 60)

    data = fetch_cdr_data()

    print()
    if data is not None:
        result = save_to_csv(data)
        print("=" * 60)
        if result:
            print(f"✅ Process completed — output: {result}")
            print("=" * 60)
            sys.exit(0)
        else:
            print("❌ Fetch succeeded but CSV save failed.")
            print("=" * 60)
            sys.exit(1)
    else:
        print("=" * 60)
        print("❌ Process failed — no data received from API.")
        print("=" * 60)
        sys.exit(1)


if __name__ == '__main__':
    main()
