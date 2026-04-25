import requests
import csv
from datetime import datetime
import json
import sys
import os
import time


# ──────────────────────────────────────────────────────────────
# CONFIG  —  values pulled from GitHub Actions secrets
# ──────────────────────────────────────────────────────────────
API_DOMAIN = os.environ.get('OZONETEL_DOMAIN',   'in1-ccaas-api.ozonetel.com')
API_KEY    = os.environ.get('OZONETEL_API_KEY',  'KK01b6bcdbcad7fdfced420ada0186393b')
USERNAME   = os.environ.get('OZONETEL_USERNAME',  'qht_regrow')

BASE_URL   = f'https://{API_DOMAIN}'
TOKEN_URL  = f'{BASE_URL}/ca_apis/CAToken/generateToken'
CDR_URL    = f'{BASE_URL}/ca_reports/fetchCDRDetails'

MAX_RETRIES  = 3    # retries on 429
RETRY_BASE_S = 15   # wait: 15s → 30s → 60s  (rate limit = 2 req/min)


# ──────────────────────────────────────────────────────────────
# STEP 1 — Generate auth token
# ──────────────────────────────────────────────────────────────
def generate_token():
    """
    POST to Ozonetel token endpoint with apiKey + userName.
    Returns the token string, or None on failure.
    """
    print("\n🔑 Generating auth token...")

    headers = {
        'Content-Type': 'application/json',
        'accept':       'application/json',
    }
    payload = {
        'apiKey':   API_KEY,
        'userName': USERNAME,
    }

    try:
        resp = requests.post(TOKEN_URL, headers=headers, json=payload, timeout=30)
        print(f"   Token status : {resp.status_code}")

        if resp.status_code == 200:
            data = resp.json()
            # Try common key names used by Ozonetel
            token = (
                data.get('token')
                or data.get('authToken')
                or (data.get('data') or {}).get('token')
                or (data.get('data') or {}).get('authToken')
            )
            if token:
                print("   ✅ Token received.")
                return token
            else:
                print(f"   ❌ Token key not found. Full response: {data}")
                return None
        else:
            print(f"   ❌ Token generation failed ({resp.status_code}): {resp.text[:300]}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"   ❌ Token request error: {e}")
        return None


# ──────────────────────────────────────────────────────────────
# STEP 2 — Fetch CDR data (with retry on 429)
# ──────────────────────────────────────────────────────────────
def fetch_cdr_data(token):
    """
    GET CDR records using the Bearer token.
    Params sent as JSON body per Ozonetel docs.
    Retries up to MAX_RETRIES times on 429.
    """
    today     = datetime.now()
    from_date = today.strftime('%Y-%m-%d 00:00:00')
    to_date   = today.strftime('%Y-%m-%d 23:59:59')

    print(f"\n📞 Fetching CDR data")
    print(f"   From : {from_date}")
    print(f"   To   : {to_date}")
    print(f"   User : {USERNAME}")

    headers = {
        'accept':        'application/json',
        'Content-Type':  'application/json',
        'Authorization': f'Bearer {token}',
    }
    payload = {
        'fromDate': from_date,
        'toDate':   to_date,
        'userName': USERNAME,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n🔄 Attempt {attempt}/{MAX_RETRIES}...")
        try:
            resp = requests.get(CDR_URL, headers=headers, json=payload, timeout=30)
            print(f"   Status : {resp.status_code}")

            if resp.status_code == 200:
                try:
                    data = resp.json()
                    print("✅ CDR data received!")
                    if isinstance(data, list):
                        print(f"   Records : {len(data)}")
                    elif isinstance(data, dict):
                        print(f"   Keys    : {list(data.keys())}")
                    return data
                except json.JSONDecodeError as e:
                    print(f"   ❌ JSON decode error: {e}")
                    print(f"   Raw: {resp.text[:300]}")
                    return None

            elif resp.status_code == 429:
                wait = RETRY_BASE_S * (2 ** (attempt - 1))   # 15 → 30 → 60
                if attempt < MAX_RETRIES:
                    print(f"   ⏳ Rate limited. Waiting {wait}s then retrying...")
                    time.sleep(wait)
                else:
                    print("   ❌ Rate limited — max retries exhausted.")
                    return None

            elif resp.status_code == 401:
                print("   ❌ 401 Unauthorised — token rejected or expired.")
                return None

            elif resp.status_code == 404:
                print("   ❌ 404 Not Found — verify CDR_URL.")
                return None

            else:
                print(f"   ❌ Unexpected {resp.status_code}: {resp.text[:300]}")
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


# ──────────────────────────────────────────────────────────────
# STEP 3 — Save to CSV
# ──────────────────────────────────────────────────────────────
def save_to_csv(data, filename='cdr_data_master.csv'):
    if not data:
        print("⚠️  No data to save.")
        return None

    try:
        # Unwrap common envelope keys
        if isinstance(data, dict):
            for key in ('data', 'records', 'results', 'calls', 'cdrData'):
                if key in data and isinstance(data[key], list):
                    print(f"   Unwrapping '{key}' field...")
                    data = data[key]
                    break

        if isinstance(data, list):
            if len(data) == 0:
                print("⚠️  API returned an empty list — no records for this date range.")
                return None

            if not isinstance(data[0], dict):
                return _save_json_fallback(data)

            file_existed = os.path.isfile(filename)
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)

            action = "Refreshed" if file_existed else "Created"
            print(f"\n💾 {action} '{filename}'  ({len(data)} records)")
            return filename

        elif isinstance(data, dict):
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data.keys())
                writer.writeheader()
                writer.writerow(data)
            print(f"\n💾 Saved single-row dict to '{filename}'")
            return filename

        else:
            return _save_json_fallback(data)

    except Exception as e:
        print(f"❌ Error saving CSV: {e}")
        import traceback
        traceback.print_exc()
        return None


def _save_json_fallback(data):
    ts    = datetime.now().strftime('%Y%m%d_%H%M%S')
    fname = f'cdr_data_{ts}.json'
    with open(fname, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"⚠️  Unexpected format — saved as '{fname}'")
    print(f"   Preview: {str(data)[:200]}")
    return fname


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("🚀 CDR Data Fetch — Starting")
    print("=" * 60)

    # 1. Generate token
    token = generate_token()
    if not token:
        print("\n❌ Cannot proceed without a valid token.")
        print("=" * 60)
        sys.exit(1)

    # 2. Brief pause so token call doesn't count against CDR rate limit window
    time.sleep(2)

    # 3. Fetch CDR data
    data = fetch_cdr_data(token)

    # 4. Save result
    print()
    if data is not None:
        result = save_to_csv(data)
        print("=" * 60)
        if result:
            print(f"✅ Process completed — output: {result}")
            print("=" * 60)
            sys.exit(0)
        else:
            print("❌ Fetch OK but save failed.")
            print("=" * 60)
            sys.exit(1)
    else:
        print("=" * 60)
        print("❌ Process failed — no CDR data received.")
        print("=" * 60)
        sys.exit(1)


if __name__ == '__main__':
    main()
