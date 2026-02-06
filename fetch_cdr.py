import requests
import csv
from datetime import datetime, timedelta
import json
import sys

def fetch_cdr_data():
    """Fetch CDR data from Ozonetel API"""
    
    # API Configuration
    API_URL = 'https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails'
    API_KEY = 'KK01b6bcdbcad7fdfced420ada0186393b'
    USERNAME = 'qht_regrow'
    
    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    from_date = yesterday.strftime('%Y-%m-%d 10:00:00')
    to_date = yesterday.strftime('%Y-%m-%d 22:59:59')
    
    print(f"📞 Fetching CDR data")
    print(f"   From: {from_date}")
    print(f"   To: {to_date}")
    print(f"   User: {USERNAME}")
    
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json',
        'apiKey': API_KEY
    }
    
    payload = {
        "fromDate": from_date,
        "toDate": to_date,
        "userName": USERNAME
    }
    
    print(f"\n🔄 Attempting API request...")
    
    try:
        # Try POST method
        response = requests.post(API_URL, headers=headers, json=payload)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            raw_text = response.text
            print(f"✅ Successfully fetched data!")
            print(f"📊 Response length: {len(raw_text)} characters")
            print(f"📊 First 200 chars: {raw_text[:200]}")
            
            # Try to parse as JSON
            try:
                data = json.loads(raw_text)
                return data
            except json.JSONDecodeError:
                # If not valid JSON, return as text
                print("⚠️ Response is not JSON, returning raw text")
                return raw_text
        else:
            print(f"❌ API Error: Status {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return None
        
    except Exception as e:
        print(f"❌ Request Error: {e}")
        return None

def parse_cdr_response(data):
    """Parse the CDR response into a list of records"""
    
    if not data:
        return None
    
    # If data is already a list, return it
    if isinstance(data, list):
        print(f"✅ Data is already a list with {len(data)} records")
        return data
    
    # If data is a dict, check for common keys
    if isinstance(data, dict):
        for key in ['data', 'records', 'results', 'calls', 'cdrDetails']:
            if key in data:
                print(f"✅ Found data in '{key}' field")
                return data[key]
        
        # If dict has call-related keys, wrap in list
        if 'CallID' in data or 'callID' in data or 'AgentID' in data:
            print("✅ Single record found, wrapping in list")
            return [data]
    
    # If it's a string that looks like JSON
    if isinstance(data, str):
        # Try to parse it
        try:
            parsed = json.loads(data)
            return parse_cdr_response(parsed)
        except:
            print(f"⚠️ Could not parse string as JSON")
            # Try to find JSON-like structure
            if data.startswith('[{') or data.startswith('{'):
                try:
                    # Clean up the string
                    cleaned = data.strip()
                    parsed = json.loads(cleaned)
                    return parse_cdr_response(parsed)
                except:
                    pass
    
    print(f"⚠️ Unexpected data format: {type(data)}")
    return None

def save_to_csv(records):
    """Save records to CSV file"""
    
    if not records:
        print("⚠️ No records to save")
        return None
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'cdr_data_{timestamp}.csv'
    latest_filename = 'cdr_data_latest.csv'
    
    try:
        # Ensure we have a list
        if not isinstance(records, list):
            records = [records]
        
        if len(records) == 0:
            print("⚠️ Empty records list")
            return None
        
        # Get all unique keys from all records
        all_keys = set()
        for record in records:
            if isinstance(record, dict):
                all_keys.update(record.keys())
        
        all_keys = sorted(list(all_keys))
        
        if not all_keys:
            print("⚠️ No keys found in records")
            return None
        
        print(f"📊 Found {len(all_keys)} columns: {', '.join(list(all_keys)[:10])}...")
        
        # Save both files
        for fname in [filename, latest_filename]:
            with open(fname, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=all_keys)
                writer.writeheader()
                for record in records:
                    if isinstance(record, dict):
                        writer.writerow(record)
        
        print(f"✅ Saved {len(records)} records")
        print(f"   - {filename}")
        print(f"   - {latest_filename}")
        return filename
            
    except Exception as e:
        print(f"❌ Error saving CSV: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function"""
    print("=" * 60)
    print("🚀 CDR Data Fetch - Starting")
    print("=" * 60)
    
    # Fetch data
    raw_data = fetch_cdr_data()
    
    if not raw_data:
        print("❌ No data received from API")
        sys.exit(1)
    
    # Parse the response
    records = parse_cdr_response(raw_data)
    
    if not records:
        print("❌ Could not parse data into records")
        # Save raw data as JSON for debugging
        with open('raw_response.json', 'w') as f:
            json.dump(raw_data, f, indent=2)
        print("💾 Saved raw response to raw_response.json for debugging")
        sys.exit(1)
    
    # Save to CSV
    result = save_to_csv(records)
    
    print("=" * 60)
    if result:
        print("✅ Process completed successfully!")
        print("=" * 60)
        sys.exit(0)
    else:
        print("❌ Failed to save data")
        print("=" * 60)
        sys.exit(1)

if __name__ == '__main__':
    main()
