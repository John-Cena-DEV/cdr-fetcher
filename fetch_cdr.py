import requests
import csv
import json
import sys
import subprocess
from datetime import datetime, timedelta

def fetch_cdr_data():
    """Fetch CDR data using curl (exactly like your original command)"""
    
    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    from_date = yesterday.strftime('%Y-%m-%d 10:00:00')
    to_date = yesterday.strftime('%Y-%m-%d 22:59:59')
    
    print(f"📞 Fetching CDR data")
    print(f"   From: {from_date}")
    print(f"   To: {to_date}")
    print(f"   User: qht_regrow")
    
    # Use curl command exactly as it works
    payload_json = json.dumps({
        "fromDate": from_date,
        "toDate": to_date,
        "userName": "qht_regrow"
    })
    
    curl_command = [
        'curl',
        '--location',
        '--request', 'GET',
        'https://in1-ccaas-api.ozonetel.com/ca_reports/fetchCDRDetails',
        '--header', 'Content-Type: application/json',
        '--header', 'accept: application/json',
        '--header', 'apiKey: KK01b6bcdbcad7fdfced420ada0186393b',
        '--data', payload_json
    ]
    
    try:
        print("🔄 Running curl command...")
        result = subprocess.run(curl_command, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print(f"✅ curl succeeded!")
            print(f"📊 Response length: {len(result.stdout)} characters")
            print(f"📊 First 300 chars: {result.stdout[:300]}")
            
            try:
                data = json.loads(result.stdout)
                print(f"✅ Parsed as JSON successfully")
                return data
            except json.JSONDecodeError as e:
                print(f"⚠️ Response is not valid JSON: {e}")
                print(f"Full response: {result.stdout[:1000]}")
                return None
        else:
            print(f"❌ curl failed with return code: {result.returncode}")
            print(f"Error: {result.stderr}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
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
        for key in ['data', 'records', 'results', 'calls', 'cdrDetails', 'callDetails']:
            if key in data:
                print(f"✅ Found data in '{key}' field")
                if isinstance(data[key], list):
                    return data[key]
        
        # If dict has call-related keys, wrap in list
        if 'CallID' in data or 'callID' in data or 'AgentID' in data:
            print("✅ Single record found, wrapping in list")
            return [data]
        
        # Check if all values are similar (might be single record)
        print(f"📊 Dictionary keys: {list(data.keys())[:20]}")
        
        # If it looks like a single record, wrap it
        if len(data) > 5:  # Reasonable number of fields
            print("✅ Treating as single record")
            return [data]
    
    print(f"⚠️ Unexpected data format: {type(data)}")
    print(f"Data structure: {str(data)[:500]}")
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
        
        print(f"📊 Found {len(all_keys)} columns")
        print(f"📊 First 10 columns: {', '.join(list(all_keys)[:10])}")
        
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
        print("=" * 60)
        print("❌ No data received from API")
        print("=" * 60)
        sys.exit(1)
    
    # Parse the response
    records = parse_cdr_response(raw_data)
    
    if not records:
        print("=" * 60)
        print("❌ Could not parse data into records")
        # Save raw data as JSON for debugging
        try:
            with open('raw_response.json', 'w') as f:
                json.dump(raw_data, f, indent=2)
            print("💾 Saved raw response to raw_response.json for debugging")
        except:
            pass
        print("=" * 60)
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
