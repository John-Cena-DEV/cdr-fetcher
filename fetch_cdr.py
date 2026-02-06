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
    yesterday = datetime.now() 
    from_date = yesterday.strftime('%Y-%m-%d 00:00:00')
    to_date = yesterday.strftime('%Y-%m-%d 23:59:59')
    
    print(f"📞 Fetching CDR data")
    print(f"   From: {from_date}")
    print(f"   To: {to_date}")
    print(f"   User: {USERNAME}")
    
    # Try Method 1: GET with JSON body
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
        # Try as POST first (more compatible)
        print("Trying POST method...")
        response = requests.post(API_URL, headers=headers, json=payload)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 405:  # Method not allowed
            print("POST not allowed, trying GET...")
            response = requests.get(API_URL, headers=headers, json=payload)
            print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            try:
                cdr_data = response.json()
                print(f"✅ Successfully fetched data!")
                print(f"📊 Response type: {type(cdr_data)}")
                
                if isinstance(cdr_data, list):
                    print(f"📊 Number of records: {len(cdr_data)}")
                elif isinstance(cdr_data, dict):
                    print(f"📊 Response keys: {list(cdr_data.keys())}")
                
                return cdr_data
            except json.JSONDecodeError as e:
                print(f"❌ JSON Decode Error: {e}")
                print(f"Raw response: {response.text[:500]}")
                return None
        else:
            print(f"❌ API Error: Status {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return None
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error: {e}")
        return None

def save_to_csv(data):
    """Save data to CSV file"""
    
    if not data:
        print("⚠️ No data to save")
        return None
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'cdr_data_{timestamp}.csv'
    latest_filename = 'cdr_data_latest.csv'
    
    try:
        # Handle list of dictionaries
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                keys = data[0].keys()
                
                # Save both files
                for fname in [filename, latest_filename]:
                    with open(fname, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=keys)
                        writer.writeheader()
                        writer.writerows(data)
                
                print(f"✅ Saved {len(data)} records")
                print(f"   - {filename}")
                print(f"   - {latest_filename}")
                return filename
            else:
                print(f"⚠️ List items are not dictionaries: {type(data[0])}")
        
        # Handle dictionary with list inside
        elif isinstance(data, dict):
            # Check if there's a key containing the actual data
            for key in ['data', 'records', 'results', 'calls']:
                if key in data and isinstance(data[key], list):
                    print(f"Found data in '{key}' field")
                    return save_to_csv(data[key])
            
            # Save the dict itself
            print("Saving dictionary as single row")
            for fname in [filename, latest_filename]:
                with open(fname, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=data.keys())
                    writer.writeheader()
                    writer.writerow(data)
            
            print(f"✅ Saved to {filename}")
            return filename
        
        else:
            # Save as JSON for unknown formats
            json_filename = f'cdr_data_{timestamp}.json'
            with open(json_filename, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"⚠️ Unexpected format. Saved as {json_filename}")
            print(f"Data type: {type(data)}")
            print(f"Data preview: {str(data)[:200]}")
            return json_filename
            
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
    data = fetch_cdr_data()
    
    # Save to CSV
    if data:
        result = save_to_csv(data)
        print("=" * 60)
        if result:
            print("✅ Process completed successfully!")
            print("=" * 60)
            sys.exit(0)
        else:
            print("❌ Failed to save data")
            print("=" * 60)
            sys.exit(1)
    else:
        print("=" * 60)
        print("❌ Process failed - no data received")
        print("=" * 60)
        sys.exit(1)

if __name__ == '__main__':
    main()
