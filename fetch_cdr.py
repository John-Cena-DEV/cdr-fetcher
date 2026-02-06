import requests
import csv
from datetime import datetime, timedelta
import json

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
    
    print(f"📞 Fetching CDR data from {from_date} to {to_date}")
    
    # API Request
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
    
    try:
        response = requests.get(API_URL, headers=headers, json=payload)
        response.raise_for_status()
        
        cdr_data = response.json()
        print(f"✅ Successfully fetched data!")
        print(f"📊 Response type: {type(cdr_data)}")
        
        return cdr_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        return None

def save_to_csv(data):
    """Save data to CSV file"""
    
    if not data:
        print("⚠️ No data to save")
        return None
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'cdr_data_{timestamp}.csv'
    
    # Also keep a latest file
    latest_filename = 'cdr_data_latest.csv'
    
    try:
        # Handle if data is a list of dictionaries
        if isinstance(data, list) and len(data) > 0:
            keys = data[0].keys()
            
            # Save timestamped file
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)
            
            # Save latest file (for Google Sheets import)
            with open(latest_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)
            
            print(f"✅ Saved {len(data)} records to {filename}")
            print(f"✅ Updated {latest_filename}")
            return filename
            
        # Handle if data is a dictionary
        elif isinstance(data, dict):
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data.keys())
                writer.writeheader()
                writer.writerow(data)
            
            with open(latest_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=data.keys())
                writer.writeheader()
                writer.writerow(data)
            
            print(f"✅ Saved data to {filename}")
            return filename
        
        else:
            # Save as JSON if format is unexpected
            with open(f'cdr_data_{timestamp}.json', 'w') as f:
                json.dump(data, f, indent=2)
            print(f"⚠️ Unexpected format. Saved as JSON")
            return None
            
    except Exception as e:
        print(f"❌ Error saving CSV: {e}")
        return None

def main():
    """Main function"""
    print("=" * 50)
    print("🚀 Starting CDR Data Fetch")
    print("=" * 50)
    
    # Fetch data
    data = fetch_cdr_data()
    
    # Save to CSV
    if data:
        save_to_csv(data)
        print("=" * 50)
        print("✅ Process completed successfully!")
        print("=" * 50)
    else:
        print("=" * 50)
        print("❌ Process failed - no data received")
        print("=" * 50)

if __name__ == '__main__':
    main()
