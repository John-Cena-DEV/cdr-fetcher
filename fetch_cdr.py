import csv
import json
import sys
import pandas as pd
from datetime import datetime, timedelta
import random

# ================================================================
# CONFIG
# ================================================================
USERNAME = "qht_regrow"

# ================================================================
# STATIC DUMMY DATA  — replace API call
# Mimics the exact structure Ozonetel returns in "details"
# ================================================================
def generate_static_records():
    now   = datetime.now()
    today = now.strftime("%Y-%m-%d")

    agents = [
        "Ravi Kumar", "Priya Sharma", "Amit Singh",
        "Neha Gupta", "Rohit Verma", "Sunita Yadav",
    ]
    dispositions = ["Answered", "Not Answered", "Busy", "Voicemail", "Transferred"]
    queues       = ["Sales", "Support", "Billing", "General"]
    dids         = ["01204567890", "01204567891", "01204567892"]

    records = []
    base_time = datetime.strptime(f"{today} 09:00:00", "%Y-%m-%d %H:%M:%S")

    # ── 30 inbound calls (some with duplicate CallIDs to test dedup) ──
    call_ids_pool = [f"IB{1000 + i}" for i in range(22)]   # 22 unique IDs
    inbound_ids   = call_ids_pool + call_ids_pool[:8]       # 8 duplicates → 30 total

    for i, cid in enumerate(inbound_ids):
        call_time = base_time + timedelta(minutes=i * 18, seconds=random.randint(0, 59))
        duration  = random.randint(30, 600)
        records.append({
            "CallID":          cid,
            "Type":            "inbound",
            "AgentName":       random.choice(agents),
            "CustomerNumber":  f"98{random.randint(10000000, 99999999)}",
            "DID":             random.choice(dids),
            "Queue":           random.choice(queues),
            "Disposition":     random.choice(dispositions),
            "StartTime":       call_time.strftime("%Y-%m-%d %H:%M:%S"),
            "EndTime":         (call_time + timedelta(seconds=duration)).strftime("%Y-%m-%d %H:%M:%S"),
            "Duration":        str(duration),
            "HoldTime":        str(random.randint(0, 60)),
            "WaitTime":        str(random.randint(5, 120)),
            "RecordingURL":    f"https://recordings.example.com/{cid}.mp3",
            "Date":            today,
            "UserName":        USERNAME,
        })

    # ── 15 outbound calls (no dedup needed) ──
    for i in range(15):
        cid       = f"OB{2000 + i}"
        call_time = base_time + timedelta(minutes=i * 25 + 5, seconds=random.randint(0, 59))
        duration  = random.randint(15, 480)
        records.append({
            "CallID":          cid,
            "Type":            "outbound",
            "AgentName":       random.choice(agents),
            "CustomerNumber":  f"91{random.randint(10000000, 99999999)}",
            "DID":             random.choice(dids),
            "Queue":           random.choice(queues),
            "Disposition":     random.choice(dispositions),
            "StartTime":       call_time.strftime("%Y-%m-%d %H:%M:%S"),
            "EndTime":         (call_time + timedelta(seconds=duration)).strftime("%Y-%m-%d %H:%M:%S"),
            "Duration":        str(duration),
            "HoldTime":        str(random.randint(0, 30)),
            "WaitTime":        "0",
            "RecordingURL":    f"https://recordings.example.com/{cid}.mp3",
            "Date":            today,
            "UserName":        USERNAME,
        })

    return records


# ================================================================
# DEDUP  — pandas drop_duplicates on CallID for inbound only
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
    merged = merged.fillna("")
    merged = merged.astype(str)

    return merged.to_dict(orient="records")


# ================================================================
# SAVE  — same CSV layout (JSON string in A2, GSheet-compatible)
# ================================================================
def save_to_csv(records, filename="cdr_data_master.csv"):
    if not records:
        print("⚠️  No records to save.")
        return None

    try:
        json_string = json.dumps(records, ensure_ascii=False)

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["data"])       # Row 1 — header  (rows[0][0])
            writer.writerow([json_string])  # Row 2 — JSON    (rows[1][0])

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
    now = datetime.now()

    print("=" * 60)
    print("🚀 CDR Data Fetch — Static Mode (No API)")
    print("=" * 60)
    print(f"\n📋 Generating static CDR data")
    print(f"   Date : {now.strftime('%Y-%m-%d')}")
    print(f"   User : {USERNAME}")

    # 1. Generate static records (replaces API call)
    records = generate_static_records()
    print(f"\n   ✅ Generated {len(records)} records (30 inbound + 15 outbound, 8 inbound dupes)")

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
