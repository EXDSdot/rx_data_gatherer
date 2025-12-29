import pandas as pd
import json
import os
from dateutil import parser

# ================= SETUP =================
INPUT_FILE = 'input.xlsx'
JSON_FOLDER = './json_data'

# Adjust these if your excel headers are different
COL_CIK = 'cikbefore'
COL_DATE = 'date10kbefore'
# =========================================

def normalize_cik(cik):
    # Turns 6201 into '0000006201'
    return str(int(cik)).zfill(10)

def main():
    print(f"--- STARTING DIAGNOSTIC ON {INPUT_FILE} ---")
    
    try:
        df = pd.read_excel(INPUT_FILE)
        # Force column names to lowercase to avoid CaseSensitive errors
        df.columns = df.columns.str.strip().str.lower()
    except Exception as e:
        print(f"CRITICAL ERROR: Could not read Excel file. {e}")
        return

    # Check if columns exist
    if COL_CIK not in df.columns or COL_DATE not in df.columns:
        print(f"CRITICAL ERROR: Columns '{COL_CIK}' or '{COL_DATE}' not found in Excel.")
        print(f"Found headers: {list(df.columns)}")
        return

    # Loop through ONLY the first 5 rows
    for index, row in df.head(5).iterrows():
        print("\n" + "="*60)
        print(f"ROW {index + 1}")
        print("="*60)

        # 1. READ EXCEL DATA
        raw_cik = row[COL_CIK]
        raw_date = row[COL_DATE]
        
        # Clean the data
        cik_clean = normalize_cik(raw_cik)
        
        # Handle date parsing safely
        try:
            if pd.isna(raw_date):
                target_date = "N/A"
            else:
                # If it's already a timestamp, convert to string
                if hasattr(raw_date, 'strftime'):
                    target_date = raw_date.strftime('%Y-%m-%d')
                else:
                    # If string, parse it
                    target_date = parser.parse(str(raw_date)).strftime('%Y-%m-%d')
        except:
            target_date = str(raw_date)

        print(f"EXCEL GOAL -> CIK: {cik_clean} | Looking for Date: {target_date}")

        # 2. LOCATE FILE
        filename = f"CIK{cik_clean}.json"
        filepath = os.path.join(JSON_FOLDER, filename)
        
        if not os.path.exists(filepath):
            print(f"❌ FILE MISSING: Could not find {filepath}")
            continue
        
        print(f"✅ FILE FOUND: {filename}")

        # 3. OPEN AND READ JSON
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Print Internal Name to prove we read it
            entity_name = data.get('entityName', 'Unknown Entity')
            print(f"📖 JSON READ SUCCESS. Entity Name: {entity_name}")
            
            # 4. HUNT FOR ASSETS
            facts = data.get('facts', {}).get('us-gaap', {})
            
            if 'Assets' not in facts:
                print("❌ 'Assets' tag NOT found in us-gaap.")
                print(f"   (Available tags sample: {list(facts.keys())[:5]}...)")
                continue
            
            print("✅ 'Assets' tag found.")
            
            units = facts['Assets'].get('units', {}).get('USD', [])
            print(f"   Found {len(units)} entries for Assets.")

            # 5. DUMP RELEVANT DATES
            # We will print the first 3 entries + any entry that matches our year
            print("   --- DUMPING CONTENTS (First 3 entries) ---")
            
            target_year = target_date[:4] if target_date != "N/A" else "0000"
            
            matched = False
            for i, entry in enumerate(units):
                val = entry.get('val')
                end_date = entry.get('end', 'No End Date')
                filed_date = entry.get('filed', 'No File Date')
                
                # Print first 3 unconditionally
                if i < 3:
                    print(f"   Entry {i}: Val={val} | End={end_date} | Filed={filed_date}")
                
                # Check for exact match
                if end_date == target_date:
                    print(f"   🎯 HIT! Exact 'end' date match found: {val}")
                    matched = True
                elif filed_date == target_date:
                    print(f"   🎯 HIT! Exact 'filed' date match found: {val}")
                    matched = True
                # Check for year match (Partial)
                elif target_year in end_date or target_year in filed_date:
                    print(f"   ⚠️ CLOSE MATCH (Same Year): Val={val} | End={end_date} | Filed={filed_date}")

            if not matched:
                print(f"❌ NO EXACT MATCH for {target_date} found in file.")

        except Exception as e:
            print(f"❌ ERROR READING JSON: {e}")

if __name__ == "__main__":
    main()