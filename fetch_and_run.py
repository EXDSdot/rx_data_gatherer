import requests
import zipfile
import io
import os
import shutil
import pandas as pd

# 1. Setup
url = "https://lopucki.law.ufl.edu/download_cases_table.php"
extract_dir = "./data"
target_path = os.path.join(extract_dir, "brd.xlsx")

# Using a standard Mac User-Agent to avoid being blocked by the server
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

print(f"Fetching stream from: {url}...")

try:
    # 2. Download
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # 3. Extract
    # We wrap the content in BytesIO so zipfile treats it like a file on disk
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        print(f"Archive contains: {z.namelist()}")
        
        # Create ./data if it doesn't exist
        os.makedirs(extract_dir, exist_ok=True)
        
        # Extract all files
        z.extractall(extract_dir)
        
        # Find the .xlsx file within the extracted list
        excel_file = next((f for f in z.namelist() if f.endswith(('.xlsx', '.xls'))), None)

        if excel_file:
            # 4. Rename
            original_path = os.path.join(extract_dir, excel_file)
            shutil.move(original_path, target_path)
            print(f"Renamed '{excel_file}' to 'brd.xlsx'")
            
            # 5. Verify Load
            df = pd.read_excel(target_path)
            print("-" * 30)
            print(f"Success. DataFrame shape: {df.shape}")
            print("First 5 columns:", df.columns[:5].tolist())
        else:
            print("Error: No Excel file found inside the ZIP.")

except requests.exceptions.RequestException as e:
    print(f"Network error: {e}")
except zipfile.BadZipFile:
    print("Error: The server response was not a valid ZIP file (it might be HTML/Text if blocked).")