import requests
import zipfile
import io
import os
import shutil
import pandas as pd
import subprocess
import sys

# --- Configuration ---
DOWNLOAD_URL = "https://lopucki.law.ufl.edu/download_cases_table.php"
EXTRACT_DIR = "data"
TARGET_FILENAME = "brd.xlsx"
TARGET_PATH = os.path.join(EXTRACT_DIR, TARGET_FILENAME)

# --- Execution Config ---
PYTHON_SCRIPT = "./run.sh"
PYTHON_ARGS = ["submissions", "-i", TARGET_PATH, "-n", "0", "-r", "6"]

R_SCRIPT_CMD = ["Rscript", "new_script.r"]

def fetch_and_sanitize():
    """
    Downloads data, extracts it, and 'sanitizes' the Excel file by
    reading/writing with Pandas to ensure valid XML structure.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print(f"1. Fetching stream from: {DOWNLOAD_URL}...")

    try:
        response = requests.get(DOWNLOAD_URL, headers=headers)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            os.makedirs(EXTRACT_DIR, exist_ok=True)
            z.extractall(EXTRACT_DIR)
            
            raw_file = next((f for f in z.namelist() if f.endswith(('.xlsx', '.xls'))), None)

            if raw_file:
                raw_path = os.path.join(EXTRACT_DIR, raw_file)
                
                # Sanitize: Read with Pandas -> Write clean XLSX
                print("   Sanitizing Excel file...")
                df = pd.read_excel(raw_path)
                df.to_excel(TARGET_PATH, index=False)
                
                # Cleanup if renamed
                if raw_path != TARGET_PATH:
                    os.remove(raw_path)
                    
                print(f"   Saved clean file to: '{TARGET_PATH}'")
                return True
            else:
                print("   Error: No Excel file found inside the ZIP.")
                return False

    except Exception as e:
        print(f"   Error fetching data: {e}")
        return False

def run_python_pipeline():
    """Executes the main python/shell pipeline."""
    if not os.path.exists(PYTHON_SCRIPT):
        print(f"\nError: '{PYTHON_SCRIPT}' not found.")
        return False

    cmd = [PYTHON_SCRIPT] + PYTHON_ARGS
    print(f"\n2. Launching Python Pipeline...")
    print(f"   Command: {' '.join(cmd)}")
    
    try:
        if not os.access(PYTHON_SCRIPT, os.X_OK):
            os.chmod(PYTHON_SCRIPT, 0o755)

        subprocess.run(cmd, check=True)
        return True
        
    except subprocess.CalledProcessError:
        print(f"\n   Python pipeline failed.")
        return False
    except Exception as e:
        print(f"\n   Failed to execute pipeline: {e}")
        return False

def run_r_script():
    """Executes the R script."""
    r_file = R_SCRIPT_CMD[1]
    if not os.path.exists(r_file):
        print(f"\nError: '{r_file}' not found.")
        return False

    print(f"\n3. Launching R Script...")
    print(f"   Command: {' '.join(R_SCRIPT_CMD)}")

    try:
        subprocess.run(R_SCRIPT_CMD, check=True)
        return True
    except subprocess.CalledProcessError:
        print(f"\n   R script failed.")
        return False
    except Exception as e:
        print(f"\n   Failed to execute R script: {e}")
        return False

if __name__ == "__main__":
    # 1. Fetch & Sanitize
    if not fetch_and_sanitize():
        sys.exit(1)
    
    # 2. Run Python Pipeline
    if not run_python_pipeline():
        sys.exit(1)
        
    # 3. Run R Script (only if Python succeeded)
    if not run_r_script():
        sys.exit(1)
    
    print("\nAll steps completed successfully.")