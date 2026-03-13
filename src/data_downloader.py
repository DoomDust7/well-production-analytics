"""
Downloads raw CSV files from GitHub and saves to data/raw/.
Falls back gracefully if network is unavailable.
"""
import os
import requests

from src.spark_session import RAW_DIR

GITHUB_BASE = "https://raw.githubusercontent.com/DoomDust7/Shell-Final-CaseStudy/main"

FILES = {
    "flaring.csv": f"{GITHUB_BASE}/FLARING.csv",
    "operator_well_counts.csv": f"{GITHUB_BASE}/Operator%20with%20highest%20well%20counts.csv",
    "shale_play_production.csv": f"{GITHUB_BASE}/shale%20play%203.csv",
    "well_metadata.csv": f"{GITHUB_BASE}/average%20well%20length%20%282%29.csv",
    "chemical_production.csv": f"{GITHUB_BASE}/Question%206th.csv",
}


def download_all(verbose: bool = True) -> dict:
    """
    Download all CSVs. Returns {filename: local_path} for successfully downloaded files.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    results = {}

    for filename, url in FILES.items():
        dest = os.path.join(RAW_DIR, filename)
        if os.path.exists(dest) and os.path.getsize(dest) > 100:
            if verbose:
                print(f"  [SKIP] {filename} already exists ({os.path.getsize(dest):,} bytes)")
            results[filename] = dest
            continue
        try:
            if verbose:
                print(f"  [DOWN] {filename} <- {url}")
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            with open(dest, "wb") as f:
                f.write(resp.content)
            if verbose:
                print(f"         Saved {os.path.getsize(dest):,} bytes")
            results[filename] = dest
        except Exception as e:
            if verbose:
                print(f"  [WARN] Failed to download {filename}: {e}")

    return results


def local_paths() -> dict:
    """Return {filename: path} for all expected raw files (whether or not they exist)."""
    return {fname: os.path.join(RAW_DIR, fname) for fname in FILES}
