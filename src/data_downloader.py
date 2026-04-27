"""
Downloads raw CSV files from the Wells-Dataset GitHub repo and saves to data/raw/.
Falls back gracefully if network is unavailable.
"""
import os
import requests

from src.spark_session import RAW_DIR

GITHUB_BASE = "https://raw.githubusercontent.com/DoomDust7/Wells-Dataset/main"

FILES = {
    "WELLHEADER.csv":        f"{GITHUB_BASE}/WELLHEADER.csv",
    "WELLEUR.csv":           f"{GITHUB_BASE}/WELLEUR.csv",
    "PRODUCTION.csv":        f"{GITHUB_BASE}/PRODUCTION.csv",
    "PRODUCTIONFLARING.csv": f"{GITHUB_BASE}/PRODUCTIONFLARING.csv",
    "WATERPRODUCTION.csv":   f"{GITHUB_BASE}/WATERPRODUCTION.csv",
    "INITIALPRODUCTION.csv": f"{GITHUB_BASE}/INITIALPRODUCTION.csv",
    "ECONOMICSCOST.csv":     f"{GITHUB_BASE}/ECONOMICSCOST.csv",
    "EUR.csv":               f"{GITHUB_BASE}/EUR.csv",
    "PRICES.csv":            f"{GITHUB_BASE}/PRICES.csv",
    "OPERATOR.csv":          f"{GITHUB_BASE}/OPERATOR.csv",
}

# These files are large enough to warrant chunked streaming
LARGE_FILES = {"PRODUCTION.csv", "WATERPRODUCTION.csv", "PRODUCTIONFLARING.csv"}


def download_large_file(url: str, dest: str, chunk_size_mb: int = 8, verbose: bool = True) -> bool:
    """Stream a large file in chunks to avoid loading it all into memory."""
    chunk_size = chunk_size_mb * 1024 * 1024
    try:
        with requests.get(url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                downloaded = 0
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
            if verbose:
                print(f"         Saved {os.path.getsize(dest):,} bytes")
        return True
    except Exception as e:
        if verbose:
            print(f"  [WARN] Chunked download failed: {e}")
        return False


def download_all(verbose: bool = True) -> dict:
    """
    Download all CSVs. Returns {filename: local_path} for successfully downloaded files.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    results = {}

    for filename, url in FILES.items():
        dest = os.path.join(RAW_DIR, filename)
        if os.path.exists(dest) and os.path.getsize(dest) > 1000:
            if verbose:
                print(f"  [SKIP] {filename} already exists ({os.path.getsize(dest):,} bytes)")
            results[filename] = dest
            continue
        try:
            if verbose:
                print(f"  [DOWN] {filename} <- {url}")
            if filename in LARGE_FILES:
                ok = download_large_file(url, dest, verbose=verbose)
                if ok:
                    results[filename] = dest
            else:
                resp = requests.get(url, timeout=60)
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
