from pathlib import Path
import os

BASE_DIR = Path.cwd()
os.mkdir(BASE_DIR / 'data', exist_ok=True)
DB_PATH = BASE_DIR / 'data' / 'okx_prices.db'