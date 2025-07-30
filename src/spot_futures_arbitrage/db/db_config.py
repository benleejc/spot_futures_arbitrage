from pathlib import Path
import os

BASE_DIR = Path.cwd()
if not os.path.exists(BASE_DIR / 'data'):
    os.makedir (BASE_DIR / 'data')
DB_PATH = BASE_DIR / 'data' / 'okx_prices.db'