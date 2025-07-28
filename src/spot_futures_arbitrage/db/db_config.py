from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.resolve()
DB_PATH = BASE_DIR / 'data' / 'okx_prices.db'