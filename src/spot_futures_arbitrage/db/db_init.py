import sqlite3
import logging
from db_config import DB_PATH

logger = logging.getLogger('okx_prices')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def create_price_table(db):
    """Create a SQLite database and a table for storing prices."""
    try:
        with sqlite3.connect(db) as conn:
            cur = conn.cursor()
            # check if the table already exists
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prices'")
            if cur.fetchone() is not None:
                logger.info("Table 'prices' already exists.")
                return
            cur.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                timestamp INTEGER,
                datetime DATETIME,
                symbol TEXT,
                last REAL,
                bid REAL,
                ask REAL,
                high REAL,
                low REAL,
                futures INTEGER DEFAULT 0,
                expiration_date DATETIME DEFAULT NULL,
                PRIMARY KEY (timestamp, symbol, last)
            )
            """)
            conn.commit()
        logger.info("Database and table was created.")
    except sqlite3.Error as e:
        logger.error(f"An error occurred while creating the database: {e}")
        raise

if __name__ == "__main__":
    create_price_table(DB_PATH)