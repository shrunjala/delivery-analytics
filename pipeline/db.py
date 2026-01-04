import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/raw_orders.duckdb")

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("""
    CREATE TABLE IF NOT EXISTS raw_orders (
      message_id TEXT,
      platform TEXT,
      account_email TEXT,
      label TEXT,
      order_ts TIMESTAMP,
      merchant TEXT,
      subtotal DOUBLE,
      fees DOUBLE,
      total DOUBLE,
      currency TEXT,
      raw_subject TEXT
    );
    """)
    con.close()

def upsert_orders(rows):
    if not rows:
        return 0
    con = duckdb.connect(str(DB_PATH))
    df = pd.DataFrame(rows)
    con.register("df", df)

    res = con.execute("""
      INSERT INTO raw_orders (
        message_id, platform, account_email, label, order_ts,
        merchant, subtotal, fees, total, currency, raw_subject
      )
      SELECT
        message_id, platform, account_email, label, order_ts,
        merchant, subtotal, fees, total, currency, raw_subject
      FROM df
      WHERE message_id NOT IN (SELECT message_id FROM raw_orders)
    """).rowcount

    con.close()
    return res

