# src/init_db.py
import sqlite3, pathlib, json

SCHEMA = """
CREATE TABLE IF NOT EXISTS wine_batch (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  winery_code TEXT,
  vintage INTEGER,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  json_data TEXT
);
CREATE TABLE IF NOT EXISTS transport_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER,
  location TEXT,
  temperature REAL,
  event_time DATETIME,
  json_data TEXT
);
CREATE TABLE IF NOT EXISTS bottle (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER,
  serial TEXT UNIQUE,
  state TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  json_data TEXT
);
"""

def main():
    db_path = pathlib.Path("trace.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    print(f"✅ 数据库初始化完成 → {db_path.resolve()}")

    # 演示插入一条
    sample = {"note": "hello sqlite"}
    conn.execute(
        "INSERT INTO wine_batch (winery_code, vintage, json_data) VALUES (?,?,?)",
        ("WX01", 2023, json.dumps(sample))
    )
    conn.commit()
    rows = conn.execute("SELECT * FROM wine_batch").fetchall()
    print("📦 当前 wine_batch 内容：", rows)

if __name__ == "__main__":
    main()
