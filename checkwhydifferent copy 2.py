#!/usr/bin/env python
# deliver_debug.py —— 校验零售售出 (deliver) 段
# 用法示例:
#   python deliver_debug.py --event-json sold_coco1514.json --bottle-id coco1514
# 依赖: pip install web3 python-dotenv tabulate

import json, os, sqlite3, argparse, hashlib
from eth_utils import keccak
from web3       import Web3
from dotenv     import load_dotenv
from tabulate   import tabulate

# ─── 1. CLI ───────────────────────────────────────────────────
cli = argparse.ArgumentParser(description="打印 deliver 段哈希所有差异")
cli.add_argument("--event-json", required=True, help="售出 JSON (与上链时同一份)")
cli.add_argument("--bottle-id",  required=True, help="瓶子 ID")
args = cli.parse_args()
bid = args.bottle_id

# 1.1 读取 JSON
with open(args.event_json, encoding="utf-8") as f:
    deliver_json = json.load(f)
ts = deliver_json["ts"]

# ─── 2. 链 ────────────────────────────────────────────────────
load_dotenv("customer.env")               # 只需 RPC_URL
cfg = json.load(open("config.json"))
w3  = Web3(Web3.HTTPProvider(cfg["rpc_url"]))
audit = w3.eth.contract(address=cfg["audit_addr"],
                        abi=json.load(open(cfg["audit_abi"])))

# ─── 3. DB 行（sold_event）────────────────────────────────────
conn = sqlite3.connect("wine_demo.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("""
    SELECT * FROM sold_event
    WHERE bottle_id=? AND ts=?;
""", (bid, ts))
db_row = cur.fetchone()
conn.close()
if not db_row:
    raise SystemExit(f"❌ DB 无 bottle {bid} ts={ts} 的 sold_event 行")

# ─── 4. 紧凑 JSON & 哈希 ────────────────────────────────────
json_str_file = json.dumps(deliver_json, separators=(",", ":"))

db_core = {
    "bottle_id":  db_row["bottle_id"],
    "store":      db_row["store"],
    "ts":         db_row["ts"]
}
json_str_db = json.dumps(db_core, separators=(",", ":"))

hash_file = hashlib.sha256(json_str_file.encode()).digest()
hash_db   = hashlib.sha256(json_str_db.encode()).digest()

# rowKey 与 retailer_deliver.py 完全一致
row_key   = keccak(text=f"deliver:{bid}:{ts}")
chain_hash = audit.functions.getProof(row_key).call()[0]      # bytes32

# ─── 5. 打印对比 ─────────────────────────────────────────────
print("\n参与哈希的紧凑 JSON")
print("JSON 文件:", json_str_file)
print("DB 行    :", json_str_db)

table = [
    ["rowKey",            "0x" + row_key.hex()],
    ["链上 hash",          chain_hash.hex()],
    ["JSON 文件 sha256",   hash_file.hex()],
    ["DB 行 sha256",       hash_db.hex()],
]

print("\ndeliver 段指纹详细对比")
print(tabulate(table, tablefmt="github"))
