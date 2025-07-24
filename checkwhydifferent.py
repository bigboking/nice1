#!/usr/bin/env python
# produce_debug.py —— 复刻 winery_ingest_and_chain 的 produce 段并打印全部对比
# 用法示例：
#   python produce_debug.py --bottle-json bottle.json --bottle-id coco1514
# 依赖：pip install web3 python-dotenv tabulate

import json, os, sqlite3, argparse, hashlib
from eth_utils import keccak
from web3 import Web3
from dotenv import load_dotenv
from tabulate import tabulate

# ─── 1. CLI ────────────────────────────────────────────────────
cli = argparse.ArgumentParser(description="打印 produce 哈希所有差异")
cli.add_argument("--bottle-json", required=True, help="当初上链用的瓶子 JSON")
cli.add_argument("--bottle-id",   required=True, help="瓶子 ID")
args = cli.parse_args()
bid = args.bottle_id

with open(args.bottle_json, encoding="utf-8") as f:
    bottle_json = json.load(f)

# ─── 2. 链 ─────────────────────────────────────────────────────
load_dotenv("customer.env")                      # 只需要 RPC_URL
cfg = json.load(open("config.json"))
w3  = Web3(Web3.HTTPProvider(cfg["rpc_url"]))
audit = w3.eth.contract(address=cfg["audit_addr"],
                        abi=json.load(open(cfg["audit_abi"])))

# ─── 3. DB 行 ─────────────────────────────────────────────────
conn = sqlite3.connect("wine_demo.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("SELECT * FROM bottle WHERE id=?;", (bid,))
db_row = cur.fetchone()
conn.close()
if not db_row:
    raise SystemExit(f"❌ DB 中没找到 bottle {bid}")

# ─── 4. 紧凑 JSON 串 & 哈希 ───────────────────────────────────
json_str_file = json.dumps(bottle_json, separators=(",", ":"))
json_str_db   = json.dumps(dict(db_row), separators=(",", ":"))

hash_file = hashlib.sha256(json_str_file.encode()).digest()
hash_db   = hashlib.sha256(json_str_db.encode()).digest()

# rowKey 算法与写链脚本一致
row_key = keccak(text=f"wine_batch:{bid}")
chain_hash = audit.functions.getProof(row_key).call()[0]      # bytes32

# ─── 5. 打印差异 ─────────────────────────────────────────────
print("\n参与哈希的紧凑 JSON")
print("JSON 文件:", json_str_file)
print("DB 行    :", json_str_db)

table = [
    ["rowKey",            "0x" + row_key.hex()],
    ["链上 hash",          chain_hash.hex()],
    ["JSON 文件 sha256",   hash_file.hex()],
    ["DB 行 sha256",       hash_db.hex()],
]

print("\nproduce 段指纹详细对比")
print(tabulate(table, tablefmt="github"))
#chekchek timepoint1， 我什么都没做只是想生成一个新的分支的一个新的节点