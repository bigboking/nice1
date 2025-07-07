#!/usr/bin/env python
# winery_ingest_and_chain.py —— 生产端：DB 写入 + produce / storeHash 上链
# pip install web3 python-dotenv
import json, os, sqlite3, argparse, hashlib
from eth_account import Account
from eth_utils import keccak
from web3 import Web3
from dotenv import load_dotenv

# ─── 1·解析 CLI ────────────────────────────────────────────────
cli = argparse.ArgumentParser()
cli.add_argument("--batch-json",  required=True, help="批次 JSON 文件")
cli.add_argument("--bottle-json", required=True, help="瓶子 JSON 文件")
args = cli.parse_args()

batch  = json.load(open(args.batch_json,  encoding="utf-8"))
bottle = json.load(open(args.bottle_json, encoding="utf-8"))
bid, batch_id = bottle["id"], batch["id"]

# ─── 2·链连接 ─────────────────────────────────────────────────
load_dotenv("winery.env")
acct = Account.from_key(os.environ["PRIVATE_KEY"])
cfg  = json.load(open("config.json"))
w3   = Web3(Web3.HTTPProvider(cfg["rpc_url"]))
w3.eth.default_account = acct.address
nonce = w3.eth.get_transaction_count(acct.address)

life  = w3.eth.contract(address=cfg["life_addr"],  abi=json.load(open(cfg["life_abi"])))
audit = w3.eth.contract(address=cfg["audit_addr"], abi=json.load(open(cfg["audit_abi"])))

def send(tx):
    """签名并发送交易，返回交易哈希 hex 字符串"""
    global nonce
    tx.update({"from":acct.address,"nonce":nonce,
               "chainId":cfg["chain_id"],"gas":300_000
               })
    signed = acct.sign_transaction(tx)
    txh = w3.eth.send_raw_transaction(signed.raw_transaction)
    rec = w3.eth.wait_for_transaction_receipt(txh)
    if rec.status != 1:
        raise RuntimeError(f"Tx reverted: {txh.hex()}")
    nonce += 1
    return txh.hex()

# ─── 3·本地 DB 事务 ───────────────────────────────────────────
conn = sqlite3.connect("wine_demo.db", isolation_level=None)
cur  = conn.cursor()

try:
    cur.execute("BEGIN;")

    # ① wine_batch（若不存在则插）
    cur.execute("SELECT 1 FROM wine_batch WHERE id=?;", (batch_id,))
    if not cur.fetchone():
        cur.execute("""INSERT INTO wine_batch(id,harvest_year,variety,vineyard)
                       VALUES(:id,:harvest_year,:variety,:vineyard);""", batch)
        print(f"📥 新建批次 {batch_id}")

    # ② bottle（若不存在则插）
    cur.execute("SELECT 1 FROM bottle WHERE id=?;", (bid,))
    if cur.fetchone():
        raise ValueError(f"瓶子 {bid} 已存在")
    cur.execute("""INSERT INTO bottle(id,batch_id,current_status,retailer,bottle_key)
                   VALUES(:id,:batch_id,:current_status,:retailer,:bottle_key);""",
                {
                    "id": bid,
                    "batch_id": batch_id,
                    "current_status": bottle.get("current_status","Produced"),
                    "retailer": bottle.get("retailer",""),
                    "bottle_key": bottle.get("bottle_key","")
                })
    print(f"📥 新建瓶子 {bid}")

    # ③ 生成紧凑 JSON、row_key、row_hash
    compact_json = json.dumps(bottle, separators=(",",":"))
    row_hash = hashlib.sha256(compact_json.encode()).digest()
    bottle_key = keccak(text="bottle:"+bid)
    row_key    = keccak(text=f"wine_batch:{bid}")

    print("\n=== 上链前调试 ===")
    print("compact JSON :", compact_json)
    print("row_key      :", "0x"+row_key.hex())
    print("row_hash     :", "0x"+row_hash.hex())

    # ④ 链上双交易
    tx1 = send(life.functions.produce(bottle_key).build_transaction())
    print("⛓  produce tx =", tx1)
    tx2 = send(audit.functions.storeHash(row_key, row_hash).build_transaction())
    print("⛓  storeHash tx =", tx2)

    # ⑤ 查询链上 hash 以确认
    chain_hash = audit.functions.getProof(row_key).call()[0].hex()
    print("\n链上实际 hash  :", chain_hash)
    print("本地 row_hash  :", "0x"+row_hash.hex())
    if chain_hash == "0x"+row_hash.hex():
        print("✅ 链上 hash 与本地一致")
    else:
        print("❌ 链上 hash 与本地不一致（应进一步排查）")

    conn.commit()
    print("\n✅ 全部成功：DB 写入 & 链上写入")

except Exception as e:
    conn.rollback()
    print("\n❌ 失败已回滚：", e)

finally:
    conn.close()
