#!/usr/bin/env python
# retailer_deliver.py —— 售出：DB + deliver / storeHash 上链（含调试输出）
# pip install web3 python-dotenv
import json, os, sqlite3, argparse, hashlib
from eth_account import Account
from eth_utils  import keccak
from web3       import Web3
from dotenv     import load_dotenv

# ── 1·CLI ─────────────────────────────────────
cli = argparse.ArgumentParser()
cli.add_argument("--event-json", required=True, help="JSON: {bottle_id,store,ts}")
args = cli.parse_args()
ev = json.load(open(args.event_json, encoding="utf-8"))
bid, store, ts = ev["bottle_id"], ev["store"], ev["ts"]

# ── 2·链连接 ──────────────────────────────────
load_dotenv("retailer.env")
acct = Account.from_key(os.environ["PRIVATE_KEY"])
cfg  = json.load(open("config.json"))
w3   = Web3(Web3.HTTPProvider(cfg["rpc_url"]))
w3.eth.default_account = acct.address
nonce = w3.eth.get_transaction_count(acct.address)

life  = w3.eth.contract(address=cfg["life_addr"],  abi=json.load(open(cfg["life_abi"])))
audit = w3.eth.contract(address=cfg["audit_addr"], abi=json.load(open(cfg["audit_abi"])))

def send(tx):
    global nonce
    tx.update({"from":acct.address,"nonce":nonce,
               "chainId":cfg["chain_id"],
               "gas":300_000})
    signed = acct.sign_transaction(tx)
    txh = w3.eth.send_raw_transaction(signed.raw_transaction)
    w3.eth.wait_for_transaction_receipt(txh)
    print("⛓  tx =", txh.hex())
    nonce += 1
    return txh.hex()

# ── 3·DB 事务 ─────────────────────────────────
conn = sqlite3.connect("wine_demo.db", isolation_level=None)
cur  = conn.cursor()

try:
    cur.execute("BEGIN;")
    cur.execute("SELECT 1 FROM bottle WHERE id=?;", (bid,))
    if not cur.fetchone():
        raise ValueError("瓶子不存在")

    cur.execute("INSERT INTO sold_event(bottle_id,store,ts) VALUES(?,?,?);",
                (bid, store, ts))

    compact_json = json.dumps(ev, separators=(",",":"))
    row_hash = hashlib.sha256(compact_json.encode()).digest()
    bottle_key = keccak(text="bottle:"+bid)
    row_key    = keccak(text=f"deliver:{bid}:{ts}")

    print("\n=== 上链前调试 ===")
    print("compact JSON :", compact_json)
    print("row_key      :", "0x"+row_key.hex())
    print("row_hash     :", "0x"+row_hash.hex())

    send(life.functions.deliver(bottle_key).build_transaction())
    send(audit.functions.storeHash(row_key, row_hash).build_transaction())

    chain_hash = audit.functions.getProof(row_key).call()[0].hex()
    print("链上 hash     :", chain_hash)
    print("本地 row_hash :", "0x"+row_hash.hex())
    print("匹配结果      :", "✓" if chain_hash=="0x"+row_hash.hex() else "×")

    conn.commit()
    print("✅ deliver 完成 —— Sold 写库并上链")

except Exception as e:
    conn.rollback()
    print("❌ deliver 失败已回滚：", e)

finally:
    conn.close()
