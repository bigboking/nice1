#!/usr/bin/env python
# shipper_ship.py —— 运输方：DB 写入 + ship/storeHash 上链（含调试输出）
# pip install web3 python-dotenv
import json, os, sqlite3, argparse, hashlib
from eth_account import Account
from eth_utils  import keccak
from web3       import Web3
from dotenv     import load_dotenv

# ───────── 1·CLI ─────────
cli = argparse.ArgumentParser()
cli.add_argument("--bottle-id",  required=True, help="瓶子 ID")
cli.add_argument("--event-json", required=True, help="运输里程碑 JSON 文件")
args = cli.parse_args()

ship_row = json.load(open(args.event_json, encoding="utf-8"))

# 如 JSON 自带 bottle_id 则校验一致，否则补上
if "bottle_id" in ship_row and ship_row["bottle_id"] != args.bottle_id:
    raise ValueError("event-json 内 bottle_id 与参数不一致")
ship_row["bottle_id"] = args.bottle_id      # 保证字段存在

# ───────── 2·链配置 ─────────
load_dotenv("shipper.env")
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

# ───────── 3·DB 事务 ─────────
conn = sqlite3.connect("wine_demo.db", isolation_level=None)
cur  = conn.cursor()

try:
    cur.execute("BEGIN;")

    # 0️⃣ 确认瓶子存在
    cur.execute("SELECT 1 FROM bottle WHERE id=?;", (args.bottle_id,))
    if not cur.fetchone():
        raise ValueError(f"瓶子 {args.bottle_id} 不存在，请先 produce")

    # ① INSERT transport_event
    cur.execute("""
        INSERT INTO transport_event(bottle_id,location,status,ts,is_milestone)
        VALUES (:bottle_id,:location,:status,:ts,:is_milestone);
    """, ship_row)

    # ② 若是里程碑才上链
    if ship_row.get("is_milestone") == 1:
        bottle_key   = keccak(text="bottle:"+args.bottle_id)
        row_key      = keccak(text=f"ship:{args.bottle_id}:{ship_row['ts']}")

        # --- 生成紧凑 JSON & row_hash ---
        compact_json = json.dumps(ship_row, separators=(",",":"))
        row_hash     = hashlib.sha256(compact_json.encode()).digest()

        print("\n=== 上链前调试 ===")
        print("compact JSON :", compact_json)
        print("row_key      :", "0x"+row_key.hex())
        print("row_hash     :", "0x"+row_hash.hex())

        # --- 两笔链上交易 ---
        tx1 = send(life.functions.ship(bottle_key).build_transaction())
        tx2 = send(audit.functions.storeHash(row_key, row_hash).build_transaction())

        # --- 链上校验 ---
        chain_hash = audit.functions.getProof(row_key).call()[0].hex()
        print("\n链上实际 hash :", chain_hash)
        print("本地 row_hash :", "0x"+row_hash.hex())
        if chain_hash == "0x"+row_hash.hex():
            print("✅ 链上 hash 与本地一致")
        else:
            print("❌ 链上 hash 与本地不一致，请排查")

        print("🔗  里程碑已上链")
    else:
        print("📄  普通节点：只落库，不上链")

    conn.commit()
    print("\n✅ ship 完成 —— DB 提交")

except Exception as e:
    conn.rollback()
    print("\n❌ ship 失败已回滚：", e)

finally:
    conn.close()
