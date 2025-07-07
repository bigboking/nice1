#!/usr/bin/env python
# customer_verify.py – verify produce / ship / deliver hashes and show details
# pip install web3 python-dotenv tabulate
import json, os, sqlite3, argparse, hashlib
from eth_utils import keccak
from web3 import Web3
from dotenv import load_dotenv
from tabulate import tabulate

# ───────── 1. CLI ─────────────────────────────────────────────
cli = argparse.ArgumentParser(description="Verify bottle provenance on-chain")
cli.add_argument("--bottle-id", required=True, help="Bottle ID")
args = cli.parse_args()
bid = args.bottle_id

# ───────── 2. Chain connection (read-only) ───────────────────
load_dotenv("customer.env")               # needs only RPC_URL
cfg = json.load(open("config.json"))
w3  = Web3(Web3.HTTPProvider(cfg["rpc_url"]))
life  = w3.eth.contract(address=cfg["life_addr"],  abi=json.load(open(cfg["life_abi"])))
audit = w3.eth.contract(address=cfg["audit_addr"], abi=json.load(open(cfg["audit_abi"])))

# ───────── 3. Read local DB rows ─────────────────────────────
conn = sqlite3.connect("wine_demo.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("SELECT * FROM bottle WHERE id=?;", (bid,))
bottle_row = cur.fetchone()
if not bottle_row:
    raise SystemExit(f"No bottle '{bid}' in local DB.")

batch_id = bottle_row["batch_id"]

cur.execute("SELECT * FROM wine_batch WHERE id=?;", (batch_id,))
batch_row = cur.fetchone()

cur.execute("""
  SELECT * FROM transport_event
  WHERE bottle_id=? AND is_milestone=1
  ORDER BY ts DESC LIMIT 1;
""", (bid,))
ship_row_latest = cur.fetchone()

cur.execute("SELECT * FROM transport_event WHERE bottle_id=? ORDER BY ts;", (bid,))
ship_rows_all = cur.fetchall()

cur.execute("SELECT * FROM sold_event WHERE bottle_id=? LIMIT 1;", (bid,))
sold_row = cur.fetchone()
conn.close()

# ───────── 4. Build compact-JSON & SHA-256 per rule ─────────
def sha256_hex(obj: dict) -> str:
    return hashlib.sha256(json.dumps(obj, separators=(",", ":")).encode()).hexdigest()

records = []   # (row_key_bytes, local_hex, label)

# a) produce
row_key_prod = keccak(text=f"wine_batch:{bid}")
records.append((row_key_prod, sha256_hex(dict(bottle_row)), "produce"))

# b) ship (latest milestone)
if ship_row_latest:
    ts_ship = ship_row_latest["ts"]
    ship_core = {
        "location":     ship_row_latest["location"],
        "status":       ship_row_latest["status"],
        "ts":           ts_ship,
        "is_milestone": ship_row_latest["is_milestone"],
        "bottle_id":    bid
    }
    row_key_ship = keccak(text=f"ship:{bid}:{ts_ship}")
    records.append((row_key_ship, sha256_hex(ship_core), "ship"))
else:
    records.append((None, None, "ship (no milestone)"))

# c) deliver
if sold_row:
    ts_del = sold_row["ts"]
    deliver_core = {
        "bottle_id": sold_row["bottle_id"],
        "store":     sold_row["store"],
        "ts":        ts_del
    }
    row_key_deliver = keccak(text=f"deliver:{bid}:{ts_del}")
    records.append((row_key_deliver, sha256_hex(deliver_core), "deliver"))
else:
    records.append((None, None, "deliver (no sold_event)"))

# ───────── 5. Compare with on-chain values ───────────────────
print("\nDebug: rowKey / chain hash / local hash")
results = []
all_ok = True

for rk_bytes, local_hex, tag in records:
    if rk_bytes is None:
        print(f"[{tag}] missing locally")
        results.append([tag, "missing locally", "×"])
        all_ok = False
        continue

    chain_hex = audit.functions.getProof(rk_bytes).call()[0].hex()
    local_prefixed = local_hex
    print(f"[{tag}]\n  rowKey : 0x{rk_bytes.hex()}\n  chain  : {chain_hex}\n  local  : {local_prefixed}")

    ok = "✓" if chain_hex == local_prefixed else "×"
    if ok == "×":
        all_ok = False
    results.append([tag, "match" if ok == "✓" else "mismatch", ok])

# ───────── 6. Bottle lifecycle status ────────────────────────
status_code = life.functions.bottles(keccak(text="bottle:"+bid)).call()[0]
status_map  = {0:"None",1:"Produced",2:"InTransit",3:"Delivered"}
life_status = status_map.get(status_code, str(status_code))

# ───────── 7. Summary table ──────────────────────────────────
print("\nOff-chain / On-chain hash check")
print(tabulate(results, headers=["Stage", "Result", "✓/×"], tablefmt="github"))
print(f"\nOn-chain lifecycle status: {life_status}")

# ───────── 8. If everything matches, print full details ──────
if all_ok:
    print("\nAll stages verified ✓ – detailed information follows:\n")

    print("Bottle row:")
    print(tabulate([bottle_row], headers=bottle_row.keys(), tablefmt="github"))

    if batch_row:
        print("\nBatch row:")
        print(tabulate([batch_row], headers=batch_row.keys(), tablefmt="github"))

    if ship_rows_all:
        print("\nTransport events:")
        print(tabulate(ship_rows_all, headers=ship_rows_all[0].keys(), tablefmt="github"))

    if sold_row:
        print("\nSold event:")
        print(tabulate([sold_row], headers=sold_row.keys(), tablefmt="github"))
else:
    print("\n✗ At least one stage failed – no detailed dump shown.")
