import json
from web3 import Web3
from eth_account import Account

# ====== 手动填写连接信息 ======
RPC_URL      = "https://eth-sepolia.g.alchemy.com/v2/f3OunY6zRNeWeXd81Ayw4JQGVnIxeJ3M"  # Sepolia RPC
PRIVATE_KEY  = "69bde6afea620346af6b3a0532c366bf001ab62dc9bb4ccebea244553331ecc6"         # 你的部署/管理钱包私钥
CONTRACT_ADDR = "0xc2798D6ce4b3CBEab4F044611262f50B55A5d0e0"  # 已部署合约地址
CHAIN_ID = 11155111                                # Sepolia chainId
ABI_PATH = "LunchVenuenew_abi.json"                # ABI 文件路径
# =================================

# 1. 连接链
w3 = Web3(Web3.HTTPProvider(RPC_URL))
assert w3.is_connected(), "无法连接到 Sepolia 节点"

# 2. 账户对象
acct = Account.from_key(PRIVATE_KEY)

# 3. 加载 ABI 并实例化合约
with open(ABI_PATH, "r") as f:
    abi = json.load(f)

contract = w3.eth.contract(address=CONTRACT_ADDR, abi=abi)

# ---------- 只读调用示例 ----------
phase = contract.functions.phase().call()
num_friends = contract.functions.numFriends().call()
print(f"当前阶段: {phase}，已添加好友数: {num_friends}")

# ---------- 发送交易示例 ----------
def add_restaurant(name: str):
    txn = contract.functions.addRestaurant(name).build_transaction({
        "from": acct.address,
        "nonce": w3.eth.get_transaction_count(acct.address),
        "gas": 200_000,
        "maxFeePerGas": w3.to_wei(30, "gwei"),
        "maxPriorityFeePerGas": w3.to_wei(2, "gwei"),
        "chainId": CHAIN_ID,
    })
    signed = acct.sign_transaction(txn)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    print("AddRestaurant 交易已上链，区块号:", receipt.blockNumber)

# 调用例子
# add_restaurant("Sushi Bar")
