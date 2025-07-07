from dotenv import load_dotenv
import os
load_dotenv("winery.env")  # 或 ".env"
print(os.getenv("PRIVATE_KEY"))
