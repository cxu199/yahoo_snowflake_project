# upload_to_snowflake_2025.py
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os

# ---------- 0. 载入环境变量 ----------
load_dotenv()

# ---------- 1. 连接 Snowflake ----------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cursor = conn.cursor()
print("✅ 成功连接到 Snowflake")

# ---------- 2. 确保表存在 ----------
cursor.execute("""
CREATE TABLE IF NOT EXISTS YAHOO_TOP50_MARKETCAP (
    TICKER STRING,
    DATE DATE,
    OPEN FLOAT,
    HIGH FLOAT,
    LOW FLOAT,
    CLOSE FLOAT,
    VOLUME FLOAT,
    MARKET_CAP FLOAT
);
""")
print("📄 已确认表 YAHOO_TOP50_MARKETCAP 存在")

# ---------- 3. 读取 CSV 文件 ----------
data_folder = "data"
file_path = os.path.join(data_folder, "top50_data_250101_251014.csv")

if not os.path.exists(file_path):
    raise FileNotFoundError(f"❌ 未找到 CSV 文件: {file_path}")

df = pd.read_csv(file_path)
print(f"📁 读取 CSV 文件成功，共 {len(df)} 行")

# 统一列名格式
df.columns = [c.upper() for c in df.columns]

# 确保必要列存在
required_cols = ["SYMBOL", "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"缺少列: {col}")

# MARKET_CAP 有的可能缺失，用 NaN 填充
if "MARKET_CAP" not in df.columns:
    df["MARKET_CAP"] = None

# ---------- 4. 上传数据到 Snowflake ----------
print("🚀 正在上传历史数据到 Snowflake ...")

insert_sql = """
INSERT INTO YAHOO_TOP50_MARKETCAP
(TICKER, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, MARKET_CAP)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
"""

count = 0
for _, row in df.iterrows():
    cursor.execute(insert_sql, (
        row["SYMBOL"], row["DATE"], row["OPEN"], row["HIGH"],
        row["LOW"], row["CLOSE"], row["VOLUME"], row["MARKET_CAP"]
    ))
    count += 1

conn.commit()
conn.close()
print(f"✅ 成功上传 {count} 行数据至 Snowflake 表：YAHOO_TOP50_MARKETCAP")
