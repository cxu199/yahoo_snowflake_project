# upload_to_snowflake.py
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
from datetime import date

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

# ---------- 3. 找到当天最新的 CSV ----------
data_folder = "data"
today = date.today()
date_str = today.strftime("%y%m%d")
file_path = os.path.join(data_folder, f"top50_data_{date_str}.csv")

if not os.path.exists(file_path):
    raise FileNotFoundError(f"未找到当天 CSV 文件: {file_path}")

df = pd.read_csv(file_path)
print(f"📁 读取 CSV 文件: {file_path}")

# ---------- 4. 上传数据到 Snowflake ----------
# 删除当天旧数据，避免重复
cursor.execute(f"DELETE FROM YAHOO_TOP50_MARKETCAP WHERE DATE = '{today}'")

# 插入新数据
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO YAHOO_TOP50_MARKETCAP
        (TICKER, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, MARKET_CAP)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        row["TICKER"], row["DATE"], row["OPEN"], row["HIGH"],
        row["LOW"], row["CLOSE"], row["VOLUME"], row["MARKET_CAP"]
    ))

conn.commit()
conn.close()
print(f"✅ 今日数据已成功写入 Snowflake 表：YAHOO_TOP50_MARKETCAP")
