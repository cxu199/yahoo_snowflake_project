import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# === 载入环境变量 ===
load_dotenv()

USER = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# === 连接到 Snowflake ===
try:
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    print("✅ 成功连接到 Snowflake")
except Exception as e:
    print(f"❌ Snowflake 连接失败: {e}")
    exit(1)

# === 加载 CSV 数据 ===
csv_path = "data/top50_data_250101_251014.csv"

if not os.path.exists(csv_path):
    print(f"❌ 找不到文件: {csv_path}")
    exit(1)

df = pd.read_csv(csv_path)
print(f"📄 已加载 {len(df)} 行数据。")

# === 创建表（如果不存在） ===
table_name = "TOP50_STOCKS"

create_table_query = f"""
CREATE OR REPLACE TABLE {table_name} (
    symbol STRING,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    adjclose FLOAT
)
"""

try:
    with conn.cursor() as cur:
        cur.execute(create_table_query)
    print(f"✅ 表 {table_name} 已创建或存在")
except Exception as e:
    print(f"❌ 创建表失败: {e}")
    conn.close()
    exit(1)

# === 上传数据 ===
try:
    success, nchunks, nrows, _ = conn.write_pandas(df, table_name.upper())
    print(f"✅ 成功上传 {nrows} 行数据到表 {table_name}")
except Exception as e:
    print(f"❌ 上传数据失败: {e}")

conn.close()
