# test_snowflake_connection.py
import snowflake.connector
from dotenv import load_dotenv
import os
from datetime import date

# 1️⃣ 载入 .env
load_dotenv()

# 2️⃣ 获取环境变量
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")

print("🌟 正在尝试连接 Snowflake ...")

try:
    # 3️⃣ 建立连接
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    cursor = conn.cursor()

    # 4️⃣ 测试查询
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()[0]
    print(f"✅ 成功连接 Snowflake! 版本: {version}")

    # 5️⃣ 打印今天日期
    today = date.today()
    print(f"📅 今天日期: {today}")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"❌ 连接失败: {e}")
