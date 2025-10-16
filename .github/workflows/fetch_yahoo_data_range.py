import os
import pandas as pd
import datetime
from yahooquery import Screener, Ticker

# === 参数设定 ===
START_DATE = "2025-01-01"
END_DATE = "2025-10-14"

# 生成文件名
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "top50_data_250101_251014.csv")

# === 获取纳斯达克前50市值股票 ===
print("🔍 正在从 Yahoo Finance 获取纳斯达克前50市值股票...")

try:
    screener = Screener()
    data = screener.get_screeners('most_actives', count=100)
    results = data['most_actives']['quotes']

    df_stocks = pd.DataFrame(results)
    df_stocks = df_stocks[df_stocks['exchange'] == 'NMS']  # NMS = NASDAQ
    df_stocks = df_stocks.sort_values('marketCap', ascending=False).head(50)
    symbols = df_stocks['symbol'].tolist()

    print(f"✅ 获取成功，共 {len(symbols)} 支股票。")
except Exception as e:
    print(f"❌ 获取股票列表失败: {e}")
    exit(1)

# === 获取这些股票的历史数据 ===
print(f"📊 正在下载 {len(symbols)} 支股票的 {START_DATE} 至 {END_DATE} 历史数据...")

try:
    ticker = Ticker(symbols)
    history = ticker.history(start=START_DATE, end=END_DATE)

    # 如果返回多层索引（symbol, date），展开
    if isinstance(history.index, pd.MultiIndex):
        history = history.reset_index()

    # 保存结果
    history.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ 数据已保存到 {OUTPUT_FILE}")

except Exception as e:
    print(f"❌ 下载或保存数据时出错: {e}")
