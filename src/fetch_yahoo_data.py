# fetch_yahoo_data.py
import pandas as pd
from yahooquery import Screener, Ticker
from datetime import date
import os

# ---------- 0. 创建 data 文件夹 ----------
data_folder = "data"
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

# ---------- 1. 获取市值前 50 股票 ----------
print("🔍 正在从 Yahoo Finance 获取市值前 50 股票...")
s = Screener()
data = s.get_screeners("most_actives", count=200)["most_actives"]["quotes"]
df = pd.DataFrame(data)
df = df.sort_values("marketCap", ascending=False).head(50)
tickers = df["symbol"].tolist()
print(f"✅ 获取到前50股票: {tickers}")

# ---------- 2. 获取每日行情 ----------
prices = []
today = date.today()
date_str = today.strftime("%y%m%d")  # 生成类似 251015 格式的日期
for t in tickers:
    try:
        ticker = Ticker(t)
        hist = ticker.history(period="1d")
        if not hist.empty:
            latest = hist.reset_index().iloc[-1]
            prices.append({
                "TICKER": t,
                "DATE": today,
                "OPEN": latest["open"],
                "HIGH": latest["high"],
                "LOW": latest["low"],
                "CLOSE": latest["close"],
                "VOLUME": latest["volume"],
                "MARKET_CAP": df[df["symbol"] == t]["marketCap"].values[0]
            })
    except Exception as e:
        print(f"⚠️ {t} 数据获取失败: {e}")

df_prices = pd.DataFrame(prices)

# ---------- 3. 保存到 data 文件夹，文件名加日期 ----------
file_path = os.path.join(data_folder, f"top50_data_{date_str}.csv")
df_prices.to_csv(file_path, index=False)
print(f"📁 已保存最新前50股票数据到 {file_path}")
