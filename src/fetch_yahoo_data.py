# src/fetch_yahoo_data.py

import yfinance as yf
import pandas as pd
from datetime import datetime

def fetch_stock_data(ticker: str, period: str = "1d", interval: str = "1d"):
    """
    从 Yahoo Finance 抓取指定股票的历史数据
    :param ticker: 股票代码（例如 "AAPL", "TSLA"）
    :param period: 时间范围（1d, 5d, 1mo, 3mo, 1y, etc.）
    :param interval: 数据频率（1d, 1h, 5m, etc.）
    """
    print(f"Fetching {ticker} data from Yahoo Finance...")
    data = yf.download(ticker, period=period, interval=interval)
    data.reset_index(inplace=True)
    data["Ticker"] = ticker
    return data


def save_to_csv(data: pd.DataFrame, ticker: str):
    """保存数据为 CSV 文件"""
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"data/{ticker}_{today}.csv"
    data.to_csv(filename, index=False)
    print(f"✅ Saved: {filename}")


if __name__ == "__main__":
    # 可修改股票列表
    tickers = ["AAPL", "MSFT", "TSLA"]

    all_data = pd.DataFrame()
    for t in tickers:
        df = fetch_stock_data(t)
        save_to_csv(df, t)
        all_data = pd.concat([all_data, df])

    print("🎯 All data fetched successfully.")
