import yfinance as yf
import pandas as pd
from datetime import datetime

def get_stock_data(symbol="AAPL"):
    data = yf.download(symbol, period="1d")
    data.reset_index(inplace=True)
    data["symbol"] = symbol
    print(data.tail())
    data.to_csv("latest_data.csv", index=False)
    return data

if __name__ == "__main__":
    get_stock_data("AAPL")
