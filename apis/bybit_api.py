# 2025.03.13  18.00
import pandas as pd
import ccxt
import ccxt.async_support as ccxt_async
import asyncio
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
import dash
from dash import dcc, html, dash_table, callback
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.express as px
from sqlalchemy import create_engine,  text
from datetime import datetime

# ----- 1. CONFIGURATION -----
DB_CONFIG = "postgresql+psycopg://sql_admin:sql_pass@postgresql:5432/n8n"
sql_engine = create_engine(DB_CONFIG, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=1800,      
    connect_args={'connect_timeout': 5, 'keepalives': 1, 'keepalives_idle': 30, 'keepalives_interval': 10, 'keepalives_count': 5})

SYMBOLS = ["BTC/USDT","ETH/USDT","SOL/USDT","XRP/USDT","SUI/USDT","HYPE/USDT","LTC/USDT","ETC/USDT","ADA/USDT","BNB/USDT","COMP/USDT","AVAX/USDT", \
    "LINK/USDT","BCH/USDT","ICP/USDT","TIA/USDT","ZEN/USDT","RENDER/USDT","APT/USDT","MNT/USDT","ENA/USDT","ONDO/USDT","SEI/USDT","DOT/USDT","AXS/USDT", \
    "AAPLX/USDT", "NVDAX/USDT", "TSLAX/USDT",  "AMZNX/USDT", "METAX/USDT", "COINX/USDT",  "HOODX/USDT"]

# ----- 2. FASTAPI/APIRouter -----
router = APIRouter()

bybit = ccxt.bybit() 
bybit_async = ccxt_async.bybit({'enableRateLimit': True, 'options': { 'defaultType': 'linear'}})
TIMEFRAME = '5m' 
limit = 101 

async def fetch_one_symbol(symbol: str, ticker_data: dict):
    try:     
        ohlcv = await bybit_async.fetch_ohlcv(symbol, TIMEFRAME, limit=110)     
        if len(ohlcv) < 101: 
            return []

        df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        df['ema100'] = df['c'].ewm(span=100, adjust=False).mean()
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        signal = "NON-CROSS"
        if prev['c'] <= prev['ema100'] and curr['c'] > curr['ema100']:
            signal = "BULL-CROSS"
        elif prev['c'] >= prev['ema100'] and curr['c'] < curr['ema100']:
            signal = "BEAR-CROSS"
        
        return [{
            "symbol": symbol.split(':')[0],
            "timestamp": int(curr['ts']),
            "open": float(curr['o']),
            "high": float(curr['h']),
            "low": float(curr['l']),
            "close": float(curr['c']),
            "volume": float(curr['v']),
            "ema_100": f"{curr['ema100']:.2f}",
            "ema_signal": signal,
            "price24hPcnt": ticker_data.get('price24hPcnt'),
            "prevPrice24h": ticker_data.get('prevPrice24h'),
            "prevPrice1h": ticker_data.get('prevPrice1h'),
            "turnover24h": ticker_data.get('turnover24h'),
            "volume24h": ticker_data.get('volume24h'),
            "fundingRate": ticker_data.get('fundingRate'), 
            "openInterest": ticker_data.get('openInterest'),
            "openInterestValue": ticker_data.get('openInterestValue')
        }]

    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return []

@router.get("/fetch-all")
async def fetch_all_cryptos():
    try:
        
        all_tickers = await bybit_async.fetch_tickers(params={'category': 'linear'})        
        tasks = [ fetch_one_symbol(symbol, all_tickers.get(f"{symbol}:USDT", {}).get("info", {})) for symbol in SYMBOLS[:-7]]
        results = await asyncio.gather(*tasks)    
        flattened_results = [item for sublist in results for item in sublist]           
        return flattened_results

    except Exception as e:
        print(f"Global fetch error: {e}")
        return []

@router.on_event("shutdown")
async def shutdown_event():
    await bybit_async.close()

# ------------------------------------------------------------------------------------------------
@router.get("/bybit")
def bybit_data():

    results = []
    timestamp = bybit.milliseconds()
    
    for symbol in SYMBOLS[:-7]:
        try:
            ohlcv = bybit.fetch_ohlcv(symbol, TIMEFRAME, limit=limit, params={'category': 'linear'})
            closes = [candle[4] for candle in ohlcv]        
            sma_100 = sum(closes[-100:]) / 100
            current_price = closes[-1]
            curr_status = "ABOVE" if current_price > sma_100 else "BELOW"
            diff_percent = ((current_price - sma_100) / sma_100) * 100
        
            prev_close = closes[-2]
            prev_sma = sum(closes[-101:-1]) / 100  # SMA100 for previous candle
            
            prev_status = "ABOVE" if prev_close > prev_sma else "BELOW"
            
            if prev_status == "BELOW" and curr_status == "ABOVE":
                price_cross = "BULL-CROSS"
            elif prev_status == "ABOVE" and curr_status == "BELOW":
                price_cross = "BEAR-CROSS"
            else:
                price_cross = "NON-CROSS"
            
            coin_name = symbol.split('/')[0]
            results.append({"symbol": coin_name, "pair": symbol, "price": round(current_price, 2), "sma_100": round(sma_100, 2),
                "price_status": curr_status, "price_cross": price_cross, "percent_diff": round(diff_percent, 2), "timestamp": timestamp
            })
            
        except Exception as e:
            coin_name = symbol.split('/')[0]
            results.append({"symbol": coin_name, "pair": symbol, "price": 0, "price_status": "ERROR", "price_cross": "ERROR", 
            "error": str(e), "timestamp": timestamp
            })
                          
    return results
