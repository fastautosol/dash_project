# 2026.03.14  11.00
from fastapi import APIRouter
import requests
from datetime import datetime, UTC

XSTOCKS = {
    'AAPLxUSD': 'Apple', 'ABBVxUSD': 'AbbVie', 'ABTxUSD': 'Abbott', 'ACNxUSD': 'Accenture',
    'AMBRxUSD': 'Ambr', 'AMDxUSD': 'AMD', 'AMZNxUSD': 'Amazon', 'APPxUSD': 'AppLovin',
    'AVGOxUSD': 'Broadcom', 'AZNxUSD': 'AstraZeneca', 'BACxUSD': 'Bank of America',
    'BMNRxUSD': 'BioMarin', 'BTBTxUSD': 'Bit Digital',
    'BTGOxUSD': 'B2Gold', 'CMCSAxUSD': 'Comcast', 'COINxUSD': 'Coinbase', 'COPXxUSD': 'Copper Miners',
    'CRCLxUSD': 'Cercadia', 'CRMxUSD': 'Salesforce', 'CRWDxUSD': 'CrowdStrike',
    'CSCOxUSD': 'Cisco', 'CVXxUSD': 'Chevron', 'DFDVxUSD': 'Defined Outcome',
    'DHRxUSD': 'Danaher', 'GLDxUSD': 'Gold ETF', 'GMExUSD': 'GameStop', 'GOOGLxUSD': 'Alphabet',
    'GSxUSD': 'Goldman Sachs', 'HONxUSD': 'Honeywell', 'HOODxUSD': 'Robinhood',
    'IBMxUSD': 'IBM', 'IEMGxUSD': 'Emerging Mkts', 'IJRxUSD': 'S&P Small-Cap',
    'INTCxUSD': 'Intel', 'IWMxUSD': 'Russell 2000', 'JNJxUSD': 'J&J', 'JPMxUSD': 'JPMorgan',
    'KOxUSD': 'Coca-Cola', 'KRAQxUSD': 'Kraken Robotics', 'LINxUSD': 'Linde',
    'LLYxUSD': 'Eli Lilly', 'MCDxUSD': 'McDonalds', 'MDTxUSD': 'Medtronic',
    'METAxUSD': 'Meta', 'MRKxUSD': 'Merck', 'MRVLxUSD': 'Marvell', 'MSFTxUSD': 'Microsoft',
    'MSTRxUSD': 'MicroStrategy', 'NFLXxUSD': 'Netflix', 'NVDAxUSD': 'NVIDIA',
    'NVOxUSD': 'Novo Nordisk', 'OPENxUSD': 'Opendoor', 'ORCLxUSD': 'Oracle',
    'PALLxUSD': 'Palladium ETF', 'PEPxUSD': 'PepsiCo', 'PFExUSD': 'Pfizer',
    'PGxUSD': 'Procter & Gamble', 'PLTRxUSD': 'Palantir', 'PMxUSD': 'Philip Morris',
    'PPLTxUSD': 'Platinum ETF', 'QQQxUSD': 'Nasdaq 100', 'SCHFxUSD': 'Intl Equity',
    'SLVxUSD': 'Silver ETF', 'SPYxUSD': 'S&P 500', 'STRCxUSD': 'Sarcos', 'TBLLxUSD': 'Treasury ETF',
    'TMOxUSD': 'Thermo Fisher', 'TONXxUSD': 'Tonix Pharma', 'TQQQxUSD': '3x Nasdaq',
    'TSLAxUSD': 'Tesla', 'UNHxUSD': 'UnitedHealth', 'VxUSD': 'Visa', 'VTIxUSD': 'Total Stock',
    'VTxUSD': 'Total World', 'XOMxUSD': 'Exxon Mobil'
}

router = APIRouter()

@router.get("/check-stocks")
def check_stocks():
    
    url = "https://api.kraken.com/0/public/Ticker"    
    params = {"pair": ",".join(list(XSTOCKS.keys())), "asset_class": "tokenized_asset" }
    
    try:
        response = requests.get(url, params=params, timeout=10).json()
        
        if response.get("error"):
            return {"status": "error", "message": response["error"]}

        data = response["result"]
        results = []

        for xsymbol, info in data.items():
            full_name = XSTOCKS.get(xsymbol, "Unknown Asset")
            current_price = float(info["c"][0])
            volume = float(info["v"][1])
            VWAP = float(info["p"][1])
            trades = float(info["t"][1])
            
            results.append({
                "xsymbol": xsymbol,
                "xname": full_name,
                "xprice": current_price,
                "xvolume24": volume,
                "xvwap24": VWAP,
                "xtrades24": trades,
                "timestamp": datetime.now(UTC) #.strftime("%Y-%m-%d %H:%M:%S")
            })

        return results
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

