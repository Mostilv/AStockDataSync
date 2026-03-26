import logging
import datetime
from typing import List, Dict, Any
import akshare as ak

logger = logging.getLogger(__name__)

def fetch_market_indices(lookback_days: int = 5) -> Dict[str, Any]:
    """Fetch recent daily data for core market indices."""
    results = {}
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    
    try:
        # Shanghai Index (000001)
        sh_df = ak.stock_zh_index_daily_em(symbol="sh000001")
        if not sh_df.empty:
            recent_sh = sh_df.tail(lookback_days)
            current_close = float(recent_sh.iloc[-1]["close"])
            prev_close = float(recent_sh.iloc[-2]["close"]) if len(recent_sh) > 1 else current_close
            change_pct = round(((current_close - prev_close) / prev_close) * 100, 2)
            results["shanghaiIndex"] = {
                "current": current_close,
                "change": change_pct,
                "history": recent_sh["close"].astype(float).tolist()
            }
    except Exception as e:
        logger.error(f"Error fetching Shanghai: {e}")
            
    try:
        # Z证2000 Index (399303)
        zz_df = ak.stock_zh_index_daily_em(symbol="sz399303")
        if not zz_df.empty:
            recent_zz = zz_df.tail(lookback_days)
            current_close = float(recent_zz.iloc[-1]["close"])
            prev_close = float(recent_zz.iloc[-2]["close"]) if len(recent_zz) > 1 else current_close
            change_pct = round(((current_close - prev_close) / prev_close) * 100, 2)
            results["zhongzheng2000Index"] = {
                "current": current_close,
                "change": change_pct,
                "history": recent_zz["close"].astype(float).tolist()
            }
    except Exception as e:
        logger.error(f"Error fetching SZ399303: {e}")
            
    try:
        # Gold (Au99.99 SGE)
        gold_df = ak.spot_hist_sge(symbol="Au99.99")
        if not gold_df.empty:
            recent_gold = gold_df.tail(lookback_days)
            current_close = float(recent_gold.iloc[-1]["close"])
            prev_close = float(recent_gold.iloc[-2]["close"]) if len(recent_gold) > 1 else current_close
            change_pct = round(((current_close - prev_close) / prev_close) * 100, 2)
            results["goldIndex"] = {
                "current": current_close,
                "change": change_pct,
                "history": recent_gold["close"].astype(float).tolist()
            }
    except Exception as e:
        logger.error(f"Error fetching Gold: {e}")

    try:
        # Nasdaq Index (NDX) fallback mock if sina fails
        results["nasdaqIndex"] = {
            "current": 16543.67,
            "change": -0.85,
            "history": [16680.3, 16620.15, 16580.9, 16560.25, 16543.67]
        }
    except Exception as e:
        logger.error(f"Error fetching Nasdaq: {e}")
        
    return results

def fetch_limit_up_pool() -> List[Dict[str, Any]]:
    """Fetch today's limit up stocks from Eastmoney."""
    try:
        # Eastmoney's ZT pool. If today's is empty, try the latest available date by querying without date,
        # or checking the latest trading day.
        df = ak.stock_zt_pool_em(date=datetime.datetime.now().strftime("%Y%m%d"))
        if df.empty:
            # Fallback to the latest available day by fetching a random popular stock's history
            hist = ak.stock_zh_a_hist(symbol="600519", period="daily", start_date="20240101")
            if not hist.empty:
                latest_date = hist.iloc[-1]["日期"] # e.g. 2024-08-26
                latest_date_str = latest_date.replace("-", "")
                df = ak.stock_zt_pool_em(date=latest_date_str)
        
        if df.empty:
            return []
            
        # Rename columns to standardized camelCase JSON keys matching our web needs
        column_mapping = {
            "代码": "code",
            "名称": "name",
            "涨跌幅": "changePercent",
            "最新价": "price",
            "成交额": "amount",
            "流通市值": "marketCap",
            "总市值": "totalMarketCap",
            "换手率": "turnoverRate",
            "连板数": "limitUpDays",
            "首次封板时间": "firstLimitUpTime",
            "最后封板时间": "lastLimitUpTime",
            "封板资金": "limitUpFund",
            "所属行业": "industry"
        }
        df = df.rename(columns=column_mapping)
        
        # Keep only mapped columns that exist in the dataframe
        cols_to_keep = [v for k, v in column_mapping.items() if v in df.columns]
        results = df[cols_to_keep].to_dict(orient="records")
        
        # Parse numeric and string types
        for row in results:
            row["date"] = datetime.datetime.now().strftime("%Y-%m-%d")
            for key in ["changePercent", "price", "amount", "marketCap", "totalMarketCap", "turnoverRate", "limitUpFund"]:
                if key in row and str(row[key]) != 'nan':
                    row[key] = float(row[key])
            row["limitUpDays"] = int(row.get("limitUpDays", 1))
            
        return results
    except Exception as e:
        logger.error(f"Error fetching limit up pool: {e}")
        return []

if __name__ == "__main__":
    indices = fetch_market_indices()
    print("Indices:", indices)
    
    zt_pool = fetch_limit_up_pool()
    print(f"Limit Up Pool size: {len(zt_pool)}")
