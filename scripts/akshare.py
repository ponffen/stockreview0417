import pandas as pd
import requests

stock_list = [
    ("hk_hk02259", "2026-01-22", "2026-04-22"),
]

# ===================== 【修复】按你给的JSON格式解析 =====================
def fetch_sina_new(symbol, start_date, end_date):
    try:
        base_url = "https://quotes.sina.cn/hq/api/openapi.php/MarketCenterService.getDailyK_Batch"
        params = {
            "symbols": symbol,
            "start": start_date,
            "end": end_date,
            "asc": 0
        }
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://quotes.sina.cn/"}

        # 打印完整请求接口
        full_url = f"{base_url}?symbols={symbol}&start={start_date}&end={end_date}&asc=0"
        print(f"✅ 请求URL：\n{full_url}\n")

        resp = requests.get(base_url, params=params, headers=headers, timeout=10)
        result = resp.json()

        # ---------------- 【正确解析】严格匹配你给的返回格式 ----------------
        data_list = result.get("result", {}).get("data", {}).get(symbol, [])
        
        if not data_list:
            print(f"❌ {symbol} 无数据\n")
            return pd.DataFrame()

        df = pd.DataFrame(data_list)
        df["股票代码"] = symbol
        
        # 字段：day -> 日期，close -> 收盘价
        df = df[["股票代码", "day", "close"]]
        df.columns = ["股票代码", "日期", "收盘价"]
        return df

    except Exception as e:
        print(f"❌ {symbol} 拉取失败: {str(e)}\n")
        return pd.DataFrame()

# ===================== 批量拉取 + 导出Excel =====================
all_data = []
for code, s, e in stock_list:
    print("=" * 70)
    print(f"拉取: {code:15} | {s} ~ {e}")
    df = fetch_sina_new(code, s, e)
    if not df.empty:
        all_data.append(df)

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_excel("全量日K收盘价_最终版.xlsx", index=False)
    print("\n✅ 导出成功：全量日K收盘价_最终版.xlsx")
else:
    print("\n❌ 未获取到数据")