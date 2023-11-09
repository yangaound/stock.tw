import collections
import datetime
from typing import Optional

import pandas
from dateutil.relativedelta import relativedelta

from stock_tw.變易 import fin_stmt, pera, price, revenue, security, util

"""
This module performs analysis on financial data.
It imports various modules and defines several functions to retrieve and analyze data from below tables:
- securities
- prices
- peras
- revenues
- income_sheets
- cumulate_income_sheets
- balance_sheets
- cash_flows
"""

securities: pandas.DataFrame
prices: pandas.DataFrame
peras: pandas.DataFrame
revenues: pandas.DataFrame
income_sheets: pandas.DataFrame
cumulate_income_sheets: pandas.DataFrame
balance_sheets: pandas.DataFrame
cash_flows: pandas.DataFrame

datatime_range: dict[str, Optional[datetime.datetime]] = {
    "max_price": None,
    "max_pera": None,
    "max_revenue": None,
    "max_fin_stmt": None,
    "min_price": None,
    "min_pera": None,
    "min_revenue": None,
    "min_fin_stmt": None,
}

_FIN_DATA_START_DT = datetime.datetime.today() - relativedelta(years=6)


def refresh_securities(connection):
    global securities
    securities = security.read_sql(conn=connection)
    securities.sort_index(ascending=True, inplace=True)


def refresh_prices(
    connection,
    dt: datetime.datetime = datetime.datetime.today() - relativedelta(months=6),
):
    global prices
    prices = price.read_sql(conn=connection, start_time=dt)
    prices.sort_index(ascending=True, inplace=True)
    datatime_range["max_price"] = max(ts for ts, code in prices.index)
    datatime_range["min_price"] = min(ts for ts, code in prices.index)


def refresh_peras(
    connection,
):
    global peras

    pera_sql = f"""
    SELECT * FROM `{pera.PERA_TB_NAME}` AS b
    JOIN (
        SELECT code AS code_a, YEAR(ts) AS year, MAX(ts) AS max_ts FROM `{pera.PERA_TB_NAME}`
        WHERE ts >= '{_FIN_DATA_START_DT}'
        GROUP BY code, YEAR(ts)
        ) AS a
    ON a.code_a = b.code AND a.max_ts = b.ts
    """

    peras = pera.read_sql(conn=connection, sql=pera_sql)
    peras.sort_index(ascending=True, inplace=True)
    peras["股利年度"] = peras["股利年度"].fillna(0).astype(int)
    datatime_range["max_pera"] = max(ts for ts, code in peras.index)
    datatime_range["min_pera"] = min(ts for ts, code in peras.index)


def refresh_revenues(
    connection, dt: datetime.datetime = _FIN_DATA_START_DT - relativedelta(months=5)
):
    global revenues
    revenues = revenue.read_sql(conn=connection, start_time=dt)
    revenues.sort_index(ascending=True, inplace=True)
    datatime_range["max_revenue"] = max(ts for ts, code in revenues.index)
    datatime_range["min_revenue"] = min(ts for ts, code in revenues.index)


def refresh_fin_stmt(connection, dt: datetime.datetime = _FIN_DATA_START_DT):
    global income_sheets, cumulate_income_sheets, balance_sheets, cash_flows
    income_sheets = fin_stmt.income_sheet.read_sql(conn=connection, start_time=dt)
    income_sheets.sort_index(ascending=True, inplace=True)
    cumulate_income_sheets = fin_stmt.cumulate_income_sheet.read_sql(
        conn=connection, start_time=dt
    )
    cumulate_income_sheets.sort_index(ascending=True, inplace=True)
    balance_sheets = fin_stmt.balance_sheet.read_sql(conn=connection, start_time=dt)
    balance_sheets.sort_index(ascending=True, inplace=True)
    cash_flows = fin_stmt.cash_flow.read_sql(conn=connection, start_time=dt)
    cash_flows.sort_index(ascending=True, inplace=True)
    datatime_range["max_fin_stmt"] = max(ts for ts, code in balance_sheets.index)
    datatime_range["min_fin_stmt"] = min(ts for ts, code in balance_sheets.index)


# New DB session
_connection = util.get_db_proxy().connection
_sqlite3 = util.get_sqlite3()

# Refresh data from DB
refresh_securities(_connection)
refresh_prices(_connection)
refresh_peras(_connection)
refresh_revenues(_connection)
refresh_fin_stmt(_sqlite3)

# Close DB session
_connection.close()
_sqlite3.close()

# These column name lists are used to organize and refer to specific columns in data tables or
# for calculations and analysis.
# Column names related to table security_list
TB_STOCK_COLs = ["name", "group"]
# Column names related to table price
TB_PRICE_COLs = ["收盤價", "漲跌幅(%)"]
# Column names related to table balance_sheet
TB_BALANCE_SHEET_COLs = ["普通股股本", "資產總計", "權益總額"]
# Column names related to table income_sheet
TB_INCOME_SHEET_COLs = [
    "基本每股盈餘合計",
    "營業外收入及支出合計",
    "營業毛利（毛損）",
    "營業毛利（毛損）淨額",
    "本期淨利（淨損）",
    "營業收入合計",
    "繼續營業單位稅前淨利（淨損）",
    "繼續營業單位本期淨利（淨損）",
    "母公司業主（淨利／損）",
    "繼續營業單位淨利（淨損）",
    "停業單位淨利（淨損）",
]
# Column names related to revenues
TB_REVENUE_COLs = ["updated_ts", "當月營收", "當月累計營收", "去年累計營收"]

# Analysis field names that can be calculated based on pera data
ANAL_PERA_COLs = [
    "本益比",  # TB Column
    "股價淨值比",  # TB Column
    "殖利率(%)",  # TB Column
    "股利年度",  # TB Column
    "股利連續N年",  # TB Column
]
# Analysis field names that can be calculated based on prices data
ANAL_PRICE_COLs = [
    "成交股數(M)",  # 分析場（平均值）
    "成交筆數(M)",  # 分析場（平均值）
    "成交金額(M)",  # 分析場（平均值）
]
# Analysis field names that can be calculated based on revenue data
ANAL_REVENUE_COLs = ["updated_ts", "當月營收", "YoY", "MoM", "IsM3"]
# Analysis field names that can be calculated based on "fin_stmt" data
ANAL_QUARTER_COLs = ["本期淨利（淨損）", "YoY", "QoQ", "IsQ3"] + [
    "YoE",
    "QoE",
    "IsE3",
]
# Historical column names related to "fin_stmt"
_HIS_PROFIT_COLs = [
    "GPM",
    "NIM",
    "ROA",
    "ROE",
    "DBR",
]
# Column names related to EPS analysis
ANAL_EPS_COLs = [
    "EPS",
    "(C)EPS",
    "(C)PER",
    "E(Sum)",
    "E(Avg)",
    "E(Std)",
    "E(0)",
    "E(1)",
    "E(2)",
    "E(3)",
    "外(0)",
    "外(1)",
    "外(2)",
    "外(3)",
    "GPM(0)",
    "NIM(0)",
    "ROA(0)",
    "ROE(0)",
    "DBR(0)",
]
# Column names related to custom profit analysis based on revenues
CUST_ANAL_PROFIT_COLs = [
    "(C)營收合計",
    "(C)平均月營收",
    "(C)合計月數",
]

# Column names related to profit analysis
ANAL_PROFIT_COLs = (
    ANAL_EPS_COLs
    + _HIS_PROFIT_COLs
    + [
        "EPS(0)+",
        "GPM+",
        "NIM+",
        "ROA+",
        "ROE+",
        "DBR+",
        "股本+",
        "資產+",
        "權益+",
    ]
    + ANAL_QUARTER_COLs
    + CUST_ANAL_PROFIT_COLs
)

# Global variables for analysis
daily_price: pandas.DataFrame
his_profits: pandas.DataFrame
anal_per: pandas.DataFrame
anal_profit: pandas.DataFrame
anal_revenue: pandas.DataFrame
anal_quarter: pandas.DataFrame


def analyze_prices(ts: datetime.datetime = None):
    """Calculates prices and merge them into the latest date price data"""
    global daily_price
    ts = ts or datatime_range["max_price"]
    daily_price = prices.loc[ts].copy()

    # Init ANAL_PRICE_COLs
    assert set(ANAL_PRICE_COLs) == {"成交股數(M)", "成交筆數(M)", "成交金額(M)"}
    daily_price["成交股數(M)"] = 0
    daily_price["成交筆數(M)"] = 0
    daily_price["成交金額(M)"] = 0

    # Calculates ANAL_PRICE_COLs
    reversed_index_prices = reverse_df_index(prices)
    for code, row in daily_price.iterrows():
        daily_price.loc[code, "成交股數(M)"] = int(
            reversed_index_prices.loc[code, "成交股數"].mean()
        )
        daily_price.loc[code, "成交筆數(M)"] = int(
            reversed_index_prices.loc[code, "成交筆數"].mean()
        )
        daily_price.loc[code, "成交金額(M)"] = int(
            reversed_index_prices.loc[code, "成交金額"].mean()
        )

    return daily_price


def analyze_peras():
    """
    It retrieves the latest `peras` data and calculates the number of consecutive dividend years.
    """
    global peras, anal_per

    anal_per = peras.loc[datatime_range["max_pera"]].copy()

    # Calculate consecutive dividend years for each security
    df = peras[(peras["股利年度"] > 90) & (peras["殖利率(%)"] > 0)].copy()
    df.sort_values(["code", "ts"], ascending=False, inplace=True)

    code_grouped_yields = collections.defaultdict(set)
    for (ts, code), row in df.iterrows():
        code_grouped_yields[code].add(row["股利年度"])

    anal_per["股利連續N年"] = 0
    now = datetime.datetime.now()
    for (ts, code), row in df.iterrows():
        years = sorted(code_grouped_yields[code], reverse=True)
        year_count = 0
        for i, year in enumerate(
            range(now.year - 1912, now.year - 1912 - len(years), -1)
        ):
            if years[i] == year:
                year_count += 1
            else:
                break
        anal_per.loc[code, "股利連續N年"] = year_count

    return anal_per


def calculate_his_fin_stmt(columns: list[str] = None) -> pandas.DataFrame:
    """Perform calculations and analysis on the financial data"""
    global his_profits
    columns = columns or _HIS_PROFIT_COLs + CUST_ANAL_PROFIT_COLs

    # 損益表["營業毛利（毛損）", "本期淨利（淨損）", "營業收入合計"]
    # 加入資產負債表["普通股股本", "資產總計", "權益總額"]
    tmp = income_sheets[TB_INCOME_SHEET_COLs].merge(
        balance_sheets[TB_BALANCE_SHEET_COLs], on=util.TIMED_INDEX_COLs
    )
    tmp["ROA"] = tmp["本期淨利（淨損）"] / tmp["資產總計"] * 100
    tmp["ROE"] = tmp["本期淨利（淨損）"] / tmp["權益總額"] * 100
    tmp["DBR"] = (tmp["資產總計"] - tmp["權益總額"]) / tmp["資產總計"] * 100
    tmp["GPM"] = tmp["營業毛利（毛損）"] / tmp["營業收入合計"] * 100
    tmp["NIM"] = tmp["本期淨利（淨損）"] / tmp["營業收入合計"] * 100

    # 加入月營收，以季合計，作為與損益表["營業收入合計"] 做對照
    ifrs_dated_revenues = collections.defaultdict(list)
    for (ts, code), _revenue in revenues["當月營收"].items():
        dt = ts - relativedelta(months=1)  # '2023-06-10' represent '2023-05's revenue
        ifrs_date = util.IFRSDateIter.dt_in_ifrs_dt(dt)
        ifrs_dated_revenues[(ifrs_date, code)].append(_revenue)
    values = (
        (
            ts,
            code,
            sum(_revenues),
            sum(_revenues) / len(_revenues),
            len(_revenues),
        )
        for (ts, code), _revenues in ifrs_dated_revenues.items()
    )
    cust_tmp = pandas.DataFrame(
        values, columns=util.TIMED_INDEX_COLs + CUST_ANAL_PROFIT_COLs
    )
    cust_tmp.set_index(util.TIMED_INDEX_COLs, inplace=True)

    # 合併 cust_tmp["(C)營收合計", "(C)平均月營收","(C)合計月數"]
    tmp = tmp.merge(cust_tmp, on=util.TIMED_INDEX_COLs)
    his_profits = tmp

    return his_profits[columns]


def analyze_revenue(ts: datetime.datetime = None):
    """Retrieve and analyze the latest revenue data"""
    global anal_revenue
    ts = ts or datatime_range["max_revenue"]

    m0_revenue = revenues.loc[ts]
    m1_revenue = revenues.loc[ts - relativedelta(months=1)]
    m2_revenue = revenues.loc[ts - relativedelta(months=2)]
    my_revenue = revenues.loc[ts - relativedelta(years=1)]

    tmp = m0_revenue[TB_REVENUE_COLs]
    tmp = tmp.merge(
        right=m1_revenue["當月營收"], on=["code"], how="left", suffixes=("", "_1")
    )
    tmp = tmp.merge(
        right=m2_revenue["當月營收"], on=["code"], how="left", suffixes=("", "_2")
    )
    tmp = tmp.merge(
        right=my_revenue["當月營收"], on=["code"], how="left", suffixes=("", "_y")
    )
    column_map = {
        "當月營收_1": "R(1)",
        "當月營收_2": "R(2)",
        "當月營收_y": "R(y)",
    }
    tmp.rename(columns=column_map, inplace=True)
    tmp["YoY"] = (tmp["當月營收"] - tmp["R(y)"]) / tmp["R(y)"] * 100
    tmp["MoM"] = (tmp["當月營收"] - tmp["R(1)"]) / tmp["R(1)"] * 100
    tmp["IsM3"] = (tmp["當月營收"] > tmp["R(1)"]) & (tmp["R(1)"] > tmp["R(2)"])
    tmp.sort_values("MoM", ascending=False)

    anal_revenue = tmp
    return anal_revenue[
        ANAL_REVENUE_COLs + ["當月累計營收", "去年累計營收", "R(1)", "R(2)", "R(y)"]
    ]


def append_stock_info(df: pandas.DataFrame) -> pandas.DataFrame:
    return securities[TB_STOCK_COLs].merge(df, on=[util.SECURITY_ID_NAME], how="right")


def append_price_info(df: pandas.DataFrame) -> pandas.DataFrame:
    return daily_price[TB_PRICE_COLs].merge(
        df,
        on=[util.SECURITY_ID_NAME],
        how="right",
    )


def reverse_df_index(df: pandas.DataFrame) -> pandas.DataFrame:
    tmp = df.reset_index()
    tmp.set_index(list(df.index.names)[::-1], inplace=True)
    return tmp


analyze_prices()
calculate_his_fin_stmt()
analyze_revenue()
analyze_peras()


def analyze_base(
    daily_ts: datetime.datetime = None, ifrs_ts: datetime.datetime = None
) -> pandas.DataFrame:
    daily_ts = daily_ts or datatime_range["max_price"]
    ifrs_ts = ifrs_ts or datatime_range["max_fin_stmt"]
    # Make below fields
    cols = (
        TB_STOCK_COLs
        + TB_PRICE_COLs
        + ANAL_PRICE_COLs
        + ANAL_PERA_COLs
        + TB_BALANCE_SHEET_COLs
    )

    ret = anal_per[ANAL_PERA_COLs]
    ret = ret.merge(
        right=analyze_prices(daily_ts)[TB_PRICE_COLs + ANAL_PRICE_COLs],
        on=["code"],
        how="left",
    )
    ret = ret.merge(
        right=balance_sheets.loc[ifrs_ts][TB_BALANCE_SHEET_COLs],
        on=["code"],
        how="left",
    )
    ret = append_stock_info(ret)
    ret["權益比(%)"] = ret["權益總額"] / ret["資產總計"] * 100

    return ret[cols + ["權益比(%)"]]


def analyze_profit(
    ifrs_ts: datetime.datetime = None, columns: list[str] = None
) -> pandas.DataFrame:
    global anal_profit
    columns = columns or ANAL_PROFIT_COLs

    ifrs_ts = ifrs_ts or datatime_range["max_fin_stmt"]
    ifrs_iter = util.IFRSDateIter(ifrs_dt=ifrs_ts)

    # Based on pera_df
    tmp = anal_per.merge(
        his_profits.loc[ifrs_iter.current_ifrs_dt()],  # 最近一季 ifrs date
        on=[util.SECURITY_ID_NAME],
        how="outer",
        suffixes=("", ""),
    )
    tmp = tmp.merge(
        his_profits.loc[ifrs_iter.previous_ifrs_dt()],  # 最近一季的前 1 季
        on=[util.SECURITY_ID_NAME],
        how="left",
        suffixes=("", "_q1"),
    )
    tmp = tmp.merge(
        his_profits.loc[ifrs_iter.previous_ifrs_dt()],  # 最近一季的前第 2 季
        on=[util.SECURITY_ID_NAME],
        how="left",
        suffixes=("", "_q2"),
    )
    tmp = tmp.merge(
        his_profits.loc[ifrs_iter.previous_ifrs_dt()],  # 最近一季的前第 3 季
        on=[util.SECURITY_ID_NAME],
        how="left",
        suffixes=("", "_q3"),
    )
    tmp = tmp.merge(
        his_profits.loc[ifrs_iter.previous_ifrs_dt()],  # 最近一季的前第 4 季
        on=[util.SECURITY_ID_NAME],
        how="left",
        suffixes=("", "_q4"),
    )
    column_map = {
        "基本每股盈餘合計": "E(0)",
        "基本每股盈餘合計_q1": "E(1)",
        "基本每股盈餘合計_q2": "E(2)",
        "基本每股盈餘合計_q3": "E(3)",
        "基本每股盈餘合計_q4": "E(4)",
        "營業外收入及支出合計": "外(0)",
        "營業外收入及支出合計_q1": "外(1)",
        "營業外收入及支出合計_q2": "外(2)",
        "營業外收入及支出合計_q3": "外(3)",
        "營業外收入及支出合計_q4": "外(4)",
        "GPM": "GPM(0)",
        "NIM": "NIM(0)",
        "ROA": "ROA(0)",
        "ROE": "ROE(0)",
        "DBR": "DBR(0)",
    }
    tmp.rename(columns=column_map, inplace=True)
    # (C)EPS
    tmp["E(Sum)"] = tmp[["E(0)", "E(1)", "E(2)", "E(3)"]].sum(axis=1)
    tmp["E(Avg)"] = tmp[["E(0)", "E(1)", "E(2)", "E(3)"]].mean(axis=1)
    tmp["E(Std)"] = tmp[["E(0)", "E(1)", "E(2)", "E(3)"]].std(axis=1)
    tmp["EPS"] = tmp[["E(0)", "E(1)", "E(2)", "E(3)"]].sum(axis=1)
    tmp["(C)EPS"] = tmp["本期淨利（淨損）"] / tmp["普通股股本"] * 10

    # (C)PER
    tmp = append_price_info(tmp)
    tmp["(C)PER"] = tmp["收盤價"] / (tmp["(C)EPS"] * 4)

    # 業外收入
    tmp["外(0)"] = tmp["外(0)"] / tmp["本期淨利（淨損）"] * 100
    tmp["外(1)"] = tmp["外(1)"] / tmp["本期淨利（淨損）_q1"] * 100
    tmp["外(2)"] = tmp["外(2)"] / tmp["本期淨利（淨損）_q2"] * 100
    tmp["外(3)"] = tmp["外(3)"] / tmp["本期淨利（淨損）_q3"] * 100

    tmp["NIM"] = tmp[["NIM(0)", "NIM_q1", "NIM_q2", "NIM_q3"]].sum(axis=1)
    tmp["GPM"] = tmp[["GPM(0)", "GPM_q1", "GPM_q2", "GPM_q3"]].sum(axis=1)
    tmp["ROA"] = tmp[
        [
            "ROA(0)",
            "ROA_q1",
            "ROA_q2",
            "ROA_q3",
        ]
    ].sum(axis=1)
    tmp["ROE"] = tmp[
        [
            "ROE(0)",
            "ROE_q1",
            "ROE_q2",
            "ROE_q3",
        ]
    ].sum(axis=1)
    tmp["DBR"] = tmp[
        [
            "DBR(0)",
            "DBR_q1",
            "DBR_q2",
            "DBR_q3",
        ]
    ].sum(axis=1)

    tmp["EPS(0)+"] = tmp["E(0)"] - tmp["E(1)"]
    tmp["GPM+"] = tmp["GPM(0)"] - tmp["GPM_q1"]
    tmp["NIM+"] = tmp["NIM(0)"] - tmp["NIM_q1"]
    tmp["ROA+"] = tmp["ROA(0)"] - tmp["ROA_q1"]
    tmp["ROE+"] = tmp["ROE(0)"] - tmp["ROE_q1"]
    tmp["DBR+"] = tmp["DBR(0)"] - tmp["DBR_q1"]

    tmp["股本+"] = tmp["普通股股本"] - tmp["普通股股本_q1"]
    tmp["資產+"] = tmp["資產總計"] - tmp["資產總計_q1"]
    tmp["權益+"] = tmp["權益總額"] - tmp["權益總額_q1"]

    tmp["YoY"] = (
        (tmp["本期淨利（淨損）"] - tmp["本期淨利（淨損）_q4"])
        / tmp["本期淨利（淨損）_q4"]
        * 100
    )
    tmp["QoQ"] = (
        (tmp["本期淨利（淨損）"] - tmp["本期淨利（淨損）_q1"])
        / tmp["本期淨利（淨損）_q1"]
        * 100
    )
    tmp["IsQ3"] = (tmp["本期淨利（淨損）"] > tmp["本期淨利（淨損）_q1"]) & (
        tmp["本期淨利（淨損）_q1"] > tmp["本期淨利（淨損）_q2"]
    )

    tmp["YoE"] = (tmp["E(0)"] - tmp["E(4)"]) / tmp["E(0)"] * 100
    tmp["QoE"] = (tmp["E(0)"] - tmp["E(1)"]) / tmp["E(1)"] * 100
    tmp["IsE3"] = (tmp["E(0)"] > tmp["E(1)"]) & (tmp["E(1)"] > tmp["E(2)"])

    anal_profit = tmp
    return anal_profit[columns]
