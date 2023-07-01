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

_FIN_DATA_START_DT = datetime.datetime.today() - relativedelta(years=5)


def refresh_securities(connection):
    global securities
    securities = security.read_sql(conn=connection)
    securities.sort_index(ascending=True, inplace=True)


def refresh_prices(
    connection,
    dt: datetime.datetime = datetime.datetime.today() - relativedelta(days=5),
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
    peras = pera.read_sql(conn=connection)
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
# for performing calculations and analysis.
# Column names related to table security_list
STOCK_COLs = ["name", "group"]
# Column names related to table price
PRICE_COLs = ["收盤價", "漲跌幅(%)"]
# Column names related to table income_sheet
INCOME_SHEET_COLs = [
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
# Column names related to table pera
PERA_COLs = [
    "殖利率(%)",
    "股利年度",
    "股利連續N年",
    "本益比",
    "股價淨值比",
]
# Column names related to revenues
REVENUE_COLs = ["當月營收", "當月累計營收", "去年累計營收"]
# Analysis column names that can be calculated based on revenue data
ANAL_REVENUE_COLs = ["當月營收", "YoY(%)", "MoM(%)", "IsM3"]
# Analysis column names that can be calculated based on "fin_stmt" data
ANAL_QUARTER_INCOME_COLs = ["本期淨利（淨損）", "YoY(%)", "MoM(%)", "IsQ3"]
# Historical column names related to "fin_stmt"
_HIS_PROFIT_COLs = [
    "GPM(%)",
    "NIM(%)",
    "ROA(%)",
    "ROE(%)",
    "DBR(%)",
]
# 4 quarters analysis column names related to "fin_stmt"
_ANAL_PROFIT_COLs = [
    "EPS(0)+",
    "GPM(%)+",
    "NIM(%)+",
    "ROA(%)+",
    "ROE(%)+",
    "DBR(%)+",
    "股本+",
    "資產+",
    "權益+",
]
# Column names related to EPS analysis
ANAL_EPS_COLs = (
    _HIS_PROFIT_COLs
    + _ANAL_PROFIT_COLs
    + [
        "(C)PER",
        "(C)EPS",
        "E(Sum)",
        "E(Avg)",
        "E(Std)",
        "E(0)",
        "E(1)",
        "E(2)",
        "E(3)",
        "外%(0)",
        "外%(1)",
        "外%(2)",
        "外%(3)",
    ]
)
# Column names related to profit analysis
ANAL_PROFIT_COLs = ["EPS"] + _HIS_PROFIT_COLs + _ANAL_PROFIT_COLs
# Column names related to custom profit analysis based on revenues
CUST_ANAL_PROFIT_COLs = [
    "(C)營收合計",
    "(C)平均月營收",
    "(C)合計月數",
]


# Global variables for analysis
his_profits: pandas.DataFrame
per_anal: pandas.DataFrame
price_anal: pandas.DataFrame
eps_anal: pandas.DataFrame
profit_anal: pandas.DataFrame
revenue_anal: pandas.DataFrame
quarter_income_anal: pandas.DataFrame


def analyze_per():
    """
    It retrieves the latest `peras` data and calculates the number of consecutive dividend years.
    """
    global peras, per_anal

    per_anal = peras.loc[datatime_range["max_pera"]]

    # Calculate consecutive dividend years for each security
    df = peras[(peras["股利年度"] > 90) & (peras["殖利率(%)"] > 0)]
    df.sort_values(["code", "ts"], ascending=False, inplace=True)

    code_grouped_yields = collections.defaultdict(set)
    for (ts, code), row in df.iterrows():
        code_grouped_yields[code].add(row["股利年度"])

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
        per_anal.loc[code, "股利連續N年"] = year_count


def calculate_his_profits(columns: list[str] = None) -> pandas.DataFrame:
    """Perform calculations and analysis on the financial data"""
    global his_profits
    columns = columns or _HIS_PROFIT_COLs + CUST_ANAL_PROFIT_COLs

    # 損益表["營業毛利（毛損）", "本期淨利（淨損）", "營業收入合計"]
    # 加入資產負債表["普通股股本", "資產總計", "權益總額"]
    tmp = income_sheets[INCOME_SHEET_COLs].merge(
        balance_sheets[["普通股股本", "資產總計", "權益總額"]], on=util.TIMED_INDEX_COLs
    )
    tmp["ROA(%)"] = tmp["本期淨利（淨損）"] / tmp["資產總計"] * 100
    tmp["ROE(%)"] = tmp["本期淨利（淨損）"] / tmp["權益總額"] * 100
    tmp["DBR(%)"] = (tmp["資產總計"] - tmp["權益總額"]) / tmp["資產總計"] * 100
    tmp["GPM(%)"] = tmp["營業毛利（毛損）"] / tmp["營業收入合計"] * 100
    tmp["NIM(%)"] = tmp["本期淨利（淨損）"] / tmp["營業收入合計"] * 100

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


def analyze_price(ts: datetime.datetime = None):
    """Retrieve and analyze the latest price data"""
    global price_anal
    ts = ts or datatime_range["max_price"]

    price_anal = prices.loc[ts]
    return price_anal


def analyze_revenue(ts: datetime.datetime = None):
    """Retrieve and analyze the latest revenue data"""
    global revenue_anal
    ts = ts or datatime_range["max_revenue"]

    m0_revenue = revenues.loc[ts]
    m1_revenue = revenues.loc[ts - relativedelta(months=1)]
    m2_revenue = revenues.loc[ts - relativedelta(months=2)]
    my_revenue = revenues.loc[ts - relativedelta(years=1)]

    tmp = m0_revenue[REVENUE_COLs]
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
    tmp["YoY(%)"] = (tmp["當月營收"] - tmp["R(y)"]) / tmp["R(y)"] * 100
    tmp["MoM(%)"] = (tmp["當月營收"] - tmp["R(1)"]) / tmp["R(1)"] * 100
    tmp["IsM3"] = (tmp["當月營收"] > tmp["R(1)"]) & (tmp["R(1)"] > tmp["R(2)"])
    tmp.sort_values("MoM(%)", ascending=False)

    revenue_anal = tmp
    return revenue_anal[
        ANAL_REVENUE_COLs + ["當月累計營收", "去年累計營收", "R(1)", "R(2)", "R(y)"]
    ]


def analyze_quarter_income(ts: datetime.datetime = None, columns: list[str] = None):
    global quarter_income_anal
    columns = columns or ANAL_QUARTER_INCOME_COLs
    analyze_eps(ts)
    tmp = eps_anal

    tmp["YoY(%)"] = (
        (tmp["本期淨利（淨損）"] - tmp["本期淨利（淨損）_q4"])
        / tmp["本期淨利（淨損）_q4"]
        * 100
    )
    tmp["MoM(%)"] = (
        (tmp["本期淨利（淨損）"] - tmp["本期淨利（淨損）_q1"])
        / tmp["本期淨利（淨損）_q1"]
        * 100
    )
    tmp["IsQ3"] = (tmp["本期淨利（淨損）"] > tmp["本期淨利（淨損）_q1"]) & (
        tmp["本期淨利（淨損）_q1"] > tmp["本期淨利（淨損）_q2"]
    )
    tmp = tmp.sort_values(["MoM(%)", "YoY(%)"], ascending=False)

    quarter_income_anal = tmp
    return quarter_income_anal[columns]


def analyze_eps(
    ts: datetime.datetime = None, columns: list[str] = None
) -> pandas.DataFrame:
    global eps_anal
    ts = ts or datatime_range["max_fin_stmt"]
    columns = columns or (PRICE_COLs + PERA_COLs + ANAL_EPS_COLs)

    ifrs_iter = util.IFRSDateIter(ifrs_dt=ts)
    q0_ts = ifrs_iter.current_ifrs_dt()

    # Based on pera_df
    # merge income_sheets["本期淨利（淨損）", "基本每股盈餘合計", "營業外收入及支出合計"] from `fin_profits_df`
    tmp = per_anal.merge(
        his_profits.loc[q0_ts],
        on=[util.SECURITY_ID_NAME],
        how="outer",
        suffixes=("", ""),
    )
    tmp = tmp.merge(
        his_profits.loc[ifrs_iter.previous_ifrs_dt()],
        on=[util.SECURITY_ID_NAME],
        how="left",
        suffixes=("", "_q1"),
    )
    tmp = tmp.merge(
        his_profits.loc[ifrs_iter.previous_ifrs_dt()],
        on=[util.SECURITY_ID_NAME],
        how="left",
        suffixes=("", "_q2"),
    )
    tmp = tmp.merge(
        his_profits.loc[ifrs_iter.previous_ifrs_dt()],
        on=[util.SECURITY_ID_NAME],
        how="left",
        suffixes=("", "_q3"),
    )
    tmp = tmp.merge(
        his_profits.loc[ifrs_iter.previous_ifrs_dt()],
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
    }
    tmp.rename(columns=column_map, inplace=True)
    # (C)EPS
    tmp["E(Sum)"] = tmp[["E(0)", "E(1)", "E(2)", "E(3)"]].sum(axis=1)
    tmp["E(Avg)"] = tmp[["E(0)", "E(1)", "E(2)", "E(3)"]].mean(axis=1)
    tmp["E(Std)"] = tmp[["E(0)", "E(1)", "E(2)", "E(3)"]].std(axis=1)
    tmp["(C)EPS"] = tmp["本期淨利（淨損）"] / tmp["普通股股本"] * 10

    # 業外收入
    tmp["外%(0)"] = tmp["外(0)"] / tmp["本期淨利（淨損）"] * 100
    tmp["外%(1)"] = tmp["外(1)"] / tmp["本期淨利（淨損）_q1"] * 100
    tmp["外%(2)"] = tmp["外(2)"] / tmp["本期淨利（淨損）_q2"] * 100
    tmp["外%(3)"] = tmp["外(3)"] / tmp["本期淨利（淨損）_q3"] * 100

    # (C)PER
    tmp = append_price_info(tmp)
    tmp["(C)PER"] = tmp["收盤價"] / (tmp["(C)EPS"] * 4)

    tmp["EPS(0)+"] = tmp["E(0)"] - tmp["E(1)"]
    tmp["GPM(%)+"] = tmp["GPM(%)_q1"] - tmp["GPM(%)_q1"]
    tmp["NIM(%)+"] = tmp["NIM(%)_q1"] - tmp["NIM(%)_q1"]
    tmp["ROA(%)+"] = tmp["ROA(%)"] - tmp["ROA(%)_q1"]
    tmp["ROE(%)+"] = tmp["ROE(%)"] - tmp["ROE(%)_q1"]
    tmp["DBR(%)+"] = tmp["DBR(%)"] - tmp["DBR(%)_q1"]

    tmp["股本+"] = tmp["普通股股本"] - tmp["普通股股本_q1"]
    tmp["資產+"] = tmp["資產總計"] - tmp["資產總計_q1"]
    tmp["權益+"] = tmp["權益總額"] - tmp["權益總額_q1"]

    eps_anal = tmp
    return eps_anal[columns]


def analyze_profit(
    ts: datetime.datetime = None, columns: list[str] = None
) -> pandas.DataFrame:
    global profit_anal
    columns = columns or (PERA_COLs + ANAL_PROFIT_COLs)
    analyze_eps(ts)
    tmp = eps_anal

    tmp["EPS"] = tmp[["E(0)", "E(1)", "E(2)", "E(3)"]].sum(axis=1)
    tmp["NIM(%)"] = tmp[["NIM(%)", "NIM(%)_q1", "NIM(%)_q2", "NIM(%)_q3"]].sum(axis=1)
    tmp["GPM(%)"] = tmp[["GPM(%)", "GPM(%)_q1", "GPM(%)_q2", "GPM(%)_q3"]].sum(axis=1)
    tmp["ROA(%)"] = tmp[
        [
            "ROA(%)",
            "ROA(%)_q1",
            "ROA(%)_q2",
            "ROA(%)_q3",
        ]
    ].sum(axis=1)
    tmp["ROE(%)"] = tmp[
        [
            "ROE(%)",
            "ROE(%)_q1",
            "ROE(%)_q2",
            "ROE(%)_q3",
        ]
    ].sum(axis=1)

    profit_anal = tmp
    return profit_anal[columns]


def append_stock_info(df: pandas.DataFrame) -> pandas.DataFrame:
    return securities[STOCK_COLs].merge(df, on=[util.SECURITY_ID_NAME], how="right")


def append_price_info(df: pandas.DataFrame) -> pandas.DataFrame:
    return price_anal[PRICE_COLs].merge(
        df,
        on=[util.SECURITY_ID_NAME],
        how="right",
    )


def reverse_df_index(df: pandas.DataFrame) -> pandas.DataFrame:
    tmp = df.reset_index()
    tmp.set_index(list(df.index.names)[::-1], inplace=True)
    return tmp


calculate_his_profits()
analyze_revenue()
analyze_per()
analyze_price()
analyze_eps()
analyze_profit()
