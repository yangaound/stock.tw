import calendar
import datetime
import io
import logging
import sqlite3
from typing import Union

import MySQLdb
import numpy
import pandas
import requests
from dateutil.relativedelta import relativedelta

from . import util

PRICE_TB_NAME = "daily_price"
PRICE_TB_COLs = [
    "成交股數",
    "成交筆數",
    "成交金額",
    "開盤價",
    "最高價",
    "最低價",
    "收盤價",
    "漲跌幅(%)",
    "漲跌價差",
    "最後揭示買價",
    "最後揭示買量",
    "最後揭示賣價",
    "最後揭示賣量",
    "本益比",
]


def extract_price(ts: datetime.datetime) -> pandas.DataFrame:
    if ts.date() > datetime.date.today():
        raise ValueError(f"The date `{ts}` must be in the past.")

    if weekday := calendar.weekday(ts.year, ts.month, ts.day) in (5, 6):
        weekdays = {5: "Saturday", 6: "Sunday"}
        raise ValueError(f"The date `{ts}` is {weekdays[weekday]}.")

    # TWSE
    twse_df = _crawl_daily_price_from_twse(ts)[PRICE_TB_COLs]
    # TPEX
    try:
        tpex_df = _crawl_daily_price_from_tpex(ts)[PRICE_TB_COLs]
    except util.YiException as e:
        logging.warning(str(e))
        tpex_df = pandas.DataFrame()

    df = pandas.concat([twse_df, tpex_df])
    return df


def read_sql(
    conn: Union[sqlite3.Connection, MySQLdb.Connection],
    start_time: datetime.datetime = None,
) -> pandas.DataFrame:
    _start_time = start_time or (datetime.datetime.now() - relativedelta(days=10))

    df = pandas.read_sql(
        (
            f"SELECT * FROM `{PRICE_TB_NAME}` WHERE `{util.TIME_COL_NAME}` >="
            f" '{_start_time}';"
        ),
        con=conn,
        index_col=util.TIMED_INDEX_COLs,
        parse_dates=[util.TIME_COL_NAME],
    )

    return df


def write_sqlite3(
    df: pandas.DataFrame, table_name: str, conn: sqlite3.Connection
) -> int:
    is_table_existed = util.is_table_existed_in_sqlite3(table_name, con=conn)
    if is_table_existed:
        existing_df = read_sql(conn=conn, start_time=datetime.datetime(1990, 1, 1))
    else:
        existing_df = pandas.DataFrame()

    merged_df = pandas.concat([existing_df, df])
    merged_df.drop_duplicates(inplace=True, keep="last")
    merged_df.sort_index()
    merged_df.to_sql(table_name, con=conn, if_exists="replace")

    return len(merged_df) - len(existing_df)


def _crawl_daily_price_from_twse(ts: datetime.datetime) -> pandas.DataFrame:
    # Download page
    url = (
        "https://www.twse.com.tw/exchangeReport/MI_INDEX?"
        f"response=csv&date={ts.year}{ts.month:02d}{ts.day:02d}&type=ALLBUT0999"
    )
    response = requests.get(url)

    # Raise ValueError if empty data
    response.raise_for_status()
    if response.text == "":
        raise util.YiException(f"The daily price table could not be found on `{url}`.")

    # Replace the character '=' with an empty string in the response body
    content = response.text.replace("=", "")

    # Filter the rows with at least 12 columns
    lines = content.split("\n")
    lines = list(filter(lambda l: len(l.split('",')) > 12, lines))

    # Concatenate all the lines into a single string with '\n' as the separator
    content = "\n".join(lines)

    # Convert raw data to DataFrame
    df = pandas.read_csv(io.StringIO(content))
    df["漲跌價差"] = df["漲跌(+/-)"].astype(str) + df["漲跌價差"].astype(str)

    # Replace the comma (',') in scalars
    df = df.astype(str)
    df = df.apply(lambda scalar: scalar.str.replace(",", ""))

    # Replace the Chinese column names with English column names for indexing
    column_mapping = {
        "證券代號": util.SECURITY_ID_NAME,
    }
    df.rename(columns=column_mapping, inplace=True)
    df[util.TIME_COL_NAME] = pandas.to_datetime(
        datetime.datetime(ts.year, ts.month, ts.day)
    )
    df.set_index(util.TIMED_INDEX_COLs, inplace=True)

    # Convert all the scalar to number
    # error='coerce', will set NaN for all the scalars which are invalid parsed
    df = df.apply(lambda scalar: pandas.to_numeric(scalar, errors="coerce"))

    # Cutout the columns consist of empty value
    df = df[df.columns[df.isnull().all() == False]]

    df["漲跌幅(%)"] = df["漲跌價差"] / df["收盤價"] * 100

    # check columns
    if set(PRICE_TB_COLs) - set(df.columns):
        raise TypeError(
            f"Miss columns {set(PRICE_TB_COLs) - set(df.columns)} from `{url}`."
        )

    return df


def _crawl_daily_price_from_tpex(ts: datetime.datetime) -> pandas.DataFrame:
    # Download page
    url = (
        "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?"
        f"l=zh-tw&d={ts.year - 1911}/{ts.month:02d}/{ts.day:02d}&se=EW"
    )
    response = requests.get(url)

    # Raise ValueError if empty data
    response.raise_for_status()
    data = response.json()
    if len(data.get("aaData", [])) == 0:
        raise util.YiException(f"The daily price table could not be found on `{url}`.")

    df = pandas.DataFrame(
        data["aaData"],
        columns=[
            "代號",
            "名稱",
            "收盤",
            "漲跌",
            "開盤",
            "最高",
            "最低",
            "成交股數",
            "成交金額(元)",
            "成交筆數",
            "最後買價",
            "最後買量(千股)",
            "最後賣價",
            "最後賣量(千股)",
            "發行股數",
            "次日漲停價",
            "次日跌停價",
        ],
    )
    column_map = {
        "收盤": "收盤價",
        "漲跌": "漲跌價差",
        "開盤": "開盤價",
        "最高": "最高價",
        "最低": "最低價",
        "成交金額(元)": "成交金額",
        "最後買價": "最後揭示買價",
        "最後買量(千股)": "最後揭示買量",
        "最後賣價": "最後揭示賣價",
        "最後賣量(千股)": "最後揭示賣量",
    }
    df.rename(columns=column_map, inplace=True)

    # Replace the comma (',') in scalars
    df = df.astype(str)
    df = df.apply(lambda scalar: scalar.str.replace(",", ""))

    # Replace the Chinese column names with English column names for indexing
    column_mapping = {
        "代號": util.SECURITY_ID_NAME,
    }
    df.rename(columns=column_mapping, inplace=True)
    df[util.TIME_COL_NAME] = pandas.to_datetime(
        datetime.datetime(ts.year, ts.month, ts.day)
    )
    df.set_index(util.TIMED_INDEX_COLs, inplace=True)

    # Convert all the scalar to number
    # error='coerce', will set NaN for all the scalars which are invalid parsed
    df = df.apply(lambda scalar: pandas.to_numeric(scalar, errors="coerce"))

    # Cutout the columns consist of empty value
    df = df[df.columns[df.isnull().all() == False]]

    df["漲跌幅(%)"] = df["漲跌價差"] / df["收盤價"] * 100
    df["本益比"] = numpy.NaN

    # check columns
    if set(PRICE_TB_COLs) - set(df.columns):
        raise TypeError(f"Miss expected columns {set(PRICE_TB_COLs) - set(df.columns)}")

    return df
