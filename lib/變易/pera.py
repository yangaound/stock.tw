import calendar
import datetime
import logging
import sqlite3
from typing import Union

import MySQLdb
import numpy
import pandas
import requests
from dateutil.relativedelta import relativedelta

from . import util

PERA_TB_NAME = "pera"
PERA_TB_COLs = ["殖利率(%)", "股利年度", "本益比", "股價淨值比", "每股股利(註)"]


def extract(ts: datetime.datetime) -> pandas.DataFrame:
    if ts.date() > datetime.date.today():
        raise ValueError(f"The date `{ts}` must be in the past.")

    if weekday := calendar.weekday(ts.year, ts.month, ts.day) in (5, 6):
        weekdays = {5: "Saturday", 6: "Sunday"}
        raise ValueError(f"The date `{ts}` is {weekdays[weekday]}.")

    # TWSE
    twse_df = _crawl_pera_from_twse(ts)
    # TPEX
    try:
        tpex_df = _crawl_pera_from_tpex(ts)
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
            f"SELECT * FROM `{PERA_TB_NAME}` WHERE `{util.TIME_COL_NAME}` >="
            f" '{_start_time}';"
        ),
        con=conn,
        index_col=util.TIMED_INDEX_COLs,
        parse_dates=[util.TIME_COL_NAME],
    )

    return df


def _crawl_pera_from_twse(ts: datetime.datetime) -> pandas.DataFrame:
    # Download page
    url = (
        "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?"
        f"date={ts.year}{ts.month:02d}{ts.day:02d}&selectType=ALL&response=json"
    )
    response = requests.get(url)

    # Return empty DataFrame
    response.raise_for_status()
    data = response.json()
    if len(data.get("data", [])) == 0:
        raise util.YiException(f"The PER-analysis table could not be found on `{url}`.")

    df = pandas.DataFrame(
        data["data"],
        columns=data["fields"],
    )
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

    df["每股股利(註)"] = numpy.NaN

    # check columns
    if set(PERA_TB_COLs) - set(df.columns):
        raise TypeError(f"Miss expected columns {set(PERA_TB_COLs) - set(df.columns)}")

    return df


def _crawl_pera_from_tpex(ts: datetime.datetime) -> pandas.DataFrame:
    # Download page
    url = (
        "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?"
        f"l=zh-tw&d={ts.year - 1911}/{ts.month:02d}/{ts.day:02d}&c="
    )
    response = requests.get(url)

    # Raise ValueError if empty data
    response.raise_for_status()
    data = response.json()
    if len(data.get("aaData", [])) == 0:
        raise util.YiException(f"The PER-analysis table could not be found on `{url}`.")

    df = pandas.DataFrame(
        data["aaData"],
        columns=[
            "股票代號",
            "名稱",
            "本益比",
            "每股股利(註)",
            "股利年度",
            "殖利率(%)",
            "股價淨值比",
        ],
    )
    # Replace the comma (',') in scalars
    df = df.astype(str)
    df = df.apply(lambda scalar: scalar.str.replace(",", ""))

    # Replace the Chinese column names with English column names for indexing
    column_mapping = {
        "股票代號": util.SECURITY_ID_NAME,
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

    # check columns
    if set(PERA_TB_COLs) - set(df.columns):
        raise TypeError(f"Miss expected columns {set(PERA_TB_COLs) - set(df.columns)}")

    return df[PERA_TB_COLs]
