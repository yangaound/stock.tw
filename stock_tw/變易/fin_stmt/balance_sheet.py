import datetime
import sqlite3
from typing import Union

import MySQLdb
import pandas
from dateutil.relativedelta import relativedelta

from stock_tw.變易 import util

BALANCE_TB_NAME = "balance_sheet"
BALANCE_TB_COLs: list[str] = util.CONF["資產負債表頭"]


def read_sql(
    conn: Union[sqlite3.Connection, MySQLdb.Connection],
    start_time: datetime.datetime = None,
) -> pandas.DataFrame:
    _start_time = start_time or (
        datetime.datetime.now() - relativedelta(years=1, months=4)
    )
    _fields = ", ".join(map(lambda field: f"`{field}`", BALANCE_TB_COLs))

    sql_stmt = f"""
        SELECT {_fields}
        FROM `{BALANCE_TB_NAME}`
        WHERE 1
            AND `{util.TIME_COL_NAME}` >= '{_start_time}'
        ;"""

    df = pandas.read_sql(
        sql_stmt,
        con=conn,
        index_col=util.TIMED_INDEX_COLs,
        parse_dates=[util.TIME_COL_NAME],
    )

    return df


def write_sqlite3(new_df: pandas.DataFrame, conn: sqlite3.Connection) -> int:
    is_table_existed = util.is_table_existed_in_sqlite3(BALANCE_TB_NAME, con=conn)
    if is_table_existed:
        existing_df = read_sql(conn, start_time=datetime.datetime(1990, 1, 1))
    else:
        existing_df = pandas.DataFrame()

    df = pandas.concat([existing_df, new_df])
    df.drop_duplicates(inplace=True, keep="last")
    df.sort_index(ascending=True, inplace=True)
    df.to_sql(BALANCE_TB_NAME, con=conn, if_exists="replace")

    return len(df) - len(existing_df)
