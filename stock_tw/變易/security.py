import sqlite3
from typing import Optional, Union

import MySQLdb
import pandas

from . import util

SECURITY_TB_NAME = "security_list"
SECURITY_TB_COLs = [
    "type",
    "name",
    "ISIN",
    "start",
    "market",
    "group",
    "CFI",
]


def read_sql(
    conn: Union[sqlite3.Connection, MySQLdb.Connection],
    security_types: Optional[list[str]] = None,
) -> pandas.DataFrame:
    if security_types is None:
        sql_stmt = f"SELECT * FROM `{SECURITY_TB_NAME}`;"
    else:
        _type_str = ",".join(map(lambda item: "'%s'" % item, security_types))
        sql_stmt = f"SELECT * FROM `{SECURITY_TB_NAME}` WHERE `type` IN ({_type_str});"

    df = pandas.read_sql(
        sql_stmt,
        con=conn,
        index_col=[util.SECURITY_ID_NAME],
    )

    return df


def write_sqlite3(df: pandas.DataFrame, conn: sqlite3.Connection) -> int:
    is_table_existed = util.is_table_existed_in_sqlite3(SECURITY_TB_NAME, con=conn)
    if is_table_existed:
        existing_df = read_sql(conn)
    else:
        existing_df = pandas.DataFrame()

    merged_df = pandas.concat([existing_df, df])
    merged_df.drop_duplicates(inplace=True, keep="last")
    merged_df.sort_index(ascending=True, inplace=True)
    merged_df.to_sql(SECURITY_TB_NAME, con=conn, if_exists="replace")

    return len(merged_df) - len(existing_df)


def extract_securities():
    import twstock

    securities = (
        (
            security.code,
            security.type,
            security.name,
            security.ISIN,
            security.start,
            security.market,
            security.group,
            security.CFI,
        )
        for security in twstock.codes.values()
    )
    df = pandas.DataFrame(
        securities, columns=[util.SECURITY_ID_NAME] + SECURITY_TB_COLs
    ).set_index([util.SECURITY_ID_NAME])

    # check columns
    if set(SECURITY_TB_COLs) - set(df.columns):
        raise TypeError(
            f"Miss expected columns {set(SECURITY_TB_COLs) - set(df.columns)}."
        )

    return df
