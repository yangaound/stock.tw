import datetime
import logging
import os
import os.path
import sqlite3
from typing import Any, Union

import dbman
import MySQLdb
import pandas
import sqlalchemy
import yaml

logging.basicConfig(
    format="[%(asctime)s][%(levelname)s][%(name)s][%(module)s]: %(message)s",
    level=logging.DEBUG,
)

TIME_COL_NAME = "ts"
SECURITY_ID_NAME = "code"
TIMED_INDEX_COLs = [TIME_COL_NAME, SECURITY_ID_NAME]

CONF: dict[str, Any]
DB_ENGINE: sqlalchemy.Engine


with open(os.getenv("CONF_PATH"), encoding="UTF-8") as _fp:
    CONF = yaml.load(_fp, yaml.SafeLoader)

with open(os.getenv("DB_CONF_PATH")) as _fp:
    _stock_tw_db_conf = yaml.load(_fp, Loader=yaml.SafeLoader)
    _db_conf = _stock_tw_db_conf["stock-tw"]["connect_kwargs"]
    _conn_str = (
        f"mysql+mysqldb://{_db_conf['user']}:{_db_conf['passwd']}@{_db_conf['host']}/{_db_conf['db']}"
        f"?charset={_db_conf['charset']}"
    )
    DB_ENGINE = sqlalchemy.create_engine(_conn_str)


class YiException(Exception):
    pass


def get_sqlite3() -> sqlite3.Connection:
    sqlite_path = os.path.join(os.getenv("STORAGE_ROOT"), "stock-tw.db")
    return sqlite3.connect(sqlite_path)


def get_db_proxy() -> dbman.DBProxy:
    return dbman.DBProxy(connection=DB_ENGINE.raw_connection())


def is_table_existed_in_sqlite3(table_name: str, con: sqlite3.Connection):
    sql = (
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' and"
        f" name='{table_name}';"
    )
    results = list(con.execute(sql))
    return results[0][0] == 1


def read_csv(
    file_path: str, index_col: list[str] = None, parse_dates: list[str] = None
) -> pandas.DataFrame:
    index_col = TIMED_INDEX_COLs if index_col is None else index_col
    parse_dates = [TIME_COL_NAME] if parse_dates is None else parse_dates

    df = pandas.read_csv(
        file_path,
        index_col=index_col,
        parse_dates=parse_dates,
    )

    return df


def read_sql(
    sql: str,
    conn: Union[sqlite3.Connection, MySQLdb.Connection],
    index_col: list[str] = None,
    parse_dates: list[str] = None,
) -> pandas.DataFrame:
    index_col = TIMED_INDEX_COLs if index_col is None else index_col
    parse_dates = [TIME_COL_NAME] if parse_dates is None else parse_dates

    df = pandas.read_sql(
        sql,
        con=conn,
        index_col=index_col,
        parse_dates=parse_dates,
    )

    return df


def write_csv(new_df: pandas.DataFrame, file_path: str) -> int:
    if os.path.exists(file_path):
        existing_df = read_csv(file_path)
    else:
        existing_df = pandas.DataFrame()

    df = pandas.concat([existing_df, new_df])
    df.drop_duplicates(inplace=True, keep="last")
    df.sort_index(ascending=True, inplace=True)
    df.to_csv(file_path)

    return len(df) - len(existing_df)


class IFRSDateIter:
    """
    Q1 -> datetime.datetime(year,  5, 15)
    Q2 -> datetime.datetime(year,  8, 14)
    Q3 -> datetime.datetime(year, 11, 14)
    Q4 -> datetime.datetime(year + 1,  3, 31)
    """

    def __init__(
        self,
        year: int = None,
        quarter: int = None,
        ifrs_dt: datetime.datetime = None,
        end_year=2100,
        end_quarter=4,
    ):
        if not (year or quarter or ifrs_dt):
            raise ValueError(
                "either one of `year-quarter` or `ifrs_dt` should be given"
            )

        if not (year or quarter) and ifrs_dt:
            year, quarter = IFRSDateIter.ifrs_dt2quarter(ifrs_dt)

        self.year = year
        self.quarter = quarter
        self.end_yaar = end_year
        self.end_quarter = end_quarter

    @staticmethod
    def ifrs_dt2quarter(ts: datetime.datetime) -> tuple[int, int]:
        if (ts.month, ts.day) == (3, 31):
            year = ts.year - 1
            quarter = 4
        elif (ts.month, ts.day) == (5, 15):
            quarter = 1
            year = ts.year
        elif (ts.month, ts.day) == (8, 14):
            quarter = 2
            year = ts.year
        elif (ts.month, ts.day) == (11, 14):
            quarter = 3
            year = ts.year
        else:
            raise ValueError(f"Invalid parse datetime {ts}")

        return year, quarter

    @staticmethod
    def dt_to_closest_ifrs_dt(ts: datetime.datetime) -> datetime.datetime:
        # for case week delay
        ts = ts - datetime.timedelta(days=4)

        year, season = None, None
        if datetime.datetime(ts.year, 3, 31) > ts <= datetime.datetime(ts.year, 5, 15):
            season = 1
            year = ts.year
        elif (
            datetime.datetime(ts.year, 5, 15) > ts <= datetime.datetime(ts.year, 8, 14)
        ):
            season = 2
            year = ts.year
        elif (
            datetime.datetime(ts.year, 8, 14) > ts <= datetime.datetime(ts.year, 11, 14)
        ):
            season = 3
            year = ts.year
        else:
            season = 4
            if ts.month not in (1, 2, 3):
                year = ts.year - 1

        return IFRSDateIter(year, season).current_ifrs_dt()

    @staticmethod
    def dt_in_season(ts: datetime.datetime) -> tuple[int, int]:
        year = ts.year
        if ts.month in (1, 2, 3):
            season = 1
        elif ts.month in (4, 5, 6):
            season = 2
        elif ts.month in (7, 8, 9):
            season = 3
        else:
            season = 4
        return year, season

    @staticmethod
    def dt_in_ifrs_dt(ts: datetime.datetime) -> datetime.datetime:
        year, quarter = IFRSDateIter.dt_in_season(ts)
        return IFRSDateIter(year, quarter).current_ifrs_dt()

    def current_ifrs_dt(self) -> datetime.datetime:
        if self.quarter == 4:
            ts = datetime.datetime(self.year + 1, 3, 31)
        elif self.quarter == 1:
            ts = datetime.datetime(self.year, 5, 15)
        elif self.quarter == 2:
            ts = datetime.datetime(self.year, 8, 14)
        else:
            ts = datetime.datetime(self.year, 11, 14)

        return ts

    def previous_ifrs_dt(self) -> datetime.datetime:
        if self.quarter == 1:
            self.year -= 1
            self.quarter = 4
        else:
            self.quarter -= 1

        return self.current_ifrs_dt()

    def next_ifrs_dt(self) -> datetime.datetime:
        if self.quarter == 4:
            self.year += 1
            self.quarter = 1
        else:
            self.quarter += 1

        return self.current_ifrs_dt()

    def __iter__(self):
        return self

    def __next__(self):
        start = self.year * 10 + self.quarter
        end = self.end_yaar * 10 + self.end_quarter
        if start <= end:
            return self.next_ifrs_dt()
        else:
            raise StopIteration


def time2monthly_date(ts: datetime.datetime) -> datetime.datetime:
    return datetime.datetime(ts.year, ts.month, 10)
