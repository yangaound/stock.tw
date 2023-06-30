import datetime

import pandas

from stock_tw.變易 import util

from . import balance_sheet, cash_flow, cumulate_income_sheet, income_sheet


def extract(ts: datetime.datetime) -> dict[str, pandas.DataFrame]:
    connection = util.get_sqlite3()
    ret: dict[str, pandas.DataFrame] = {
        "balance_sheet": balance_sheet.read_sql(
            conn=connection,
            sql_stmt=(
                "SELECT `ts`, `code`, `資產總計` FROM balance_sheet WHERE `ts` ="
                f" '{ts}'"
            ),
        ),
        "cash_flow": cash_flow.read_sql(
            conn=connection,
            sql_stmt=(
                "SELECT `ts`, `code`, `本期稅前淨利（淨損）` FROM cash_flow WHERE `ts`"
                f" = '{ts}'"
            ),
        ),
        "cumulate_income_sheet": income_sheet.read_sql(
            conn=connection,
            sql_stmt=(
                "SELECT `ts`, `code`, `累計基本每股盈餘合計` FROM"
                f" cumulate_income_sheet WHERE `ts` = '{ts}'"
            ),
        ),
        "income_sheet": cumulate_income_sheet.read_sql(
            conn=connection,
            sql_stmt=(
                "SELECT `ts`, `code`, `基本每股盈餘合計` FROM income_sheet WHERE `ts`"
                f" = '{ts}'"
            ),
        ),
    }
    connection.close()

    return ret
