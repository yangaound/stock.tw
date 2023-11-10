import argparse
import datetime
import logging
import traceback

import numpy as np
import pytz

from stock_tw import util
from stock_tw.變易.fin_stmt import balance_sheet


def main(ts: datetime.datetime):
    db_proxy = None
    try:
        # Extract DataFrame from SQLite3
        logging.info(f"Extract `{ts}`")
        conn = util.get_sqlite3()
        sql_stmt = f"""
            SELECT `ts`, `code`, `資產總計` 
            FROM {balance_sheet.BALANCE_TB_NAME}
            WHERE `ts` = '{ts}';
        """
        df = balance_sheet.read_sql(conn=conn, sql_stmt=sql_stmt)
        conn.close()
        logging.info(f"Extracted data {len(df)} rows")

        db_proxy = util.get_db_proxy()
        # Transform DataFrame
        _reset_df = df.replace({np.nan: None})
        _reset_df.reset_index(inplace=True)
        table_content = [list(_reset_df.columns)]
        table_content.extend(list(row) for row in _reset_df.values)
        table_unique_keys = list(df.index.names)

        # Load data into DB
        count = db_proxy.todb(
            table_content,
            unique_key=table_unique_keys,
            table_name=f"{balance_sheet.BALANCE_TB_NAME}_metatime",
            mode="UPDATE",
        )
        logging.info(
            f"upsert table `{balance_sheet.BALANCE_TB_NAME}_metatime`"
            f" {count} rows"
        )
    except util.YiException as e:
        logging.warning(str(e))
    except Exception:
        logging.error(traceback.format_exc())
        raise
    finally:
        db_proxy and db_proxy.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-quarter", help="The year and quarter values, example: 20231")
    args = parser.parse_args()

    year: int
    quarter: int
    if args.quarter:
        year, quarter = int(args.quarter[:4]), int(args.quarter[4])
        # in case '20230' -> '20224'
        if quarter == 0:
            year -= 1
            quarter = 4
    else:
        now = datetime.datetime.now(tz=pytz.timezone("Asia/Taipei")).replace(
            tzinfo=None
        )
        year = now.year
        quarter = now.month // 3
        if quarter == 0:
            year -= 1
            quarter = 4

    ifrs_dt: datetime = util.IFRSDateIter(year, quarter).current_ifrs_dt()
    main(ifrs_dt)
