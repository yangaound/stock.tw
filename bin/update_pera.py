import argparse
import datetime
import logging
import time
import traceback

import numpy as np
import pytz
from dateutil.relativedelta import relativedelta

from stock_tw.變易 import pera, util


def main(stime: datetime.datetime, etime: datetime.datetime):
    max_retry, retry = 10, 0
    while stime <= etime and retry < 10:
        db_proxy = None
        try:
            # Extract DataFrame from internet
            logging.info(f"Extract date `{stime}`")
            df = pera.extract(stime)
            logging.info(f"Extracted data {len(df)} rows")

            # Transform DataFrame
            _reset_df = df.replace({np.nan: None})
            _reset_df.reset_index(inplace=True)
            table_content = [list(_reset_df.columns)]
            table_content.extend(list(row) for row in _reset_df.values)
            table_unique_keys = list(df.index.names)

            # Load data into DB
            db_proxy = util.get_db_proxy()
            count = db_proxy.todb(
                table_content,
                unique_key=table_unique_keys,
                table_name=pera.PERA_TB_NAME,
                mode="UPDATE",
            )
            logging.info(f"Upsert table `{pera.PERA_TB_NAME}` {count} rows")
            tmp = stime + relativedelta(years=1)
            stime = datetime.datetime(tmp.year, 12, 31)
            retry = 0
        except util.YiException as e:
            logging.warning(str(e))
            stime += datetime.timedelta(days=-1)
            retry += 1
        except Exception:
            logging.error(traceback.format_exc())
        finally:
            db_proxy and db_proxy.close()
        time.sleep(15)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-syear", help="The start year in format 'YYYY'")
    parser.add_argument("-edate", help="The end date in format 'YYYY-MM-DD'")
    args = parser.parse_args()

    # Determine end time, parse from command line or default to today
    if args.edate:
        end_time = datetime.datetime.strptime(args.edate, "%Y-%m-%d")
    else:
        # Determine end time
        now = datetime.datetime.now(tz=pytz.timezone("Asia/Taipei")).replace(
            tzinfo=None
        )
        today = datetime.datetime(now.year, now.month, now.day)
        if (now.hour, now.minute) > (19, 0):
            end_time = today
        else:
            end_time = today - datetime.timedelta(days=1)

    # Determine start time, parse from command line or use end_time
    if args.syear:
        start_time = datetime.datetime.strptime(args.syear + "-12-31", "%Y-%m-%d")
    else:
        start_time = end_time

    main(start_time, end_time)
