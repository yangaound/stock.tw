import argparse
import calendar
import datetime
import logging
import time
import traceback

import numpy as np
import pytz
from 變易 import price, util


def main(stime: datetime.datetime, etime: datetime.datetime):
    while stime <= etime:
        if calendar.weekday(stime.year, stime.month, stime.day) in (5, 6):
            logging.info(f"Skip date `{stime}`")
            stime += datetime.timedelta(days=1)
            continue

        db_proxy = None
        try:
            # Extract DataFrame from internet
            logging.info(f"Extract date`{stime}`")
            df = price.extract_price(stime)
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
                table_name=price.PRICE_TB_NAME,
                mode="UPDATE",
            )
            logging.info(f"Upsert table `{price.PRICE_TB_NAME}` {count} rows")
        except util.YiException as e:
            logging.warning(str(e))
        except Exception:
            logging.error(traceback.format_exc())
        finally:
            db_proxy and db_proxy.close()

        stime += datetime.timedelta(days=1)
        time.sleep(15)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-edate", help="end date (YYYYmmdd).")
    parser.add_argument("-sdate", help="start date (YYYYmmdd).")
    args = parser.parse_args()

    # Determine end time, parse from command line or default to today
    if args.edate:
        end_time = datetime.datetime.strptime(args.edate, "%Y%m%d")
    else:
        # Determine end time
        now = datetime.datetime.now(tz=pytz.timezone("Asia/Taipei")).replace(
            tzinfo=None
        )
        today = datetime.datetime(now.year, now.month, now.day)
        if (now.hour, now.minute) > (15, 0):
            end_time = today
        else:
            end_time = today - datetime.timedelta(days=1)

    # Determine start time, parse from command line or use end_time
    if args.sdate:
        start_time = datetime.datetime.strptime(args.sdate, "%Y%m%d")
    else:
        start_time = end_time

    main(start_time, end_time)
