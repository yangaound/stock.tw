import argparse
import datetime
import logging
import time
import traceback

import numpy as np
from dateutil.relativedelta import relativedelta

from stock_tw.變易 import revenue
from stock_tw import util


def main(stime: datetime.datetime, etime: datetime.datetime):
    while stime <= etime:
        db_proxy = None
        try:
            # Extract DataFrame from internet
            logging.info(f"Extract `{stime}`")
            df = revenue.extract(stime)
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
                table_name=revenue.REVENUE_TB_NAME,
                mode="UPDATE",
            )
            logging.info(f"upsert table `{revenue.REVENUE_TB_NAME}` {count} rows")
        except util.YiException as e:
            logging.warning(str(e))
        except Exception:
            logging.error(traceback.format_exc())
            raise
        finally:
            db_proxy and db_proxy.close()
        time.sleep(10)
        stime += relativedelta(months=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-smonth",
        help="The start year and month of the target revenue in the format 'YYYY-MM'",
    )
    parser.add_argument(
        "-emonth",
        help="The end year and month of the target revenue in the format 'YYYY-MM'",
    )
    args = parser.parse_args()

    # Parse datetime from command line or default to last month.
    if args.emonth:
        end_month = datetime.datetime.strptime(f"{args.emonth}-10", "%Y-%m-%d")
    else:
        _ = datetime.datetime.today() - relativedelta(months=1)
        end_month = datetime.datetime(_.year, _.month, 10)

    # Parse datetime from command line or default to the end time.
    if args.smonth:
        start_month = datetime.datetime.strptime(f"{args.smonth}-10", "%Y-%m-%d")
    else:
        start_month = end_month

    main(start_month, end_month)
