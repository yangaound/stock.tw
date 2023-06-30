import argparse
import datetime
import logging
import traceback

import numpy as np
from dateutil.relativedelta import relativedelta

from stock_tw.變易 import revenue, util


def main(ts: datetime.datetime):
    db_proxy = None
    try:
        # Extract DataFrame from internet
        logging.info(f"Extract `{ts}`")
        df = revenue.extract(ts)
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
    finally:
        db_proxy and db_proxy.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-month", help="start date of month (YYYYmm).")
    args = parser.parse_args()

    # Parse datetime from command line or default to last month.
    if args.month:
        _ts = datetime.datetime.strptime(f"{args.month}10", "%Y%m%d")
    else:
        _ = datetime.datetime.today() - relativedelta(months=1)
        _ts = datetime.datetime(_.year, _.month, 1)

    main(_ts)
