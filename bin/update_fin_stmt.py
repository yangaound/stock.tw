import argparse
import datetime
import logging
import traceback

import numpy as np
import pytz

from stock_tw.變易 import fin_stmt, util


def main(ts: datetime.datetime):
    db_proxy = None
    try:
        # Extract DataFrame from internet
        logging.info(f"Extract `{ts}`")
        df_map = fin_stmt.extract(ts)

        db_proxy = util.get_db_proxy()
        for tb_name, df in df_map.items():
            logging.info(f"Extracted data `{tb_name}` {len(df)} rows")
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
                table_name=tb_name,
                mode="UPDATE",
            )
            logging.info(f"upsert table `{tb_name}` {count} rows")
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

    if args.quarter:
        quarter = int(args.quarter)
        year, quarter = quarter // 10, quarter % 10
        ifrs_dt = util.IFRSDateIter(year, quarter).current_ifrs_dt()
    else:
        now = datetime.datetime.now(tz=pytz.timezone("Asia/Taipei")).replace(
            tzinfo=None
        )
        ifrs_dt = util.IFRSDateIter.dt_to_closest_ifrs_dt(now)

    main(ifrs_dt)
