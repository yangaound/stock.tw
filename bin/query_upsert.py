import argparse
import calendar
import datetime
import json
import logging
import traceback

import pandas
import pytz

from stock_tw.變易 import util


def main(table_name, stime: datetime.datetime, etime: datetime.datetime):
    sql = f"""
    SELECT `ts`, `updated_ts`, `created_ts`, `code`
        FROM `{table_name}`
    WHERE 1
        AND `updated_ts` >= '{stime}'
        AND `updated_ts` <= '{etime}'
    ;"""
    logging.info(sql)

    db_proxy = None
    try:
        db_proxy = util.get_db_proxy()
        df = pandas.read_sql(sql, con=db_proxy.connection)
    except Exception:
        logging.error(traceback.format_exc())
        raise
    finally:
        db_proxy and db_proxy.close()

    output = {
        "code": [v for v in df["code"]],
        "created_ts": [v.to_pydatetime().isoformat() for v in df["created_ts"]],
        "updated_ts": [v.to_pydatetime().isoformat() for v in df["updated_ts"]],
        "ts": [v.to_pydatetime().isoformat() for v in df["ts"]],
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("table_name", help="query table name")
    parser.add_argument("-stime", help="start time in format 'YYYY-mm-ddTHH:MM:SS'")
    parser.add_argument("-etime", help="end time in format 'YYYY-mm-ddTHH:MM:SS'")
    args = parser.parse_args()

    now = datetime.datetime.now(tz=pytz.timezone("Asia/Taipei")).replace(tzinfo=None)

    # Determine end time, parse from command line or default to now
    if args.etime:
        end_time = datetime.datetime.strptime(args.etime, "%Y-%m-%dT%H:%M:%S")
    else:
        end_time = now

    # Determine start time, parse from command line or use today
    if args.stime:
        start_time = datetime.datetime.strptime(args.stime, "%Y-%m-%dT%H:%M:%S")
    else:
        start_time = datetime.datetime(now.year, now.month, now.day)
        if (
            weekday := calendar.weekday(
                start_time.year, start_time.month, start_time.day
            )
        ) and weekday in (5, 6):
            start_time -= datetime.timedelta(days=weekday - 4)
            logging.info(f"Shift `{weekday - 4}` days")

    main(args.table_name, start_time, end_time)
