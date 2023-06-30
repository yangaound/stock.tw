import logging
import traceback

import numpy as np

from stock_tw.變易 import security, util


def main():
    db_proxy = None
    try:
        # Extract DataFrame from internet
        df = security.extract_securities()
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
            table_name=security.SECURITY_TB_NAME,
            mode="update",
        )
        logging.info(f"upsert table `{security.SECURITY_TB_NAME}` {count} rows")
    except util.YiException as e:
        logging.warning(str(e))
    except Exception:
        logging.error(traceback.format_exc())
        raise
    finally:
        db_proxy and db_proxy.close()


if __name__ == "__main__":
    main()
