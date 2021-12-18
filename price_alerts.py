import os
import requests
from datetime import datetime
from requests_retry import requests_retry_session


def process_price_alerts(mydb, cursor, logger, symbol, price_alert_symbol_map):
    new_price_alerts = []
    price_info_url = "{}api/get_spiels?channel={}".format(
        os.environ['BASE_URL'],
        symbol)
    try:
        price_info_response = requests_retry_session().get(price_info_url, timeout=10)
    except Exception as x:
        logger.error("{} : {}".format(repr(x), price_info_url))
        return []

    if price_info_response.status_code != 200:
        logger.error("{} : {}".format(price_info_response.status_code, price_info_url))
        return []

    price_info_response = price_info_response.json()
    percent_change = float(price_info_response["quote"]["percent_change"])
    percent_change_threshold = int(os.environ['PERCENT_CHANGE_THRESHOLD'])
    date_last_price_alert = price_alert_symbol_map[symbol]
    date_last_quote_update = datetime.utcfromtimestamp(price_info_response["quote"]["timestamp"]/1000)
    if (percent_change < -percent_change_threshold or percent_change > percent_change_threshold) and date_last_price_alert.date() != date_last_quote_update.date():
        created_at = date_last_quote_update.strftime("%Y-%m-%dT%H:%M:%SZ")
        embed = {
            "author": {
                "name": symbol
            },
            "title": "Price alert: {}%".format(percent_change),
            "description": "{}{}".format("https://finance.yahoo.com/quote/", price_info_response["stock_info"]["symbol"]),
            "timestamp": created_at
        }
        new_price_alerts.append(embed)

        try:
            statement = "UPDATE watchlist SET date_last_price_alert = current_timestamp() WHERE symbol = %s"
            values = (symbol)
            cursor.execute(statement, values)
            mydb.commit()
        except Exception as x:
            logger.error("{} : {}".format(repr(x), price_info_url))
            return []

    return new_price_alerts
