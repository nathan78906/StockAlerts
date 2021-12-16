import os
import pymysql
import logging
import sentry_sdk
import time
from datetime import datetime
from transactions import process_transactions
from news_releases import process_news_releases
from price_alerts import process_price_alerts
from requests_retry import requests_retry_session

sentry_sdk.init(dsn=os.environ['SENTRY'])
logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)

mydb = pymysql.connect(host=os.environ['MARIADB_HOSTNAME'],
    user=os.environ['MARIADB_USERNAME'],
    passwd=os.environ['MARIADB_PASSWORD'],
    db=os.environ['MARIADB_DATABASE'])
cursor = mydb.cursor()

cursor.execute("call active_watchlist()")
watchlist = [{"symbol": item[0], "isHighTransactionTraffic": int(item[1]), "sedarId": item[2]} for item in cursor]
cursor.execute("call seen_transactions()")
transaction_list = [item[0] for item in cursor]
cursor.execute("call seen_news_releases()")
news_releases_list = [item[0] for item in cursor]
cursor.execute("call last_price_alerts()")
price_alert_symbol_map = {}
for item in cursor:
    # symbol to date_last_price_alert map
    price_alert_symbol_map[item[0]] = item[1]

embeds = []

for item in watchlist:
    is_high_transaction_traffic = item["isHighTransactionTraffic"]
    if not is_high_transaction_traffic:
        new_transactions = process_transactions(mydb, cursor, logger, item["symbol"], transaction_list)
        embeds += new_transactions
    new_news_releases = process_news_releases(mydb, cursor, logger, item["symbol"], item["sedarId"], news_releases_list)
    embeds += new_news_releases
    new_price_alerts = process_price_alerts(mydb, cursor, logger, item["symbol"],
        price_alert_symbol_map)
    embeds += new_price_alerts
    time.sleep(1)


cursor.close()

if embeds:
    data = {"embeds": embeds[::-1][:10]}
    try:
        discord_response = requests_retry_session().post(os.environ['DISCORD_WEBHOOK'],
                                                         json=data,
                                                         timeout=10)
    except Exception as x:
        logger.error("{} : {}".format(repr(x), data))

    if discord_response.status_code != 204:
        logger.error("{} : {}".format(discord_response.status_code, data))

    logger.info(discord_response.status_code)
    logger.info(discord_response.headers)
    logger.info(discord_response.text)
else:
    logger.info("No new alerts for: {}".format(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
