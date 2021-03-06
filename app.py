import sendgrid
import os
import pymysql
import logging
import sentry_sdk
from datetime import datetime
from sendgrid.helpers.mail import *
from transactions import process_transactions
from news_releases import process_news_releases

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
watchlist = [item[0] for item in cursor]
cursor.execute("call seen_transactions()")
transaction_list = [item[0] for item in cursor]
cursor.execute("call seen_news_releases()")
news_releases_list = [item[0] for item in cursor]

email_list = []

for symbol in watchlist:
    new_transactions = process_transactions(mydb, cursor, logger, symbol, transaction_list)
    email_list += new_transactions
    new_news_releases = process_news_releases(mydb, cursor, logger, symbol, news_releases_list)
    email_list += new_news_releases

cursor.close()
now = datetime.now()

if email_list:
    sg = sendgrid.SendGridAPIClient(api_key=os.environ['SENDGRID_API_KEY'])
    from_email = From(os.environ['FROM_EMAIL'], os.environ['FROM_NAME'])
    to_email = To(os.environ['TO_EMAIL'])
    subject = "{} - {}".format(os.environ['FROM_NAME'], now.strftime("%m/%d/%Y, %H:%M:%S"))
    content = Content("text/plain", "\n\n".join(email_list))
    mail = Mail(from_email, to_email, subject, content)
    response = sg.send(mail)
    logger.info(response.status_code)
    logger.info(response.body)
    logger.info(response.headers)
else:
    logger.info("No new alerts for: {}".format(now.strftime("%m/%d/%Y, %H:%M:%S")))
