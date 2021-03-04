import sendgrid
import os
import json
import pymysql
import logging
import sentry_sdk
from datetime import datetime
from sendgrid.helpers.mail import *
from requests_retry import requests_retry_session

# sentry_sdk.init(dsn=os.environ['SENTRY'])
# logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
# logging.basicConfig(format=logFormatter, level=logging.DEBUG)
# logger = logging.getLogger(__name__)

# mydb = pymysql.connect(host=os.environ['MARIADB_HOSTNAME'],
#     user=os.environ['MARIADB_USERNAME'],
#     passwd=os.environ['MARIADB_PASSWORD'],
#     db=os.environ['MARIADB_DATABASE'])
# cursor = mydb.cursor()

# cursor.execute("call links()")
# links_list = [{'name': item[0], 'url': item[1], 'type': item[2]} for item in cursor]
# cursor.execute("call completed()")
# completed_list = [item[0] for item in cursor]

watchlist = ["EXRO", "CJT"]


for symbol in watchlist:
    print(symbol)
