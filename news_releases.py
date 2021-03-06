import os
from datetime import datetime
from requests_retry import requests_retry_session


def process_news_releases(mydb, cursor, logger, symbol, news_releases_list):
    new_news_releases = []
    news_releases_url = os.environ['BASE_URL'] + "api/articles/load_more?channel={}&type=news&before={}".format(symbol, int(datetime.utcnow().timestamp()*1e3))
    try:
        news_releases_response = requests_retry_session().get(news_releases_url, timeout=10)
    except Exception as x:
        logger.error("{} : {}".format(x.__class__.__name__, news_releases_url))
        return

    if news_releases_response.status_code != 200:
        logger.error("{} : {}".format(news_releases_response.status_code, news_releases_url))
        return

    news_releases_response = news_releases_response.json()[:30]
    for news_release in news_releases_response:
        if news_release["channel"] not in news_releases_list:
            new_news_releases.append("{} - {}: {}".format(symbol,
                                                          news_release["title"],
                                                          os.environ['BASE_URL'] + news_release["channel"]))

            try:
                cursor.execute("INSERT INTO news_releases(`channel`,"
                               "`symbol`,"
                               "`title`,"
                               "`url`,"
                               "`created_at`)"
                               " VALUES('{}','{}','{}','{}','{}')"
                               .format(
                                news_release["channel"],
                                symbol,
                                news_release["title"],
                                os.environ['BASE_URL'] + news_release["channel"],
                                datetime.utcfromtimestamp(news_release["created_at"]/1000).
                                    strftime('%Y-%m-%d %H:%M:%S')
                                ))
                mydb.commit()
            except Exception as x:
                logger.error("{} : {}".format(x.__class__.__name__, news_releases_url))
                continue
    return new_news_releases
