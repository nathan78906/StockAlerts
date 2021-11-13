import os
from datetime import datetime
from requests_retry import requests_retry_session


def process_news_releases(mydb, cursor, logger, symbol, sedar_id, news_releases_list):
    new_news_releases = []
    news_releases_url = "{}api/articles/load_more?channel={}&type=news&before={}".format(
        os.environ['BASE_URL'],
        symbol,
        int(datetime.utcnow().timestamp()*1e3))
    try:
        news_releases_response = requests_retry_session().get(news_releases_url, timeout=10)
    except Exception as x:
        logger.error("{} : {}".format(repr(x), news_releases_url))
        return

    if news_releases_response.status_code != 200:
        logger.error("{} : {}".format(news_releases_response.status_code, news_releases_url))
        return

    news_releases_response = news_releases_response.json()[:10]
    for news_release in news_releases_response:
        if news_release["channel"] not in news_releases_list:
            created_at = datetime.utcfromtimestamp(news_release["created_at"]/1000).\
                strftime("%Y-%m-%dT%H:%M:%SZ")
            url = os.environ['BASE_URL'] + news_release["channel"]
            sedar_url = os.environ['SEDAR_URL'] + sedar_id if sedar_id else ""
            embed = {
                "author": {
                    "name": symbol
                },
                "title": news_release["title"],
                "url": url,
                "description": "{}\n\n[See SEDAR filings]({})".format(url, sedar_url),
                "timestamp": created_at
            }
            new_news_releases.append(embed)

            try:
                statement = "INSERT INTO news_releases(`channel`, `symbol`, `title`, `url`, `created_at`) VALUES (%s, %s, %s, %s, %s)"
                values = (news_release["channel"], symbol, news_release["title"], url, created_at)
                cursor.execute(statement, values)
                mydb.commit()
            except Exception as x:
                logger.error("{} : {}".format(repr(x), news_releases_url))
                continue
    return new_news_releases
