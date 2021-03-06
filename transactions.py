import os
from requests_retry import requests_retry_session


def process_transactions(mydb, cursor, logger, symbol, transaction_list):
    new_transactions = []
    transactions_url = os.environ['BASE_URL'] + "api/sedi/filings?symbol={}".format(symbol)
    try:
        transactions_response = requests_retry_session().get(transactions_url, timeout=10)
    except Exception as x:
        logger.error("{} : {}".format(x.__class__.__name__, transactions_url))
        return

    if transactions_response.status_code != 200:
        logger.error("{} : {}".format(transactions_response.status_code, transactions_url))
        return

    transactions_response = transactions_response.json()[:30]
    for transaction in transactions_response:
        transaction = transaction["datab"]
        if str(transaction["Transaction ID"]) not in transaction_list:
            units = transaction["Number or value acquired or disposed of"].replace(',', '')
            unit_price = transaction["Unit price or exercise price"]
            try:
                total = '${0:+,.2f}'.format(float(units or 0) * float(unit_price or 0))
                total = total.rstrip('0').rstrip('.') if '.' in total else total
            except Exception as x:
                logger.debug("{} : {}".format(x.__class__.__name__, transactions_url))
                continue
            price_statement = ""
            if units and unit_price:
                price_statement += "{} ".format(total)

            if units:
                price_statement += "{} vol ".format(units)
            if unit_price:
                price_statement += "{} each".format(unit_price)

            new_transactions.append("{} - {}, {}: {}".format(transaction["Issuer Name"],
                                                    transaction["Insider Name"],
                                                    transaction["Nature of transaction"],
                                                    price_statement.strip()))

            try:
                cursor.execute("INSERT INTO transactions(`transaction_id`,"
                                                           "`symbol`,"
                                                           "`transaction_date`,"
                                                           "`insider_name`,"
                                                           "`issuer_name`,"
                                                           "`relationship_to_issuer`,"
                                                           "`nature_of_transaction`,"
                                                           "`price`,"
                                                           "`units`)"
                               " VALUES('{}','{}','{}','{}','{}','{}','{}','{}','{}')"
                               .format(
                                    transaction["Transaction ID"],
                                    symbol,
                                    transaction["Date of transaction"],
                                    transaction["Insider Name"],
                                    transaction["Issuer Name"],
                                    transaction["Insider's Relationship to Issuer"],
                                    transaction["Nature of transaction"],
                                    transaction["Unit price or exercise price"],
                                    transaction["Number or value acquired or disposed of"]
                                ))

                mydb.commit()
            except Exception as x:
                logger.error("{} : {}".format(x.__class__.__name__, transactions_url))
                continue
    return new_transactions
