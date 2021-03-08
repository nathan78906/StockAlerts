import os
from datetime import datetime
from requests_retry import requests_retry_session


def process_transactions(mydb, cursor, logger, symbol, transaction_list):
    new_transactions = []
    transactions_url = "{}api/sedi/filings?symbol={}".format(os.environ['BASE_URL'], symbol)
    try:
        transactions_response = requests_retry_session().get(transactions_url, timeout=10)
    except Exception as x:
        logger.error("{} : {}".format(repr(x), transactions_url))
        return

    if transactions_response.status_code != 200:
        logger.error("{} : {}".format(transactions_response.status_code, transactions_url))
        return

    transactions_response = transactions_response.json()[:10]
    for transaction in transactions_response:
        transaction_b = transaction["datab"]
        if str(transaction_b["Transaction ID"]) not in transaction_list:
            units = transaction_b["Number or value acquired or disposed of"].replace(',', '')
            unit_price = transaction_b["Unit price or exercise price"]
            try:
                total = '${0:+,.2f}'.format(float(units or 0) * float(unit_price or 0))
                total = total.rstrip('00').rstrip('.') if '.' in total else total
            except Exception as x:
                logger.debug("{} : {}".format(repr(x), transactions_url))
                continue
            price_statement = []
            if units and unit_price:
                price_statement.append({
                    "name": "Total",
                    "value": total,
                    "inline": True})
            if units:
                price_statement.append({
                    "name": "Volume",
                    "value": units,
                    "inline": True})
            if unit_price:
                price_statement.append({
                    "name": "Unit Price",
                    "value": unit_price,
                    "inline": True})

            created_at = datetime.utcfromtimestamp(transaction["timestamp"]/1000).\
                strftime("%Y-%m-%dT%H:%M:%SZ")
            embed = {
                "author": {
                    "name": transaction_b["Issuer Name"]
                },
                "title": transaction_b["Nature of transaction"],
                "description": "{}\n{}\nDate of Transcation: {}\n{}[See more transactions]({})".format(
                    transaction_b["Insider Name"],
                    transaction_b["Insider's Relationship to Issuer"],
                    transaction_b["Date of transaction"],
                    os.environ['BASE_URL'] + "api/sedi?symbol={}".format(symbol)),
                "fields": price_statement,
                "timestamp": created_at
            }
            new_transactions.append(embed)

            try:
                statement = "INSERT INTO transactions(`transaction_id`, `symbol`, `transaction_date`, `insider_name`, `issuer_name`, `relationship_to_issuer`, `nature_of_transaction`, `price`, `units`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = (transaction_b["Transaction ID"],
                          symbol,
                          transaction_b["Date of transaction"],
                          transaction_b["Insider Name"],
                          transaction_b["Issuer Name"],
                          transaction_b["Insider's Relationship to Issuer"],
                          transaction_b["Nature of transaction"],
                          transaction_b["Unit price or exercise price"],
                          transaction_b["Number or value acquired or disposed of"])
                cursor.execute(statement, values)
                mydb.commit()
            except Exception as x:
                logger.error("{} : {}".format(repr(x), transactions_url))
                continue
    return new_transactions
