import sys
import os
from time import sleep
from datetime import datetime

import boto
import requests
from pyquery import PyQuery as pq
from pytz import timezone


TODAY_URL = 'http://www.capitol.state.tx.us/Reports/Report.aspx?ID=todayfiled'
AWS_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
FROM_EMAIL = os.environ['FROM_EMAIL']
RECEIVING_EMAILS = [a.strip() for a in os.environ['RECEIVING_EMAILS'].split(',')]
PREFERRED_TZ = 'US/Central'


def prepare_email_body(bill_list):
    payload = ''
    for bill in bill_list:
        link = '<a href="{0}">{1}</a> by {2}<br>'.format(bill[3], bill[0], bill[1])
        payload = payload + link + '{0}<br><br>'.format(bill[2])

    return payload


def send_new_bill_email(payload):
    d = datetime.now(timezone(PREFERRED_TZ))
    format = '%I:%M %p %m/%d/%y'
    conn = boto.connect_ses(
        AWS_ID,
        AWS_KEY
    )

    conn.send_email(
        FROM_EMAIL,
        'New bills have been filed! ({0})'.format(d.strftime(format)),
        payload,
        RECEIVING_EMAILS,
        format='html',
    )


def get_set_of_bills_on_page(pq_doc):
    bills = set()
    tables = pq_doc.find('table')

    for table in tables:
        pytable = pq(table)
        bill_name = pytable.find('td').eq(0).text()
        bill_author = pytable.find('td').eq(2).text()
        bill_caption = pytable.find('td').eq(8).text()
        bill_url = pytable.find('a').eq(0).attr('href')
        bills.add((bill_name, bill_author, bill_caption, bill_url))

    return bills


def count_tables(pq_doc):
    return pq_doc.find('table').length


def main():
    try:
        while True:
            try:
                inital_request = pq(url=TODAY_URL)
                initial_count = count_tables(inital_request)
                initial_bills = get_set_of_bills_on_page(inital_request)
                break
            except requests.exceptions.ConnectionError:
                print('\nCould not connect to pull initial table count, trying again in 10 seconds')
                sleep(10)
                continue

        while True:
            try:
                doc = pq(url=TODAY_URL)
                current_count = count_tables(doc)
                current_bills = get_set_of_bills_on_page(doc)
            except requests.exceptions.ConnectionError:
                print('\nError connecting, trying again in 60 seconds.')
                sleep(60)
                continue

            if current_count == initial_count:
                sys.stdout.write('.')
                sys.stdout.flush()
                sleep(60 * 5)
            else:
                new_bills = list(current_bills - initial_bills)
                print('\nNew things have been filed!')
                email_body = prepare_email_body(new_bills)
                send_new_bill_email(email_body)
                break
        main()

    except KeyboardInterrupt:
        print('\nStopping...')


if __name__ == '__main__':
    main()
