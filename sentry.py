import os
import sys
from time import sleep
from json import dumps

import requests
from pyquery import PyQuery as pq
from redis import from_url

TODAY_URL = 'http://www.capitol.state.tx.us/Reports/Report.aspx?ID=todayfiled'
FIRST_RUN = True
REDIS_CONN = from_url(os.environ['REDISTOGO_URL'])


def redis_new_bill_loader(bills):
    payload = []
    for bill in bills:
        payload.append(dumps({
            'bill': bill[0],
            'name': bill[1],
            'bill_text': bill[2],
            'bill_url': bill[3]
            }))

    REDIS_CONN.rpush('bills', *payload)


def get_bills(pqdoc):
    bills = set()
    tables = pqdoc.find('table')

    for table in tables:
        bills.add(get_bill_text(table))

    return bills


def get_bill_text(table):
    pyt = pq(table)
    return (
        pyt.find('td').eq(0).text(),
        pyt.find('td').eq(2).text(),
        pyt.find('td').eq(8).text(),
        pyt.find('a').eq(0).attr('href')
    )


def count_bills(page):
    return page.find('table').length


def make_request(url, initial_pull_status=False):
    while True:
        try:
            request = pq(url)
            break
        except requests.exceptions.ConnectionError:
            print('Could not connect, trying again in 10 seconds.')
            sleep(10)
            continue

    if initial_pull_status:
        REDIS_CONN.set('last_initial_pull', request)

    return request


def main():
    global FIRST_RUN

    try:
        if FIRST_RUN:
            print('Initialized. Grabbing last pull from Redis.')
            initial_pull = make_request(REDIS_CONN.get('last_initial_pull'))
            FIRST_RUN = False
        else:
            print('A new beginning.')
            initial_pull = make_request(TODAY_URL, initial_pull_status=True)

        while True:
            current_pull = make_request(TODAY_URL)

            initial_count = count_bills(initial_pull)
            current_count = count_bills(current_pull)

            if current_count == initial_count:
                sys.stdout.write('.')
                sys.stdout.flush()
                sleep(60)
            elif current_count < initial_count:
                print('Page reset!')
                break
            else:
                new_bills = get_bills(current_pull) - get_bills(initial_pull)
                redis_new_bill_loader(new_bills)
                break
        main()

    except KeyboardInterrupt:
        print('Stopping...')


if __name__ == '__main__':
    main()
