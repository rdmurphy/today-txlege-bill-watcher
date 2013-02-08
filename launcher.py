import os
from time import sleep
from json import loads
from datetime import datetime

import boto
from pytz import timezone
from redis import from_url

REDIS_CONN = from_url(os.environ['REDISTOGO_URL'])

AWS_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
FROM_EMAIL = os.environ['FROM_EMAIL']
RECEIVING_EMAILS = [a.strip() for a in os.environ['RECEIVING_EMAILS'].split(',')]
PREFERRED_TZ = 'US/Central'


def prepare_email_body(bill_list):
    payload = ''
    for bill in bill_list:
        link = '<a href="{0}">{1}</a> by {2}<br>'.format(bill['bill_url'], bill['bill'], bill['name'])
        payload = payload + link + '{0}<br><br>'.format(bill['bill_text'])

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


def main():
    try:
        while True:
            if REDIS_CONN.llen('bills'):
                all_the_bills = [loads(x) for x in REDIS_CONN.lrange('bills', 0, -1)]
                try:
                    email_body = prepare_email_body(all_the_bills)
                    send_new_bill_email(email_body)
                except Exception, err:
                    print('Something fell down.')
                    print(err)
                    sleep(120)
                    continue
                REDIS_CONN.delete('bills')
                sleep(60 * 10)  # wait 10 minutes after successful send
            else:
                sleep(60)  # if there are no bills, chill for 60 seconds

    except KeyboardInterrupt:
        print('Stopping...')
