# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        Twitter-trends crawler
# Author:      Alexander Tolmach
# Created:     09.04.2013
# Licence:     CC BY-SA
#-------------------------------------------------------------------------------
#!/usr/bin/env python2.7

import oauth2 as oauth

import time
import datetime

import MySQLdb
import json

from settings import *

def oauth_req(url, key, secret, http_method="GET", post_body='', http_headers=None):
    consumer = oauth.Consumer(key=TW_CONSUMER_KEY, secret=TW_CONSUMER_SECRET)
    token = oauth.Token(key=key, secret=secret)
    client = oauth.Client(consumer, token)

    resp, content = client.request(
        url,
        method=http_method,
        body=post_body,
        headers=http_headers,
    )
    return content

def get_statuses():
    db = MySQLdb.connect(DB_HOST,
    	DB_USER,
    	DB_PASS,
    	DB_NAME, 
    	charset = 'utf8')
    cursor = db.cursor()
    trendlist = [23424936, 1]
    for region in trendlist:
        errort = 10
        while True:
            try:
                api_answer = oauth_req('https://api.twitter.com/1.1/trends/place.json?id={0}'.format(region), TW_KEY, TW_SECRET)
                trends = json.loads(api_answer)
                if isinstance(trends, list) and 'trends' in trends[0]:
                    break
                else:
                    print 'trends error'
                    print trends
                    time.sleep(errort)
                    errort *= 2
                    continue
            except:
                print 'error'
                #print api_answer
                time.sleep(errort)
                errort *= 2
                continue

        timer = (datetime.datetime.strptime(trends[0]['as_of'], '%Y-%m-%dT%H:%M:%SZ') + datetime.timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')
        i = 0
        for trend in trends[0]['trends']:
            i +=1
            cursor.execute('INSERT INTO tw_trends(timestamp, position, tag, region) VALUES ("{0}", "{1}", "{2}", "{3}")'.format(
                timer,
                i,
                MySQLdb.escape_string(trend['name'].encode('utf-8', 'replace')),
                MySQLdb.escape_string(trends[0]['locations'][0]['name'])
            ))
    db.commit()
    db.close()

def inf_loop():
    while True:
        startt = time.clock()
        get_statuses()
        tdelta = 60*5 - (time.clock() - startt)
        if tdelta > 0:
            time.sleep(tdelta)
        else:
            time.sleep(60*5)

inf_loop()
