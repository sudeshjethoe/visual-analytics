#!/usr/bin/env python3
"""
    Dump tweets in elasticsearch
"""
import argparse
import configparser
import csv
import os
import sys
import time
from datetime import datetime
from time import mktime
import IPython
from elasticsearch import Elasticsearch
from twarc import Twarc


config = configparser.ConfigParser()
config.read('/home/svjethoe/.twarc')
index = 'visualanalytics'
es = Elasticsearch()

t = Twarc(config.get('main', 'consumer_key'),
        config.get('main', 'consumer_secret'),
        config.get('main', 'access_token'),
        config.get('main', 'access_token_secret'))


def readtokens(f='binance-subset.csv'):
    records = []
    with open(f, 'r') as csvfile:
        for row in csv.reader(csvfile):
            records.append("#%s" % row[1])

    return records


def create_index(index):
    settings = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        },
        'mappings': {
            'tweet': {
                'properties': {
                    'country': {'type': 'text'},
                    'timestamp': {
                        'type': 'date'},
                    'text': {'type': 'text'},
                    'url': {'type': 'text'}
                }
            }
        }
    }
    if not es.indices.exists(index):
        es.indices.create(index, settings)


def post(tweet):
    try:
        url = tweet['entities']['urls'][0]['url']
    except:
        url = None
    ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
    doc = {
        'url': url,
        'timestamp': ts,
        'type': 'tweet',
        'country': tweet['user']['location'],
        'tags': [x['text'].lower() for x in tweet['entities']['hashtags']],
        'text': tweet['full_text'] or tweet['extended_tweet']['full_text']}

    res = es.index(
        index=index,
        doc_type='tweet',
        id=tweet['id'],
        body=doc)

    return res


def main():
    tokens = readtokens()
    create_index(index)
    # tweets = t.search("#bitcoin")
    searchtokens = " OR ".join(tokens)
    since = "2018-02-11"
    until = "2018-02-12"
    # for tweet in t.search("#btc OR #eth since:2018-01-01 until:2018-01-28"):
    for tweet in t.search("%s since:%s until:%s" % (searchtokens, since, until)):
        post(tweet)
        # IPython.embed()
        # sys.exit()


if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 fdm=indent nocompatible
