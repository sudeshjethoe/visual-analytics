#!/usr/bin/env python3
"""
    Dump tweets in elasticsearch
"""
import argparse
import configparser
import os
import sys
import IPython
import elasticsearch_dsl
from datetime import datetime
from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text, connections
from twarc import Twarc

config = configparser.ConfigParser()
config.read('/home/svjethoe/.twarc')
index = 'visualanalytics'

t = Twarc(config.get('main', 'consumer_key'),
        config.get('main', 'consumer_secret'),
        config.get('main', 'access_token'),
        config.get('main', 'access_token_secret'))


class Tweet(DocType):
    title = Text(analyzer='snowball', fields={'raw': Keyword()})
    body = Text(analyzer='snowball')
    tags = Keyword()
    published_from = Date()
    lines = Integer()

    class Meta:
        index = index
        id =

    def save(self, ** kwargs):
        self.lines = len(self.body.split())
        return super(Tweet, self).save(** kwargs)

    def is_published(self):
        return datetime.now() > self.published_from


def dump(tweet):
    ''' dump a tweet '''
    print(tweet['created_at'])
    print(tweet['id'])
    print(tweet['user']['location'])
    hashtags = [x['text'] for x in tweet['entities']['hashtags']]
    print(hashtags)
    print(tweet['entities']['urls'][0]['url'])
    print(tweet['full_text'] or tweet['extended_tweet']['full_text'])


def main():
    # for tweet in t.search("ferguson"):
    # print(tweet["full_text"])

    tweets = t.search("#bitcoin")
    i = next(tweets)
    dump(i)

    IPython.embed()
    sys.exit()


if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 fdm=indent nocompatible
