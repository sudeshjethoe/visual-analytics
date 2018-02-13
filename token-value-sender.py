#!/usr/bin/env python3
"""
    Dump coins in elasticsearch
"""
import argparse
import json
import hashlib
import time
import sys
import dateparser
import pytz
import IPython

from datetime import datetime
from binance.client import Client
from elasticsearch import Elasticsearch

index = 'visualanalytics'
es = Elasticsearch()

# create the Binance client, no need for api key
client = Client("", "")


def readsymbols(f='symbols'):
    with open(f, 'r') as fh:
        symbols = fh.read().split('\n')
    return symbols[:-1]


def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds

    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"

    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/

    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)


def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds

    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str

    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms


def get_historical_klines(symbol, interval, start_str, end_str=None):
    """Get Historical Klines from Binance

    See dateparse docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/

    If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"

    :param symbol: Name of symbol pair e.g BNBBTC
    :type symbol: str
    :param interval: Binance Kline interval
    :type interval: str
    :param start_str: Start date string in UTC format
    :type start_str: str
    :param end_str: optional - end date string in UTC format
    :type end_str: str

    :return: list of OHLCV values

    """
    # init our list
    output_data = []

    # setup the max limit
    limit = 500

    # convert interval to useful value in seconds
    timeframe = interval_to_milliseconds(interval)

    # convert our date strings to milliseconds
    start_ts = date_to_milliseconds(start_str)

    # if an end time was passed convert it
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)

    idx = 0
    # it can be difficult to know when a symbol was listed on Binance so allow start time to be before list date
    symbol_existed = False
    while True:
        # fetch the klines from start_ts up to max 500 entries or the end_ts if set
        temp_data = client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )

        # handle the case where our start date is before the symbol pair listed on Binance
        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            # append this loops data to our output data
            output_data += temp_data

            # update our start timestamp using the last value in the array and add the interval timeframe
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            # it wasn't listed yet, increment our start date
            start_ts += timeframe

        idx += 1
        # check if we received less than the required limit and exit the loop
        if len(temp_data) < limit:
            # exit the while loop
            break

        # sleep after every 3rd call to be kind to the API
        if idx % 3 == 0:
            time.sleep(1)

    return output_data


def get_klines(symbol='ETHBTC', start='24 hours ago', end='now'):
    interval = Client.KLINE_INTERVAL_30MINUTE
    klines = get_historical_klines(symbol, interval, start, end)
    return klines


def parse_kline(kline):
    keys = [
        'open_time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'close_time',
        'quote_asset_volume',
        'number_of_trades',
        'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume',
        'ignore']
    return dict(zip((keys), kline))


def post(symbol, kline):
    '''process a single record and send to elasticsearch
    kline:
    {'close': '0.00013915',
     'close_time': 1518434999999,
     'high': '0.00013997',
     'ignore': '0',
     'low': '0.00013801',
     'number_of_trades': 218,
     'open': '0.00013869',
     'open_time': 1518433200000,
     'quote_asset_volume': '4.30812629',
     'taker_buy_base_asset_volume': '20036.00000000',
     'taker_buy_quote_asset_volume': '2.78273283',
     'volume': '31037.00000000'}
    '''
    coin = symbol[:3].lower()
    value = (float(kline['close']) + float(kline['open'])) / 2
    utc_time = time.gmtime(kline['open_time'] / 1000)
    ts = time.strftime('%Y-%m-%dT%H:%M:%S', utc_time)
    doc = {
        'tags': [coin, symbol],
        'type': 'tokenvalue',
        'value': value,
        'timestamp': ts}

    hashstr = "{0}{1}{2}".format(coin, ts, value).encode('utf-8')
    res = es.index(
        index=index,
        doc_type='tweet',
        id=hashlib.md5(hashstr).hexdigest(),
        body=doc)

    return res


def process_symbol(symbol):
    klines = get_klines(symbol)
    mapped_klines = [parse_kline(k) for k in klines]
    [post(symbol, kline) for kline in mapped_klines]


def main():
    symbols = readsymbols()
    print(symbols[0])
    for symbol in symbols:
        process_symbol(symbol)


if __name__ == "__main__":
    main()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 fdm=indent nocompatible
