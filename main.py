#!/usr/bin/python3
"""
    Dump tweets in elasticsearch
"""
import ConfigParser
import argparse
import json
import os
import socket
import sys
import IPython

from prometheus_client import start_http_server

debug = False
verbose = False


def tr_env(e):
    """translate e to an environment or return e"""
    dc = {
        'DEV': 'd',
        'dev': 'd',
        'TST': 't',
        'tst': 't',
        'test': 't',
        'ACC': 'a',
        'acc': 'a',
        'PRD': 'p',
        'prod': 'p'
    }
    try:
        return dc[e]
    except KeyError:
        return e


def parse_release(payload):
    """Convert message.value to useful information
    payload format:
    '{"application":"gChannelsAppSV","version":"2.4.83","environment":"TST","result":"FAILED","user":"sd59bg","apptype":"TCS","repository":"artifactory","timestamp":"2016-09-12T12:47:44Z","previous-version":"2.4.83"}'
    """
    previous = payload['previous-version'] if 'previous-version' in payload else 'no_previous'
    message = "Upgrade on host %s from %s to %s was: %s" % (
        payload['host'],
        previous,
        payload['version'],
        payload['result']
    )
    payload.update({
        'tags': [
            payload['apptype'],
            "environment: %s" % payload['log_environment'],
        ],
        'message': message
    })
    return payload


def parse_deployment(payload):
    """Convert message.value to useful information
    payload format:
        {
        "application":"pCreditcardsChangeRepaymentAPI",
        "version":"00.01.00.008",
        "riafversion":"4.7.0",
        "environment":"acc.site3.green",
        "host":"lrv1534l",
        "testcriterium":"0.2 - Melding deployment per omgeving",
        "result":"OK",
        "user":"uc59io",
        "apptype":"TCS",
        "JVMInstanceNr":"tcserver94",
        "timestamp":"2016-09-12T13:13:57.523Z"
        }
    """
    message = "Upgrade on host %s to %s was: %s" % (
        payload['host'],
        payload['version'],
        payload['result'])

    jvm_inst = payload['JVMInstanceNr'] if 'JVMInstanceNr' in payload else 'noJVM'
    apptype = payload['apptype'] if 'apptype' in payload else 'no_type'
    payload.update({
        'tags': [
            apptype,
            jvm_inst,
            "environment: %s" % payload['log_environment'],
        ],
        'message': message
    })
    return payload


def read_arguments():
    """define and read commandline arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='config file', required=True)
    parser.add_argument('-d', '--debug', help='debug mode', action='store_true')
    parser.add_argument('-v', '--verbose', help='verbose logging', action='store_true')
    parser.add_argument('-t', '--topic', help='kafka topic name (nolio-releases, nolio-deployments)', required=True)
    args = parser.parse_args()

    # set global parameters
    debug = args.debug
    verbose = args.verbose

    if not os.path.isfile(args.config):
        print "%s is not a file!"
        sys.exit()
    return args


def main():
    args = read_arguments()

    config = ConfigParser.ConfigParser()
    config.read(args.config)

    # start prometheus metric exporter on specified port for this topic
    start_http_server(8001)

    for n, message in enumerate(consumer, start=1):
        payload = json.loads(message.value)
        try:
            payload['host'] = payload.pop('hostname').split('.')[0]
        except KeyError:
            payload['host'] = 'undefined'

        raw_environment = payload.pop('environment').split('.')[0]
        environment = tr_env(raw_environment)
        payload['@timestamp'] = payload.pop('timestamp')
        payload.update({
            '@version': '1',
            'log_component': 'sre_nolio_forwarder',
            'log_domain': 'cio',
            'log_environment': environment,
            'log_shipper': socket.gethostname(),
            'log_type': kafka_topic_friendly,
            'log_version': 'sre_nolio_forwarder-1.0'
        })

        # update payload with topic specific parser
        payload = parse_payload_with_topic[kafka_topic](payload)

        if verbose: print payload
        if debug:
            IPython.embed()
            sys.exit()

        # write to redis
        redis_conn.rpush(redis_key, json.dumps(payload))


if __name__ == "__main__":
    main()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 fdm=indent nocompatible
