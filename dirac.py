#!/usr/bin/env python

import re
import os
import sys
import json
import argparse
import core_pb2 as schema

from pprint import pformat
from google.protobuf import json_format
from kafka import KafkaProducer
from swiftclient.service import SwiftService, SwiftUploadObject
from swiftclient.multithreading import OutputManager
from swiftclient.client import Connection

# example usage

# python dirac.py --source /Users/spanglry/Data/gdc/expression --container gdc --path expression/ --kafka 10.96.11.82:9092 --topic gdc-import

def message_to_json(message):
    json = json_format.MessageToJson(message)
    return re.sub(r' +', ' ', json.replace('\n', ''))

def get_auth():
    conn = Connection(
        authurl=os.environ['OS_AUTH_URL'],
        user=os.environ['OS_USERNAME'],
        key=os.environ['OS_PASSWORD'],
        tenant_name=os.environ['OS_TENANT_NAME'],
        auth_version=2.0)
    return conn.get_auth()

class DiracStore(object):
    def __init__(self, container, topic, kafka):
        self.container = container
        self.swift = SwiftService()
        self.out = OutputManager()
        self.kafka = kafka

        auth = get_auth()
        self.url = auth[0]
        self.token = auth[1]
        
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=kafka)

    def send(self, resource):
        json = message_to_json(resource)
        print("sending " + json)
        self.producer.send(self.topic, json.encode('utf-8'))

    def make_resource(self, stat):
        resource = schema.Resource()
        resource.type = 'file'
        resource.name = stat['Object']
        resource.location = self.url + '/' + self.container + '/' + stat['Object']
        resource.mimeType = stat['Content Type']
        resource.size = long(stat['Content Length'])
        resource.created = stat['Last Modified']

        return resource

    def stat(self, paths):
        stat = {}
        for response in self.swift.stat(container=self.container, objects=paths):
            if response['success']:
                stat[response['object']] = {item[0]: item[1] for item in response['items']}

        return stat

    def store(self, path, source):
        isdir = os.path.isdir(source)
        base = source if isdir else os.path.dirname(source)
        sources = os.listdir(source) if isdir else [source]
        locations = [os.path.join(path, os.path.basename(file)) for file in sources]
        print(str(len(locations)) + " locations!")
        stats = self.stat(locations)
        objs = [SwiftUploadObject(os.path.join(base, os.path.basename(location)), object_name=location) for location in locations if not location in stats]
        print(str(len(objs)) + " previously unseen!")

        for response in self.swift.upload(self.container, objs):
            if response['success']:
                if 'object' in response:
                    print('uploading ' + response['object'])
                    stat = self.stat([response['object']])
                    resource = self.make_resource(stat.values()[0])
                    self.send(resource)

def parse_args(args):
    args = args[1:]
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--source', type=str, help='path to files to upload')
    parser.add_argument('--container', type=str, default='gdc', help='name of swift container')
    parser.add_argument('--path', type=str, help='path in swift to store files')
    parser.add_argument('--kafka', type=str, default='localhost:9092', help='kafka server address')
    parser.add_argument('--topic', type=str, help='kafka topic to send messages to')

    return parser.parse_args(args)

def store(source, container, path, kafka, topic):
    dirac = DiracStore(container, topic, kafka)
    dirac.store(path, source)

if __name__ == '__main__':
    # ccc kafka at 10.96.11.82
    options = parse_args(sys.argv)
    store(options.source, options.container, options.path, options.kafka, options.topic)