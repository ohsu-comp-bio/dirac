#!/usr/bin/env python

import re
import os
import json
import core_pb2 as schema

from pprint import pformat
from google.protobuf import json_format
from kafka import KafkaProducer
from swiftclient.service import SwiftService, SwiftUploadObject
from swiftclient.multithreading import OutputManager
from swiftclient.client import Connection

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
    def __init__(self, container, topic):
        self.container = container
        self.swift = SwiftService()
        self.out = OutputManager()

        auth = get_auth()
        self.url = auth[0]
        self.token = auth[1]
        
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def send(self, resource):
        json = message_to_json(resource)
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

    def stat(self, path):
        stat = {}
        for response in self.swift.stat(container=self.container, objects=[path]):
            if response['success']:
                stat = {item[0]: item[1] for item in response['items']}
            else:
                print("stat failed: " + pformat(stat))

        return stat

    def store(self, base, path):
        paths = os.listdir(path) if os.path.isdir(path) else [path]
        objs = [SwiftUploadObject(file, object_name=base + os.path.basename(file)) for file in paths]

        for response in self.swift.upload(self.container, objs):
            print(pformat(response))
            if response['success']:
                if 'object' in response:
                    stat = self.stat(response['object'])
                    resource = self.make_resource(stat)
                    self.send(resource)