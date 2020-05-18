#!/usr/bin/env python
import pika
import time
import json

from dotenv import find_dotenv, load_dotenv

import os
import shutil
load_dotenv(find_dotenv())

credentials = pika.PlainCredentials(
    os.environ.get('RBQUser'), os.environ.get("RBQPass"))

connection = None

def start_loop():
    global connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get("RBQHost"), virtual_host=os.environ.get("RBQBase"), credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='maxfield-task', durable=True)
    path = "/tmp/maxfield-worker-results"
    for _dir in os.listdir(path):
        if _dir.endswith(".json"):
            print("[MaxFieldWorker] Process result " + _dir)
            with open(path + '/' + _dir) as loadf:
                loadjson = json.load(loadf)
                channel.basic_publish(
                    exchange='',
                    body=json.dumps({
                        "node": os.environ.get("NODEName"),
                        "status": loadjson["status"]
                    }), routing_key=loadjson["routing_key"], properties=pika.BasicProperties(correlation_id=loadjson["correlation_id"]))
            os.remove(path + '/' + _dir)
    time.sleep(30)

if __name__ == "__main__":
    while True:
        start_loop()
