#!/usr/bin/env python
import pika
import time
import json

from dotenv import find_dotenv, load_dotenv

import maxfield.maxfield.maxfield as maxfield

import os
load_dotenv(find_dotenv())

credentials = pika.PlainCredentials(
    os.environ.get('RBQUser'), os.environ.get("RBQPass"))

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=os.environ.get("RBQHost"), virtual_host=os.environ.get("RBQBase"), credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='maxfield-task', durable=True)
channel.basic_qos(prefetch_count=1)


def callback(ch, method, properties, body):
    print("[MaxFieldWorker] Received a new request.")
    do_max_field(json.loads(str(body, encoding="utf-8")))
    time.sleep(3)
    print("[MaxFieldWorker] Job Completed.")


def do_max_field(reqbody):
    with open("/tmp/maxfield.tmp.txt", "w", encoding="utf8") as f:
        f.write(reqbody["portal"])
    maxfield.maxfield("/tmp/maxfield.tmp.txt",
                      reqbody["agents"], outdir="/tmp/maxfield-worker")


channel.basic_consume(
    queue='maxfield-task', on_message_callback=callback, auto_ack=True)

print('[MaxFieldWorker] Service is now up.')
channel.start_consuming()
