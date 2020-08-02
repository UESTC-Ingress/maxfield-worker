#!/usr/bin/env python
import pika
import time
import json
import boto3

from dotenv import find_dotenv, load_dotenv

import os
import shutil
load_dotenv(find_dotenv())

credentials = pika.PlainCredentials(
    os.environ.get('RBQUser'), os.environ.get("RBQPass"))

connection = None
channel = None

SAVEPATH = os.environ.get('TMPPATH') + '/maxfield-worker-results'

s3_client = boto3.client('s3', region_name="nl-ams", endpoint_url="https://s3.nl-ams.scw.cloud",
                         aws_access_key_id=os.environ.get('S3ACCESSKEY'), aws_secret_access_key=os.environ.get('S3SECRETKEY'))


def upload_dir(path, id):
    for root, _, files in os.walk(path):
        for file in files:
            s3_client.upload_file(os.path.join(
                root, file), os.environ.get("S3BUCKET"),
                id + "-" + file, ExtraArgs={'ACL':'public-read'})


def init_ch():
    global connection
    global channel
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get("RBQHost"), virtual_host=os.environ.get("RBQBase"), credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='maxfield-task', durable=True)


def start_loop():
    global channel
    global connection
    init_ch()
    for _dir in os.listdir(SAVEPATH):
        if _dir.endswith(".json"):
            print("[MaxFieldWorker] Process result " + _dir)
            with open(SAVEPATH + '/' + _dir) as loadf:
                loadjson = json.load(loadf)
                channel.basic_publish(
                    exchange='',
                    body=json.dumps({
                        "node": os.environ.get("NODEName"),
                        "status": loadjson["status"]
                    }), routing_key=loadjson["routing_key"], properties=pika.BasicProperties(correlation_id=loadjson["correlation_id"]))
                upload_dir(SAVEPATH + '/' + loadjson["correlation_id"], loadjson["correlation_id"])
            os.remove(SAVEPATH + '/' + _dir)
    connection.close()
    time.sleep(30)


if __name__ == "__main__":
    while True:
        start_loop()
