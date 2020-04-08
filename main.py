#!/usr/bin/env python
import pika
import time
import json

from dotenv import find_dotenv, load_dotenv
from threading import Timer

import maxfield.maxfield.maxfield as maxfield

import os
import shutil
load_dotenv(find_dotenv())

credentials = pika.PlainCredentials(
    os.environ.get('RBQUser'), os.environ.get("RBQPass"))

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=os.environ.get("RBQHost"), virtual_host=os.environ.get("RBQBase"), credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='maxfield-task', durable=True)
channel.basic_qos(prefetch_count=1)


def delete_old_dir():
    path = "/tmp/maxfield-worker-results"
    now = time.time()
    old = now - 86400
    for _dir in os.listdir(path):
        if os.path.getmtime(path + '/' + _dir) < old:
            print("[MaxFieldWorker] Deleted expired ID "+_dir)
            os.remove(path + '/' + _dir)


def check_dir():
    if not os.path.exists("/tmp/maxfield-worker"):
        os.mkdir("/tmp/maxfield-worker")
    if not os.path.exists("/tmp/maxfield-worker-results"):
        os.mkdir("/tmp/maxfield-worker-results")


def callback(ch, method, properties, body):
    check_dir()
    print("[MaxFieldWorker] Received a new request.")
    result = do_max_field(json.loads(str(body, encoding="utf-8")))
    if result:
        shutil.move("/tmp/maxfield-worker",
                    "/tmp/maxfield-worker-results/" + properties.correlation_id)
    channel.basic_publish(
        exchange='',
        body=json.dumps({
            "node": os.environ.get("NODEName"),
            "status": result
        }), routing_key=properties.reply_to, properties=pika.BasicProperties(correlation_id=properties.correlation_id))
    print("[MaxFieldWorker] Job Completed.")
    delete_old_dir()


def do_max_field(reqbody):
    with open("/tmp/maxfield.tmp.txt", "w", encoding="utf8") as f:
        f.write(reqbody["portal"])
    try:
        google_api_key = None
        if(reqbody["googlemap"]):
            google_api_key = os.environ.get("GoogleMapAPIKey")
            google_api_secret = os.environ.get('GoogleMapAPISecret')
        maxfield.maxfield("/tmp/maxfield.tmp.txt",
                          int(reqbody["agents"]), google_api_key=google_api_key, google_api_secret=google_api_secret, res_colors=(reqbody["faction"] == "res"), num_cpus=0, output_csv=True, outdir="/tmp/maxfield-worker")
        json_object = json.dumps({
            "agents": int(reqbody["agents"])
        })
        with open("/tmp/maxfield-worker/info.json", "w") as outfile:
            outfile.write(json_object)
        return True
    except:
        return False


if __name__ == "__main__":
    channel.basic_consume(
            queue='maxfield-task', consumer_tag=os.environ.get("NODEName")+":"+os.environ.get("NODEURL"), on_message_callback=callback, auto_ack=True)

    print('[MaxFieldWorker] Service is now up.')
    channel.start_consuming()
