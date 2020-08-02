#!/usr/bin/env python
import pika
import time
import json

from dotenv import find_dotenv, load_dotenv

import maxfield.maxfield.maxfield as maxfield

import os
import shutil
load_dotenv(find_dotenv())

credentials = pika.PlainCredentials(
    os.environ.get('RBQUser'), os.environ.get("RBQPass"))

connection = None

workerpath = os.environ.get('TMPPATH') + "/maxfield-worker"
resultpath = os.environ.get('TMPPATH') + "/maxfield-worker-results"


def check_dir():
    if not os.path.exists(os.environ.get('TMPPATH')):
        os.mkdir(os.environ.get('TMPPATH'))
    if not os.path.exists(workerpath):
        os.mkdir(workerpath)
    else:
        shutil.rmtree(workerpath)
        os.mkdir(workerpath)
    if not os.path.exists(resultpath):
        os.mkdir(resultpath)


def callback(ch, method, properties, body):
    ch.basic_publish(
        exchange='',
        body=json.dumps({
            "node": os.environ.get("NODEName") + ".processing",
            "status": True
        }), routing_key=properties.reply_to, properties=pika.BasicProperties(correlation_id=properties.correlation_id))
    print("[MaxFieldWorker] Received a new request.")
    req_body = json.loads(str(body, encoding="utf-8"))
    result = do_max_field(req_body)
    if result:
        shutil.move(workerpath,
                    resultpath + "/" + properties.correlation_id)
    with open(resultpath + '/' + properties.correlation_id + ".json", "w") as postjson:
        json.dump({
            "routing_key": properties.reply_to,
            "status": result,
            "correlation_id": properties.correlation_id
        }, postjson)
    print("[MaxFieldWorker] Job Completed.")


def do_max_field(reqbody):
    with open("/tmp/maxfield.tmp.txt", "w", encoding="utf8") as f:
        f.write(reqbody["portal"])
    try:
        google_api_key = None
        google_api_secret = None
        if(reqbody.get("googlemap", False)):
            google_api_key = os.environ.get("GoogleMapAPIKey")
            google_api_secret = os.environ.get('GoogleMapAPISecret')
        print("[MaxFieldWorker] Job Started.")
        maxfield.maxfield("/tmp/maxfield.tmp.txt",
                            int(reqbody["agents"]), google_api_key=google_api_key, google_api_secret=google_api_secret, res_colors=(reqbody["faction"] == "res"), num_cpus=int(os.environ.get("CORES")), output_csv=True, outdir=workerpath)
        json_object = json.dumps({
            "agents": int(reqbody["agents"])
        })
        with open(workerpath + "/info.json", "w") as outfile:
            outfile.write(json_object)
        time.sleep(4)
        return True
    except:
        return False


def start_loop():
    global connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get("RBQHost"), virtual_host=os.environ.get("RBQBase"), credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='maxfield-task', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue='maxfield-task', consumer_tag=os.environ.get("NODEName") + ":" + os.environ.get("CORES"), on_message_callback=callback, auto_ack=True)
    print('[MaxFieldWorker] Service is now up.')
    try:
        channel.start_consuming()
    except:
        start_loop()


if __name__ == "__main__":
    check_dir()
    start_loop()
