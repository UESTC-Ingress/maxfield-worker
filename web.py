from flask import Flask, send_file
from flask_cors import CORS, cross_origin
import os.path

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/')
def index_page():
    return {
        "msg": "It works!",
        "cores": os.environ.get("RBQPass")
    }


@app.route('/<taskid>/<filename>')
@cross_origin()
def get_task_files(taskid, filename):
    if os.path.isfile('/tmp/maxfield-worker-results/' + taskid + '/' + filename):
        return send_file('/tmp/maxfield-worker-results/' + taskid + '/' + filename)
    else:
        return '404 Not Found'


if __name__ == '__main__':
    app.run(port=1331, host="0.0.0.0")
