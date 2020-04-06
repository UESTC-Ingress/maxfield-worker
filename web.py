from flask import Flask, send_file
import os.path

app = Flask(__name__)

@app.route('/')
def index_page():
    return 'It works!'

@app.route('/<taskid>/<filename>')
def get_task_files(taskid, filename):
    if os.path.isfile('/tmp/maxfield-worker-results/' + taskid + '/' + filename):
        return send_file('/tmp/maxfield-worker-results/' + taskid + '/' + filename)
    else:
        return '404 Not Found'

if __name__ == '__main__':
    app.run(port=1331,host="0.0.0.0")
