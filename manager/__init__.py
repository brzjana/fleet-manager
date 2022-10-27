from flask import Flask, jsonify, request, abort
import os
import json
import yaml
import logging

from pathlib import Path
from .manager import FleetManager
from .routeplanner import RoutePlanner

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(message)s')

# default location of config file. config-localhost.yaml is used if the environment variable isn’t set
config_file = Path(os.environ.get('CONFIG_FILE', 'config-localhost.yaml'))

# verify file exists before attempting to read and extend the configuration
if config_file.is_file():
    config = yaml.load(config_file.read_text(), Loader=yaml.FullLoader)
else:
    config = dict()
route_planner = config.get('route_planner', dict(host='localhost', port=8080))
kafka_host = config.get('event_streaming', '')

routePlanner = RoutePlanner(route_planner['host'], route_planner['port'])
fleetManager = FleetManager(routePlanner,
                            kafka_host)

app = Flask(__name__)


@app.before_first_request
def activate():
    for v in config['vehicles']:
        id = fleetManager.add_vehicle(v['speed'], v['city'])
        app.logger.info('Vehicle %s added at %s', id, v['city'])
    fleetManager.start()


@app.route('/cities')
def cities():
    return jsonify(routePlanner.get_cities())


@app.route('/job', methods=['POST'])
def job():
    rec = json.loads(request.data)
    app.logger.info(rec)
    try:
        job = fleetManager.set_job(
            rec['fromCity'], rec['toCity'])
        if job:
            return jsonify(job)
    except:
        app.logger.exception("no route found")
    return "No route found", 400


@app.route('/alive')
def alive():
    return "Yes"


@app.route('/ready')
def ready():
    if fleetManager.ready():
        return "Yes"
    else:
        abort(500)
