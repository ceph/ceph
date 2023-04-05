from flask import Flask, request, jsonify
from system import System
from redfish_system import RedfishSystem
from reporter import Reporter
from util import logger
import time

log = logger(__name__)

# must be passed as arguments
host = "https://x.x.x.x:8443"
username = "myuser"
password = "mypassword"

# create the redfish system and the obsever
log.info(f"Server initialization...")
system = RedfishSystem(host, username, password, system_endpoint='/Systems/System.Embedded.1')
reporter_agent = Reporter(system, "http://127.0.0.1:8000")

app = Flask(__name__)

@app.route('/shutdown', methods=['POST'])
def shutdown():
    system.stop_update_loop()
    system.client.logout()
    return 'Server shutting down...\n'

@app.route('/system', methods=['GET'])
def get_system():
    return jsonify({'system': system.get_system()})

@app.route('/system/memory', methods=['GET'])
def get_system_memory():
    return jsonify({'memory': system.get_memory()})

@app.route('/system/network', methods=['GET'])
def get_system_network():
    return jsonify({'network': system.get_network()})

@app.route('/system/status', methods=['GET'])
def get_system_status():
    return jsonify({'status': system.get_status()})

@app.route('/system/actions/', methods=['POST'])
def post_system():
    pass

@app.route('/system/actions/', methods=['PUT'])
def put_system():
    pass

@app.route('/system/control/', methods=['DELETE'])
def delete():
    pass

if __name__ == '__main__':
    system.start_update_loop()
    reporter_agent.run()
    app.run(debug=True)
