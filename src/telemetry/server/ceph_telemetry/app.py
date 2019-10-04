#!/usr/bin/env python3
import argparse
from flask_restful import Api
from flask import Flask
from ceph_telemetry.rest import Index, Report, Device


def create_app(name):
    app = Flask(name)
    api = Api(app, catch_all_404s=True)
    api.add_resource(Index, '/')
    api.add_resource(Report, '/report')
    api.add_resource(Device, '/device')
    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ceph Telemetry REST API')
    parser.add_argument("--host", action="store", dest="host",
                        default="::", help="Host/IP to bind on")
    parser.add_argument("--port", action="store", dest="port", type=int,
                        default=9000, help="Port to listen on")
    args = parser.parse_args()
    app = create_app(__name__)
    app.run(debug=True, host=args.host, port=args.port)
