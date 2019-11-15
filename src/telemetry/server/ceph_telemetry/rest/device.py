from flask import request, jsonify
from flask_restful import Resource
import datetime
import hashlib
import json
import copy
import psycopg2

class Device(Resource):
    def __init__(self, report=None):
        super(Device, self).__init__()
        self.report = report
        with open('/opt/telemetry/pg_pass.txt', 'r') as f:
            p = f.read()
            self.pg_password = p.strip()

    def put(self):
        self.report = request.get_json(force=True)

        self.post_to_postgres()

        return jsonify(status=True)

    def _connect_pg(self):
        return psycopg2.connect(
            host='localhost',
            database='telemetry',
            user='telemetry',
            password=self.pg_password,
            )

    def post_to_postgres(self):
        conn = self._connect_pg()
        cur = conn.cursor()
        for devid, devinfo in self.report:
            for stamp, report in devinfo:
                cur.execute(
                    'INSERT INTO device_report (device_id, report_stamp, report) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING',
                    (devid, stamp, report))
        conn.commit()
