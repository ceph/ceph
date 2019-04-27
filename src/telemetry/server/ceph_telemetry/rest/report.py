from flask import request, jsonify
from flask_restful import Resource
from elasticsearch import Elasticsearch


class Report(Resource):
    def put(self):
        report = request.get_json(force=True)
        es = Elasticsearch()
        es.index(index='telemetry', doc_type='report', id=report['report_id'],
                 body=report)
        return jsonify(status=True)
