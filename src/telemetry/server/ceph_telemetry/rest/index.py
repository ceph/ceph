from flask import jsonify
from flask_restful import Resource


class Index(Resource):
    def get(self):
        return jsonify(status=True)
