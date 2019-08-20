import json
from server.ceph_telemetry.rest import Report

f = open('es_dump.txt', 'r')
a = f.read()

j = json.loads(a)
reports = j['hits']['hits']
print(len(reports))


