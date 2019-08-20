from os import path
import json

f = open('es_dump.txt', 'r')
a = f.read()

j = json.loads(a)
reports = j['hits']['hits']
print(len(reports))

r = reports[0]
json.dumps(r)

total = len(reports)
bad = 0
exists = 0
for item in reports:
    r = item['_source']
    ts = r.get('report_timestamp')
    if not ts:
        bad += 1
        continue
    rid = r.get('report_id')
    assert rid
    fn = '/opt/telemetry/raw/' + rid + '.' + ts
    
    if path.exists(fn):
        exists += 1
    else:
        f = open(fn, 'w')
        f.write(json.dumps(r))
        f.close()

print('total %d, bad %d, exists %d' % (total, bad, exists))
