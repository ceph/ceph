import json
from os import listdir
from os.path import isfile, join
import psycopg2

DIR = '/opt/telemetry/raw'

files = [f for f in listdir(DIR) if isfile(join(DIR, f))]

f = open('/opt/telemetry/pg_pass.txt', 'r')
password = f.read().strip()
f.close()

conn = psycopg2.connect(
    host='localhost',
    database='telemetry',
    user='telemetry',
    password=password
)
cur = conn.cursor()

for fn in files:
    f = open('/opt/telemetry/raw/' + fn, 'r')
    report = f.read()
    f.close()
    j = json.loads(report)
    ts = j.get('report_timestamp')
    if not ts:
        continue
    cur.execute(
        'INSERT INTO report (cluster_id, report_stamp, report) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING',
        (j.get('report_id'),
         ts,
         report)
    )
conn.commit()
