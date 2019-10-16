import json
import psycopg2

f = open('/opt/telemetry/pg_pass.txt', 'r')
password = f.read().strip()
f.close()

conn = psycopg2.connect(
    host='localhost',
    database='telemetry',
    user='telemetry',
    password=password
)
rcur = conn.cursor()
rcur.execute("SELECT cluster_id, report FROM report")
for row in rcur.fetchall():
    cluster_id = row[0]
    report = json.loads(row[1])
    ts = report.get('report_timestamp')

    ccur = conn.cursor()

    # cluster
    ccur.execute("SELECT cluster_id,latest_report_stamp FROM cluster WHERE cluster_id=%s", (cluster_id,))
    crows = ccur.fetchall()
    update = True
    if crows:
        crow = crows[0]
        if str(crow[1]) > ts:
            print('cluster %s already has newer report %s' % (cluster_id, crow[1]))
            update = False

    if update:
        print('updating %s' % (cluster_id))
        num_pgs = 0
        for pool in report.get('pools', []):
            num_pgs += pool.get('pg_num', 0)
        ccur.execute(
            "INSERT INTO cluster (cluster_id, latest_report_stamp, num_mon, num_osd, num_pools, num_pgs, total_bytes, total_used_bytes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (cluster_id) DO UPDATE SET latest_report_stamp=%s, num_mon=%s, num_osd=%s, num_pools=%s, num_pgs=%s, total_bytes=%s, total_used_bytes=%s",
            (cluster_id,
             ts,
             report.get('mon', {}).get('count', 0),
             report.get('osd', {}).get('count', 0),
             report.get('usage', {}).get('pools', 0),
             num_pgs,
             report.get('usage', {}).get('total_bytes', 0),
             report.get('usage', {}).get('total_used_bytes', 0),
             ts,
             report.get('mon', {}).get('count', 0),
             report.get('osd', {}).get('count', 0),
             report.get('usage', {}).get('pools', 0),
             num_pgs,
             report.get('usage', {}).get('total_bytes', 0),
             report.get('usage', {}).get('total_used_bytes', 0)),)

        # cluster_version
        ccur.execute(
            "DELETE FROM cluster_version WHERE cluster_id=%s",
            (cluster_id,)
        )
        for (entity_type, info) in report.get('metadata', {}).items():
            for (version, num) in info.get('ceph_version', {}).items():
                ccur.execute(
                    "INSERT INTO cluster_version (cluster_id, entity_type, version, num_daemons) VALUES (%s, %s, %s, %s)",
                    (cluster_id,
                     entity_type,
                     version,
                     num,))

    # crash
    crashes = report.get('crashes', [])
    if isinstance(crashes, dict):
        tmp = []
        for c in crashes.valeus():
            tmp.append(c)
        crashes = tmp

    for crash in crashes:
        crash_id = crash.get('crash_id')
        if not crash_id:
            continue
        stack = str(crash.get('backtrace'))
        ccur.execute(
            "INSERT INTO crash (crash_id, cluster_id, raw_report, timestamp, entity_name, version, stack) values (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (crash_id,
             cluster_id,
             json.dumps(crash, indent=4),
             crash.get('timestamp'),
             crash.get('entity_name'),
             crash.get('ceph_version'),
             stack,
            ))

    conn.commit()

