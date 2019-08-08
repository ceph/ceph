from flask import request, jsonify
from flask_restful import Resource
from elasticsearch import Elasticsearch
import datetime
import hashlib
import json
import copy
import psycopg2

class Report(Resource):
    def __init__(self, report=None):
        super(Report, self).__init__()
        self.report = report
        with open('/opt/telemetry/pg_pass.txt', 'r') as f:
            p = f.read()
            self.pg_password = p.strip()

    def _dots_to_percent(self, obj=None):
        '''
        Key names cannot contain '.' in Elasticsearch, so change
        them to '%', first escaping any existing '%' to '%%'.
        Don't worry about values.  Modifies keys in-place.
        '''

        # handle first call; others are recursive
        if obj is None:
            obj = self.report

        for k, v in obj.items():
            if isinstance(v, dict):
                self._dots_to_percent(v)
            if '.' in k:
                del obj[k]
                newk = k.replace('%', '%%')
                newk = newk.replace('.', '%')
                obj[newk] = v

    def _crashes_to_list(self):
        '''
        Early versions of telemetry sent crashes as a dict, keyed
        by crash_id.  This isn't particularly useful, so if we see it,
        change to the current convention of "a list of crash dicts",
        which contains the crash_id.  Modifies report in-place.
        '''

        if ('crashes' in self.report and isinstance(self.report['crashes'], dict)):
            newcrashes = list()
            for crashinfo in self.report['crashes'].values():
                newcrashes.append(crashinfo)
            self.report['crashes'] = newcrashes

    def _add_timestamp(self):
        if 'report_timestamp' not in self.report:
            self.report['report_timestamp'] = datetime.datetime.utcnow().isoformat()

    def _report_id(self):
        '''
        Make a unique Elasticsearch document ID.  Earlier versions
        of telemetry did not contain a report_timestamp, so use
        time-of-receipt if not present.
        '''
        return '.'.join((self.report['report_id'], self.report['report_timestamp']))

    def _purge_hostname_from_crash(self):
        '''
        hostname can be FQDN and undesirable to make public.
        Remove from crashdump data (newer telemetry modules don't
        submit it at all).
        '''
        for crash in self.report.get('crashes', []):
            if 'utsname_hostname' in crash:
                del crash['utsname_hostname']

    def _obfuscate_entity_name(self):
        """
        Early nautilus releases did not obfuscate the entity_name, which is
        often a (short) hostname.
        """
        for crash in self.report.get('crashes', []):
            if 'entity_name' in crash:
                ls = crash.get('entity_name').split('.')
                entity_type = ls[0]
                entity_id = '.'.join(ls[1:])
                if len(entity_id) != 40:
                    m = hashlib.sha1()
                    m.update(self.report.get('report_id').encode('utf-8'))
                    m.update(entity_id.encode('utf-8'));
                    m.update(self.report.get('report_id').encode('utf-8'))
                    crash['entity_name'] = entity_type + '.' + m.hexdigest()

    def put(self):
        self.report = request.get_json(force=True)

        # clean up
        self._add_timestamp()
        self._crashes_to_list()
        self._purge_hostname_from_crash()
        self._obfuscate_entity_name()

        self.post_to_file()
        self.post_to_postgres()
        self.post_to_es()

        return jsonify(status=True)

    def post_to_file(self):
        id = self._report_id()
        with open('/opt/telemetry/raw/%s' % id, 'w') as f:
            f.write(json.dumps(self.report, indent=4))
            f.close()
    
    def post_to_es(self):
        r = copy.deepcopy(self.report)
        self._dots_to_percent(r)
        es_id = self._report_id()
        es = Elasticsearch()
        es.index(index='telemetry', doc_type='report', id=es_id,
                 body=r)

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
        cur.execute(
            'INSERT INTO report (cluster_id, report_stamp, report) VALUES (%s,%s,%s)',
            (self.report.get('report_id'),
             self.report.get('report_timestamp'),
             json.dumps(self.report))
            )
        conn.commit()
