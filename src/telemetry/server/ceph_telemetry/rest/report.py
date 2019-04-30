from flask import request, jsonify
from flask_restful import Resource
from elasticsearch import Elasticsearch
import datetime


class Report(Resource):
    def __init__(self, report=None):
        super(Report, self).__init__()
        self.report = report

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

    def _report_id(self):
        '''
        Make a unique Elasticsearch document ID.  Earlier versions
        of telemetry did not contain a report_timestamp, so use
        time-of-receipt if not present.
        '''

        if 'report_timestamp' in self.report:
            timestamp = self.report['report_timestamp']
        else:
            timestamp = datetime.datetime.utcnow().isoformat()

        return '.'.join((self.report['report_id'], timestamp))

    def _purge_hostname_from_crash(self):
        '''
        hostname can be FQDN and undesirable to make public.
        Remove from crashdump data (newer telemetry modules don't
        submit it at all).
        '''
        if 'crashes' in self.report:
            if not isinstance(self.report['crashes'], list):
                self._crashes_to_list()
            for crash in self.report['crashes']:
                if 'utsname_hostname' in crash:
                    del crash['utsname_hostname']

    def put(self):
        self.report = request.get_json(force=True)
        self._crashes_to_list()
        self._dots_to_percent()
        self._purge_hostname_from_crash()
        es_id = self._report_id()
        es = Elasticsearch()
        es.index(index='telemetry', doc_type='report', id=es_id,
                 body=self.report)
        return jsonify(status=True)
