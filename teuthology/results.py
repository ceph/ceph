#!/usr/bin/env python

import sys
import os
import yaml
import json
import re
import httplib2
import logging

from teuthology.config import config


log = logging.getLogger(__name__)


class RequestFailedError(RuntimeError):
    def __init__(self, resp, content):
        self.status = resp.status
        self.reason = resp.reason
        self.content = content
        try:
            self.content_obj = json.loads(content)
            self.message = self.content_obj['message']
        except ValueError:
            #self.message = '<no message>'
            self.message = self.content

    def __str__(self):
        templ = "Request failed with status {status}: {reason}: {message}"

        return templ.format(
            status=self.status,
            reason=self.reason,
            message=self.message,
        )


class ResultsSerializer(object):
    yamls = ('orig.config.yaml', 'config.yaml', 'info.yaml', 'summary.yaml')

    def __init__(self, archive_base):
        self.archive_base = archive_base

    def json_for_job(self, run_name, job_id, pretty=False):
        job_archive_dir = os.path.join(self.archive_base,
                                       run_name,
                                       job_id)
        job_info = {}
        for yaml_name in self.yamls:
            yaml_path = os.path.join(job_archive_dir, yaml_name)
            if not os.path.exists(yaml_path):
                continue
            with file(yaml_path) as yaml_file:
                partial_info = yaml.safe_load(yaml_file)
                if partial_info is not None:
                    job_info.update(partial_info)

        if 'job_id' not in job_info:
            job_info['job_id'] = job_id

        if pretty:
            job_json = json.dumps(job_info, sort_keys=True, indent=4)
        else:
            job_json = json.dumps(job_info)

        return job_json

    def print_pretty_json(self, json_obj):
        log.info('\n'.join([l.rstrip() for l in json_obj.splitlines()]))

    def jobs_for_run(self, run_name):
        archive_dir = os.path.join(self.archive_base, run_name)
        if not os.path.isdir(archive_dir):
            return {}
        jobs = {}
        for item in os.listdir(archive_dir):
            if not re.match('\d+$', item):
                continue
            job_id = item
            job_dir = os.path.join(archive_dir, job_id)
            if os.path.isdir(job_dir):
                jobs[job_id] = job_dir
        return jobs

    @property
    def all_runs(self):
        archive_base = self.archive_base
        if not os.path.isdir(archive_base):
            return []
        runs = []
        for run_name in os.listdir(archive_base):
            if not os.path.isdir(os.path.join(archive_base, run_name)):
                continue
            runs.append(run_name)
        return runs


class ResultsPoster(object):
    def __init__(self, archive_base, base_uri=None):
        self.archive_base = archive_base
        self.base_uri = base_uri or config.results_server
        self.base_uri = self.base_uri.rstrip('/')
        self.serializer = ResultsSerializer(archive_base)

    def post_all_runs(self):
        all_runs = self.serializer.all_runs
        last_run = self.last_run
        if last_run and last_run in all_runs:
            next_index = all_runs.index(last_run) + 1
            runs = all_runs[next_index:]
        else:
            runs = all_runs
        num_runs = len(runs)
        num_jobs = 0
        log.info("Posting %s runs", num_runs)
        for run in runs:
            job_count = self.post_run(run)
            num_jobs += job_count
            self.last_run = run
        log.info("Total: %s jobs in %s runs", num_jobs, num_runs)

    def post_run(self, run_name):
        jobs = self.serializer.jobs_for_run(run_name)
        log.info("{name} {jobs} jobs".format(
            name=run_name,
            jobs=len(jobs),
        ))
        if jobs:
            h = httplib2.Http()
            run_json = json.dumps({'name': run_name})
            resp, content = h.request(
                "{base}/runs/".format(base=self.base_uri, name=run_name),
                'POST',
                run_json,
                headers={'content-type': 'application/json'},
            )
            if resp.status == 200:
                for job_id in jobs.keys():
                    self.post_job(run_name, job_id)
            elif resp.status != 200:
                message = json.loads(content).get('message', '')
                if message.endswith('already exists'):
                    log.info("    already present; skipped")
                else:
                    raise RequestFailedError(resp, content)
        return len(jobs)

    def post_job(self, run_name, job_id):
        job_json = self.serializer.json_for_job(run_name, job_id)
        h = httplib2.Http()
        resp, content = h.request(
            "{base}/runs/{name}/".format(base=self.base_uri, name=run_name,),
            'POST',
            job_json,
            headers={'content-type': 'application/json'},
        )
        try:
            message = json.loads(content).get('message', '')
        except ValueError:
            message = ''

        if message.endswith('already exists'):
            resp, content = h.request(
                "{base}/runs/{name}/".format(
                    base=self.base_uri,
                    name=run_name,),
                'PUT',
                job_json,
                headers={'content-type': 'application/json'},
            )
        if resp.status != 200:
            raise RequestFailedError(resp, content)
        return job_id

    @property
    def last_run(self):
        if hasattr(self, '__last_run'):
            return self.__last_run
        elif os.path.exists('last_successful_run'):
            with file('last_successful_run') as f:
                self.__last_run = f.read().strip()
            return self.__last_run

    @last_run.setter
    def last_run(self, run_name):
        self.__last_run = run_name
        with file('last_successful_run', 'w') as f:
            f.write(run_name)


def main(argv):
    archive_base = os.path.abspath(os.path.expanduser(argv[1]))
    poster = ResultsPoster(archive_base)
    poster.post_all_runs()


if __name__ == "__main__":
    main(sys.argv)
