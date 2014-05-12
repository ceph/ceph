import os
import yaml
import json
import re
import requests
import logging
import socket
from datetime import datetime

import teuthology
from .config import config

report_exceptions = (requests.exceptions.RequestException, socket.error)


def init_logging():
    """
    Set up logging for the module

    :returns: a logger
    """
    # Don't need to see connection pool INFO messages
    logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(
        logging.WARNING)

    log = logging.getLogger(__name__)
    return log


def main(args):
    run = args['--run']
    job = args['--job']
    dead = args['--dead']
    refresh = dead or args['--refresh']
    server = args['--server']
    if server:
        config.results_server = server
    if args['--verbose']:
        teuthology.log.setLevel(logging.DEBUG)

    archive_base = os.path.abspath(os.path.expanduser(args['--archive'])) or \
        config.archive_base
    save = not args['--no-save']

    log = init_logging()
    reporter = ResultsReporter(archive_base, save=save, refresh=refresh,
                               log=log)
    if dead and not job:
        for run_name in run:
            try_mark_run_dead(run[0])
    elif dead and len(run) == 1 and job:
        reporter.report_jobs(run[0], job, dead=True)
    elif len(run) == 1 and job:
        reporter.report_jobs(run[0], job)
    elif run and len(run) > 1:
        reporter.report_runs(run)
    elif run:
        reporter.report_run(run[0])
    elif args['--all-runs']:
        reporter.report_all_runs()


class ResultsSerializer(object):
    """
    This class exists to poke around in the archive directory doing things like
    assembling lists of test runs, lists of their jobs, and merging sets of job
    YAML files together to form JSON objects.
    """
    yamls = ('orig.config.yaml', 'config.yaml', 'info.yaml', 'summary.yaml')

    def __init__(self, archive_base, log=None):
        self.archive_base = archive_base
        self.log = log or init_logging()

    def job_info(self, run_name, job_id, pretty=False):
        """
        Given a run name and job id, merge the job's YAML files together.

        :param run_name: The name of the run.
        :param job_id:   The job's id.
        :returns:        A dict.
        """
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

        log_path = os.path.join(job_archive_dir, 'teuthology.log')
        if os.path.exists(log_path):
            mtime = int(os.path.getmtime(log_path))
            mtime_dt = datetime.fromtimestamp(mtime)
            job_info['updated'] = str(mtime_dt)

        if 'job_id' not in job_info:
            job_info['job_id'] = job_id

        return job_info

    def json_for_job(self, run_name, job_id, pretty=False):
        """
        Given a run name and job id, merge the job's YAML files together to
        create a JSON object.

        :param run_name: The name of the run.
        :param job_id:   The job's id.
        :returns:        A JSON object.
        """
        job_info = self.job_info(run_name, job_id, pretty)
        if pretty:
            job_json = json.dumps(job_info, sort_keys=True, indent=4)
        else:
            job_json = json.dumps(job_info)

        return job_json

    def jobs_for_run(self, run_name):
        """
        Given a run name, look on the filesystem for directories containing job
        information, and return a dict mapping job IDs to job directories.

        :param run_name: The name of the run.
        :returns:        A dict like: {'1': '/path/to/1', '2': 'path/to/2'}
        """
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

    def running_jobs_for_run(self, run_name):
        """
        Like jobs_for_run(), but only returns jobs with no summary.yaml

        :param run_name: The name of the run.
        :returns:        A dict like: {'1': '/path/to/1', '2': 'path/to/2'}
        """
        jobs = self.jobs_for_run(run_name)
        for job_id in jobs.keys():
            if os.path.exists(os.path.join(jobs[job_id], 'summary.yaml')):
                jobs.pop(job_id)
        return jobs

    @property
    def all_runs(self):
        """
        Look in the base archive directory for all test runs. Return a list of
        their names.
        """
        archive_base = self.archive_base
        if not os.path.isdir(archive_base):
            return []
        runs = []
        for run_name in os.listdir(archive_base):
            if not os.path.isdir(os.path.join(archive_base, run_name)):
                continue
            runs.append(run_name)
        return runs


class ResultsReporter(object):
    last_run_file = 'last_successful_run'

    def __init__(self, archive_base=None, base_uri=None, save=False,
                 refresh=False, log=None):
        self.log = log or init_logging()
        self.archive_base = archive_base or config.archive_base
        self.base_uri = base_uri or config.results_server
        if self.base_uri:
            self.base_uri = self.base_uri.rstrip('/')

        self.serializer = ResultsSerializer(archive_base, log=self.log)
        self.save_last_run = save
        self.refresh = refresh
        self.session = self._make_session()

        if not self.base_uri:
            msg = "No results_server set in {yaml}; cannot report results"
            self.log.warn(msg.format(yaml=config.teuthology_yaml))

    def _make_session(self, max_retries=10):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
        session.mount('http://', adapter)
        return session

    def report_all_runs(self):
        """
        Report *all* runs in self.archive_dir to the results server.
        """
        all_runs = self.serializer.all_runs
        last_run = self.last_run
        if self.save_last_run and last_run and last_run in all_runs:
            next_index = all_runs.index(last_run) + 1
            runs = all_runs[next_index:]
        else:
            runs = all_runs
        return self.report_runs(runs)

    def report_runs(self, run_names):
        """
        Report several runs to the results server.

        :param run_names: The names of the runs.
        """
        num_runs = len(run_names)
        num_jobs = 0
        self.log.info("Posting %s runs", num_runs)
        for run in run_names:
            job_count = self.report_run(run)
            num_jobs += job_count
            if self.save_last_run:
                self.last_run = run
        del self.last_run
        self.log.info("Total: %s jobs in %s runs", num_jobs, len(run_names))

    def report_run(self, run_name, dead=False):
        """
        Report a single run to the results server.

        :param run_name: The name of the run.
        :returns:        The number of jobs reported.
        """
        jobs = self.serializer.jobs_for_run(run_name)
        self.log.info("{name} {jobs} jobs dead={dead}".format(
            name=run_name,
            jobs=len(jobs),
            dead=str(dead),
        ))
        if jobs:
            if not self.refresh:
                response = self.session.head("{base}/runs/{name}/".format(
                    base=self.base_uri, name=run_name))
                if response.status_code == 200:
                    self.log.info("    already present; skipped")
                    return 0
            self.report_jobs(run_name, jobs.keys(), dead=dead)
        elif not jobs:
            self.log.debug("    no jobs; skipped")
        return len(jobs)

    def report_jobs(self, run_name, job_ids, dead=False):
        """
        Report several jobs to the results server.

        :param run_name: The name of the run.
        :param job_ids:  The jobs' ids
        """
        for job_id in job_ids:
            self.report_job(run_name, job_id, dead=dead)

    def report_job(self, run_name, job_id, job_info=None, dead=False):
        """
        Report a single job to the results server.

        :param run_name: The name of the run. The run must already exist.
        :param job_id:   The job's id
        :param job_info: The job's info dict. Optional - if not present, we
                         look at the archive.
        """
        if job_info is not None and not isinstance(job_info, dict):
            raise TypeError("job_info must be a dict")
        run_uri = "{base}/runs/{name}/jobs/".format(
            base=self.base_uri, name=run_name,)
        if job_info is None:
            job_info = self.serializer.job_info(run_name, job_id)
        if dead and job_info.get('success') is None:
            job_info['status'] = 'dead'
        job_json = json.dumps(job_info)
        headers = {'content-type': 'application/json'}
        response = self.session.post(run_uri, data=job_json, headers=headers)

        if response.status_code == 200:
            return job_id

        # This call is wrapped in a try/except because of:
        #  http://tracker.ceph.com/issues/8166
        try:
            resp_json = response.json()
        except ValueError:
            resp_json = dict()

        if resp_json:
            msg = resp_json.get('message', '')
        else:
            msg = response.text

        if msg and msg.endswith('already exists'):
            job_uri = os.path.join(run_uri, job_id, '')
            response = self.session.put(job_uri, data=job_json,
                                        headers=headers)
        elif msg:
            self.log.error(
                "POST to {uri} failed with status {status}: {msg}".format(
                    uri=run_uri,
                    status=response.status_code,
                    msg=msg,
                ))
        response.raise_for_status()

        return job_id

    @property
    def last_run(self):
        """
        The last run to be successfully reported.
        """
        if hasattr(self, '__last_run'):
            return self.__last_run
        elif os.path.exists(self.last_run_file):
            with file(self.last_run_file) as f:
                self.__last_run = f.read().strip()
            return self.__last_run

    @last_run.setter
    def last_run(self, run_name):
        self.__last_run = run_name
        with file(self.last_run_file, 'w') as f:
            f.write(run_name)

    @last_run.deleter
    def last_run(self):
        self.__last_run = None
        if os.path.exists(self.last_run_file):
            os.remove(self.last_run_file)

    def get_jobs(self, run_name, fields=None):
        """
        Query the results server for jobs in a run

        :param run_name: The name of the run
        :param fields:   Optional. A list of fields to include in the result.
                         Defaults to returning all fields.
        """
        uri = "{base}/runs/{name}/jobs/".format(base=self.base_uri,
                                                name=run_name)
        if fields:
            if not 'job_id' in fields:
                fields.append('job_id')
            uri += "?fields=" + ','.join(fields)
        response = self.session.get(uri)
        response.raise_for_status()
        return response.json()

    def delete_job(self, run_name, job_id):
        """
        Delete a job from the results server.

        :param run_name: The name of the run
        :param job_id:   The job's id
        """
        uri = "{base}/runs/{name}/jobs/{job_id}/".format(
            base=self.base_uri, name=run_name, job_id=job_id)
        response = self.session.delete(uri)
        response.raise_for_status()

    def delete_jobs(self, run_name, job_ids):
        """
        Delete multiple jobs from the results server.

        :param run_name: The name of the run
        :param job_ids:  A list of job ids
        """
        for job_id in job_ids:
            self.delete_job(self, run_name, job_id)

    def delete_run(self, run_name):
        """
        Delete a run from the results server.

        :param run_name: The name of the run
        """
        uri = "{base}/runs/{name}/".format(
            base=self.base_uri, name=run_name)
        response = self.session.delete(uri)
        response.raise_for_status()


def push_job_info(run_name, job_id, job_info, base_uri=None):
    """
    Push a job's info (example: ctx.config) to the results server.

    :param run_name: The name of the run.
    :param job_id:   The job's id
    :param job_info: A dict containing the job's information.
    :param base_uri: The endpoint of the results server. If you leave it out
                     ResultsReporter will ask teuthology.config.
    """
    reporter = ResultsReporter()
    if not reporter.base_uri:
        return
    reporter.report_job(run_name, job_id, job_info)


def try_push_job_info(job_config, extra_info=None):
    """
    Wrap push_job_info, gracefully doing nothing if:
        Anything inheriting from requests.exceptions.RequestException is raised
        A socket.error is raised
        config.results_server is not set
        config['job_id'] is not present or is None

    :param job_config: The ctx.config object to push
    :param extra_info: Optional second dict to push
    """
    log = init_logging()

    if job_config.get('job_id') is None:
        log.warning('No job_id found; not reporting results')
        return

    run_name = job_config['name']
    job_id = job_config['job_id']

    if extra_info is not None:
        job_info = extra_info.copy()
        job_info.update(job_config)
    else:
        job_info = job_config

    try:
        log.debug("Pushing job info to %s", config.results_server)
        push_job_info(run_name, job_id, job_info)
        return
    except report_exceptions:
        log.exception("Could not report results to %s",
                      config.results_server)


def try_delete_jobs(run_name, job_ids, delete_empty_run=True):
    """
    Using the same error checking and retry mechanism as try_push_job_info(),
    delete one or more jobs

    :param run_name:         The name of the run.
    :param job_ids:          Either a single job_id, or a list of job_ids
    :param delete_empty_run: If this would empty the run, delete it.
    """
    log = init_logging()

    if isinstance(job_ids, int):
        job_ids = [str(job_ids)]
    elif isinstance(job_ids, basestring):
        job_ids = [job_ids]

    reporter = ResultsReporter()
    if not reporter.base_uri:
        return

    log.debug("Deleting jobs from {server}: {jobs}".format(
        server=config.results_server, jobs=str(job_ids)))

    if delete_empty_run:
        got_jobs = reporter.get_jobs(run_name, fields=['job_id'])
        got_job_ids = [j['job_id'] for j in got_jobs]
        if sorted(got_job_ids) == sorted(job_ids):
            try:
                reporter.delete_run(run_name)
                return
            except report_exceptions:
                log.exception("Run deletion failed")

    def try_delete_job(job_id):
            try:
                reporter.delete_job(run_name, job_id)
                return
            except report_exceptions:
                log.exception("Job deletion failed")

    for job_id in job_ids:
        try_delete_job(job_id)


def try_mark_run_dead(run_name):
    """
    Using the same error checking and retry mechanism as try_push_job_info(),
    mark any unfinished runs as dead.

    :param run_name:         The name of the run.
    """
    log = init_logging()
    reporter = ResultsReporter()
    if not reporter.base_uri:
        return

    log.debug("Marking run as dead: {name}".format(name=run_name))
    jobs = reporter.get_jobs(run_name, fields=['status'])
    for job in jobs:
        if job['status'] not in ['pass', 'fail', 'dead']:
            job_id = job['job_id']
            try:
                log.info("Marking job {job_id} as dead".format(job_id=job_id))
                reporter.report_job(run_name, job['job_id'], dead=True)
            except report_exceptions:
                log.exception("Could not mark job as dead: {job_id}".format(
                    job_id=job_id))
