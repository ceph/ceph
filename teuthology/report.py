#!/usr/bin/env python

import os
import yaml
import json
import re
import httplib2
import urllib
import logging
import argparse
from textwrap import dedent

from teuthology.config import config


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class RequestFailedError(RuntimeError):
    def __init__(self, uri, resp, content):
        self.uri = uri
        self.status = resp.status
        self.reason = resp.reason
        self.content = content
        try:
            self.content_obj = json.loads(content)
            self.message = self.content_obj['message']
        except ValueError:
            self.message = self.content

    def __str__(self):
        templ = "Request to {uri} failed with status {status}: {reason}: {message}"  # noqa

        return templ.format(
            uri=self.uri,
            status=self.status,
            reason=self.reason,
            message=self.message,
        )


class ResultsSerializer(object):
    """
    This class exists to poke around in the archive directory doing things like
    assembling lists of test runs, lists of their jobs, and merging sets of job
    YAML files together to form JSON objects.
    """
    yamls = ('orig.config.yaml', 'config.yaml', 'info.yaml', 'summary.yaml')

    def __init__(self, archive_base):
        self.archive_base = archive_base

    def json_for_job(self, run_name, job_id, pretty=False):
        """
        Given a run name and job id, merge the job's YAML files together to
        create a JSON object.

        :param run_name: The name of the run.
        :param job_id:   The job's id.
        :returns:        A JSON object.
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

        if 'job_id' not in job_info:
            job_info['job_id'] = job_id

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

    def __init__(self, archive_base, base_uri=None, save=False, refresh=False):
        self.archive_base = archive_base
        self.base_uri = base_uri or config.results_server
        if self.base_uri:
            self.base_uri = self.base_uri.rstrip('/')
        self.serializer = ResultsSerializer(archive_base)
        self.save_last_run = save
        self.refresh = refresh

    def _do_request(self, uri, method, json_):
        """
        Perform an actual HTTP request on a given URI. If the request was not
        successful and the reason was *not* that the object already exists,
        raise a RequestFailedError.
        """
        # Use urllib.quote() to escape things like spaces. Pass safe=':/' to
        # avoid it mangling http:// etc.
        uri = urllib.quote(uri, safe=':/')
        response, content = self.http.request(
            uri, method, json_, headers={'content-type': 'application/json'},
        )

        try:
            content_obj = json.loads(content)
        except ValueError:
            content_obj = {}

        message = content_obj.get('message', '')

        if response.status != 200 and not message.endswith('already exists'):
            raise RequestFailedError(uri, response, content)

        return response.status, message, content

    def post_json(self, uri, json_):
        """
        call self._do_request(uri, 'POST', json_)
        """
        return self._do_request(uri, 'POST', json_)

    def put_json(self, uri, json_):
        """
        call self._do_request(uri, 'PUT', json_)
        """
        return self._do_request(uri, 'PUT', json_)

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
        num_runs = len(runs)
        num_jobs = 0
        log.info("Posting %s runs", num_runs)
        for run in runs:
            job_count = self.report_run(run)
            num_jobs += job_count
            if self.save_last_run:
                self.last_run = run
        del self.last_run
        log.info("Total: %s jobs in %s runs", num_jobs, num_runs)

    def report_runs(self, run_names):
        """
        Report several runs to the results server.

        :param run_names: The names of the runs.
        """
        num_jobs = 0
        for run_name in run_names:
            num_jobs += self.report_run(run_name)
        log.info("Total: %s jobs in %s runs", num_jobs, len(run_names))

    def create_run(self, run_name):
        """
        Create a run on the results server.

        :param run_name: The name of the run.
        :returns:        The result of self.post_json()
        """
        run_uri = "{base}/runs/".format(base=self.base_uri, name=run_name)
        run_json = json.dumps({'name': run_name})
        return self.post_json(run_uri, run_json)

    def report_run(self, run_name):
        """
        Report a single run to the results server.

        :param run_name: The name of the run.
        :returns:        The number of jobs reported.
        """
        jobs = self.serializer.jobs_for_run(run_name)
        log.info("{name} {jobs} jobs".format(
            name=run_name,
            jobs=len(jobs),
        ))
        if jobs:
            status, msg, content = self.create_run(run_name)
            if status == 200:
                self.report_jobs(run_name, jobs.keys())
            elif msg.endswith('already exists'):
                if self.refresh:
                    self.report_jobs(run_name, jobs.keys())
                else:
                    log.info("    already present; skipped")
        elif not jobs:
            log.debug("    no jobs; skipped")
        return len(jobs)

    def report_jobs(self, run_name, job_ids):
        """
        Report several jobs to the results server.

        :param run_name: The name of the run.
        :param job_ids:  The jobs' ids
        """
        for job_id in job_ids:
            self.report_job(run_name, job_id)

    def report_job(self, run_name, job_id, job_json=None):
        """
        Report a single job to the results server.

        :param run_name: The name of the run.
        :param job_id:   The job's id
        :param job_json: The job's JSON object. Optional - if not present, we
                         look at the archive.
        """
        run_uri = "{base}/runs/{name}/".format(
            base=self.base_uri, name=run_name,)
        if job_json is None:
            job_json = self.serializer.json_for_job(run_name, job_id)
        status, msg, content = self.post_json(run_uri, job_json)

        if msg.endswith('already exists'):
            job_uri = os.path.join(run_uri, job_id, '')
            status, msg, content = self.put_json(job_uri, job_json)
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

    @property
    def http(self):
        if hasattr(self, '__http'):
            return self.__http
        self.__http = httplib2.Http()
        return self.__http


def create_run(run_name, base_uri=None):
    """
    Create a run on the results server. If it already exists, just smile and be
    happy.

    :param run_name: The name of the run.
    :param base_uri: The endpoint of the results server. If you leave it out
                     ResultsReporter will ask teuthology.config.
    :returns:         True if the run was successfully created.
    """
    # We are using archive_base='' here because we KNOW the serializer isn't
    # needed for this codepath.
    reporter = ResultsReporter(archive_base='', base_uri=base_uri)
    status, msg, content = reporter.create_run(run_name)
    return (status == 200 or msg.endswith('already exists'))


def push_job_info(run_name, job_id, job_info, base_uri=None):
    """
    Push a job's info (example: ctx.config) to the results server.

    :param run_name: The name of the run.
    :param job_id:   The job's id
    :param job_info: A dict containing the job's information.
    :param base_uri: The endpoint of the results server. If you leave it out
                     ResultsReporter will ask teuthology.config.
    """
    # We are using archive_base='' here because we KNOW the serializer isn't
    # needed for this codepath.
    job_json = json.dumps(job_info)
    reporter = ResultsReporter(archive_base='')
    reporter.report_job(run_name, job_id, job_json)


def try_push_job_info(job_config, job_info=None):
    """
    Wrap push_job_info, gracefully doing nothing if:
        A RequestFailedError is raised
        config.results_server is not set

    :param job_config: The ctx.config object
    :param job_info:   Dict to push (commonly None)
    """
    if not config.results_server:
        msg = "No results_server set in {yaml}; not attempting to push results"
        log.debug(msg.format(yaml=config.teuthology_yaml))
    else:
        run_name = job_config['name']
        job_id = job_config['job_id']
        if job_info is None:
            job_info = job_config

        try:
            push_job_info(run_name, job_id, job_info)
        except RequestFailedError:
            log.exception("Could not report results to %s" %
                          config.results_server)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Submit test results to a web service")
    parser.add_argument('-a', '--archive', required=True,
                        help="The base archive directory")
    parser.add_argument('-r', '--run', nargs='*',
                        help="A run (or list of runs) to submit")
    parser.add_argument('--all-runs', action='store_true',
                        help="Submit all runs in the archive")
    parser.add_argument('-R', '--refresh', action='store_true', default=False,
                        help=dedent("""Re-push any runs already stored on the
                                    server. Note that this may be slow."""))
    parser.add_argument('-s', '--server',
                        help=dedent(""""The server to post results to, e.g.
                                    http://localhost:8080/ . May also be
                                    specified in ~/.teuthology.yaml as
                                    'results_server'"""))
    parser.add_argument('-n', '--no-save', dest='save',
                        action='store_false', default=True,
                        help=dedent("""By default, when submitting all runs, we
                        remember the last successful submission in a file
                        called 'last_successful_run'. Pass this flag to disable
                        that behavior."""))
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    archive_base = os.path.abspath(os.path.expanduser(args.archive))
    reporter = ResultsReporter(archive_base, base_uri=args.server,
                               save=args.save, refresh=args.refresh)
    if args.run and len(args.run) > 1:
        reporter.report_runs(args.run)
    elif args.run:
        reporter.report_run(args.run[0])
    elif args.all_runs:
        reporter.report_all_runs()


if __name__ == "__main__":
    main()
