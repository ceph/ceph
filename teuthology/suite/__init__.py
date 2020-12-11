# this file is responsible for submitting tests into the queue
# by generating combinations of facets found in
# https://github.com/ceph/ceph-qa-suite.git

import logging
import os
import random
import time
from distutils.util import strtobool

import teuthology
from teuthology.config import config, YamlConfig
from teuthology.report import ResultsReporter
from teuthology.results import UNFINISHED_STATUSES

from teuthology.suite.run import Run
from teuthology.suite.util import schedule_fail

log = logging.getLogger(__name__)


def override_arg_defaults(name, default, env=os.environ):
    env_arg = {
        '--ceph-repo'         : 'TEUTH_CEPH_REPO',
        '--suite-repo'        : 'TEUTH_SUITE_REPO',
        '--ceph-branch'       : 'TEUTH_CEPH_BRANCH',
        '--suite-branch'      : 'TEUTH_SUITE_BRANCH',
    }
    if name in env_arg and env_arg[name] in env.keys():
        variable = env_arg[name]
        value = env[variable]
        log.debug("Default value for '{arg}' is overridden "
                  "from environment with: {val}"
                  .format(arg=name, val=value))
        return value
    else:
        return default


def process_args(args):
    conf = YamlConfig()
    rename_args = {
        'ceph': 'ceph_branch',
        'sha1': 'ceph_sha1',
        'kernel': 'kernel_branch',
        # FIXME: ceph flavor and kernel flavor are separate things
        'flavor': 'kernel_flavor',
        '<config_yaml>': 'base_yaml_paths',
        'filter': 'filter_in',
    }
    for (key, value) in args.items():
        # Translate --foo-bar to foo_bar
        key = key.lstrip('--').replace('-', '_')
        # Rename the key if necessary
        key = rename_args.get(key) or key
        if key == 'suite_branch':
            value = value or override_arg_defaults('--suite-branch', None)
        if key == 'suite' and value is not None:
            value = normalize_suite_name(value)
        if key == 'suite_relpath' and value is None:
            value = ''
        elif key in ('limit', 'priority', 'num', 'newest', 'seed'):
            value = int(value)
        elif key == 'subset' and value is not None:
            # take input string '2/3' and turn into (2, 3)
            value = tuple(map(int, value.split('/')))
        elif key in ('filter_all', 'filter_in', 'filter_out', 'rerun_statuses'):
            if not value:
                value = []
            else:
                value = [x.strip() for x in value.split(',')]
        elif key == 'ceph_repo':
            value = expand_short_repo_name(
                value,
                config.get_ceph_git_url())
        elif key == 'suite_repo':
            value = expand_short_repo_name(
                value,
                config.get_ceph_qa_suite_git_url())
        elif key in ('validate_sha1', 'filter_fragments'):
            value = strtobool(value)
        conf[key] = value
    return conf


def normalize_suite_name(name):
    return name.replace('/', ':')

def expand_short_repo_name(name, orig):
    # Allow shortname repo name 'foo' or 'foo/bar'.  This works with
    # github URLs, e.g.
    #
    #   foo -> https://github.com/ceph/foo
    #   foo/bar -> https://github.com/foo/bar
    #
    # when the orig URL is also github.  The two-level substitution may not
    # work with some configs.
    name_vec = name.split('/')
    if name_vec[-1] == '':
        del name_vec[-1]
    if len(name_vec) <= 2 and name.count(':') == 0:
        orig_vec = orig.split('/')
        if orig_vec[-1] == '':
            del orig_vec[-1]
        return '/'.join(orig_vec[:-len(name_vec)] + name_vec) + '.git'
    # otherwise, assume a full URL
    return name

def main(args):
    conf = process_args(args)
    if conf.verbose:
        teuthology.log.setLevel(logging.DEBUG)

    if not conf.machine_type or conf.machine_type == 'None':
        schedule_fail("Must specify a machine_type")
    elif 'multi' in conf.machine_type:
        schedule_fail("'multi' is not a valid machine_type. " +
                      "Maybe you want 'plana,mira,burnupi' or similar")

    if conf.email:
        config.results_email = conf.email
    if conf.archive_upload:
        config.archive_upload = conf.archive_upload
        log.info('Will upload archives to ' + conf.archive_upload)

    if conf.rerun:
        rerun_filters = get_rerun_filters(conf.rerun, conf.rerun_statuses)
        if len(rerun_filters['descriptions']) == 0:
            log.warn(
                "No jobs matched the status filters: %s",
                conf.rerun_statuses,
            )
            return
        conf.filter_in.extend(rerun_filters['descriptions'])
        conf.suite = normalize_suite_name(rerun_filters['suite'])
        conf.subset, conf.seed = get_rerun_conf(conf)
    if conf.seed < 0:
        conf.seed = random.randint(0, 9999)
        log.info('Using random seed=%s', conf.seed)

    run = Run(conf)
    name = run.name
    run.prepare_and_schedule()
    if not conf.dry_run and conf.wait:
        return wait(name, config.max_job_time,
                    conf.archive_upload_url)


def get_rerun_filters(name, statuses):
    reporter = ResultsReporter()
    run = reporter.get_run(name)
    filters = dict()
    filters['suite'] = run['suite']
    jobs = []
    for job in run['jobs']:
        if job['status'] in statuses:
            jobs.append(job)
    filters['descriptions'] = [job['description'] for job in jobs if job['description']]
    return filters


def get_rerun_conf(conf):
    reporter = ResultsReporter()
    try:
        subset, seed = reporter.get_rerun_conf(conf.rerun)
    except IOError:
        return conf.subset, conf.seed
    if seed is None:
        return conf.subset, conf.seed
    if conf.seed < 0:
        log.info('Using stored seed=%s', seed)
    elif conf.seed != seed:
        log.error('--seed {conf_seed} does not match with ' +
                  'stored seed: {stored_seed}',
                  conf_seed=conf.seed,
                  stored_seed=seed)
    if conf.subset is None:
        log.info('Using stored subset=%s', subset)
    elif conf.subset != subset:
        log.error('--subset {conf_subset} does not match with ' +
                  'stored subset: {stored_subset}',
                  conf_subset=conf.subset,
                  stored_subset=subset)
    return subset, seed


class WaitException(Exception):
    pass


def wait(name, max_job_time, upload_url):
    stale_job = max_job_time + Run.WAIT_MAX_JOB_TIME
    reporter = ResultsReporter()
    past_unfinished_jobs = []
    progress = time.time()
    log.info(f"waiting for the run {name} to complete")
    log.debug("the list of unfinished jobs will be displayed "
              "every " + str(Run.WAIT_PAUSE / 60) + " minutes")
    exit_code = 0
    while True:
        jobs = reporter.get_jobs(name, fields=['job_id', 'status'])
        unfinished_jobs = []
        for job in jobs:
            if job['status'] in UNFINISHED_STATUSES:
                unfinished_jobs.append(job)
            elif job['status'] != 'pass':
                exit_code = 1
        if len(unfinished_jobs) == 0:
            log.info("wait is done")
            break
        if (len(past_unfinished_jobs) == len(unfinished_jobs) and
                time.time() - progress > stale_job):
            raise WaitException(
                "no progress since " + str(config.max_job_time) +
                " + " + str(Run.WAIT_PAUSE) + " seconds")
        if len(past_unfinished_jobs) != len(unfinished_jobs):
            past_unfinished_jobs = unfinished_jobs
            progress = time.time()
        time.sleep(Run.WAIT_PAUSE)
        job_ids = [job['job_id'] for job in unfinished_jobs]
        log.debug('wait for jobs ' + str(job_ids))
    jobs = reporter.get_jobs(name, fields=['job_id', 'status',
                                           'description', 'log_href'])
    # dead, fail, pass : show fail/dead jobs first
    jobs = sorted(jobs, key=lambda x: x['status'])
    for job in jobs:
        if upload_url:
            url = os.path.join(upload_url, name, job['job_id'])
        else:
            url = job['log_href']
        log.info(job['status'] + " " + url + " " + job['description'])
    return exit_code
