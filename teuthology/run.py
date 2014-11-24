import os
import yaml
import StringIO
import contextlib
import sys
import logging
from traceback import format_tb

import teuthology
from . import report
from .job_status import get_status
from .misc import get_user, merge_configs
from .nuke import nuke
from .run_tasks import run_tasks
from .repo_utils import fetch_qa_suite
from .results import email_results
from .config import JobConfig, FakeNamespace

log = logging.getLogger(__name__)


def set_up_logging(verbose, archive):
    if verbose:
        teuthology.log.setLevel(logging.DEBUG)

    if archive is not None:
        os.mkdir(archive)

        teuthology.setup_log_file(os.path.join(archive, 'teuthology.log'))

    install_except_hook()


def install_except_hook():
    def log_exception(exception_class, exception, traceback):
        logging.critical(''.join(format_tb(traceback)))
        if not exception.message:
            logging.critical(exception_class.__name__)
            return
        logging.critical('{0}: {1}'.format(
            exception_class.__name__, exception))

    sys.excepthook = log_exception


def write_initial_metadata(archive, config, name, description, owner):
    if archive is not None:
        with file(os.path.join(archive, 'pid'), 'w') as f:
            f.write('%d' % os.getpid())

        with file(os.path.join(archive, 'owner'), 'w') as f:
            f.write(owner + '\n')

        with file(os.path.join(archive, 'orig.config.yaml'), 'w') as f:
            yaml.safe_dump(config, f, default_flow_style=False)

        info = {
            'name': name,
            'description': description,
            'owner': owner,
            'pid': os.getpid(),
        }
        if 'job_id' in config:
            info['job_id'] = config['job_id']

        with file(os.path.join(archive, 'info.yaml'), 'w') as f:
            yaml.safe_dump(info, f, default_flow_style=False)


def fetch_tasks_if_needed(job_config):
    """
    Fetch the suite repo (and include it in sys.path) so that we can use its
    tasks.

    Returns the suite_path. The existing suite_path will be returned if the
    tasks can be imported, if not a new suite_path will try to be determined.
    """
    # Any scheduled job will already have the suite checked out and its
    # $PYTHONPATH set. We can check for this by looking for 'suite_path'
    # in its config.
    suite_path = job_config.get('suite_path')
    if suite_path:
        log.info("suite_path is set to %s; will attempt to use it", suite_path)
        if suite_path not in sys.path:
            sys.path.insert(1, suite_path)

    try:
        import tasks
        log.info("Found tasks at %s", os.path.dirname(tasks.__file__))
        # tasks found with the existing suite branch, return it
        return suite_path
    except ImportError:
        log.info("Tasks not found; will attempt to fetch")

    ceph_branch = job_config.get('branch', 'master')
    suite_branch = job_config.get('suite_branch', ceph_branch)
    suite_path = fetch_qa_suite(suite_branch)
    sys.path.insert(1, suite_path)
    return suite_path


def setup_config(config_paths):
    """ Takes a list of config yaml files and combines them
        into a single dictionary. Processes / validates the dictionary and then
        returns a JobConfig instance.
    """
    config = merge_configs(config_paths)

    # Older versions of teuthology stored job_id as an int. Convert it to a str
    # if necessary.
    job_id = config.get('job_id')
    if job_id is not None:
        job_id = str(job_id)
        config['job_id'] = job_id

    # targets must be >= than roles
    if 'targets' in config and 'roles' in config:
        targets = len(config['targets'])
        roles = len(config['roles'])
        assert targets >= roles, \
            '%d targets are needed for all roles but found %d listed.' % (
                roles, targets)

    return JobConfig.from_dict(config)


def get_machine_type(machine_type, config):
    """ If no machine_type is given, find the appropriate machine_type
        from the given config.
    """
    if machine_type is None:
        fallback_default = config.get('machine_type', 'plana')
        machine_type = config.get('machine-type', fallback_default)

    return machine_type


def get_summary(owner, description):
    summary = dict(success=True)
    summary['owner'] = owner

    if description is not None:
        summary['description'] = description

    return summary


def validate_tasks(config):
    """ Ensures that config tasks do not include 'kernal'.

        If there is not tasks, return an empty list.
    """
    if 'tasks' not in config:
        log.warning('No tasks specified. Continuing anyway...')
        # return the default value for tasks
        return []

    for task in config['tasks']:
        msg = ('kernel installation shouldn be a base-level item, not part ' +
               'of the tasks list')
        assert 'kernel' not in task, msg


def get_initial_tasks(lock, config, machine_type):
    init_tasks = []
    if lock:
        msg = ('You cannot specify targets in a config file when using the ' +
               '--lock option')
        assert 'targets' not in config, msg
        init_tasks.append({'internal.lock_machines': (
            len(config['roles']), machine_type)})

    init_tasks.extend([
        {'internal.save_config': None},
        {'internal.check_lock': None},
        {'internal.connect': None},
        {'internal.push_inventory': None},
        {'internal.serialize_remote_roles': None},
        {'internal.check_conflict': None},
    ])

    if not config.get('use_existing_cluster', False):
        init_tasks.extend([
            {'internal.check_ceph_data': None},
            {'internal.vm_setup': None},
        ])

    if 'kernel' in config:
        init_tasks.append({'kernel': config['kernel']})

    init_tasks.extend([
        {'internal.base': None},
        {'internal.archive': None},
        {'internal.coredump': None},
        {'internal.sudo': None},
        {'internal.syslog': None},
        {'internal.timer': None},
    ])

    return init_tasks


def report_outcome(config, archive, summary, fake_ctx):
    """ Reports on the final outcome of the command. """
    status = get_status(summary)
    passed = status == 'pass'

    if not passed and bool(config.get('nuke-on-error')):
        # only unlock if we locked them in the first place
        nuke(fake_ctx, fake_ctx.lock)

    if archive is not None:
        with file(os.path.join(archive, 'summary.yaml'), 'w') as f:
            yaml.safe_dump(summary, f, default_flow_style=False)

    with contextlib.closing(StringIO.StringIO()) as f:
        yaml.safe_dump(summary, f)
        log.info('Summary data:\n%s' % f.getvalue())

    with contextlib.closing(StringIO.StringIO()) as f:
        if ('email-on-error' in config
                and not passed):
            yaml.safe_dump(summary, f)
            yaml.safe_dump(config, f)
            emsg = f.getvalue()
            subject = "Teuthology error -- %s" % summary[
                'failure_reason']
            email_results(subject, "Teuthology", config[
                          'email-on-error'], emsg)

    report.try_push_job_info(config, summary)

    if passed:
        log.info(status)
    else:
        log.info(str(status).upper())
        sys.exit(1)


def main(ctx):
    set_up_logging(ctx["--verbose"], ctx["--archive"])

    if ctx["--owner"] is None:
        ctx["--owner"] = get_user()

    ctx["<config>"] = setup_config(ctx["<config>"])

    write_initial_metadata(ctx["--archive"], ctx["<config>"], ctx["--name"], ctx["--description"], ctx["--owner"])
    report.try_push_job_info(ctx["<config>"], dict(status='running'))

    machine_type = get_machine_type(ctx["--machine-type"])

    if ctx["--block"]:
        assert ctx["--lock"], \
            'the --block option is only supported with the --lock option'

    log.debug('\n  '.join(['Config:', ] + yaml.safe_dump(
        ctx["<config>"], default_flow_style=False).splitlines()))

    ctx["summary"] = get_summary(ctx["--owner"], ctx["--description"])

    ctx["<config>"]["tasks"] = validate_tasks(ctx["<config>"])

    init_tasks = get_initial_tasks(ctx["--lock"], ctx["<config>"], machine_type)

    ctx["<config>"]['tasks'].insert(0, init_tasks)

    if ctx["--suite-path"] is not None:
        ctx["<config>"]['suite_path'] = ctx["--suite-path"]

    # fetches the tasks and returns a new suite_path if needed
    ctx["<config>"]["suite_path"] = fetch_tasks_if_needed(ctx["<config>"])

    # create a FakeNamespace instance that mimics the old argparse way of doing things
    # we do this so we can pass it to run_tasks without porting those tasks to the
    # new way of doing things right now
    fake_ctx = FakeNamespace(ctx)

    try:
        run_tasks(tasks=ctx["<config>"]['tasks'], ctx=fake_ctx)
    finally:
        # print to stdout the results and possibly send an email on any errors
        report_outcome(ctx["<config>"], ctx["--archive"], ctx["summary"], fake_ctx)
