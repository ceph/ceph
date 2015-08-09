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
from .config import FakeNamespace
from .config import config as teuth_config

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
    """
    Takes a list of config yaml files and combines them
    into a single dictionary. Processes / validates the dictionary and then
    returns it.
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

    return config


def get_machine_type(machine_type, config):
    """
    If no machine_type is given, find the appropriate machine_type
    from the given config.
    """
    if machine_type is None:
        fallback_default = config.get('machine_type',
                                      teuth_config.default_machine_type)
        machine_type = config.get('machine-type', fallback_default)

    return machine_type


def get_summary(owner, description):
    summary = dict(success=True)
    summary['owner'] = owner

    if description is not None:
        summary['description'] = description

    return summary


def validate_tasks(config):
    """
    Ensures that config tasks is a list and doesn't include 'kernel'.

    Returns the original tasks key if found.  If not, returns an
    empty list.
    """
    if 'tasks' not in config:
        log.warning('No tasks specified. Continuing anyway...')
        # return the default value for tasks
        return []

    msg = "Expected list in 'tasks'; instead got: {0}".format(config['tasks'])
    assert isinstance(config['tasks'], list), msg

    for task in config['tasks']:
        msg = ('kernel installation should be a base-level item, not part ' +
               'of the tasks list')
        assert 'kernel' not in task, msg

    return config["tasks"]


def get_initial_tasks(lock, config, machine_type):
    init_tasks = [{'internal.check_packages': None}]
    if lock:
        msg = ('You cannot specify targets in a config file when using the ' +
               '--lock option')
        assert 'targets' not in config, msg
        init_tasks.append({'internal.lock_machines': (
            len(config['roles']), machine_type)})

    init_tasks.extend([
        {'internal.save_config': None},
        {'internal.check_lock': None},
        {'internal.add_remotes': None},
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
        {'internal.archive_upload': None},
        {'internal.sudo': None},
        {'internal.syslog': None},
        {'internal.timer': None},
        {'selinux': None},
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


def get_teuthology_command(args):
    """
    Rebuilds the teuthology command used to run this job
    and returns it as a string.
    """
    cmd = ["teuthology"]
    for key, value in args.iteritems():
        if value:
            # an option, not an argument
            if not key.startswith("<"):
                cmd.append(key)
            else:
                # this is the <config> argument
                for arg in value:
                    cmd.append(str(arg))
                continue
            # so we don't print something like --verbose True
            if isinstance(value, str):
                cmd.append(value)
    return " ".join(cmd)


def main(args):
    verbose = args["--verbose"]
    archive = args["--archive"]
    owner = args["--owner"]
    config = args["<config>"]
    name = args["--name"]
    description = args["--description"]
    machine_type = args["--machine-type"]
    block = args["--block"]
    lock = args["--lock"]
    suite_path = args["--suite-path"]
    os_type = args["--os-type"]
    os_version = args["--os-version"]

    set_up_logging(verbose, archive)

    # print the command being ran
    log.debug("Teuthology command: {0}".format(get_teuthology_command(args)))

    if owner is None:
        args["--owner"] = owner = get_user()

    config = setup_config(config)

    write_initial_metadata(archive, config, name, description, owner)
    report.try_push_job_info(config, dict(status='running'))

    machine_type = get_machine_type(machine_type, config)
    args["--machine-type"] = machine_type

    if block:
        assert lock, \
            'the --block option is only supported with the --lock option'

    log.debug(
        '\n  '.join(['Config:', ] + yaml.safe_dump(
            config, default_flow_style=False).splitlines()))

    args["summary"] = get_summary(owner, description)

    config["tasks"] = validate_tasks(config)

    init_tasks = get_initial_tasks(lock, config, machine_type)

    # prepend init_tasks to the front of the task list
    config['tasks'][:0] = init_tasks

    if suite_path is not None:
        config['suite_path'] = suite_path

    # fetches the tasks and returns a new suite_path if needed
    config["suite_path"] = fetch_tasks_if_needed(config)

    # overwrite the config value of os_type if --os-type is provided
    if os_type:
        config["os_type"] = os_type

    # overwrite the config value of os_version if --os-version is provided
    if os_version:
        config["os_version"] = os_version

    # create a FakeNamespace instance that mimics the old argparse way of doing
    # things we do this so we can pass it to run_tasks without porting those
    # tasks to the new way of doing things right now
    args["<config>"] = config
    fake_ctx = FakeNamespace(args)

    # store on global config if interactive-on-error, for contextutil.nested()
    # FIXME this should become more generic, and the keys should use
    # '_' uniformly
    if fake_ctx.config.get('interactive-on-error'):
        teuthology.config.config.ctx = fake_ctx

    try:
        run_tasks(tasks=config['tasks'], ctx=fake_ctx)
    finally:
        # print to stdout the results and possibly send an email on any errors
        report_outcome(config, archive, fake_ctx.summary, fake_ctx)
