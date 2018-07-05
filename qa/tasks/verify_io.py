import yaml
import contextlib
import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run
log = logging.getLogger(__name__)


def test_exec(ctx, config, client):

    script_name = config['verification_script'] + ".py"
    yaml_name = 'io_info.yaml'

    client.run(args=['ls', '-lt', yaml_name])

    if ctx.multisite_test.version == 'v1':

        client.run(
            args=[
                run.Raw(
                    'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/v1/lib/%s '
                    % script_name)])

    elif ctx.multisite_test.version == 'v2':

        client.run(
            args=[
                run.Raw(
                    'sudo venv/bin/python2.7 rgw-tests/ceph-qe-scripts/rgw/v2/lib/%s '
                    % script_name)])


@contextlib.contextmanager
def task(ctx, config):

    log.info('starting the task')

    log.info('config %s' % config)

    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    remotes = ctx.cluster.only(teuthology.is_type('rgw'))
    for remote, roles_for_host in remotes.remotes.iteritems():

        remote.run(args=['virtualenv', 'venv'])
        remote.run(
            args=[
                'source',
                'venv/bin/activate',
                run.Raw(';'),
                run.Raw('pip install boto boto3 names PyYaml ConfigParser'),
                run.Raw(';'),
                'deactivate'])
        test_exec(ctx, config, remote)

    try:
        yield
    finally:

        remotes = ctx.cluster.only(teuthology.is_type('rgw'))
        for remote, roles_for_host in remotes.remotes.iteritems():

            log.info('Verification completed')

            remote.run(args=[run.Raw('sudo rm -rf %s' % 'io_info.yaml')])
