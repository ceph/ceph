"""
rgw s3tests logging wrappers
"""
from io import BytesIO
from configobj import ConfigObj
import contextlib
import logging
from tasks import s3tests

from teuthology import misc as teuthology
from teuthology import contextutil

log = logging.getLogger(__name__)


@contextlib.contextmanager
def download(ctx, config):
    """
    Run s3tests download function
    """
    return s3tests.download(ctx, config)

def _config_user(s3tests_conf, section, user):
    """
    Run s3tests user config function
    """
    return s3tests._config_user(s3tests_conf, section, user)

@contextlib.contextmanager
def create_users(ctx, config):
    """
    Run s3tests user create function
    """
    return s3tests.create_users(ctx, config)

@contextlib.contextmanager
def configure(ctx, config):
    """
    Run s3tests user configure function
    """
    return s3tests.configure(ctx, config)

@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run remote netcat tests
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.items():
        client_config['extra_args'] = [
            's3tests.functional.test_s3:test_bucket_list_return_data',
        ]
#        args = [
#                'S3TEST_CONF={tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client),
#                '{tdir}/s3-tests/virtualenv/bin/nosetests'.format(tdir=testdir),
#                '-w',
#                '{tdir}/s3-tests'.format(tdir=testdir),
#                '-v',
#		's3tests.functional.test_s3:test_bucket_list_return_data',
#                ]
#        if client_config is not None and 'extra_args' in client_config:
#            args.extend(client_config['extra_args'])
#
#        ctx.cluster.only(client).run(
#            args=args,
#            )

    s3tests.run_tests(ctx, config)

    netcat_out = BytesIO()

    for client, client_config in config.items():
        ctx.cluster.only(client).run(
            args = [
                'netcat',
                '-w', '5',
                '-U', '{tdir}/rgw.opslog.sock'.format(tdir=testdir),
                ],
             stdout = netcat_out,
        )

        out = netcat_out.getvalue()

        assert len(out) > 100

        log.info('Received', out)

    yield


@contextlib.contextmanager
def task(ctx, config):
    """
    Run some s3-tests suite against rgw, verify opslog socket returns data

    Must restrict testing to a particular client::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests: [client.0]

    To pass extra arguments to nose (e.g. to run a certain test)::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests:
            client.0:
              extra_args: ['test_s3:test_object_acl_grand_public_read']
            client.1:
              extra_args: ['--exclude', 'test_100_continue']
    """
    assert hasattr(ctx, 'rgw'), 'rgw-logsocket must run after the rgw task'
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task rgw-logsocket only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    overrides = ctx.config.get('overrides', {})
    # merge each client section, not the top level.
    for (client, cconf) in config.items():
        teuthology.deep_merge(cconf, overrides.get('rgw-logsocket', {}))

    log.debug('config is %s', config)

    s3tests_conf = {}
    for client in clients:
        endpoint = ctx.rgw.role_endpoints.get(client)
        assert endpoint, 'rgw-logsocket: no rgw endpoint for {}'.format(client)

        s3tests_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'DEFAULT':
                    {
                    'port'      : endpoint.port,
                    'is_secure' : endpoint.cert is not None,
                    },
                'fixtures' : {},
                's3 main'  : {},
                's3 alt'   : {},
                }
            )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                s3tests_conf=s3tests_conf,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                s3tests_conf=s3tests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        yield
