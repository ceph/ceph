"""
Run a set of s3 tests on rgw.
"""
from cStringIO import StringIO
from configobj import ConfigObj
import base64
import contextlib
import logging
import os
import random
import string

import util.rgw as rgw_utils

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.config import config as teuth_config
from teuthology.orchestra import run
from teuthology.orchestra.connection import split_user

log = logging.getLogger(__name__)

def extract_sync_client_data(ctx, client_name):
    """
    Extract synchronized client rgw zone and rgw region information.

    :param ctx: Context passed to the s3tests task
    :param name: Name of client that we are synching with
    """
    return_region_name = None
    return_dict = None
    client = ctx.ceph['ceph'].conf.get(client_name, None)
    if client:
        current_client_zone = client.get('rgw zone', None)
        if current_client_zone:
            (endpoint_host, endpoint_port) = ctx.rgw.role_endpoints.get(client_name, (None, None))
            # pull out the radosgw_agent stuff
            regions = ctx.rgw.regions
            for region in regions:
                log.debug('jbuck, region is {region}'.format(region=region))
                region_data = ctx.rgw.regions[region]
                log.debug('region data is {region}'.format(region=region_data))
                zones = region_data['zones']
                for zone in zones:
                    if current_client_zone in zone:
                        return_region_name = region
                        return_dict = dict()
                        return_dict['api_name'] = region_data['api name']
                        return_dict['is_master'] = region_data['is master']
                        return_dict['port'] = endpoint_port
                        return_dict['host'] = endpoint_host

                        # The s3tests expect the sync_agent_[addr|port} to be
                        # set on the non-master node for some reason
                        if not region_data['is master']:
                            (rgwagent_host, rgwagent_port) = ctx.radosgw_agent.endpoint
                            (return_dict['sync_agent_addr'], _) = ctx.rgw.role_endpoints[rgwagent_host]
                            return_dict['sync_agent_port'] = rgwagent_port

        else: #if client_zone:
            log.debug('No zone info for {host}'.format(host=client_name))
    else: # if client
        log.debug('No ceph conf for {host}'.format(host=client_name))

    return return_region_name, return_dict

def update_conf_with_region_info(ctx, config, s3tests_conf):
    """
    Scan for a client (passed in s3tests_conf) that is an s3agent
    with which we can sync.  Update information in local conf file
    if such a client is found.
    """
    for key in s3tests_conf.keys():
        # we'll assume that there's only one sync relationship (source / destination) with client.X
        # as the key for now

        # Iterate through all of the radosgw_agent (rgwa) configs and see if a
        # given client is involved in a relationship.
        # If a given client isn't, skip it
        this_client_in_rgwa_config = False
        for rgwa in ctx.radosgw_agent.config.keys():
            rgwa_data = ctx.radosgw_agent.config[rgwa]

            if key in rgwa_data['src'] or key in rgwa_data['dest']:
                this_client_in_rgwa_config = True
                log.debug('{client} is in an radosgw-agent sync relationship'.format(client=key))
                radosgw_sync_data = ctx.radosgw_agent.config[key]
                break
        if not this_client_in_rgwa_config:
            log.debug('{client} is NOT in an radosgw-agent sync relationship'.format(client=key))
            continue

        source_client = radosgw_sync_data['src']
        dest_client = radosgw_sync_data['dest']

        # #xtract the pertinent info for the source side
        source_region_name, source_region_dict = extract_sync_client_data(ctx, source_client)
        log.debug('\t{key} source_region {source_region} source_dict {source_dict}'.format
            (key=key,source_region=source_region_name,source_dict=source_region_dict))

        # The source *should* be the master region, but test anyway and then set it as the default region
        if source_region_dict['is_master']:
            log.debug('Setting {region} as default_region'.format(region=source_region_name))
            s3tests_conf[key]['fixtures'].setdefault('default_region', source_region_name)

        # Extract the pertinent info for the destination side
        dest_region_name, dest_region_dict = extract_sync_client_data(ctx, dest_client)
        log.debug('\t{key} dest_region {dest_region} dest_dict {dest_dict}'.format
            (key=key,dest_region=dest_region_name,dest_dict=dest_region_dict))

        # now add these regions to the s3tests_conf object
        s3tests_conf[key]['region {region_name}'.format(region_name=source_region_name)] = source_region_dict
        s3tests_conf[key]['region {region_name}'.format(region_name=dest_region_name)] = dest_region_dict

@contextlib.contextmanager
def download(ctx, config):
    """
    Download the s3 tests from the git builder.
    Remove downloaded s3 file upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading s3-tests...')
    testdir = teuthology.get_testdir(ctx)
    s3_branches = [ 'giant', 'firefly', 'firefly-original', 'hammer' ]
    for (client, cconf) in config.items():
        branch = cconf.get('force-branch', None)
        if not branch:
            ceph_branch = ctx.config.get('branch')
            suite_branch = ctx.config.get('suite_branch', ceph_branch)
            if suite_branch in s3_branches:
                branch = cconf.get('branch', suite_branch)
	    else:
                branch = cconf.get('branch', 'ceph-' + suite_branch)
        if not branch:
            raise ValueError(
                "Could not determine what branch to use for s3tests!")
        else:
            log.info("Using branch '%s' for s3tests", branch)
        sha1 = cconf.get('sha1')
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', branch,
                teuth_config.ceph_git_base_url + 's3-tests.git',
                '{tdir}/s3-tests'.format(tdir=testdir),
                ],
            )
        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/s3-tests'.format(tdir=testdir),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                    ],
                )
    try:
        yield
    finally:
        log.info('Removing s3-tests...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    ],
                )


def _config_user(s3tests_conf, section, user):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    s3tests_conf[section].setdefault('user_id', user)
    s3tests_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
    s3tests_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))
    s3tests_conf[section].setdefault('access_key', ''.join(random.choice(string.uppercase) for i in xrange(20)))
    s3tests_conf[section].setdefault('secret_key', base64.b64encode(os.urandom(40)))


@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create a main and an alternate s3 user.
    """
    assert isinstance(config, dict)
    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    users = {'s3 main': 'foo', 's3 alt': 'bar'}
    for client in config['clients']:
        s3tests_conf = config['s3tests_conf'][client]
        s3tests_conf.setdefault('fixtures', {})
        s3tests_conf['fixtures'].setdefault('bucket prefix', 'test-' + client + '-{random}-')
        for section, user in users.iteritems():
            _config_user(s3tests_conf, section, '{user}.{client}'.format(user=user, client=client))
            log.debug('Creating user {user} on {host}'.format(user=s3tests_conf[section]['user_id'], host=client))
            ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client,
                    'user', 'create',
                    '--uid', s3tests_conf[section]['user_id'],
                    '--display-name', s3tests_conf[section]['display_name'],
                    '--access-key', s3tests_conf[section]['access_key'],
                    '--secret', s3tests_conf[section]['secret_key'],
                    '--email', s3tests_conf[section]['email'],
                ],
            )
    try:
        yield
    finally:
        for client in config['clients']:
            for user in users.itervalues():
                uid = '{user}.{client}'.format(user=user, client=client)
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client,
                        'user', 'rm',
                        '--uid', uid,
                        '--purge-data',
                        ],
                    )


@contextlib.contextmanager
def configure(ctx, config):
    """
    Configure the s3-tests.  This includes the running of the
    bootstrap code and the updating of local conf files.
    """
    assert isinstance(config, dict)
    log.info('Configuring s3-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].iteritems():
        s3tests_conf = config['s3tests_conf'][client]
        if properties is not None and 'rgw_server' in properties:
            host = None
            for target, roles in zip(ctx.config['targets'].iterkeys(), ctx.config['roles']):
                log.info('roles: ' + str(roles))
                log.info('target: ' + str(target))
                if properties['rgw_server'] in roles:
                    _, host = split_user(target)
            assert host is not None, "Invalid client specified as the rgw_server"
            s3tests_conf['DEFAULT']['host'] = host
        else:
            s3tests_conf['DEFAULT']['host'] = 'localhost'

        if properties is not None and 'slow_backend' in properties:
	    s3tests_conf['fixtures']['slow backend'] = properties['slow_backend']

        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'cd',
                '{tdir}/s3-tests'.format(tdir=testdir),
                run.Raw('&&'),
                './bootstrap',
                ],
            )
        conf_fp = StringIO()
        s3tests_conf.write(conf_fp)
        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )

    log.info('Configuring boto...')
    boto_src = os.path.join(os.path.dirname(__file__), 'boto.cfg.template')
    for client, properties in config['clients'].iteritems():
        with file(boto_src, 'rb') as f:
            (remote,) = ctx.cluster.only(client).remotes.keys()
            conf = f.read().format(
                idle_timeout=config.get('idle_timeout', 30)
                )
            teuthology.write_file(
                remote=remote,
                path='{tdir}/boto.cfg'.format(tdir=testdir),
                data=conf,
                )

    try:
        yield

    finally:
        log.info('Cleaning up boto...')
        for client, properties in config['clients'].iteritems():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rm',
                    '{tdir}/boto.cfg'.format(tdir=testdir),
                    ],
                )

@contextlib.contextmanager
def sync_users(ctx, config):
    """
    Sync this user.
    """
    assert isinstance(config, dict)
    # do a full sync if this is a multi-region test
    if rgw_utils.multi_region_enabled(ctx):
        log.debug('Doing a full sync')
        rgw_utils.radosgw_agent_sync_all(ctx)
    else:
        log.debug('Not a multi-region config; skipping the metadata sync')

    yield

@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run the s3tests after everything is set up.

    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    attrs = ["!fails_on_rgw"]
    if not ctx.rgw.use_fastcgi:
        attrs.append("!fails_on_mod_proxy_fcgi")
    for client, client_config in config.iteritems():
        args = [
            'S3TEST_CONF={tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client),
            'BOTO_CONFIG={tdir}/boto.cfg'.format(tdir=testdir),
            '{tdir}/s3-tests/virtualenv/bin/nosetests'.format(tdir=testdir),
            '-w',
            '{tdir}/s3-tests'.format(tdir=testdir),
            '-v',
            '-a', ','.join(attrs),
            ]
        if client_config is not None and 'extra_args' in client_config:
            args.extend(client_config['extra_args'])

        ctx.cluster.only(client).run(
            args=args,
            label="s3 tests against rgw"
            )
    yield

@contextlib.contextmanager
def task(ctx, config):
    """
    Run the s3-tests suite against rgw.

    To run all tests on all clients::

        tasks:
        - ceph:
        - rgw:
        - s3tests:

    To restrict testing to particular clients::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests: [client.0]

    To run against a server on client.1 and increase the boto timeout to 10m::

        tasks:
        - ceph:
        - rgw: [client.1]
        - s3tests:
            client.0:
              rgw_server: client.1
              idle_timeout: 600

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
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task s3tests only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    overrides = ctx.config.get('overrides', {})
    # merge each client section, not the top level.
    for client in config.iterkeys():
        if not config[client]:
            config[client] = {}
        teuthology.deep_merge(config[client], overrides.get('s3tests', {}))

    log.debug('s3tests config is %s', config)

    s3tests_conf = {}
    for client in clients:
        s3tests_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'DEFAULT':
                    {
                    'port'      : 7280,
                    'is_secure' : 'no',
                    },
                'fixtures' : {},
                's3 main'  : {},
                's3 alt'   : {},
                }
            )

    # Only attempt to add in the region info if there's a radosgw_agent configured
    if hasattr(ctx, 'radosgw_agent'):
        update_conf_with_region_info(ctx, config, s3tests_conf)

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                s3tests_conf=s3tests_conf,
                )),
        lambda: sync_users(ctx=ctx, config=config),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                s3tests_conf=s3tests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        pass
    yield
