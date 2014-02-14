"""
calamari - set up various machines with roles for participating
in Calamari management app testing.  Requires secret info for
accessing authenticated package repos to install Calamari, supplied
in an override: clause for calamari.reposetup below.  Contains
five tasks:

- calamari.reposetup: set up the calamari package repos (all targets)
- calamari.agent: install stats collection daemons (all cluster targets)
- calamari.restapi: cluster-management api access (one monitor target)
- calamari.server: main webapp/gui front end (management target)
- calamari.test: run automated test against calamari.server target (local)

calamari.test runs on the local machine, as it accesses the Calamari
server across https using requests.py, which must be present.  It uses
several external modules in calamari_test/.
"""

from cStringIO import StringIO
import contextlib
import logging
import os
import subprocess
import teuthology.misc as teuthology
import teuthology.packaging as pkg
import textwrap
import time
from ..orchestra import run

log = logging.getLogger(__name__)


def _edit_diamond_config(remote, serverhost):
    """ Edit remote's diamond config to send stats to serverhost """
    ret = remote.run(args=['sudo',
                     'sed',
                     '-i',
                     's/calamari/{host}/'.format(host=serverhost),
                     '/etc/diamond/diamond.conf'],
                     stdout=StringIO())
    if not ret:
        return False
    return remote.run(args=['sudo', 'service', 'diamond', 'restart'])


def _disable_default_nginx(remote):
    """
    Fix up nginx values
    """
    script = textwrap.dedent('''
        if [ -f /etc/nginx/conf.d/default.conf ]; then
            mv /etc/nginx/conf.d/default.conf \
                /etc/nginx/conf.d/default.disabled
        fi
        if [ -f /etc/nginx/sites-enabled/default ] ; then
            rm /etc/nginx/sites-enabled/default
        fi
        service nginx restart
        service {service} restart
    ''')
    service = pkg.get_service_name('httpd', remote)
    script = script.format(service=service)
    teuthology.sudo_write_file(remote, '/tmp/disable.nginx', script)
    return remote.run(args=['sudo', 'bash', '/tmp/disable.nginx'],
                      stdout=StringIO())


def _setup_calamari_cluster(remote, restapi_remote):
    """
    Add restapi db entry to the server.
    """
    restapi_hostname = str(restapi_remote).split('@')[1]
    sqlcmd = 'insert into ceph_cluster (name, api_base_url) ' \
             'values ("{host}", "http://{host}:5000/api/v0.1/");'. \
             format(host=restapi_hostname)
    teuthology.write_file(remote, '/tmp/create.cluster.sql', sqlcmd)
    return remote.run(args=['cat',
                            '/tmp/create.cluster.sql',
                            run.Raw('|'),
                            'sudo',
                            'sqlite3',
                            '/opt/calamari/webapp/calamari/db.sqlite3'],
                      stdout=StringIO())


"""
Tasks
"""


@contextlib.contextmanager
def agent(ctx, config):
    """
    task agent
    calamari.agent: install stats collection (for each cluster host)

    For example::

        tasks:
        - calamari.agent:
           roles:
               - mon.0
               - osd.0
               - osd.1
           server: server.0
    """

    log.info('calamari.agent starting')
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('calamari.agent', {}))

    remotes = teuthology.roles_to_remotes(ctx.cluster, config)
    try:
        for rem in remotes:
            log.info('Installing calamari-agent on %s', rem)
            pkg.install_package('calamari-agent', rem)
            server_role = config.get('server')
            if not server_role:
                raise RuntimeError('must supply \'server\' config key')
            server_remote = ctx.cluster.only(server_role).remotes.keys()[0]
            # why isn't shortname available by default?
            serverhost = server_remote.name.split('@')[1]
            log.info('configuring Diamond for {}'.format(serverhost))
            if not _edit_diamond_config(rem, serverhost):
                raise RuntimeError(
                    'Diamond config edit failed on {0}'.format(rem)
                )
        yield
    finally:
            for rem in remotes:
                pkg.remove_package('calamari-agent', rem)


@contextlib.contextmanager
def reposetup(ctx, config):
    """
    task reposetup
    Sets up calamari repository on all remotes; cleans up when done

    calamari.reposetup:
        pkgdir:
        username:
        password:

    Supply the above in an override file if you need to manage the
    secret repo credentials separately from the test definition (likely).

    pkgdir encodes package directory (possibly more than one path component)
    as in https://<username>:<password>@SERVER/<pkgdir>/{deb,rpm}{..}

    """
    overrides = ctx.config.get('overrides', {})
    # XXX deep_merge returns the result, which matters if either is None
    # make sure that doesn't happen
    if config is None:
        config = {'dummy': 'dummy'}
    teuthology.deep_merge(config, overrides.get('calamari.reposetup', {}))

    try:
        pkgdir = config['pkgdir']
        username = config['username']
        password = config['password']
    except KeyError:
        raise RuntimeError('requires pkgdir, username, and password')

    remotes = ctx.cluster.remotes.keys()

    try:
        for rem in remotes:
            log.info(rem)
            keypath = 'http://download.inktank.com/keys/release.asc'
            pkg.install_repokey(rem, keypath)
            pkg.install_repo(rem, 'download.inktank.com', pkgdir,
                             username, password)
        yield

    finally:
        for rem in remotes:
            pkg.remove_repo(rem)


@contextlib.contextmanager
def restapi(ctx, config):
    """
    task restapi

    Calamari Rest API

    For example::

        tasks:
        - calamari.restapi:
          roles: mon.0
    """
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('calamari.restapi', {}))

    remotes = teuthology.roles_to_remotes(ctx.cluster, config)

    try:
        for rem in remotes:
            log.info(rem)
            pkg.install_package('calamari-restapi', rem)
        yield

    finally:
        for rem in remotes:
            pkg.remove_package('calamari-restapi', rem)


@contextlib.contextmanager
def server(ctx, config):
    """
    task server:

    Calamari server setup.  "roles" is a list of roles that should run
    the webapp, and "restapi_server" is a list of roles that will
    be running the calamari-restapi package.  Both lists probably should
    have only one entry (only the first is used).

    For example::

        roles: [[server.0], [mon.0], [osd.0, osd.1]]
        tasks:
        - calamari.server:
            roles: [server.0]
            restapi_server: [mon.0]
    """
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('calamari.server', {}))

    remote = teuthology.roles_to_remotes(ctx.cluster, config)[0]
    restapi_remote = teuthology.roles_to_remotes(ctx.cluster, config,
                                                 attrname='restapi_server')[0]
    if not restapi_remote:
        raise RuntimeError('Must supply restapi_server')

    try:
        # sqlite3 command is required; on some platforms it's already
        # there and not removable (required for, say yum)
        sqlite_package = pkg.get_package_name('sqlite', remote)
        if sqlite_package and not pkg.install_package(sqlite_package, remote):
            raise RuntimeError('{} install failed'.format(sqlite_package))

        if not pkg.install_package('calamari-server', remote) or \
           not pkg.install_package('calamari-clients', remote) or \
           not _disable_default_nginx(remote) or \
           not _setup_calamari_cluster(remote, restapi_remote):
            raise RuntimeError('Server installation failure')

        log.info('client/server setup complete')
        yield
    finally:
        pkg.remove_package('calamari-server', remote)
        pkg.remove_package('calamari-clients', remote)
        if sqlite_package:
            pkg.remove_package(sqlite_package, remote)


def test(ctx, config):
    """
    task test
    Run the calamari smoketest
    Tests to run are in calamari_testdir.
    delay: wait this long before starting

        tasks:
        - calamari.test:
            delay: 30
            server: server.0
    """
    delay = config.get('delay', 0)
    if delay:
        log.info("delaying %d sec", delay)
        time.sleep(delay)
    testhost = ctx.cluster.only(config['server']).remotes.keys()[0].name
    testhost = testhost.split('@')[1]
    mypath = os.path.dirname(__file__)
    cmd_list = [os.path.join(mypath, 'calamari', 'servertest_1_0.py')]
    os.environ['CALAMARI_BASE_URI'] = 'http://{0}/api/v1/'.format(testhost)
    log.info("testing %s", testhost)
    return subprocess.call(cmd_list)
