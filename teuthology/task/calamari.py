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

Sample configuration:
roles:
- [osd.0, osd.1, mon.0, calamari.restapi]
- [osd.2, osd.3, calamari.server]

tasks:
- install:
    branch: dumpling
- ceph:
- calamari.reposetup:
- calamari.agent:
- calamari.restapi:
- calamari.server:
- calamari.test:
    delay: 40

calamari.reposetup will happen on all osd/mon/calamari.* remotes
calamari.agent will run on all osd/mon
calamari.restapi must run on a remote with a monitor
calamari.server must be able to find calamari.restapi to talk to
calamari.test has an optional delay to allow the webapp to settle before
 talking to it (we could make the test retry/timeout instead)

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


def _remotes(ctx, selector):
    return ctx.cluster.only(selector).remotes.keys()

"""
Tasks
"""


@contextlib.contextmanager
def agent(ctx, config):
    """
    task agent
    calamari.agent: install stats collection
       (for each role of type 'mon.' or 'osd.')

    For example::

        roles:
        - [osd.0, mon.a]
        - [osd.1]
        tasks:
        - calamari.agent:
    """

    log.info('calamari.agent starting')
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('calamari.agent', {}))

    # agent gets installed on any remote with role mon or osd
    def needs_agent(role):
        for type in 'mon.', 'osd.':
            if role.startswith(type):
                return True
        return False

    remotes = _remotes(ctx, needs_agent)
    if remotes is None:
        raise RuntimeError('No role configured')
    try:
        for rem in remotes:
            log.info('Installing calamari-agent on %s', rem)
            pkg.install_package('calamari-agent', rem)
            server_remote = _remotes(ctx,
                lambda r: r.startswith('calamari.server'))
            if not server_remote:
                raise RuntimeError('No calamari.server role available')
            server_remote = server_remote[0]
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
    Sets up calamari repository on all 'osd', 'mon', and 'calamari.' remotes;
     cleans up when done

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

    # repo gets installed on any remote with role mon, osd, or calamari
    def needs_repo(role):
        for type in 'mon.', 'osd.', 'calamari.':
            if role.startswith(type):
                return True
        return False

    remotes = _remotes(ctx, needs_repo)
    if remotes is None:
        raise RuntimeError('No roles configured')

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

        roles:
        - [mon.a, osd.0, osd.1, calamari.restapi]
        - [osd.2, osd.3]
        tasks:
        - calamari.restapi:
    """
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('calamari.restapi', {}))

    remotes_and_roles = \
        ctx.cluster.only(lambda r: r.startswith('calamari.restapi')).remotes
    if remotes_and_roles is None:
        raise RuntimeError('No role configured')

    # check that the role selected also has at least one mon role
    for rem, roles in remotes_and_roles.iteritems():
        if not any([r for r in roles if r.startswith('mon.')]):
            raise RuntimeError('no mon on remote with roles %s', roles)

    try:
        for rem in remotes_and_roles.iterkeys():
            log.info(rem)
            pkg.install_package('calamari-restapi', rem)
        yield

    finally:
        for rem in remotes_and_roles.iterkeys():
            pkg.remove_package('calamari-restapi', rem)


@contextlib.contextmanager
def server(ctx, config):
    """
    task server:

    Calamari server setup.  Add role 'calamari.server' to the remote
    that will run the webapp.  'calamari.restapi' role must be present
    to serve as the cluster-api target for calamari-server.  Only one
    of calamari.server and calamari.restapi roles is supported currently.

    For example::

        roles:
        - [calamari.server]
        - [mon.0, calamari.restapi]
        - [osd.0, osd.1]
        tasks:
        - calamari.restapi:
        - calamari.server:
    """
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('calamari.server', {}))

    remote = _remotes(ctx, lambda r: r.startswith('calamari.server'))
    if not remote:
        raise RuntimeError('No role configured')

    restapi_remote = _remotes(ctx, lambda r: r.startswith('calamari.restapi'))
    if not restapi_remote:
        raise RuntimeError('Must supply calamari.restapi role')

    remote = remote[0]
    restapi_remote = restapi_remote[0]

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
    Run the calamari smoketest on the teuthology host (no role required)
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
    testhost = _remotes(ctx, lambda r: r.startswith('calamari.server'))[0]
    testhost = testhost.name.split('@')[1]
    mypath = os.path.dirname(__file__)
    cmd_list = [os.path.join(mypath, 'calamari', 'servertest_1_0.py')]
    os.environ['CALAMARI_BASE_URI'] = 'http://{0}/api/v1/'.format(testhost)
    log.info("testing %s", testhost)
    return subprocess.call(cmd_list)
