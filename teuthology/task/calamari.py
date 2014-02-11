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
    service = _http_service_name(remote)
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


RELEASE_MAP = {
    'Ubuntu precise': dict(flavor='deb', release='ubuntu', version='precise'),
    'Debian wheezy': dict(flavor='deb', release='debian', version='wheezy'),
    'CentOS 6.4': dict(flavor='rpm', release='centos', version='6.4'),
    'RedHatEnterpriseServer 6.4': dict(flavor='rpm', release='rhel',
                                       version='6.4'),
}


def _get_relmap(rem):
    relmap = getattr(rem, 'relmap', None)
    if relmap is not None:
        return relmap
    lsb_release_out = StringIO()
    rem.run(args=['lsb_release', '-ics'], stdout=lsb_release_out)
    release = lsb_release_out.getvalue().replace('\n', ' ').rstrip()
    if release in RELEASE_MAP:
        rem.relmap = RELEASE_MAP[release]
        return rem.relmap
    else:
        lsb_release_out = StringIO()
        rem.run(args=['lsb_release', '-irs'], stdout=lsb_release_out)
        release = lsb_release_out.getvalue().replace('\n', ' ').rstrip()
        if release in RELEASE_MAP:
            rem.relmap = RELEASE_MAP[release]
            return rem.relmap
    raise RuntimeError('Can\'t get release info for {}'.format(rem))


def _sqlite_package_name(rem):
    name = 'sqlite3' if _get_relmap(rem)['flavor'] == 'deb' else None
    return name


def _http_service_name(rem):
    name = 'httpd' if _get_relmap(rem)['flavor'] == 'rpm' else 'apache2'
    return name


def _install_repo(remote, pkgdir, username, password):
    # installing repo is assumed to be idempotent

    relmap = _get_relmap(remote)
    log.info('Installing repo on %s', remote)
    if relmap['flavor'] == 'deb':
        contents = 'deb https://{username}:{password}@download.inktank.com/' \
                   '{pkgdir}/deb {codename} main'
        contents = contents.format(username=username, password=password,
                                   pkgdir=pkgdir, codename=relmap['version'],)
        teuthology.sudo_write_file(remote,
                                   '/etc/apt/sources.list.d/inktank.list',
                                   contents)
        remote.run(args=['sudo',
                         'apt-get',
                         'install',
                         'apt-transport-https',
                         '-y'])
        result = remote.run(args=['sudo', 'apt-get', 'update', '-y'],
                            stdout=StringIO())
        return True

    elif relmap['flavor'] == 'rpm':
        baseurl = 'https://{username}:{password}@download.inktank.com/' \
                  '{pkgdir}/rpm/{release}{version}'
        contents = textwrap.dedent('''
            [inktank]
            name=Inktank Storage, Inc.
            baseurl={baseurl}
            gpgcheck=1
            enabled=1
            '''.format(baseurl=baseurl))
        contents = contents.format(username=username,
                                   password=password,
                                   pkgdir=pkgdir,
                                   release=relmap['release'],
                                   version=relmap['version'])
        teuthology.sudo_write_file(remote,
                                   '/etc/yum.repos.d/inktank.repo',
                                   contents)
        return remote.run(args=['sudo', 'yum', 'makecache'])

    else:
        return False


def _remove_repo(remote):
    log.info('Removing repo on %s', remote)
    flavor = _get_relmap(remote)['flavor']
    if flavor == 'deb':
        teuthology.delete_file(remote, '/etc/apt/sources.list.d/inktank.list',
                               sudo=True, force=True)
        result = remote.run(args=['sudo', 'apt-get', 'update', '-y'],
                            stdout=StringIO())
        return True

    elif flavor == 'rpm':
        teuthology.delete_file(remote, '/etc/yum.repos.d/inktank.repo',
                               sudo=True, force=True)
        return remote.run(args=['sudo', 'yum', 'makecache'])

    else:
        return False


def _install_repokey(remote):
    # installing keys is assumed to be idempotent
    log.info('Installing repo key on %s', remote)
    flavor = _get_relmap(remote)['flavor']
    if flavor == 'deb':
        return remote.run(args=['wget',
                                '-q',
                                '-O-',
                                'http://download.inktank.com/keys/release.asc',
                                run.Raw('|'),
                                'sudo',
                                'apt-key',
                                'add',
                                '-'])
    elif flavor == 'rpm':
        args = ['sudo', 'rpm', '--import',
                'http://download.inktank.com/keys/release.asc']
        return remote.run(args=args)
    else:
        return False


def _install_package(package, remote):
    """
    package: name
    remote: Remote() to install on
    release: deb only, 'precise' or 'wheezy'
    pkgdir: may or may not include a branch name, so, say, either
            packages or packages-staging/master
    """
    log.info('Installing package %s on %s', package, remote)
    flavor = _get_relmap(remote)['flavor']
    if flavor == 'deb':
        pkgcmd = ['DEBIAN_FRONTEND=noninteractive',
                  'sudo',
                  '-E',
                  'apt-get',
                  '-y',
                  'install',
                  '{package}'.format(package=package)]
    elif flavor == 'rpm':
        pkgcmd = ['sudo',
                  'yum',
                  '-y',
                  'install',
                  '{package}'.format(package=package)]
    else:
        log.error('_install_package: bad flavor ' + flavor + '\n')
        return False
    return remote.run(args=pkgcmd)


def _remove_package(package, remote):
    """
    remove package from remote
    """
    flavor = _get_relmap(remote)['flavor']
    if flavor == 'deb':
        pkgcmd = ['DEBIAN_FRONTEND=noninteractive',
                  'sudo',
                  '-E',
                  'apt-get',
                  '-y',
                  'purge',
                  '{package}'.format(package=package)]
    elif flavor == 'rpm':
        pkgcmd = ['sudo',
                  'yum',
                  '-y',
                  'erase',
                  '{package}'.format(package=package)]
    else:
        log.error('_remove_package: bad flavor ' + flavor + '\n')
        return False
    return remote.run(args=pkgcmd)

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
            _install_package('calamari-agent', rem)
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
                _remove_package('calamari-agent', rem)


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
            _install_repokey(rem)
            _install_repo(rem, pkgdir, username, password)
        yield

    finally:
        for rem in remotes:
            _remove_repo(rem)


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
            _install_package('calamari-restapi', rem)
        yield

    finally:
        for rem in remotes:
            _remove_package('calamari-restapi', rem)


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
        sqlite_package = _sqlite_package_name(remote)
        if sqlite_package and not _install_package(sqlite_package, remote):
            raise RuntimeError('{} install failed'.format(sqlite_package))

        if not _install_package('calamari-server', remote) or \
           not _install_package('calamari-clients', remote) or \
           not _disable_default_nginx(remote) or \
           not _setup_calamari_cluster(remote, restapi_remote):
            raise RuntimeError('Server installation failure')

        log.info('client/server setup complete')
        yield
    finally:
        _remove_package('calamari-server', remote)
        _remove_package('calamari-clients', remote)
        if sqlite_package:
            _remove_package(sqlite_package, remote)


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
    cmd_list = [os.path.join(mypath, 'calamari_testdir',
                             'test_server_1_0.py')]
    os.environ['CALAMARI_BASE_URI'] = 'http://{0}/api/v1/'.format(testhost)
    log.info("testing %s", testhost)
    return subprocess.call(cmd_list)
