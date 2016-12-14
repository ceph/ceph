import contextlib
import logging
import os
import textwrap
import yaml

from cStringIO import StringIO
from teuthology import contextutil
from teuthology import misc
from teuthology import packaging
from teuthology.orchestra import run

log = logging.getLogger(__name__)

# extra stuff we need to do our job here
EXTRA_PKGS = [
    'git',
]

# stuff that would be in a devmode install, but should be
# installed in the system for running nosetests against
# a production install.
EXTRA_NOSETEST_PKGS = [
    'python-psutil',
    'python-mock',
]


def find_client0(cluster):
    ''' Find remote that has client.0 role, or None '''
    for rem, roles in cluster.remotes.iteritems():
        if 'client.0' in roles:
            return rem
    return None


def pip(remote, package, venv=None, uninstall=False, force=False):
    ''' {un}install a package with pip, possibly in a virtualenv '''
    if venv:
        pip = os.path.join(venv, 'bin', 'pip')
        args = ['sudo', pip]
    else:
        args = ['sudo', 'pip']

    if uninstall:
        args.extend(['uninstall', '-y'])
    else:
        args.append('install')
        if force:
            args.append('-I')

    args.append(package)
    remote.run(args=args)


@contextlib.contextmanager
def install_epel(remote):
    ''' install a disabled-by-default epel repo config file '''
    remove = False
    try:
        if remote.os.package_type == 'deb':
            yield
        else:
            remove = True
            distromajor = remote.os.version.split('.')[0]

            repofiledata = textwrap.dedent('''
                [epel]
                name=epel{version}
                metalink=http://mirrors.fedoraproject.org/metalink?repo=epel-{version}&arch=$basearch
                enabled=0
                gpgcheck=0
            ''').format(version=distromajor)

            misc.create_file(remote, '/etc/yum.repos.d/epel.repo',
                             data=repofiledata, sudo=True)
            remote.run(args='sudo yum clean all')
            yield

    finally:
        if remove:
            misc.delete_file(remote, '/etc/yum.repos.d/epel.repo', sudo=True)


def enable_epel(remote, enable=True):
    ''' enable/disable the epel repo '''
    args = 'sudo sed -i'.split()
    if enable:
        args.extend(['s/enabled=0/enabled=1/'])
    else:
        args.extend(['s/enabled=1/enabled=0/'])
    args.extend(['/etc/yum.repos.d/epel.repo'])

    remote.run(args=args)
    remote.run(args='sudo yum clean all')


@contextlib.contextmanager
def install_extra_pkgs(client):
    ''' Install EXTRA_PKGS '''
    try:
        for pkg in EXTRA_PKGS:
            packaging.install_package(pkg, client)
        yield

    finally:
        for pkg in EXTRA_PKGS:
            packaging.remove_package(pkg, client)


@contextlib.contextmanager
def clone_calamari(config, client):
    ''' clone calamari source into current directory on remote '''
    branch = config.get('calamari_branch', 'master')
    url = config.get('calamari_giturl', 'git://github.com/ceph/calamari')
    try:
        out = StringIO()
        # ensure branch is present (clone -b will succeed even if
        # the branch doesn't exist, falling back to master)
        client.run(
            args='git ls-remote %s %s' % (url, branch),
            stdout=out,
            label='check for calamari branch %s existence' % branch
        )
        if len(out.getvalue()) == 0:
            raise RuntimeError("Calamari branch %s doesn't exist" % branch)
        client.run(args='git clone -b %s %s' % (branch, url))
        yield
    finally:
        # sudo python setup.py develop may have left some root files around
        client.run(args='sudo rm -rf calamari')


@contextlib.contextmanager
def write_info_yaml(cluster, client):
    ''' write info.yaml to client for nosetests '''
    try:
        info = {
            'cluster': {
                rem.name: {'roles': roles}
                for rem, roles in cluster.remotes.iteritems()
            }
        }
        misc.create_file(client, 'calamari/info.yaml',
                         data=yaml.safe_dump(info, default_flow_style=False))
        yield
    finally:
        misc.delete_file(client, 'calamari/info.yaml')


@contextlib.contextmanager
def write_test_conf(client):
    ''' write calamari/tests/test.conf to client for nosetests '''
    try:
        testconf = textwrap.dedent('''
            [testing]

            calamari_control = external
            ceph_control = external
            bootstrap = False
            api_username = admin
            api_password = admin
            embedded_timeout_factor = 1
            external_timeout_factor = 3
            external_cluster_path = info.yaml
        ''')
        misc.create_file(client, 'calamari/tests/test.conf', data=testconf)
        yield

    finally:
        misc.delete_file(client, 'calamari/tests/test.conf')


@contextlib.contextmanager
def prepare_nosetest_env(client):
    try:
        # extra dependencies that would be in the devmode venv
        if client.os.package_type == 'rpm':
            enable_epel(client, enable=True)
        for package in EXTRA_NOSETEST_PKGS:
            packaging.install_package(package, client)
        if client.os.package_type == 'rpm':
            enable_epel(client, enable=False)

        # install nose itself into the calamari venv, force it in case it's
        # already installed in the system, so we can invoke it by path without
        # fear that it's not present
        pip(client, 'nose', venv='/opt/calamari/venv', force=True)

        # install a later version of requests into the venv as well
        # (for precise)
        pip(client, 'requests', venv='/opt/calamari/venv', force=True)

        # link (setup.py develop) calamari/rest-api into the production venv
        # because production does not include calamari_rest.management, needed
        # for test_rest_api.py's ApiIntrospection
        args = 'cd calamari/rest-api'.split() + [run.Raw(';')] + \
               'sudo /opt/calamari/venv/bin/python setup.py develop'.split()
        client.run(args=args)

        # because, at least in Python 2.6/Centos, site.py uses
        # 'os.path.exists()' to process .pth file entries, and exists() uses
        # access(2) to check for existence, all the paths leading up to
        # $HOME/calamari/rest-api need to be searchable by all users of
        # the package, which will include the WSGI/Django app, running
        # as the Apache user.  So make them all world-read-and-execute.
        args = 'sudo chmod a+x'.split() + \
            ['.', './calamari', './calamari/rest-api']
        client.run(args=args)

        # make one dummy request just to get the WSGI app to do
        # all its log creation here, before the chmod below (I'm
        # looking at you, graphite -- /var/log/calamari/info.log and
        # /var/log/calamari/exception.log)
        client.run(args='wget -q -O /dev/null http://localhost')

        # /var/log/calamari/* is root-or-apache write-only
        client.run(args='sudo chmod a+w /var/log/calamari/*')

        yield

    finally:
        args = 'cd calamari/rest-api'.split() + [run.Raw(';')] + \
               'sudo /opt/calamari/venv/bin/python setup.py develop -u'.split()
        client.run(args=args)
        for pkg in ('nose', 'requests'):
            pip(client, pkg, venv='/opt/calamari/venv', uninstall=True)
        for package in EXTRA_NOSETEST_PKGS:
            packaging.remove_package(package, client)


@contextlib.contextmanager
def run_nosetests(client):
    ''' Actually run the tests '''
    args = [
        'cd',
        'calamari',
        run.Raw(';'),
        'CALAMARI_CONFIG=/etc/calamari/calamari.conf',
        '/opt/calamari/venv/bin/nosetests',
        '-v',
        'tests/',
    ]
    client.run(args=args)
    yield


@contextlib.contextmanager
def task(ctx, config):
    """
    Run Calamari tests against an instance set up by 'calamari_server'.

    -- clone the Calamari source into $HOME (see options)
    -- write calamari/info.yaml describing the cluster
    -- write calamari/tests/test.conf containing
        'external' for calamari_control and ceph_control
        'bootstrap = False' to disable test bootstrapping (installing minions)
        no api_url necessary (inferred from client.0)
        'external_cluster_path = info.yaml'
    -- modify the production Calamari install to allow test runs:
        install nose in the venv
        install EXTRA_NOSETEST_PKGS
        link in, with setup.py develop, calamari_rest (for ApiIntrospection)
    -- set CALAMARI_CONFIG to point to /etc/calamari/calamari.conf
    -- nosetests -v tests/

    Options are:
        calamari_giturl: url from which to git clone calamari
                         (default: git://github.com/ceph/calamari)
        calamari_branch: git branch of calamari to check out
                         (default: master)

    Note: the tests must find a clean cluster, so don't forget to
    set the crush default type appropriately, or install min_size OSD hosts
    """
    client0 = find_client0(ctx.cluster)
    if client0 is None:
        raise RuntimeError("must have client.0 role")

    with contextutil.nested(
        lambda: install_epel(client0),
        lambda: install_extra_pkgs(client0),
        lambda: clone_calamari(config, client0),
        lambda: write_info_yaml(ctx.cluster, client0),
        lambda: write_test_conf(client0),
        lambda: prepare_nosetest_env(client0),
        lambda: run_nosetests(client0),
    ):
        yield
