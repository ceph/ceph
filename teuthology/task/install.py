from cStringIO import StringIO

import argparse
import contextlib
import logging
import os
import sys

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)


def _update_deb_package_list_and_install(ctx, remote, debs, branch):
    """
    updates the package list so that apt-get can
    download the appropriate packages
    """

    # check for ceph release key
    r = remote.run(
        args=[
            'sudo', 'apt-key', 'list', run.Raw('|'), 'grep', 'Ceph',
            ],
        stdout=StringIO(),
        )
    if r.stdout.getvalue().find('Ceph automated package') == -1:
        # if it doesn't exist, add it
        remote.run(
                args=[
                    'wget', '-q', '-O-',
                    'https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/autobuild.asc',
                    run.Raw('|'),
                    'sudo', 'apt-key', 'add', '-',
                    ],
                stdout=StringIO(),
                )

    # get ubuntu release (precise, quantal, etc.)
    r = remote.run(
            args=['lsb_release', '-sc'],
            stdout=StringIO(),
            )

    out = r.stdout.getvalue().strip()
    log.info("release type:" + out)

    gitbuilder_host = ctx.teuthology_config.get('gitbuilder_host',
                                                'gitbuilder.ceph.com')

    # get package version string
    r = remote.run(
        args=[
            'wget', '-q', '-O-',
            'http://{host}/ceph-deb-'.format(host=gitbuilder_host) +
               out + '-x86_64-basic/ref/' + branch + '/version',
            ],
        stdout=StringIO(),
        )
    version = r.stdout.getvalue().strip()
    log.info('package version is %s', version)

    remote.run(
        args=[
            'echo', 'deb',
            'http://{host}/ceph-deb-'.format(host=gitbuilder_host) +
               out + '-x86_64-basic/ref/' + branch,
            out, 'main', run.Raw('|'),
            'sudo', 'tee', '/etc/apt/sources.list.d/ceph.list'
            ],
        stdout=StringIO(),
        )
    remote.run(
        args=[
            'sudo', 'apt-get', 'update', run.Raw('&&'),
            'sudo', 'apt-get', '-y', '--force-yes',
            'install',
            ] + ['%s=%s' % (d, version) for d in debs],
        stdout=StringIO(),
        )

def install_debs(ctx, debs, branch):
    """
    installs Debian packages.
    """
    log.info("Installing ceph debian packages: {debs}".format(
            debs=', '.join(debs)))
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(
                _update_deb_package_list_and_install, ctx, remote, debs, branch)

def _remove_deb(remote, debs):
    args=[
        'sudo', 'apt-get', '-y', '--force-yes', 'purge',
        ]
    args.extend(debs)
    args.extend([
            run.Raw('||'),
            'true'
            ])
    remote.run(args=args)
    remote.run(
        args=[
            'sudo', 'apt-get', '-y', '--force-yes',
            'autoremove',
            ],
        stdout=StringIO(),
        )

def remove_debs(ctx, debs):
    log.info("Removing/purging debian packages {debs}".format(
            debs=', '.join(debs)))
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(_remove_deb, remote, debs)

def _remove_sources_list(remote):
    remote.run(
        args=[
            'sudo', 'rm', '-f', '/etc/apt/sources.list.d/ceph.list',
            run.Raw('&&'),
            'sudo', 'apt-get', 'update',
            ],
        stdout=StringIO(),
       )

def remove_sources(ctx):
    log.info("Removing ceph sources list from apt")
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            p.spawn(_remove_sources_list, remote)


@contextlib.contextmanager
def install(ctx, config):
    debs = [
        'ceph',
        'ceph-dbg',
        'ceph-mds',
        'ceph-mds-dbg',
        'ceph-common',
        'ceph-common-dbg',
        'ceph-test',
        'ceph-test-dbg',
        'radosgw',
        'radosgw-dbg',
        'python-ceph',
        ]
    # install lib deps (so we explicitly specify version), but do not
    # uninstall them, as 
    debs_install = debs + [
        'librados2',
        'librados2-dbg',
        'librbd1',
        'librbd1-dbg',
        ]
    branch = config.get('branch', 'master')
    log.info('branch: {b}'.format(b=branch))
    install_debs(ctx, debs_install, branch)
    try:
        yield
    finally:
        remove_debs(ctx, debs)
        remove_sources(ctx)


@contextlib.contextmanager
def task(ctx, config):
    """
    Install ceph packages
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task ceph only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph', {}))

    # Flavor tells us what gitbuilder to fetch the prebuilt software
    # from. It's a combination of possible keywords, in a specific
    # order, joined by dashes. It is used as a URL path name. If a
    # match is not found, the teuthology run fails. This is ugly,
    # and should be cleaned up at some point.

    dist = 'precise'
    arch = 'x86_64'
    flavor = 'basic'

    # First element: controlled by user (or not there, by default):
    # used to choose the right distribution, e.g. "oneiric".
    flavor = config.get('flavor', 'basic')

    if config.get('path'):
        # local dir precludes any other flavors
        flavor = 'local'
    else:
        if config.get('valgrind'):
            log.info('Using notcmalloc flavor and running some daemons under valgrind')
            flavor = 'notcmalloc'
        else:
            if config.get('coverage'):
                log.info('Recording coverage for this run.')
                flavor = 'gcov'

    ctx.summary['flavor'] = flavor
    
    with contextutil.nested(
        lambda: install(ctx=ctx, config=dict(
                branch=config.get('branch', 'master'),
                tag=config.get('tag'),
                sha1=config.get('sha1'),
                flavor=flavor,
                dist=config.get('dist', dist),
                arch=arch
                )),
        ):
        yield
