"""
Qemu task
"""
from cStringIO import StringIO

import contextlib
import logging
import os

from teuthology import misc as teuthology
from teuthology import contextutil
from tasks import rbd
from teuthology.orchestra import run

log = logging.getLogger(__name__)

DEFAULT_NUM_RBD = 1
DEFAULT_IMAGE_URL = 'http://ceph.com/qa/ubuntu-12.04.qcow2'
DEFAULT_MEM = 4096 # in megabytes

def create_images(ctx, config, managers):
    for client, client_config in config.iteritems():
        num_rbd = client_config.get('num_rbd', 1)
        clone = client_config.get('clone', False)
        assert num_rbd > 0, 'at least one rbd device must be used'
        for i in xrange(num_rbd):
            create_config = {
                client: {
                    'image_name': '{client}.{num}'.format(client=client, num=i),
                    'image_format': 2 if clone else 1,
                    }
                }
            managers.append(
                lambda create_config=create_config:
                rbd.create_image(ctx=ctx, config=create_config)
                )

def create_clones(ctx, config, managers):
    for client, client_config in config.iteritems():
        num_rbd = client_config.get('num_rbd', 1)
        clone = client_config.get('clone', False)
        if clone:
            for i in xrange(num_rbd):
                create_config = {
                    client: {
                        'image_name':
                        '{client}.{num}-clone'.format(client=client, num=i),
                        'parent_name':
                        '{client}.{num}'.format(client=client, num=i),
                        }
                    }
                managers.append(
                    lambda create_config=create_config:
                    rbd.clone_image(ctx=ctx, config=create_config)
                    )

@contextlib.contextmanager
def create_dirs(ctx, config):
    """
    Handle directory creation and cleanup
    """
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.iteritems():
        assert 'test' in client_config, 'You must specify a test to run'
        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'install', '-d', '-m0755', '--',
                '{tdir}/qemu'.format(tdir=testdir),
                '{tdir}/archive/qemu'.format(tdir=testdir),
                ]
            )
    try:
        yield
    finally:
        for client, client_config in config.iteritems():
            assert 'test' in client_config, 'You must specify a test to run'
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rmdir', '{tdir}/qemu'.format(tdir=testdir), run.Raw('||'), 'true',
                    ]
                )

@contextlib.contextmanager
def generate_iso(ctx, config):
    """Execute system commands to generate iso"""
    log.info('generating iso...')
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.iteritems():
        assert 'test' in client_config, 'You must specify a test to run'
        (remote,) = ctx.cluster.only(client).remotes.keys()
        src_dir = os.path.dirname(__file__)
        userdata_path = os.path.join(testdir, 'qemu', 'userdata.' + client)
        metadata_path = os.path.join(testdir, 'qemu', 'metadata.' + client)

        with file(os.path.join(src_dir, 'userdata_setup.yaml'), 'rb') as f:
            test_setup = ''.join(f.readlines())
            # configuring the commands to setup the nfs mount
            mnt_dir = "/export/{client}".format(client=client)
            test_setup = test_setup.format(
                mnt_dir=mnt_dir
            )

        with file(os.path.join(src_dir, 'userdata_teardown.yaml'), 'rb') as f:
            test_teardown = ''.join(f.readlines())

        user_data = test_setup
        if client_config.get('type', 'filesystem') == 'filesystem':
            for i in xrange(0, client_config.get('num_rbd', DEFAULT_NUM_RBD)):
                dev_letter = chr(ord('b') + i)
                user_data += """
- |
  #!/bin/bash
  mkdir /mnt/test_{dev_letter}
  mkfs -t xfs /dev/vd{dev_letter}
  mount -t xfs /dev/vd{dev_letter} /mnt/test_{dev_letter}
""".format(dev_letter=dev_letter)

        # this may change later to pass the directories as args to the
        # script or something. xfstests needs that.
        user_data += """
- |
  #!/bin/bash
  test -d /mnt/test_b && cd /mnt/test_b
  /mnt/cdrom/test.sh > /mnt/log/test.log 2>&1 && touch /mnt/log/success
""" + test_teardown

        teuthology.write_file(remote, userdata_path, StringIO(user_data))

        with file(os.path.join(src_dir, 'metadata.yaml'), 'rb') as f:
            teuthology.write_file(remote, metadata_path, f)

        test_file = '{tdir}/qemu/{client}.test.sh'.format(tdir=testdir, client=client)
        remote.run(
            args=[
                'wget', '-nv', '-O', test_file,
                client_config['test'],
                run.Raw('&&'),
                'chmod', '755', test_file,
                ],
            )
        remote.run(
            args=[
                'genisoimage', '-quiet', '-input-charset', 'utf-8',
                '-volid', 'cidata', '-joliet', '-rock',
                '-o', '{tdir}/qemu/{client}.iso'.format(tdir=testdir, client=client),
                '-graft-points',
                'user-data={userdata}'.format(userdata=userdata_path),
                'meta-data={metadata}'.format(metadata=metadata_path),
                'test.sh={file}'.format(file=test_file),
                ],
            )
    try:
        yield
    finally:
        for client in config.iterkeys():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rm', '-f',
                    '{tdir}/qemu/{client}.iso'.format(tdir=testdir, client=client),
                    os.path.join(testdir, 'qemu', 'userdata.' + client),
                    os.path.join(testdir, 'qemu', 'metadata.' + client),
                    '{tdir}/qemu/{client}.test.sh'.format(tdir=testdir, client=client),
                    ],
                )

@contextlib.contextmanager
def download_image(ctx, config):
    """Downland base image, remove image file when done"""
    log.info('downloading base image')
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.iteritems():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        base_file = '{tdir}/qemu/base.{client}.qcow2'.format(tdir=testdir, client=client)
        remote.run(
            args=[
                'wget', '-nv', '-O', base_file, DEFAULT_IMAGE_URL,
                ]
            )
    try:
        yield
    finally:
        log.debug('cleaning up base image files')
        for client in config.iterkeys():
            base_file = '{tdir}/qemu/base.{client}.qcow2'.format(
                tdir=testdir,
                client=client,
                )
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rm', '-f', base_file,
                    ],
                )


def _setup_nfs_mount(remote, client, mount_dir):
    """
    Sets up an nfs mount on the remote that the guest can use to
    store logs. This nfs mount is also used to touch a file
    at the end of the test to indiciate if the test was successful
    or not.
    """
    export_dir = "/export/{client}".format(client=client)
    log.info("Creating the nfs export directory...")
    remote.run(args=[
        'sudo', 'mkdir', '-p', export_dir,
    ])
    log.info("Mounting the test directory...")
    remote.run(args=[
        'sudo', 'mount', '--bind', mount_dir, export_dir,
    ])
    log.info("Adding mount to /etc/exports...")
    export = "{dir} *(rw,no_root_squash,no_subtree_check,insecure)".format(
        dir=export_dir
    )
    remote.run(args=[
        'sudo', 'sed', '-i', '/^\/export\//d', "/etc/exports",
    ])
    remote.run(args=[
        'echo', export, run.Raw("|"),
        'sudo', 'tee', '-a', "/etc/exports",
    ])
    log.info("Restarting NFS...")
    if remote.os.package_type == "deb":
        remote.run(args=['sudo', 'service', 'nfs-kernel-server', 'restart'])
    else:
        remote.run(args=['sudo', 'systemctl', 'restart', 'nfs'])


def _teardown_nfs_mount(remote, client):
    """
    Tears down the nfs mount on the remote used for logging and reporting the
    status of the tests being ran in the guest.
    """
    log.info("Tearing down the nfs mount for {remote}".format(remote=remote))
    export_dir = "/export/{client}".format(client=client)
    log.info("Stopping NFS...")
    if remote.os.package_type == "deb":
        remote.run(args=[
            'sudo', 'service', 'nfs-kernel-server', 'stop'
        ])
    else:
        remote.run(args=[
            'sudo', 'systemctl', 'stop', 'nfs'
        ])
    log.info("Unmounting exported directory...")
    remote.run(args=[
        'sudo', 'umount', export_dir
    ])
    log.info("Deleting exported directory...")
    remote.run(args=[
        'sudo', 'rm', '-r', '/export'
    ])
    log.info("Deleting export from /etc/exports...")
    remote.run(args=[
        'sudo', 'sed', '-i', '$ d', '/etc/exports'
    ])
    log.info("Starting NFS...")
    if remote.os.package_type == "deb":
        remote.run(args=[
            'sudo', 'service', 'nfs-kernel-server', 'start'
        ])
    else:
        remote.run(args=[
            'sudo', 'systemctl', 'start', 'nfs'
        ])


@contextlib.contextmanager
def run_qemu(ctx, config):
    """Setup kvm environment and start qemu"""
    procs = []
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.iteritems():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        log_dir = '{tdir}/archive/qemu/{client}'.format(tdir=testdir, client=client)
        remote.run(
            args=[
                'mkdir', log_dir, run.Raw('&&'),
                'sudo', 'modprobe', 'kvm',
                ]
            )

        # make an nfs mount to use for logging and to
        # allow to test to tell teuthology the tests outcome
        _setup_nfs_mount(remote, client, log_dir)

        base_file = '{tdir}/qemu/base.{client}.qcow2'.format(
            tdir=testdir,
            client=client
        )
        qemu_cmd = 'qemu-system-x86_64'
        if remote.os.package_type == "rpm":
            qemu_cmd = "/usr/libexec/qemu-kvm"
        args=[
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'daemon-helper',
            'term',
            qemu_cmd, '-enable-kvm', '-nographic',
            '-m', str(client_config.get('memory', DEFAULT_MEM)),
            # base OS device
            '-drive',
            'file={base},format=qcow2,if=virtio'.format(base=base_file),
            # cd holding metadata for cloud-init
            '-cdrom', '{tdir}/qemu/{client}.iso'.format(tdir=testdir, client=client),
            ]

        cachemode = 'none'
        ceph_config = ctx.ceph['ceph'].conf.get('global', {})
        ceph_config.update(ctx.ceph['ceph'].conf.get('client', {}))
        ceph_config.update(ctx.ceph['ceph'].conf.get(client, {}))
        if ceph_config.get('rbd cache'):
            if ceph_config.get('rbd cache max dirty', 1) > 0:
                cachemode = 'writeback'
            else:
                cachemode = 'writethrough'

        clone = client_config.get('clone', False)
        for i in xrange(client_config.get('num_rbd', DEFAULT_NUM_RBD)):
            suffix = '-clone' if clone else ''
            args.extend([
                '-drive',
                'file=rbd:rbd/{img}:id={id},format=raw,if=virtio,cache={cachemode}'.format(
                    img='{client}.{num}{suffix}'.format(client=client, num=i,
                                                        suffix=suffix),
                    id=client[len('client.'):],
                    cachemode=cachemode,
                    ),
                ])

        log.info('starting qemu...')
        procs.append(
            remote.run(
                args=args,
                logger=log.getChild(client),
                stdin=run.PIPE,
                wait=False,
                )
            )

    try:
        yield
    finally:
        log.info('waiting for qemu tests to finish...')
        run.wait(procs)

        log.debug('checking that qemu tests succeeded...')
        for client in config.iterkeys():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            # teardown nfs mount
            _teardown_nfs_mount(remote, client)
            # check for test status
            remote.run(
                args=[
                    'test', '-f',
                    '{tdir}/archive/qemu/{client}/success'.format(
                        tdir=testdir,
                        client=client
                        ),
                    ],
                )


@contextlib.contextmanager
def task(ctx, config):
    """
    Run a test inside of QEMU on top of rbd. Only one test
    is supported per client.

    For example, you can specify which clients to run on::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://ceph.com/qa/test.sh
            client.1:
              test: http://ceph.com/qa/test2.sh

    Or use the same settings on all clients:

        tasks:
        - ceph:
        - qemu:
            all:
              test: http://ceph.com/qa/test.sh

    For tests that don't need a filesystem, set type to block::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://ceph.com/qa/test.sh
              type: block

    The test should be configured to run on /dev/vdb and later
    devices.

    If you want to run a test that uses more than one rbd image,
    specify how many images to use::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://ceph.com/qa/test.sh
              type: block
              num_rbd: 2

    You can set the amount of memory the VM has (default is 1024 MB)::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://ceph.com/qa/test.sh
              memory: 512 # megabytes

    If you want to run a test against a cloned rbd image, set clone to true::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://ceph.com/qa/test.sh
              clone: true
    """
    assert isinstance(config, dict), \
           "task qemu only supports a dictionary for configuration"

    config = teuthology.replace_all_with_clients(ctx.cluster, config)

    managers = []
    create_images(ctx=ctx, config=config, managers=managers)
    managers.extend([
        lambda: create_dirs(ctx=ctx, config=config),
        lambda: generate_iso(ctx=ctx, config=config),
        lambda: download_image(ctx=ctx, config=config),
        ])
    create_clones(ctx=ctx, config=config, managers=managers)
    managers.append(
        lambda: run_qemu(ctx=ctx, config=config),
        )

    with contextutil.nested(*managers):
        yield
