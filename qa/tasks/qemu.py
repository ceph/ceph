"""
Qemu task
"""
from cStringIO import StringIO

import contextlib
import logging
import os
import time

from teuthology import misc as teuthology
from teuthology import contextutil
from tasks import rbd
from teuthology.orchestra import run

log = logging.getLogger(__name__)

DEFAULT_NUM_RBD = 1
DEFAULT_IMAGE_URL = 'https://cloud-images.ubuntu.com/releases/14.04/release/ubuntu-14.04-server-cloudimg-amd64-disk1.img'
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

        user_data = test_setup
        generate_id_rsa = ['cat', '/dev/zero', run.Raw('|'), 'ssh-keygen',
                           '-q', '-N', run.Raw('""')]
        remote.run(args=generate_id_rsa, check_status=False)
        remote.run(args=['chmod', '600', '.ssh/id_rsa.pub'])
        id_rsa_pub = StringIO()
        remote.run(args=['cat', '.ssh/id_rsa.pub'], stdout=id_rsa_pub)
        id_rsa_pub = ''.join(id_rsa_pub.readlines())
        ssh_auth_key = 'ssh_authorized_keys: \n'
        user_data += ssh_auth_key + '  - ' + id_rsa_pub + '\n'
        teuthology.write_file(remote, userdata_path, StringIO(user_data))

        with file(os.path.join(src_dir, 'metadata.yaml'), 'rb') as f:
            teuthology.write_file(remote, metadata_path, f)

        remote.run(
            args=[
                'genisoimage', '-quiet', '-input-charset', 'utf-8',
                '-volid', 'cidata', '-joliet', '-rock',
                '-o', '{tdir}/qemu/{client}.iso'.format(tdir=testdir, client=client),
                '-graft-points',
                'user-data={userdata}'.format(userdata=userdata_path),
                'meta-data={metadata}'.format(metadata=metadata_path),
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

        base_file = '{tdir}/qemu/base.{client}.qcow2'.format(
            tdir=testdir,
            client=client
        )
        # Hack to make sure /dev/kvm permissions are set correctly
        # See http://tracker.ceph.com/issues/17977 and
        # https://bugzilla.redhat.com/show_bug.cgi?id=1333159
        remote.run(args='sudo udevadm control --reload')
        remote.run(args='sudo udevadm trigger /dev/kvm')
        remote.run(args='ls -l /dev/kvm')

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
            '-redir', run.Raw('tcp:5555::22')
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
        log.info("Sleeping 90 seconds for cloud-config to take effect")
        time.sleep(90)
        log.info("Running xfs tests")
        xfs_cmd = ['ssh', '-p', '5555',
                   run.Raw('ubuntu@localhost'),
                   'sudo', 'bash', '/run_xfstests_krbd.sh']
        xfs_param = ['-c', '1', '-f', 'xfs', '-t',
                     '/dev/vdb', '-s', '/dev/vdc']
        xfs_cmd.extend(xfs_param)
        xfs_out = StringIO()
        remote.run(args=xfs_cmd, stdout=xfs_out)
        yield
    finally:
            kill_qemu = ['sudo', 'pkill', 'qemu']
            remote.run(args=kill_qemu, check_status=False)
            log.info("xfs tests completed")


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
              test: http://download.ceph.com/qa/test.sh
            client.1:
              test: http://download.ceph.com/qa/test2.sh

    Or use the same settings on all clients:

        tasks:
        - ceph:
        - qemu:
            all:
              test: http://download.ceph.com/qa/test.sh

    For tests that don't need a filesystem, set type to block::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://download.ceph.com/qa/test.sh
              type: block

    The test should be configured to run on /dev/vdb and later
    devices.

    If you want to run a test that uses more than one rbd image,
    specify how many images to use::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://download.ceph.com/qa/test.sh
              type: block
              num_rbd: 2

    You can set the amount of memory the VM has (default is 1024 MB)::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://download.ceph.com/qa/test.sh
              memory: 512 # megabytes

    If you want to run a test against a cloned rbd image, set clone to true::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://download.ceph.com/qa/test.sh
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
