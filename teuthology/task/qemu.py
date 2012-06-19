from cStringIO import StringIO

import contextlib
import logging
import os

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.task import rbd
from ..orchestra import run

log = logging.getLogger(__name__)

DEFAULT_NUM_RBD = 1
DEFAULT_IMAGE_URL = 'http://ceph.com/qa/ubuntu-12.04.qcow2'
DEFAULT_MEM = 1024 # in megabytes

@contextlib.contextmanager
def create_dirs(ctx, config):
    for client, client_config in config.iteritems():
        assert 'test' in client_config, 'You must specify a test to run'
        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'install', '-d', '-m0755', '--',
                '/tmp/cephtest/qemu',
                '/tmp/cephtest/archive/qemu',
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
                    'rmdir', '/tmp/cephtest/qemu', run.Raw('||'), 'true',
                    ]
                )

@contextlib.contextmanager
def generate_iso(ctx, config):
    log.info('generating iso...')
    for client, client_config in config.iteritems():
        assert 'test' in client_config, 'You must specify a test to run'
        src_dir = os.path.dirname(__file__)
        userdata_path = os.path.join('/tmp/cephtest/qemu', 'userdata.' + client)
        metadata_path = os.path.join('/tmp/cephtest/qemu', 'metadata.' + client)

        with file(os.path.join(src_dir, 'userdata_setup.yaml'), 'rb') as f:
            test_setup = ''.join(f.readlines())

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

        (remote,) = ctx.cluster.only(client).remotes.keys()
        teuthology.write_file(remote, userdata_path, StringIO(user_data))

        with file(os.path.join(src_dir, 'metadata.yaml'), 'rb') as f:
            teuthology.write_file(remote, metadata_path, f)

        test_file = '/tmp/cephtest/qemu/{client}.test.sh'.format(client=client)
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
                '-o', '/tmp/cephtest/qemu/{client}.iso'.format(client=client),
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
                    '/tmp/cephtest/qemu/{client}.iso'.format(client=client),
                    os.path.join('/tmp/cephtest/qemu', 'userdata.' + client),
                    os.path.join('/tmp/cephtest/qemu', 'metadata.' + client),
                    '/tmp/cephtest/qemu/{client}.test.sh'.format(client=client),
                    ],
                )

@contextlib.contextmanager
def download_image(ctx, config):
    log.info('downloading base image')
    for client, client_config in config.iteritems():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        base_file = '/tmp/cephtest/qemu/base.{client}.qcow2'.format(client=client)
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
            base_file = '/tmp/cephtest/qemu/base.{client}.qcow2'.format(
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
    procs = []
    for client, client_config in config.iteritems():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        log_dir = '/tmp/cephtest/archive/qemu/{client}'.format(client=client)
        remote.run(
            args=[
                'mkdir', log_dir,
                ]
            )

        base_file = '/tmp/cephtest/qemu/base.{client}.qcow2'.format(client=client)
        args=[
            run.Raw('LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib'),
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            '/tmp/cephtest/archive/coverage',
            '/tmp/cephtest/daemon-helper',
            'term',
            'kvm', '-enable-kvm', '-nographic',
            '-m', str(client_config.get('memory', DEFAULT_MEM)),
            # base OS device
            '-drive',
            'file={base},format=qcow2,if=virtio'.format(base=base_file),
            # cd holding metadata for cloud-init
            '-cdrom', '/tmp/cephtest/qemu/{client}.iso'.format(client=client),
            # virtio 9p fs for logging
            '-fsdev',
            'local,id=log,path={log},security_model=none'.format(log=log_dir),
            '-device',
            'virtio-9p-pci,fsdev=log,mount_tag=test_log',
            ]

        for i in xrange(client_config.get('num_rbd', DEFAULT_NUM_RBD)):
            args.extend([
                '-drive',
                'file=rbd:rbd/{img}:conf={conf}:id={id},format=rbd,if=virtio'.format(
                    conf='/tmp/cephtest/ceph.conf',
                    img='{client}.{num}'.format(client=client, num=i),
                    id=client[len('client.'):],
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
            remote.run(
                args=[
                    'test', '-f',
                    '/tmp/cephtest/archive/qemu/{client}/success'.format(
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
    """
    assert isinstance(config, dict), \
           "task qemu only supports a dictionary for configuration"

    config = teuthology.replace_all_with_clients(ctx.cluster, config)

    managers = []
    for client, client_config in config.iteritems():
        num_rbd = client_config.get('num_rbd', 1)
        assert num_rbd > 0, 'at least one rbd device must be used'
        for i in xrange(num_rbd):
            create_config = {
                client: {
                    'image_name':
                    '{client}.{num}'.format(client=client, num=i),
                    }
                }
            managers.append(
                lambda create_config=create_config:
                rbd.create_image(ctx=ctx, config=create_config)
                )

    managers.extend([
        lambda: create_dirs(ctx=ctx, config=config),
        lambda: generate_iso(ctx=ctx, config=config),
        lambda: download_image(ctx=ctx, config=config),
        lambda: run_qemu(ctx=ctx, config=config),
        ])

    with contextutil.nested(*managers):
        yield
