"""
Qemu task
"""

import contextlib
import logging
import os
import yaml
import time

from tasks import rbd
from tasks.util.workunit import get_refspec_after_overrides
from teuthology import contextutil
from teuthology import misc as teuthology
from teuthology.config import config as teuth_config
from teuthology.orchestra import run

log = logging.getLogger(__name__)

DEFAULT_NUM_DISKS = 2
DEFAULT_IMAGE_URL = 'http://download.ceph.com/qa/ubuntu-12.04.qcow2'
DEFAULT_IMAGE_SIZE = 10240 # in megabytes
DEFAULT_CPUS = 1
DEFAULT_MEM = 4096 # in megabytes

def create_images(ctx, config, managers):
    for client, client_config in config.items():
        disks = client_config.get('disks', DEFAULT_NUM_DISKS)
        if not isinstance(disks, list):
            disks = [{} for n in range(int(disks))]
        clone = client_config.get('clone', False)
        assert disks, 'at least one rbd device must be used'
        for i, disk in enumerate(disks[1:]):
            create_config = {
                client: {
                    'image_name': '{client}.{num}'.format(client=client,
                                                          num=i + 1),
                    'image_format': 2 if clone else 1,
                    'image_size': (disk or {}).get('image_size',
                                                   DEFAULT_IMAGE_SIZE),
                    }
                }
            managers.append(
                lambda create_config=create_config:
                rbd.create_image(ctx=ctx, config=create_config)
                )

def create_clones(ctx, config, managers):
    for client, client_config in config.items():
        clone = client_config.get('clone', False)
        if clone:
            num_disks = client_config.get('disks', DEFAULT_NUM_DISKS)
            if isinstance(num_disks, list):
                num_disks = len(num_disks)
            for i in range(num_disks):
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
    for client, client_config in config.items():
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
        for client, client_config in config.items():
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

    # use ctx.config instead of config, because config has been
    # through teuthology.replace_all_with_clients()
    refspec = get_refspec_after_overrides(ctx.config, {})

    git_url = teuth_config.get_ceph_qa_suite_git_url()
    log.info('Pulling tests from %s ref %s', git_url, refspec)

    for client, client_config in config.items():
        assert 'test' in client_config, 'You must specify a test to run'
        test = client_config['test']

        (remote,) = ctx.cluster.only(client).remotes.keys()

        clone_dir = '{tdir}/qemu_clone.{role}'.format(tdir=testdir, role=client)
        remote.run(args=refspec.clone(git_url, clone_dir))

        src_dir = os.path.dirname(__file__)
        userdata_path = os.path.join(testdir, 'qemu', 'userdata.' + client)
        metadata_path = os.path.join(testdir, 'qemu', 'metadata.' + client)

        with open(os.path.join(src_dir, 'userdata_setup.yaml')) as f:
            test_setup = ''.join(f.readlines())
            # configuring the commands to setup the nfs mount
            mnt_dir = "/export/{client}".format(client=client)
            test_setup = test_setup.format(
                mnt_dir=mnt_dir
            )

        with open(os.path.join(src_dir, 'userdata_teardown.yaml')) as f:
            test_teardown = ''.join(f.readlines())

        user_data = test_setup
        if client_config.get('type', 'filesystem') == 'filesystem':
            num_disks = client_config.get('disks', DEFAULT_NUM_DISKS)
            if isinstance(num_disks, list):
                num_disks = len(num_disks)
            for i in range(1, num_disks):
                dev_letter = chr(ord('a') + i)
                user_data += """
- |
  #!/bin/bash
  mkdir /mnt/test_{dev_letter}
  mkfs -t xfs /dev/vd{dev_letter}
  mount -t xfs /dev/vd{dev_letter} /mnt/test_{dev_letter}
""".format(dev_letter=dev_letter)

        user_data += """
- |
  #!/bin/bash
  test -d /etc/ceph || mkdir /etc/ceph
  cp /mnt/cdrom/ceph.* /etc/ceph/
"""

        cloud_config_archive = client_config.get('cloud_config_archive', [])
        if cloud_config_archive:
          user_data += yaml.safe_dump(cloud_config_archive, default_style='|',
                                      default_flow_style=False)

        # this may change later to pass the directories as args to the
        # script or something. xfstests needs that.
        user_data += """
- |
  #!/bin/bash
  test -d /mnt/test_b && cd /mnt/test_b
  /mnt/cdrom/test.sh > /mnt/log/test.log 2>&1 && touch /mnt/log/success
""" + test_teardown

        user_data = user_data.format(
            ceph_branch=ctx.config.get('branch'),
            ceph_sha1=ctx.config.get('sha1'))
        teuthology.write_file(remote, userdata_path, user_data)

        with open(os.path.join(src_dir, 'metadata.yaml'), 'rb') as f:
            teuthology.write_file(remote, metadata_path, f)

        test_file = '{tdir}/qemu/{client}.test.sh'.format(tdir=testdir, client=client)

        log.info('fetching test %s for %s', test, client)
        remote.run(
            args=[
                'cp', '--', os.path.join(clone_dir, test), test_file,
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
                'ceph.conf=/etc/ceph/ceph.conf',
                'ceph.keyring=/etc/ceph/ceph.keyring',
                'test.sh={file}'.format(file=test_file),
                ],
            )
    try:
        yield
    finally:
        for client in config.keys():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rm', '-rf',
                    '{tdir}/qemu/{client}.iso'.format(tdir=testdir, client=client),
                    os.path.join(testdir, 'qemu', 'userdata.' + client),
                    os.path.join(testdir, 'qemu', 'metadata.' + client),
                    '{tdir}/qemu/{client}.test.sh'.format(tdir=testdir, client=client),
                    '{tdir}/qemu_clone.{client}'.format(tdir=testdir, client=client),
                    ],
                )

@contextlib.contextmanager
def download_image(ctx, config):
    """Downland base image, remove image file when done"""
    log.info('downloading base image')
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        base_file = '{tdir}/qemu/base.{client}.qcow2'.format(tdir=testdir, client=client)
        image_url = client_config.get('image_url', DEFAULT_IMAGE_URL)
        remote.run(
            args=[
                'wget', '-nv', '-O', base_file, image_url,
                ]
            )

        disks = client_config.get('disks', None)
        if not isinstance(disks, list):
            disks = [{}]
        image_name = '{client}.0'.format(client=client)
        image_size = (disks[0] or {}).get('image_size', DEFAULT_IMAGE_SIZE)
        remote.run(
            args=[
                'qemu-img', 'convert', '-f', 'qcow2', '-O', 'raw',
                base_file, 'rbd:rbd/{image_name}'.format(image_name=image_name)
                ]
            )
        remote.run(
            args=[
                'rbd', 'resize',
                '--size={image_size}M'.format(image_size=image_size),
                image_name,
                ]
            )
    try:
        yield
    finally:
        log.debug('cleaning up base image files')
        for client in config.keys():
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


def _setup_nfs_mount(remote, client, service_name, mount_dir):
    """
    Sets up an nfs mount on the remote that the guest can use to
    store logs. This nfs mount is also used to touch a file
    at the end of the test to indicate if the test was successful
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
    log.info("Deleting export from /etc/exports...")
    remote.run(args=[
        'sudo', 'sed', '-i', "\|{export_dir}|d".format(export_dir=export_dir),
        '/etc/exports'
    ])
    remote.run(args=[
        'echo', export, run.Raw("|"),
        'sudo', 'tee', '-a', "/etc/exports",
    ])
    log.info("Restarting NFS...")
    if remote.os.package_type == "deb":
        remote.run(args=['sudo', 'service', 'nfs-kernel-server', 'restart'])
    else:
        remote.run(args=['sudo', 'systemctl', 'restart', service_name])


def _teardown_nfs_mount(remote, client, service_name):
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
            'sudo', 'systemctl', 'stop', service_name
        ])
    log.info("Unmounting exported directory...")
    remote.run(args=[
        'sudo', 'umount', export_dir
    ])
    log.info("Deleting export from /etc/exports...")
    remote.run(args=[
        'sudo', 'sed', '-i', "\|{export_dir}|d".format(export_dir=export_dir),
        '/etc/exports'
    ])
    log.info("Starting NFS...")
    if remote.os.package_type == "deb":
        remote.run(args=[
            'sudo', 'service', 'nfs-kernel-server', 'start'
        ])
    else:
        remote.run(args=[
            'sudo', 'systemctl', 'start', service_name
        ])


@contextlib.contextmanager
def run_qemu(ctx, config):
    """Setup kvm environment and start qemu"""
    procs = []
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        log_dir = '{tdir}/archive/qemu/{client}'.format(tdir=testdir, client=client)
        remote.run(
            args=[
                'mkdir', log_dir, run.Raw('&&'),
                'sudo', 'modprobe', 'kvm',
                ]
            )

        nfs_service_name = 'nfs'
        if remote.os.name in ['rhel', 'centos'] and float(remote.os.version) >= 8:
            nfs_service_name = 'nfs-server'

        # make an nfs mount to use for logging and to
        # allow to test to tell teuthology the tests outcome
        _setup_nfs_mount(remote, client, nfs_service_name, log_dir)

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
            qemu_cmd, '-enable-kvm', '-nographic', '-cpu', 'host',
            '-smp', str(client_config.get('cpus', DEFAULT_CPUS)),
            '-m', str(client_config.get('memory', DEFAULT_MEM)),
            # cd holding metadata for cloud-init
            '-cdrom', '{tdir}/qemu/{client}.iso'.format(tdir=testdir, client=client),
            ]

        cachemode = 'none'
        ceph_config = ctx.ceph['ceph'].conf.get('global', {})
        ceph_config.update(ctx.ceph['ceph'].conf.get('client', {}))
        ceph_config.update(ctx.ceph['ceph'].conf.get(client, {}))
        if ceph_config.get('rbd cache', True):
            if ceph_config.get('rbd cache max dirty', 1) > 0:
                cachemode = 'writeback'
            else:
                cachemode = 'writethrough'

        clone = client_config.get('clone', False)
        num_disks = client_config.get('disks', DEFAULT_NUM_DISKS)
        if isinstance(num_disks, list):
            num_disks = len(num_disks)
        for i in range(num_disks):
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
        time_wait = client_config.get('time_wait', 0)

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

        if time_wait > 0:
            log.debug('waiting {time_wait} sec for workloads detect finish...'.format(
                time_wait=time_wait));
            time.sleep(time_wait)

        log.debug('checking that qemu tests succeeded...')
        for client in config.keys():
            (remote,) = ctx.cluster.only(client).remotes.keys()

            # ensure we have permissions to all the logs
            log_dir = '{tdir}/archive/qemu/{client}'.format(tdir=testdir,
                                                            client=client)
            remote.run(
                args=[
                    'sudo', 'chmod', 'a+rw', '-R', log_dir
                    ]
                )

            # teardown nfs mount
            _teardown_nfs_mount(remote, client, nfs_service_name)
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
        log.info("Deleting exported directory...")
        for client in config.keys():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(args=[
                'sudo', 'rm', '-r', '/export'
            ])


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
              disks: 2

    - or -

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://ceph.com/qa/test.sh
              type: block
              disks:
                - image_size: 1024
                - image_size: 2048

    You can set the amount of CPUs and memory the VM has (default is 1 CPU and
    4096 MB)::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://download.ceph.com/qa/test.sh
              cpus: 4
              memory: 512 # megabytes

    If you want to run a test against a cloned rbd image, set clone to true::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://download.ceph.com/qa/test.sh
              clone: true

    If you need to configure additional cloud-config options, set cloud_config
    to the required data set::

        tasks:
        - ceph
        - qemu:
            client.0:
                test: http://ceph.com/qa/test.sh
                cloud_config_archive:
                    - |
                      #/bin/bash
                      touch foo1
                    - content: |
                        test data
                      type: text/plain
                      filename: /tmp/data

    If you need to override the default cloud image, set image_url:

        tasks:
        - ceph
        - qemu:
            client.0:
                test: http://ceph.com/qa/test.sh
                image_url: https://cloud-images.ubuntu.com/releases/16.04/release/ubuntu-16.04-server-cloudimg-amd64-disk1.img
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
