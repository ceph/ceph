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
from teuthology.packaging import install_package, remove_package

log = logging.getLogger(__name__)

DEFAULT_NUM_DISKS = 2
DEFAULT_IMAGE_URL = 'http://download.ceph.com/qa/ubuntu-12.04.qcow2'
DEFAULT_IMAGE_SIZE = 10240 # in megabytes
ENCRYPTION_HEADER_SIZE = 16 # in megabytes
DEFAULT_CPUS = 1
DEFAULT_MEM = 4096 # in megabytes

def normalize_disks(config):
    # normalize the 'disks' parameter into a list of dictionaries
    for client, client_config in config.items():
        clone = client_config.get('clone', False)
        image_url = client_config.get('image_url', DEFAULT_IMAGE_URL)
        device_type = client_config.get('type', 'filesystem')
        encryption_format = client_config.get('encryption_format', 'none')
        parent_encryption_format = client_config.get(
            'parent_encryption_format', 'none')

        disks = client_config.get('disks', DEFAULT_NUM_DISKS)
        if not isinstance(disks, list):
            disks = [{'image_name': '{client}.{num}'.format(client=client,
                                                            num=i)}
                     for i in range(int(disks))]
            client_config['disks'] = disks

        for i, disk in enumerate(disks):
            if 'action' not in disk:
                disk['action'] = 'create'
            assert disk['action'] in ['none', 'create', 'clone'], 'invalid disk action'
            assert disk['action'] != 'clone' or 'parent_name' in disk, 'parent_name required for clone'

            if 'image_size' not in disk:
                disk['image_size'] = DEFAULT_IMAGE_SIZE
            disk['image_size'] = int(disk['image_size'])

            if 'image_url' not in disk and i == 0:
                disk['image_url'] = image_url

            if 'device_type' not in disk:
                disk['device_type'] = device_type

            disk['device_letter'] = chr(ord('a') + i)

            if 'encryption_format' not in disk:
                if clone:
                    disk['encryption_format'] = parent_encryption_format
                else:
                    disk['encryption_format'] = encryption_format
            assert disk['encryption_format'] in ['none', 'luks1', 'luks2'], 'invalid encryption format'

        assert disks, 'at least one rbd device must be used'

        if clone:
            for disk in disks:
                if disk['action'] != 'create':
                    continue
                clone = dict(disk)
                clone['action'] = 'clone'
                clone['parent_name'] = clone['image_name']
                clone['image_name'] += '-clone'
                del disk['device_letter']

                clone['encryption_format'] = encryption_format
                assert clone['encryption_format'] in ['none', 'luks1', 'luks2'], 'invalid encryption format'

                clone['parent_encryption_format'] = parent_encryption_format
                assert clone['parent_encryption_format'] in ['none', 'luks1', 'luks2'], 'invalid encryption format'

                disks.append(clone)

def create_images(ctx, config, managers):
    for client, client_config in config.items():
        disks = client_config['disks']
        for disk in disks:
            if disk.get('action') != 'create' or (
                    'image_url' in disk and
                    disk['encryption_format'] == 'none'):
                continue
            image_size = disk['image_size']
            if disk['encryption_format'] != 'none':
                image_size += ENCRYPTION_HEADER_SIZE
            create_config = {
                client: {
                    'image_name': disk['image_name'],
                    'image_format': 2,
                    'image_size': image_size,
                    'encryption_format': disk['encryption_format'],
                    }
                }
            managers.append(
                lambda create_config=create_config:
                rbd.create_image(ctx=ctx, config=create_config)
                )

def create_clones(ctx, config, managers):
    for client, client_config in config.items():
        disks = client_config['disks']
        for disk in disks:
            if disk['action'] != 'clone':
                continue

            create_config = {
                client: {
                    'image_name': disk['image_name'],
                    'parent_name': disk['parent_name'],
                    'encryption_format': disk['encryption_format'],
                    }
                }
            managers.append(
                lambda create_config=create_config:
                rbd.clone_image(ctx=ctx, config=create_config)
                )

def create_encrypted_devices(ctx, config, managers):
    for client, client_config in config.items():
        disks = client_config['disks']
        for disk in disks:
            if (disk['encryption_format'] == 'none' and
                disk.get('parent_encryption_format', 'none') == 'none') or \
                    'device_letter' not in disk:
                continue

            dev_config = {client: disk}
            managers.append(
                lambda dev_config=dev_config:
                rbd.dev_create(ctx=ctx, config=dev_config)
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
def install_block_rbd_driver(ctx, config):
    """
    Make sure qemu rbd block driver (block-rbd.so) is installed
    """
    packages = {}
    for client, _ in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        if remote.os.package_type == 'rpm':
            packages[client] = ['qemu-kvm-block-rbd']
        else:
            packages[client] = ['qemu-block-extra', 'qemu-utils']
        for pkg in packages[client]:
            install_package(pkg, remote)
    try:
        yield
    finally:
        for client, _ in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            for pkg in packages[client]:
                remove_package(pkg, remote)

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

        disks = client_config['disks']
        for disk in disks:
            if disk['device_type'] != 'filesystem' or \
                    'device_letter' not in disk or \
                    'image_url' in disk:
                continue
            if disk['encryption_format'] == 'none' and \
                    disk.get('parent_encryption_format', 'none') == 'none':
                dev_name = 'vd' + disk['device_letter']
            else:
                # encrypted disks use if=ide interface, instead of if=virtio
                dev_name = 'sd' + disk['device_letter']
            user_data += """
- |
  #!/bin/bash
  mkdir /mnt/test_{dev_name}
  mkfs -t xfs /dev/{dev_name}
  mount -t xfs /dev/{dev_name} /mnt/test_{dev_name}
""".format(dev_name=dev_name)

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
        remote.write_file(userdata_path, user_data)

        with open(os.path.join(src_dir, 'metadata.yaml'), 'rb') as f:
            remote.write_file(metadata_path, f)

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

    client_base_files = {}
    for client, client_config in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        client_base_files[client] = []
        disks = client_config['disks']
        for disk in disks:
            if disk['action'] != 'create' or 'image_url' not in disk:
                continue

            base_file = '{tdir}/qemu/base.{name}.qcow2'.format(tdir=testdir,
                                                               name=disk['image_name'])
            client_base_files[client].append(base_file)

            remote.run(
                args=[
                    'wget', '-nv', '-O', base_file, disk['image_url'],
                    ]
                )

            if disk['encryption_format'] == 'none':
                remote.run(
                    args=[
                        'qemu-img', 'convert', '-f', 'qcow2', '-O', 'raw',
                        base_file, 'rbd:rbd/{image_name}'.format(image_name=disk['image_name'])
                        ]
                    )
            else:
                dev_config = {client: {'image_name': disk['image_name'],
                                       'encryption_format': disk['encryption_format']}}
                raw_file = '{tdir}/qemu/base.{name}.raw'.format(
                    tdir=testdir, name=disk['image_name'])
                client_base_files[client].append(raw_file)
                remote.run(
                    args=[
                        'qemu-img', 'convert', '-f', 'qcow2', '-O', 'raw',
                        base_file, raw_file
                        ]
                    )
                with rbd.dev_create(ctx, dev_config):
                    remote.run(
                        args=[
                            'dd', 'if={name}'.format(name=raw_file),
                            'of={name}'.format(name=dev_config[client]['device_path']),
                            'bs=4M', 'conv=fdatasync'
                            ]
                        )

        for disk in disks:
            if disk['action'] == 'clone' or \
                    disk['encryption_format'] != 'none' or \
                    (disk['action'] == 'create' and 'image_url' not in disk):
                continue

            remote.run(
                args=[
                    'rbd', 'resize',
                    '--size={image_size}M'.format(image_size=disk['image_size']),
                    disk['image_name'], run.Raw('||'), 'true'
                    ]
                )

    try:
        yield
    finally:
        log.debug('cleaning up base image files')
        for client, base_files in client_base_files.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            for base_file in base_files:
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

        disks = client_config['disks']
        for disk in disks:
            if 'device_letter' not in disk:
                continue

            if disk['encryption_format'] == 'none' and \
                    disk.get('parent_encryption_format', 'none') == 'none':
                interface = 'virtio'
                disk_spec = 'rbd:rbd/{img}:id={id}'.format(
                    img=disk['image_name'],
                    id=client[len('client.'):]
                    )
            else:
                # encrypted disks use ide as a temporary workaround for
                # a bug in qemu when using virtio over nbd
                # TODO: use librbd encryption directly via qemu (not via nbd)
                interface = 'ide'
                disk_spec = disk['device_path']

            args.extend([
                '-drive',
                'file={disk_spec},format=raw,if={interface},cache={cachemode}'.format(
                    disk_spec=disk_spec,
                    interface=interface,
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

    For tests that want to explicitly describe the RBD images to connect:

        tasks:
        - ceph:
        - qemu:
            client.0:
                test: http://download.ceph.com/qa/test.sh
                clone: True/False (optionally clone all created disks),
                image_url: <URL> (optional default image URL)
                type: filesystem / block (optional default device type)
                disks: [
                    {
                        action: create / clone / none (optional, defaults to create)
                        image_name: <image name> (optional)
                        parent_name: <parent_name> (if action == clone),
                        type: filesystem / block (optional, defaults to filesystem)
                        image_url: <URL> (optional),
                        image_size: <MiB> (optional)
                        encryption_format: luks1 / luks2 / none (optional, defaults to none)
                    }, ...
                ]

    You can set the amount of CPUs and memory the VM has (default is 1 CPU and
    4096 MB)::

        tasks:
        - ceph:
        - qemu:
            client.0:
              test: http://download.ceph.com/qa/test.sh
              cpus: 4
              memory: 512 # megabytes

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
    """
    assert isinstance(config, dict), \
           "task qemu only supports a dictionary for configuration"

    config = teuthology.replace_all_with_clients(ctx.cluster, config)
    normalize_disks(config)

    managers = []
    create_images(ctx=ctx, config=config, managers=managers)
    managers.extend([
        lambda: create_dirs(ctx=ctx, config=config),
        lambda: install_block_rbd_driver(ctx=ctx, config=config),
        lambda: generate_iso(ctx=ctx, config=config),
        lambda: download_image(ctx=ctx, config=config),
        ])
    create_clones(ctx=ctx, config=config, managers=managers)
    create_encrypted_devices(ctx=ctx, config=config, managers=managers)
    managers.append(
        lambda: run_qemu(ctx=ctx, config=config),
        )

    with contextutil.nested(*managers):
        yield
