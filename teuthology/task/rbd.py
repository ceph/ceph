import contextlib
import logging

from orchestra import run
from teuthology import misc as teuthology
from teuthology import contextutil

log = logging.getLogger(__name__)

def default_image_name(role):
    return 'testimage.{role}'.format(role=role)

@contextlib.contextmanager
def create_image(ctx, config):
    """
    Create an rbd image.

    For example::

        tasks:
        - ceph:
        - rbd.create_image:
            client.0:
                image_name: testimage
                image_size: 100
            client.1:
    """
    assert isinstance(config, dict) or isinstance(config, list), \
        "task create_image only supports a list or dictionary for configuration"

    if isinstance(config, dict):
        images = config.items()
    else:
        images = [(role, None) for role in config]

    for role, properties in images:
        if properties is None:
            properties = {}
        name = properties.get('image_name', default_image_name(role))
        size = properties.get('image_size', 10240)
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Creating image {name} with size {size}'.format(name=name,
                                                                 size=size))
        remote.run(
            args=[
                'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/binary/usr/local/bin/rbd',
                '-c', '/tmp/cephtest/ceph.conf',
                '-p', 'rbd',
                'create',
                '-s', str(size),
                name,
                ],
            )
    try:
        yield
    finally:
        log.info('Deleting rbd images...')
        for role, properties in images:
            if properties is None:
                properties = {}
            name = properties.get('image_name', default_image_name(role))
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    '/tmp/cephtest/archive/coverage',
                    '/tmp/cephtest/binary/usr/local/bin/rbd',
                    '-c', '/tmp/cephtest/ceph.conf',
                    '-p', 'rbd',
                    'rm',
                    name,
                    ],
                )

@contextlib.contextmanager
def modprobe(ctx, config):
    """
    Load the rbd kernel module..

    For example::

        tasks:
        - ceph:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
    """
    log.info('Loading rbd kernel module...')
    for role in config:
        (remote,) = ctx.cluster.only(role).remotes.keys()
        remote.run(
            args=[
                'sudo',
                'modprobe',
                'rbd',
                ],
            )
    try:
        yield
    finally:
        log.info('Unloading rbd kernel module...')
        for role in config:
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'sudo',
                    'modprobe',
                    '-r',
                    'rbd',
                    ],
                )

@contextlib.contextmanager
def dev_create(ctx, config):
    """
    Map block devices to rbd images.

    For example::

        tasks:
        - ceph:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
        - rbd.dev_create:
            client.0: testimage.client.0
    """
    assert isinstance(config, dict) or isinstance(config, list), \
        "task dev_create only supports a list or dictionary for configuration"

    if isinstance(config, dict):
        role_images = config.items()
    else:
        role_images = [(role, None) for role in config]

    log.info('Creating rbd block devices...')
    for role, image in role_images:
        if image is None:
            image = default_image_name(role)
        (remote,) = ctx.cluster.only(role).remotes.keys()

        # add udev rule for creating /dev/rbd/pool/image
        remote.run(
            args=[
                'echo',
                'KERNEL=="rbd[0-9]*", PROGRAM="/tmp/cephtest/binary/usr/local/bin/crbdnamer %n", SYMLINK+="rbd/%c{1}/%c{2}"',
                run.Raw('>'),
                '/tmp/cephtest/51-rbd.rules',
                ],
            )
        remote.run(
            args=[
                'sudo',
                'mv',
                '/tmp/cephtest/51-rbd.rules',
                '/etc/udev/rules.d/',
                ],
            )

        secretfile = '/tmp/cephtest/data/{role}.secret'.format(role=role)
        teuthology.write_secret_file(remote, role, secretfile)

        remote.run(
            args=[
                'sudo',
                'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/binary/usr/local/bin/rbd',
                '-c', '/tmp/cephtest/ceph.conf',
                '--user', role.rsplit('.')[-1],
                '--secret', secretfile,
                '-p', 'rbd',
                'map',
                image,
                run.Raw('&&'),
                # wait for the symlink to be created by udev
                'while', 'test', '!', '-e', '/dev/rbd/rbd/{image}'.format(image=image), run.Raw(';'), 'do',
                'sleep', '1', run.Raw(';'),
                'done',
                ],
            )
    try:
        yield
    finally:
        log.info('Unmapping rbd devices...')
        for role, image in role_images:
            if image is None:
                image = default_image_name(role)
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'sudo',
                    'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                    '/tmp/cephtest/enable-coredump',
                    '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                    '/tmp/cephtest/archive/coverage',
                    '/tmp/cephtest/binary/usr/local/bin/rbd',
                    '-c', '/tmp/cephtest/ceph.conf',
                    '-p', 'rbd',
                    'unmap',
                    '/dev/rbd/rbd/{imgname}'.format(imgname=image),
                    run.Raw('&&'),
                    # wait for the symlink to be deleted by udev
                    'while', 'test', '-e', '/dev/rbd/rbd/{image}'.format(image=image),
                    run.Raw(';'),
                    'do',
                    'sleep', '1', run.Raw(';'),
                    'done',
                    ],
                )
            remote.run(
                args=[
                    'sudo',
                    'rm',
                    '/etc/udev/rules.d/51-rbd.rules',
                    ],
                wait=False,
                )

@contextlib.contextmanager
def mkfs(ctx, config):
    """
    Create a filesystem on a block device.

    For example::

        tasks:
        - ceph:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
        - rbd.dev_create: [client.0]
        - rbd.mkfs:
            client.0:
                fs_type: xfs
    """
    assert isinstance(config, list) or isinstance(config, dict), \
        "task mkfs must be configured with a list or dictionary"
    if isinstance(config, dict):
        images = config.items()
    else:
        images = [(role, None) for role in config]

    for role, properties in images:
        if properties is None:
            properties = {}
        (remote,) = ctx.cluster.only(role).remotes.keys()
        image = properties.get('image_name', default_image_name(role))
        fs = properties.get('fs_type', 'ext3')
        remote.run(
            args=[
                'sudo',
                'mkfs',
                '-t', fs,
                '/dev/rbd/rbd/{image}'.format(image=image),
                ],
            )
    yield

@contextlib.contextmanager
def mount(ctx, config):
    """
    Mount an rbd image.

    For example::

        tasks:
        - ceph:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
        - rbd.mkfs: [client.0]
        - rbd.mount:
            client.0: testimage.client.0
    """
    assert isinstance(config, list) or isinstance(config, dict), \
        "task mount must be configured with a list or dictionary"
    if isinstance(config, dict):
        role_images = config.items()
    else:
        role_images = [(role, None) for role in config]

    def strip_client_prefix(role):
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        return id_

    mnt_template = '/tmp/cephtest/mnt.{id}'
    for role, image in role_images:
        if image is None:
            image = default_image_name(role)
        (remote,) = ctx.cluster.only(role).remotes.keys()
        id_ = strip_client_prefix(role)
        mnt = mnt_template.format(id=id_)
        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
                ]
            )

        remote.run(
            args=[
                'sudo',
                'mount',
                '/dev/rbd/rbd/{image}'.format(image=image),
                mnt,
                ],
            )

    try:
        yield
    finally:
        log.info("Unmounting rbd images...")
        for role, image in role_images:
            if image is None:
                image = default_image_name(role)
            (remote,) = ctx.cluster.only(role).remotes.keys()
            id_ = strip_client_prefix(role)
            mnt = mnt_template.format(id=id_)
            remote.run(
                args=[
                    'sudo',
                    'umount',
                    mnt,
                    ],
                )

            remote.run(
                args=[
                    'rmdir',
                    '--',
                    mnt,
                    ]
                )

@contextlib.contextmanager
def task(ctx, config):
    """
    Create and mount an rbd image.

    For example::

        tasks:
        - ceph:
        - rbd: [client.0, client.1]

    Different image options::

        tasks:
        - ceph:
        - rbd:
            client.0: # uses defaults
            client.1:
                image_name: foo
                image_size: 2048
                fs_type: xfs
    """
    assert isinstance(config, list) or isinstance(config, dict), \
        "task rbd only supports a list or dict for configuration"
    if isinstance(config, dict):
        role_images = {}
        for role, properties in config.iteritems():
            if properties is None:
                properties = {}
            role_images[role] = properties.get('image_name')
    else:
        role_images = config

    with contextutil.nested(
        lambda: create_image(ctx=ctx, config=config),
        lambda: modprobe(ctx=ctx, config=config),
        lambda: dev_create(ctx=ctx, config=role_images),
        lambda: mkfs(ctx=ctx, config=config),
        lambda: mount(ctx=ctx, config=role_images),
        ):
        yield
