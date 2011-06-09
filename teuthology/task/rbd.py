import contextlib
import logging

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
        size = properties.get('image_size', 1024)
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Creating image {name} with size {size}'.format(name=name,
                                                                 size=size))
        remote.run(
            args=[
                'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
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
def task(ctx, config):
    create_image(ctx, config)
