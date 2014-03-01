"""
Common filesystem related utilities.  Originally this
code was part of rbd.py.  It was broken out so that it
could be used by other modules (tgt.py and iscsi.py for instance).
"""
import logging
import contextlib
from teuthology import misc as teuthology

log = logging.getLogger(__name__)


def default_image_name(role):
    """
    Image name used by rbd and iscsi
    """
    return 'testimage.{role}'.format(role=role)


@contextlib.contextmanager
def generic_mkfs(ctx, config, devname_rtn):
    """
    Create a filesystem (either rbd or tgt, depending on devname_rtn)

    Rbd for example, now makes the following calls:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
        - rbd.dev_create: [client.0]
        - common_fs_utils.generic_mkfs: [client.0]
        - common_fs_utils.generic_mount:
            client.0: testimage.client.0
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
        remote = teuthology.get_single_remote_value(ctx, role)
        image = properties.get('image_name', default_image_name(role))
        fs_type = properties.get('fs_type', 'ext3')
        remote.run(
            args=[
                'sudo',
                'mkfs',
                '-t', fs_type,
                devname_rtn(ctx, image),
                ],
            )
    yield


@contextlib.contextmanager
def generic_mount(ctx, config, devname_rtn):
    """
    Generic Mount an rbd or tgt image.

    Rbd for example, now makes the following calls:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
        - rbd.dev_create: [client.0]
        - common_fs_utils.generic_mkfs: [client.0]
        - common_fs_utils.generic_mount:
            client.0: testimage.client.0
    """
    assert isinstance(config, list) or isinstance(config, dict), \
        "task mount must be configured with a list or dictionary"
    if isinstance(config, dict):
        role_images = config.items()
    else:
        role_images = [(role, None) for role in config]

    def strip_client_prefix(role):
        """
        Extract the number from the name of a client role
        """
        prefix = 'client.'
        assert role.startswith(prefix)
        id_ = role[len(prefix):]
        return id_

    testdir = teuthology.get_testdir(ctx)

    mnt_template = '{tdir}/mnt.{id}'
    mounted = []
    for role, image in role_images:
        if image is None:
            image = default_image_name(role)
        remote = teuthology.get_single_remote_value(ctx, role)
        id_ = strip_client_prefix(role)
        mnt = mnt_template.format(tdir=testdir, id=id_)
        mounted.append((remote, mnt))
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
                devname_rtn(ctx, image),
                mnt,
                ],
            )

    try:
        yield
    finally:
        log.info("Unmounting rbd images... %s", mounted)
        for remote, mnt in mounted:
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
