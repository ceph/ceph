"""
Handle iscsi adm commands for tgt connections.
"""
import logging
import contextlib
import socket

from cStringIO import StringIO
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.task.common_fs_utils import generic_mkfs
from teuthology.task.common_fs_utils import generic_mount
from ..orchestra import run

log = logging.getLogger(__name__)


def _get_remote(remotes, client):
    """
    Get remote object that is associated with the client specified.
    """
    for rem in remotes:
        if client in remotes[rem]:
            return rem


def _get_remote_name(remotes, client):
    """
    Get remote name that is associated with the client specified.
    """
    rem_name = _get_remote(remotes, client).name
    rem_name = rem_name[rem_name.find('@') + 1:]
    return rem_name


def tgt_devname_get(ctx, test_image):
    """
    Get the name of the newly created device by following the by-path
    link (which is symbolically linked to the appropriate /dev/sd* file).
    """
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    rem_name = _get_remote_name(remotes, test_image)
    lnkpath = '/dev/disk/by-path/ip-%s:3260-iscsi-rbd-lun-1' % \
            socket.gethostbyname(rem_name)
    return lnkpath


def tgt_devname_rtn(ctx, test_image):
    """
    Wrapper passed to common_fs_util functions.
    """
    image = test_image[test_image.find('.') + 1:]
    return tgt_devname_get(ctx, image)


def file_io_test(rem, file_from, lnkpath):
    """
    dd to the iscsi inteface, read it, and compare with original
    """
    rem.run(
        args=[
        'sudo',
        'dd',
        'if=%s' % file_from,
        'of=%s' % lnkpath,
        'bs=1024',
        'conv=fsync',
    ])
    proc = rem.run(args=['mktemp'], stdout=StringIO(),)
    tfile2 = proc.stdout.getvalue().strip()
    rem.run(
        args=[
        'sudo',
        'rbd',
        'export',
        'iscsi-image',
        run.Raw('-'),
        run.Raw('>'),
        tfile2,
    ])
    proc = rem.run(
        args=[
            'ls',
            '-l',
            file_from,
            run.Raw('|'),
            'awk',
            '{print $5}', ],
        stdout=StringIO(),
        )
    size = proc.stdout.getvalue().strip()
    rem.run(
        args=[
            'cmp',
            '-n',
            size,
            file_from,
            tfile2,
    ])
    rem.run(args=['rm', tfile2])


def general_io_test(ctx, rem, image_name):
    """
    Do simple I/O tests to the iscsi interface before putting a
    filesystem on it.
    """
    test_phrase = 'The time has come the walrus said to speak of many things.'
    lnkpath = tgt_devname_get(ctx, image_name)
    proc = rem.run(args=['mktemp'], stdout=StringIO(),)
    tfile1 = proc.stdout.getvalue().strip()
    rem.run(
        args=[
            'echo',
            test_phrase,
            run.Raw('>'),
            tfile1,
        ])
    file_io_test(rem, tfile1, lnkpath)
    rem.run(args=['rm', tfile1])
    file_io_test(rem, '/bin/ls', lnkpath)


@contextlib.contextmanager
def start_iscsi_initiators(ctx, tgt_link):
    """
    This is the sub-task that assigns an rbd to an iscsiadm control and
    performs a login (thereby creating a /dev/sd device).  It performs
    a logout when finished.
    """
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    tgtd_list = []
    for role, host in tgt_link:
        rem = _get_remote(remotes, role)
        rem_name = _get_remote_name(remotes, host)
        rem.run(
            args=[
                'sudo',
                'iscsiadm',
                '-m',
                'discovery',
                '-t',
                'st',
                '-p',
                rem_name,
        ])
        proc = rem.run(
            args=[
                'sudo',
                'iscsiadm',
                '-m',
                'node',
                '--login',
        ])
        if proc.exitstatus == 0:
            tgtd_list.append((rem, rem_name))
        rem.run(
            args=[
                'udevadm',
                'settle',
        ])
        general_io_test(ctx, rem, host)
        with contextutil.nested(
            lambda: generic_mkfs(ctx=ctx, config={host: {'fs_type': 'xfs'}},
                    devname_rtn=tgt_devname_rtn),
            lambda: generic_mount(ctx=ctx, config={host: None},
                    devname_rtn=tgt_devname_rtn),
            ):
            pass
    try:
        yield
    finally:
        for rem_info in tgtd_list:
            rem = rem_info[0]
            rem_name = rem_info[1]
            rem.run(
                args=[
                    'sudo',
                    'iscsiadm',
                    '-m',
                    'node',
                    '--logout',
            ])
            rem.run(
                args=[
                    'sudo',
                    'iscsiadm',
                    '-m',
                    'discovery',
                    '-p',
                    rem_name,
                    '-o',
                    'delete',
            ])


@contextlib.contextmanager
def task(ctx, config):
    """
    handle iscsi admin login after a tgt connection has been established.

    Assume a default host client of client.0 and a sending client of
    client.0 if not specified otherwise.

    Sample tests could be:

    iscsi:

        This sets up a tgt link from client.0 to client.0

    iscsi: [client.1, client.2]

        This sets up a tgt link from client.1 to client.0 and a tgt link
        from client.2 to client.0

    iscsi:
        client.0: client.1
        client.1: client.0

        This sets up a tgt link from client.0 to client.1 and a tgt link
        from client.1 to client.0

    Note that the iscsi image name is iscsi-image, so this only works
    for one image being tested at any one time.
    """
    try:
        pairs = config.items()
    except AttributeError:
        pairs = [('client.0', 'client.0')]
    with contextutil.nested(
            lambda: start_iscsi_initiators(ctx=ctx, tgt_link=pairs),):
        yield
