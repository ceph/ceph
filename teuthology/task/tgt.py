"""
Task to handle tgt

Assumptions made:
    The ceph-extras tgt package may need to get installed.
    The open-iscsi package needs to get installed.
"""
import logging
import contextlib

from teuthology import misc as teuthology
from teuthology import contextutil

log = logging.getLogger(__name__)


@contextlib.contextmanager
def start_tgt_remotes(ctx, start_tgtd):
    """
    This subtask starts up a tgtd on the clients specified
    """
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    tgtd_list = []
    for rem, roles in remotes.iteritems():
        for _id in roles:
            if _id in start_tgtd:
                if not rem in tgtd_list:
                    tgtd_list.append(rem)
                    rem.run(
                        args=[
                            'rbd',
                            'create',
                            'iscsi-image',
                            '--size',
                            '500',
                    ])
                    rem.run(
                        args=[
                            'sudo',
                            'tgtadm',
                            '--lld',
                            'iscsi',
                            '--mode',
                            'target',
                            '--op',
                            'new',
                            '--tid',
                            '1',
                            '--targetname',
                            'rbd',
                        ])
                    rem.run(
                        args=[
                            'sudo',
                            'tgtadm',
                            '--lld',
                            'iscsi',
                            '--mode',
                            'logicalunit',
                            '--op',
                            'new',
                            '--tid',
                            '1',
                            '--lun',
                            '1',
                            '--backing-store',
                            'iscsi-image',
                            '--bstype',
                            'rbd',
                        ])
                    rem.run(
                        args=[
                            'sudo',
                            'tgtadm',
                            '--lld',
                            'iscsi',
                            '--op',
                            'bind',
                            '--mode',
                            'target',
                            '--tid',
                            '1',
                            '-I',
                            'ALL',
                        ])
    try:
        yield

    finally:
        for rem in tgtd_list:
            rem.run(
                args=[
                    'sudo',
                    'tgtadm',
                    '--lld',
                    'iscsi',
                    '--mode',
                    'target',
                    '--op',
                    'delete',
                    '--force',
                    '--tid',
                    '1',
                ])
            rem.run(
                args=[
                    'rbd',
                    'snap',
                    'purge',
                    'iscsi-image',
                ])
            rem.run(
                args=[
                    'sudo',
                    'rbd',
                    'rm',
                    'iscsi-image',
                ])


@contextlib.contextmanager
def task(ctx, config):
    """
    Start up tgt.

    To start on on all clients::

        tasks:
        - ceph:
        - tgt:

    To start on certain clients::

        tasks:
        - ceph:
        - tgt: [client.0, client.3]

    or

        tasks:
        - ceph:
        - tgt:
            client.0:
            client.3:

    The general flow of things here is:
        1. Find clients on which tgt is supposed to run (start_tgtd)
        2. Remotely start up tgt daemon
    On cleanup:
        3. Stop tgt daemon

    The iscsi administration is handled by the iscsi task.
    """
    start_tgtd = []
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    log.info(remotes)
    if config == None:
        start_tgtd = ['client.{id}'.format(id=id_)
            for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    else:
        start_tgtd = config
    log.info(start_tgtd)
    with contextutil.nested(
            lambda: start_tgt_remotes(ctx=ctx, start_tgtd=start_tgtd),):
        yield
