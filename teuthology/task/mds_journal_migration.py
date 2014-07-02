
import contextlib
import logging
from teuthology import misc

from teuthology.task.ceph import write_conf
from teuthology.task.ceph_fuse import task as ceph_fuse_ctx
from teuthology.task.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)


JOURNAL_FORMAT_LEGACY = 0
JOURNAL_FORMAT_RESILIENT = 1


@contextlib.contextmanager
def task(ctx, config):
    """
    Given a Ceph cluster has already been set up, exercise the migration
    of the CephFS journal from an older format to the latest format.  On
    successful completion the filesystem will be running with a journal
    in the new format.

    Optionally specify which client to use like this:

    - mds-journal_migration:
        client: client.0

    """
    if config and 'client' in config:
        # Use client specified in config
        client_role = config['client']
        client_list = list(misc.get_clients(ctx, [client_role]))
        try:
            client_id = client_list[0][0]
        except IndexError:
            raise RuntimeError("Client role '{0}' not found".format(client_role))
    else:
        # Pick one arbitrary client to use
        client_list = list(misc.all_roles_of_type(ctx.cluster, 'client'))
        try:
            client_id = client_list[0]
        except IndexError:
            raise RuntimeError("This task requires at least one client")
        else:
            client_role = "client.{0}".format(client_id)

    fs = Filesystem(ctx, config)
    old_journal_version = JOURNAL_FORMAT_LEGACY
    new_journal_version = JOURNAL_FORMAT_RESILIENT

    # Set config so that journal will be created in older format
    if not hasattr(ctx, 'ceph'):
        raise RuntimeError("This task must be nested in 'ceph' task")

    if 'mds' not in ctx.ceph.conf:
        ctx.ceph.conf['mds'] = {}
    ctx.ceph.conf['mds']['mds journal format'] = old_journal_version
    write_conf(ctx)  # XXX because we don't have the ceph task's config object, if they
                     # used a different config path this won't work.

    # Create a filesystem using the older journal format.
    fs.mds_stop()
    fs.reset()
    fs.mds_restart()

    # Do some client work so that the log is populated with something.
    with ceph_fuse_ctx(ctx, [client_role]) as client_mounts:
        mount = client_mounts[client_id]
        mount.create_files()
        mount.check_files()  # sanity, this should always pass

    # Modify the ceph.conf to ask the MDS to use the new journal format.
    ctx.ceph.conf['mds']['mds journal format'] = new_journal_version
    write_conf(ctx)

    # Restart the MDS.
    fs.mds_restart()

    # Check that files created in the initial client workload are still visible
    # in a client mount.
    with ceph_fuse_ctx(ctx, [client_role]) as client_mounts:
        mount = client_mounts[client_id]
        mount.check_files()

    # Verify that the journal really has been rewritten.
    journal_version = fs.get_journal_version()
    if journal_version != new_journal_version:
        raise RuntimeError("Journal was not upgraded, version should be {0} but is {1}".format(
            new_journal_version, journal_version()
        ))

    yield
