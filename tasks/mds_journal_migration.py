
import contextlib
import logging
from teuthology import misc

from cephfs.filesystem import Filesystem

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
    if not hasattr(ctx, 'ceph'):
        raise RuntimeError("This task must be nested in 'ceph' task")

    if not hasattr(ctx, 'mounts'):
        raise RuntimeError("This task must be nested inside 'kclient' or 'ceph_fuse' task")

    # Determine which client we will use
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

    fs = Filesystem(ctx, config)
    ctx.fs = fs
    old_journal_version = JOURNAL_FORMAT_LEGACY
    new_journal_version = JOURNAL_FORMAT_RESILIENT

    fs.set_ceph_conf('mds', 'mds journal format', old_journal_version)

    # Create a filesystem using the older journal format.
    for mount in ctx.mounts.values():
        mount.umount_wait()
    fs.mds_stop()
    fs.reset()
    fs.mds_restart()

    # Do some client work so that the log is populated with something.
    mount = ctx.mounts[client_id]
    with mount.mounted():
        mount.create_files()
        mount.check_files()  # sanity, this should always pass

    # Modify the ceph.conf to ask the MDS to use the new journal format.
    fs.set_ceph_conf('mds', 'mds journal format', new_journal_version)

    # Restart the MDS.
    fs.mds_fail_restart()
    fs.wait_for_daemons()

    # This ensures that all daemons come up into a valid state
    fs.wait_for_daemons()

    # Check that files created in the initial client workload are still visible
    # in a client mount.
    with mount.mounted():
        mount.check_files()

    # Verify that the journal really has been rewritten.
    journal_version = fs.get_journal_version()
    if journal_version != new_journal_version:
        raise RuntimeError("Journal was not upgraded, version should be {0} but is {1}".format(
            new_journal_version, journal_version()
        ))

    # Leave all MDSs and clients running for any child tasks
    for mount in ctx.mounts.values():
        mount.mount()
        mount.wait_until_mounted()

    yield
