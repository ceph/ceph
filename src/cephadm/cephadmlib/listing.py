# listing.py - listings and status of current daemons
#
# listing.py is the result of a refactor of the previous mega-function list_daemons.
# It attempts to break the operation of listing daemons and getting information about
# those daemons into the following parts:
#
# Functions daemons and daemons_matching iterate the configuration tree
# used by ceph and yields entry objects that represent either a standard
# daemon or a legacy daemon.
#
# Function daemons_summary is a convenient function that return a list
# containing the status field of entries from daemons_matching. It is designed
# to be a drop in replacement for `list_daemons` called with the `detail=False`
# argument.
#
# To provide an equivalent to the detail gathering portion of list_daemons the
# DaemonStatusUpdater base class and subclasses are designed to be a
# well-defined but flexible mechanism to gather additional data for each entry
# type and update the contents of the associated status dict.  The
# DaemonStatusUpdater class is designed to support caching. The caching can be
# set up when an updater is initialized or on the fly, being used or refreshed
# when an update method is called.  The update and legacy_update functions are
# to be provided by sub-classes that to mutate an existing status dictionary.
#
# The DaemonStatusUpdater expand method is designed to be used in an iterator
# or list comprehension in order to process the entries yielded by the listing
# methods mentioned above. This is intended to allow callers to list daemons
# with the level of detail needed rather than being forced to rely on a too-simple
# iterator and gather extra details in an ad-hoc way or get too much info than
# needed and incur extra costs getting that unwanted data.
#
# The CombinedStatusUpdater class exists so that multiple updaters can be
# easily combined. The init method of the class takes a list of other
# DaemonStatusUpdater classes and calls them (in order) to update the status
# dict.
#
# Use the CombinedStatusUpdater and a list of desired updaters to list/iterate
# with the level of detail your function needs. For example:
# >>> updater = CombinedStatusUpdater([
# ...     CoreStatusUpdater(),
# ...     PowerLevelUpdater(),
# ...     MyCoolCustomUpdater(),
# ... ])
# >>> result = [updater.expand(ctx, entry) for entry in daemons(ctx)]
#
# These six lines let you flexibly perform the equivalent of list_daemons
# with a more precice level of detail needed by the caller.


import os
import logging

from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    TypedDict,
    Union,
    cast,
)

from .context import CephadmContext
from .daemon_identity import DaemonIdentity
from .data_utils import get_legacy_daemon_fsid, is_fsid


logger = logging.getLogger()


_LEGACY_DAEMON_TYPES = ['mon', 'osd', 'mds', 'mgr', 'rgw']

LEGACY = 'legacy'
VERSION1 = 'cephadm:v1'


class BasicDaemonStatus(TypedDict):
    """Core status dict returned from a listing of configured daemons."""

    style: str
    name: str
    fsid: str
    systemd_unit: str


class LegacyDaemonEntry:
    """An entry object representing a legacy daemon found when listing
    configured daemons.
    """

    fsid: str
    daemon_type: str
    name: str
    status: BasicDaemonStatus
    data_dir: str

    def __init__(
        self,
        fsid: str,
        daemon_type: str,
        name: str,
        status: BasicDaemonStatus,
        data_dir: str,
    ) -> None:
        self.fsid = fsid
        self.daemon_type = daemon_type
        self.name = name
        self.status = status
        self.data_dir = data_dir


class DaemonEntry:
    """An entry object representing a standard daemon found when listing
    configured daemons.
    """

    identity: DaemonIdentity
    status: BasicDaemonStatus
    data_dir: str

    def __init__(
        self,
        identity: DaemonIdentity,
        status: BasicDaemonStatus,
        data_dir: str,
    ) -> None:
        self.identity = identity
        self.status = status
        self.data_dir = data_dir


def daemons(
    ctx: CephadmContext,
    legacy_dir: Optional[str] = None,
) -> Iterator[Union[LegacyDaemonEntry, DaemonEntry]]:
    """Iterate over the daemons configured on the current node."""
    data_dir = ctx.data_dir
    if legacy_dir is not None:
        data_dir = os.path.abspath(legacy_dir + data_dir)

    if not os.path.exists(data_dir):
        # data_dir (/var/lib/ceph typically) is missing. Return empty list.
        logger.warning('%s is missing: no daemon listing available', data_dir)
        return

    for dirname in os.listdir(data_dir):
        if dirname in _LEGACY_DAEMON_TYPES:
            daemon_type = dirname
            for entry in os.listdir(os.path.join(data_dir, dirname)):
                if '-' not in entry:
                    continue  # invalid entry
                (cluster, daemon_id) = entry.split('-', 1)
                fsid = get_legacy_daemon_fsid(
                    ctx,
                    cluster,
                    daemon_type,
                    daemon_id,
                    legacy_dir=legacy_dir,
                )
                legacy_unit_name = 'ceph-%s@%s' % (daemon_type, daemon_id)
                yield LegacyDaemonEntry(
                    fsid=fsid or '',
                    daemon_type=daemon_type,
                    name=legacy_unit_name,
                    status={
                        'style': LEGACY,
                        'name': '%s.%s' % (daemon_type, daemon_id),
                        'fsid': fsid if fsid is not None else 'unknown',
                        'systemd_unit': legacy_unit_name,
                    },
                    data_dir=data_dir,
                )
        elif is_fsid(dirname):
            assert isinstance(dirname, str)
            fsid = dirname
            cluster_dir = os.path.join(data_dir, fsid)
            for entry in os.listdir(cluster_dir):
                if not (
                    '.' in entry
                    and os.path.isdir(os.path.join(cluster_dir, entry))
                ):
                    continue  # invalid entry
                identity = DaemonIdentity.from_name(fsid, entry)
                yield DaemonEntry(
                    identity=identity,
                    status={
                        'style': VERSION1,
                        'name': identity.daemon_name,
                        'fsid': fsid,
                        'systemd_unit': identity.unit_name,
                    },
                    data_dir=data_dir,
                )


def daemons_matching(
    ctx: CephadmContext,
    legacy_dir: Optional[str] = None,
    daemon_name: Optional[str] = None,
    daemon_type: Optional[str] = None,
    fsid: Optional[str] = None,
    daemon_type_predicate: Optional[Callable[[str], bool]] = None,
) -> Iterator[Union[LegacyDaemonEntry, DaemonEntry]]:
    """Iterate over the daemons configured on the current node, matching daemon
    name or daemon type if supplied.
    """
    for entry in daemons(ctx, legacy_dir):
        if isinstance(entry, LegacyDaemonEntry):
            if fsid is not None and fsid != entry.fsid:
                continue
            if daemon_type is not None and daemon_type != entry.daemon_type:
                continue
            if (
                daemon_type_predicate is not None
                and not daemon_type_predicate(entry.daemon_type)
            ):
                continue
        elif isinstance(entry, DaemonEntry):
            if fsid is not None and fsid != entry.identity.fsid:
                continue
            if (
                daemon_name is not None
                and daemon_name != entry.identity.daemon_name
            ):
                continue
            if (
                daemon_type is not None
                and daemon_type != entry.identity.daemon_type
            ):
                continue
            if (
                daemon_type_predicate is not None
                and not daemon_type_predicate(entry.identity.daemon_type)
            ):
                continue
        else:
            raise ValueError(f'unexpected entry type: {entry}')
        yield entry


def daemons_summary(
    ctx: CephadmContext,
    legacy_dir: Optional[str] = None,
    daemon_name: Optional[str] = None,
    daemon_type: Optional[str] = None,
) -> List[BasicDaemonStatus]:
    """Return a list of status dicts for all daemons configured on the current
    node, matching daemon name or daemon type if supplied.
    A compatiblity function equivalent to the previous
    `list_daemons(..., detail=False)` function.
    """
    return [
        e.status
        for e in daemons_matching(
            ctx,
            legacy_dir,
            daemon_name=daemon_name,
            daemon_type=daemon_type,
        )
    ]


class DaemonStatusUpdater:
    """Base class for types that can update and/or expand the daemon information
    provided by the core listing functions in this module.
    """

    def update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        identity: DaemonIdentity,
        data_dir: str,
    ) -> None:
        """Update the val dict with new status information for the daemon with
        the given identity and configuration dir.
        """
        pass

    def legacy_update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        fsid: str,
        daemon_type: str,
        name: str,
        data_dir: str,
    ) -> None:
        """Update the val dict with new status information for a legacy daemon
        described by the given parameters and configuration dir.
        """
        pass

    def expand(
        self,
        ctx: CephadmContext,
        entry: Union[LegacyDaemonEntry, DaemonEntry],
    ) -> Dict[str, Any]:
        """Return a status dictionary based on the entry object and its status
        attribute expanded with additional information.
        """
        if isinstance(entry, LegacyDaemonEntry):
            status = cast(Dict[str, Any], entry.status)
            self.legacy_update(
                status,
                ctx,
                entry.fsid,
                entry.daemon_type,
                entry.name,
                entry.data_dir,
            )
            return status
        status = cast(Dict[str, Any], entry.status)
        self.update(status, ctx, entry.identity, entry.data_dir)
        return status


class NoOpDaemonStatusUpdater(DaemonStatusUpdater):
    """A daemon status updater that adds no new information to the status
    dictionary.
    """

    pass


class CombinedStatusUpdater(DaemonStatusUpdater):
    """A status updater that combines multiple status updaters together."""

    def __init__(self, updaters: List[DaemonStatusUpdater]):
        self.updaters = updaters

    def update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        identity: DaemonIdentity,
        data_dir: str,
    ) -> None:
        for updater in self.updaters:
            updater.update(val, ctx, identity, data_dir)

    def legacy_update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        fsid: str,
        daemon_type: str,
        name: str,
        data_dir: str,
    ) -> None:
        for updater in self.updaters:
            updater.legacy_update(val, ctx, fsid, daemon_type, name, data_dir)
