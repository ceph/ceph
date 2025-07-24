from typing import (
    Any,
    Collection,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Type,
)

import functools
import logging
import operator

from ceph.fs.earmarking import EarmarkTopScope

from . import config_store, resources
from .enums import (
    AuthMode,
    ConfigNS,
    Intent,
    JoinSourceType,
    SMBClustering,
    State,
    UserGroupSourceType,
)
from .internal import (
    ClusterEntry,
    JoinAuthEntry,
    ResourceEntry,
    ShareEntry,
    UsersAndGroupsEntry,
    resource_entry,
    resource_key,
)
from .proto import (
    ConfigStore,
    EarmarkResolver,
    EntryKey,
    PathResolver,
)
from .resources import SMBResource
from .results import ErrorResult, Result, ResultGroup
from .utils import checked

log = logging.getLogger(__name__)


class Staging:
    """A virtual store used to compile pending changes before saving them in
    the destination store.
    """

    def __init__(self, store: ConfigStore) -> None:
        self.destination_store = store
        self.incoming: Dict[EntryKey, SMBResource] = {}
        self.deleted: Dict[EntryKey, SMBResource] = {}
        self._store_keycache: Set[EntryKey] = set()
        self._virt_keycache: Set[EntryKey] = set()

    def stage(self, resource: SMBResource) -> None:
        self._virt_keycache = set()
        ekey = resource_key(resource)
        if resource.intent == Intent.REMOVED:
            self.deleted[ekey] = resource
        else:
            self.deleted.pop(ekey, None)
            self.incoming[ekey] = resource

    def _virtual_keys(self) -> Collection[EntryKey]:
        if self._virt_keycache:
            return self._virt_keycache
        self._virt_keycache = set(self._store_keys()) - set(
            self.deleted
        ) | set(self.incoming)
        return self._virt_keycache

    def _store_keys(self) -> Collection[EntryKey]:
        if not self._store_keycache:
            self._store_keycache = set(self.destination_store)
        return self._store_keycache

    def __iter__(self) -> Iterator[EntryKey]:
        return iter(self._virtual_keys())

    def namespaces(self) -> Collection[str]:
        return {k[0] for k in self}

    def contents(self, ns: str) -> Collection[str]:
        return {kname for kns, kname in self if kns == ns}

    def is_new(self, resource: SMBResource) -> bool:
        ekey = resource_key(resource)
        return ekey not in self._store_keys()

    def get_cluster(self, cluster_id: str) -> resources.Cluster:
        ekey = (str(ClusterEntry.namespace), cluster_id)
        if ekey in self.incoming:
            res = self.incoming[ekey]
            assert isinstance(res, resources.Cluster)
            return res
        return ClusterEntry.from_store(
            self.destination_store, cluster_id
        ).get_cluster()

    def get_join_auth(self, auth_id: str) -> resources.JoinAuth:
        ekey = (str(JoinAuthEntry.namespace), auth_id)
        if ekey in self.incoming:
            res = self.incoming[ekey]
            assert isinstance(res, resources.JoinAuth)
            return res
        return JoinAuthEntry.from_store(
            self.destination_store, auth_id
        ).get_join_auth()

    def get_users_and_groups(self, ug_id: str) -> resources.UsersAndGroups:
        ekey = (str(UsersAndGroupsEntry.namespace), ug_id)
        if ekey in self.incoming:
            res = self.incoming[ekey]
            assert isinstance(res, resources.UsersAndGroups)
            return res
        return UsersAndGroupsEntry.from_store(
            self.destination_store, ug_id
        ).get_users_and_groups()

    def save(self) -> ResultGroup:
        results = ResultGroup()
        for res in self.deleted.values():
            results.append(self._save(res))
        for res in self.incoming.values():
            results.append(self._save(res))
        return results

    def _save(self, resource: SMBResource) -> Result:
        entry = resource_entry(self.destination_store, resource)
        if resource.intent == Intent.REMOVED:
            removed = entry.remove()
            state = State.REMOVED if removed else State.NOT_PRESENT
        else:
            state = entry.create_or_update(resource)
        log.debug('saved resource: %r; state: %s', resource, state)
        result = Result(resource, success=True, status={'state': state})
        return result

    def _prune(
        self,
        cids: Collection[str],
        ecls: Type[ResourceEntry],
        rcls: Type[SMBResource],
    ) -> None:
        for _id in ecls.ids(self):
            assert isinstance(_id, str)
            rentry = ecls.from_store_by_key(self.destination_store, _id)
            resource = rentry.get_resource_type(rcls)
            _linked_to_cluster = getattr(resource, 'linked_to_cluster', None)
            if not _linked_to_cluster:
                continue
            if _linked_to_cluster not in cids:
                rentry.remove()

    def prune_linked_entries(self) -> None:
        cids = set(ClusterEntry.ids(self))
        self._prune(cids, JoinAuthEntry, resources.JoinAuth)
        self._prune(cids, UsersAndGroupsEntry, resources.UsersAndGroups)


def auth_refs(cluster: resources.Cluster) -> Collection[str]:
    """Return all IDs for join_auth resources referenced by the supplied
    cluster.
    """
    if cluster.auth_mode != AuthMode.ACTIVE_DIRECTORY:
        return set()
    return {
        j.ref
        for j in checked(cluster.domain_settings).join_sources
        if j.source_type == JoinSourceType.RESOURCE and j.ref
    }


def ug_refs(cluster: resources.Cluster) -> Collection[str]:
    """Return all IDs for users_and_groups resources referenced by the supplied
    cluster.
    """
    if (
        cluster.auth_mode != AuthMode.USER
        or cluster.user_group_settings is None
    ):
        return set()
    return {
        ug.ref
        for ug in cluster.user_group_settings
        if ug.source_type == UserGroupSourceType.RESOURCE and ug.ref
    }


@functools.singledispatch
def cross_check_resource(
    resource: SMBResource,
    staging: Staging,
    *,
    path_resolver: PathResolver,
    earmark_resolver: EarmarkResolver,
) -> None:
    """Check a given resource for consistency across the set of other resources
    in the virtual transaction represented by the staging store and
    resolver-helpers.

    The cross-check is only for things outside the scope of a single resource.
    """
    raise TypeError(f'not a valid smb resource: {type(resource)}')


@cross_check_resource.register
def _check_removed_cluster_resource(
    cluster: resources.RemovedCluster, staging: Staging, **_: Any
) -> None:
    assert cluster.intent == Intent.REMOVED
    share_ids = ShareEntry.ids(staging)
    clusters_used = {cid for cid, _ in share_ids}
    if cluster.cluster_id in clusters_used:
        raise ErrorResult(
            cluster,
            msg="cluster in use by shares",
            status={
                'shares': [
                    shid
                    for cid, shid in share_ids
                    if cid == cluster.cluster_id
                ]
            },
        )


@cross_check_resource.register
def _check_cluster_resource(
    cluster: resources.Cluster, staging: Staging, **_: Any
) -> None:
    assert cluster.intent == Intent.PRESENT
    cluster.validate()
    if not staging.is_new(cluster):
        _check_cluster_modifications(cluster, staging)
    for auth_ref in auth_refs(cluster):
        auth = staging.get_join_auth(auth_ref)
        if (
            auth.linked_to_cluster
            and auth.linked_to_cluster != cluster.cluster_id
        ):
            raise ErrorResult(
                cluster,
                msg="join auth linked to different cluster",
                status={
                    'other_cluster_id': auth.linked_to_cluster,
                },
            )
    for ug_ref in ug_refs(cluster):
        ug = staging.get_users_and_groups(ug_ref)
        if (
            ug.linked_to_cluster
            and ug.linked_to_cluster != cluster.cluster_id
        ):
            raise ErrorResult(
                cluster,
                msg="users and groups linked to different cluster",
                status={
                    'other_cluster_id': ug.linked_to_cluster,
                },
            )


def _check_cluster_modifications(
    cluster: resources.Cluster, staging: Staging
) -> None:
    """cluster has some fields we do not permit changing after the cluster has
    been created.
    """
    prev = ClusterEntry.from_store(
        staging.destination_store, cluster.cluster_id
    ).get_cluster()
    if cluster.auth_mode != prev.auth_mode:
        raise ErrorResult(
            cluster,
            'auth_mode value may not be changed',
            status={'existing_auth_mode': prev.auth_mode},
        )
    if cluster.auth_mode == AuthMode.ACTIVE_DIRECTORY:
        assert prev.domain_settings
        if not cluster.domain_settings:
            # should not occur
            raise ErrorResult(cluster, "domain settings missing from cluster")
        if cluster.domain_settings.realm != prev.domain_settings.realm:
            raise ErrorResult(
                cluster,
                'domain/realm value may not be changed',
                status={'existing_domain_realm': prev.domain_settings.realm},
            )
    if cluster.is_clustered() != prev.is_clustered():
        prev_clustering = prev.is_clustered()
        cterms = {True: 'enabled', False: 'disabled'}
        msg = (
            f'a cluster resource with clustering {cterms[prev_clustering]}'
            f' may not be changed to clustering {cterms[not prev_clustering]}'
        )
        opt_terms = {
            True: SMBClustering.ALWAYS.value,
            False: SMBClustering.NEVER.value,
        }
        hint = {
            'note': (
                'Set "clustering" to an explicit value that matches the'
                ' current clustering behavior'
            ),
            'value': opt_terms[prev_clustering],
        }
        raise ErrorResult(cluster, msg, status={'hint': hint})


@cross_check_resource.register
def _check_removed_share_resource(
    share: resources.RemovedShare, staging: Staging, **_: Any
) -> None:
    assert share.intent == Intent.REMOVED


@cross_check_resource.register
def _check_share_resource(
    share: resources.Share,
    staging: Staging,
    path_resolver: PathResolver,
    earmark_resolver: EarmarkResolver,
) -> None:
    """Check that the share resource can be updated."""
    assert share.intent == Intent.PRESENT
    assert isinstance(share, resources.Share)
    share.validate()
    if share.cluster_id not in ClusterEntry.ids(staging):
        raise ErrorResult(
            share,
            msg="no matching cluster id",
            status={"cluster_id": share.cluster_id},
        )
    assert share.cephfs is not None
    try:
        volpath = path_resolver.resolve_exists(
            share.cephfs.volume,
            share.cephfs.subvolumegroup,
            share.cephfs.subvolume,
            share.cephfs.path,
        )
    except (FileNotFoundError, NotADirectoryError):
        raise ErrorResult(
            share, msg="path is not a valid directory in volume"
        )
    if earmark_resolver:
        earmark = earmark_resolver.get_earmark(
            volpath,
            share.cephfs.volume,
        )
        if not earmark:
            smb_earmark = (
                f"{EarmarkTopScope.SMB.value}.cluster.{share.cluster_id}"
            )
            earmark_resolver.set_earmark(
                volpath,
                share.cephfs.volume,
                smb_earmark,
            )
        else:
            parsed_earmark = _parse_earmark(earmark)

            # Check if the top-level scope is not SMB
            if not earmark_resolver.check_earmark(
                earmark, EarmarkTopScope.SMB
            ):
                raise ErrorResult(
                    share,
                    msg=f"earmark has already been set by {parsed_earmark['scope']}",
                )

            # Check if the earmark is set by a different cluster
            if (
                parsed_earmark['cluster_id']
                and parsed_earmark['cluster_id'] != share.cluster_id
            ):
                raise ErrorResult(
                    share,
                    msg="earmark has already been set by smb cluster "
                    f"{parsed_earmark['cluster_id']}",
                )

    name_used_by = _share_name_in_use(staging, share)
    if name_used_by:
        raise ErrorResult(
            share,
            msg="share name already in use",
            status={"conflicting_share_id": name_used_by},
        )


def _share_name_in_use(
    staging: Staging, share: resources.Share
) -> Optional[str]:
    """Returns the share_id value if the share's name is already in
    use by a different share in the cluster. Returns None if no other
    shares are using the name.
    """
    share_ids = (share.cluster_id, share.share_id)
    share_ns = str(ConfigNS.SHARES)
    # look for any duplicate names in the staging area.
    # these items are already in memory
    for ekey, res in staging.incoming.items():
        if ekey[0] != share_ns:
            continue  # not a share
        assert isinstance(res, resources.Share)
        if (res.cluster_id, res.share_id) == share_ids:
            continue  # this share
        if (res.cluster_id, res.name) == (share.cluster_id, share.name):
            return res.share_id
    # look for any duplicate names in the underyling store
    found = config_store.find_in_store(
        staging.destination_store,
        share_ns,
        {'cluster_id': share.cluster_id, 'name': share.name},
    )
    # remove any shares that are deleted in staging
    found_curr = [
        entry for entry in found if entry.full_key not in staging.deleted
    ]
    # remove self-share from list
    id_pair = operator.itemgetter('cluster_id', 'share_id')
    found_curr = [
        entry for entry in found_curr if id_pair(entry.get()) != share_ids
    ]
    if not found_curr:
        return None
    if len(found_curr) != 1:
        # this should not normally happen
        log.warning(
            'multiple shares with one name in cluster: %s',
            ' '.join(s.get()['share_id'] for s in found_curr),
        )
    return found_curr[0].get()['share_id']


@cross_check_resource.register
def _check_join_auth_resource(
    join_auth: resources.JoinAuth, staging: Staging, **_: Any
) -> None:
    """Check that the JoinAuth resource can be updated."""
    if join_auth.intent == Intent.PRESENT:
        return _check_join_auths_present(join_auth, staging)
    return _check_join_auths_removed(join_auth, staging)


def _check_join_auths_removed(
    join_auth: resources.JoinAuth, staging: Staging
) -> None:
    cids = set(ClusterEntry.ids(staging))
    refs_in_use: Dict[str, List[str]] = {}
    for cluster_id in cids:
        cluster = staging.get_cluster(cluster_id)
        for ref in auth_refs(cluster):
            refs_in_use.setdefault(ref, []).append(cluster_id)
    log.debug('refs_in_use: %r', refs_in_use)
    if join_auth.auth_id in refs_in_use:
        raise ErrorResult(
            join_auth,
            msg='join auth resource in use by clusters',
            status={
                'clusters': refs_in_use[join_auth.auth_id],
            },
        )


def _check_join_auths_present(
    join_auth: resources.JoinAuth, staging: Staging
) -> None:
    if join_auth.linked_to_cluster:
        cids = set(ClusterEntry.ids(staging))
        if join_auth.linked_to_cluster not in cids:
            raise ErrorResult(
                join_auth,
                msg='linked_to_cluster id not valid',
                status={
                    'unknown_id': join_auth.linked_to_cluster,
                },
            )


@cross_check_resource.register
def _check_users_and_groups_resource(
    users_and_groups: resources.UsersAndGroups, staging: Staging, **_: Any
) -> None:
    """Check that the UsersAndGroups resource can be updated."""
    if users_and_groups.intent == Intent.PRESENT:
        return _check_users_and_groups_present(users_and_groups, staging)
    return _check_users_and_groups_removed(users_and_groups, staging)


def _check_users_and_groups_removed(
    users_and_groups: resources.UsersAndGroups, staging: Staging
) -> None:
    refs_in_use: Dict[str, List[str]] = {}
    cids = set(ClusterEntry.ids(staging))
    for cluster_id in cids:
        cluster = staging.get_cluster(cluster_id)
        for ref in ug_refs(cluster):
            refs_in_use.setdefault(ref, []).append(cluster_id)
    log.debug('refs_in_use: %r', refs_in_use)
    if users_and_groups.users_groups_id in refs_in_use:
        raise ErrorResult(
            users_and_groups,
            msg='users and groups resource in use by clusters',
            status={
                'clusters': refs_in_use[users_and_groups.users_groups_id],
            },
        )


def _check_users_and_groups_present(
    users_and_groups: resources.UsersAndGroups, staging: Staging
) -> None:
    if users_and_groups.linked_to_cluster:
        cids = set(ClusterEntry.ids(staging))
        if users_and_groups.linked_to_cluster not in cids:
            raise ErrorResult(
                users_and_groups,
                msg='linked_to_cluster id not valid',
                status={
                    'unknown_id': users_and_groups.linked_to_cluster,
                },
            )


def _parse_earmark(earmark: str) -> dict:
    parts = earmark.split('.')

    # If it only has one part (e.g., 'smb'), return None for cluster_id
    if len(parts) == 1:
        return {'scope': parts[0], 'cluster_id': None}

    return {
        'scope': parts[0],
        'cluster_id': parts[2] if len(parts) > 2 else None,
    }
