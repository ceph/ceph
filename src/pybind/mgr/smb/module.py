from typing import Any, Dict, List, Optional, cast

import logging

import orchestrator
from ceph.deployment.service_spec import PlacementSpec, SMBSpec
from mgr_module import MgrModule, Option

from . import cli, fs, handler, mon_store, rados_store, resources
from .enums import AuthMode, JoinSourceType, UserGroupSourceType
from .proto import AccessAuthorizer, Simplified

log = logging.getLogger(__name__)


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS: List[Option] = [
        Option(
            'update_orchestration',
            type='bool',
            default=True,
            desc='automatically update orchestration when smb resources are changed',
        ),
    ]

    update_orchestration: bool = True

    def __init__(self, *args: str, **kwargs: Any) -> None:
        internal_store = kwargs.pop('internal_store', None)
        priv_store = kwargs.pop('priv_store', None)
        public_store = kwargs.pop('public_store', None)
        path_resolver = kwargs.pop('path_resolver', None)
        authorizer = kwargs.pop('authorizer', None)
        update_orchestration = kwargs.pop(
            'update_orchestration', self.update_orchestration
        )
        super().__init__(*args, **kwargs)
        self._internal_store = internal_store or mon_store.ModuleConfigStore(
            self
        )
        self._priv_store = priv_store or mon_store.MonKeyConfigStore(self)
        # self._public_store = public_store or mon_store.MonKeyConfigStore(self)
        self._public_store = (
            public_store or rados_store.RADOSConfigStore.init(self)
        )
        path_resolver = path_resolver or fs.CephFSPathResolver(self)
        # Why the honk is the cast needed but path_resolver doesn't need it??
        # Sometimes mypy drives me batty.
        authorizer = cast(
            AccessAuthorizer, authorizer or fs.FileSystemAuthorizer(self)
        )
        self._handler = handler.ClusterConfigHandler(
            internal_store=self._internal_store,
            priv_store=self._priv_store,
            public_store=self._public_store,
            path_resolver=path_resolver,
            authorizer=authorizer,
            orch=(self if update_orchestration else None),
        )

    @cli.SMBCommand('apply', perm='rw')
    def apply_resources(self, inbuf: str) -> handler.ResultGroup:
        """Create, update, or remove smb configuration resources based on YAML
        or JSON specs
        """
        return self._handler.apply(resources.load_text(inbuf))

    @cli.SMBCommand('cluster ls', perm='r')
    def cluster_ls(self) -> List[str]:
        """List smb clusters by ID"""
        return [cid for cid in self._handler.cluster_ids()]

    @cli.SMBCommand('cluster create', perm='rw')
    def cluster_create(
        self,
        cluster_id: str,
        auth_mode: AuthMode,
        domain_realm: str = '',
        domain_join_ref: Optional[List[str]] = None,
        domain_join_user_pass: Optional[List[str]] = None,
        user_group_ref: Optional[List[str]] = None,
        define_user_pass: Optional[List[str]] = None,
        custom_dns: Optional[List[str]] = None,
        placement: Optional[str] = None,
    ) -> handler.Result:
        """Create an smb cluster"""
        domain_settings = None
        user_group_settings = None

        if domain_realm or domain_join_ref or domain_join_user_pass:
            join_sources: List[resources.JoinSource] = []
            # create join auth resource references
            for djref in domain_join_ref or []:
                join_sources.append(
                    resources.JoinSource(
                        source_type=JoinSourceType.RESOURCE,
                        ref=djref,
                    )
                )
            # as a "short cut" allow passing username%password combos on the
            # command line for testing / automation where the auth tokens are
            # single use or don't really matter security wise
            for djunpw in domain_join_user_pass or []:
                try:
                    username, password = djunpw.split('%', 1)
                except ValueError:
                    raise ValueError(
                        'a domain join username & password value'
                        ' must contain a "%" separator'
                    )
                join_sources.append(
                    resources.JoinSource(
                        source_type=JoinSourceType.PASSWORD,
                        auth=resources.JoinAuthValues(
                            username=username,
                            password=password,
                        ),
                    )
                )
            domain_settings = resources.DomainSettings(
                realm=domain_realm,
                join_sources=join_sources,
            )

        # we don't permit creating groups on the command line. A bit too
        # complex for very little payoff.  We do support a very simple
        # <username>%<password> split for just creating users
        # However, it's much preferred to use the declarative resources for
        # managing these.
        user_group_settings = []
        if user_group_ref:
            user_group_settings += [
                resources.UserGroupSource(
                    source_type=UserGroupSourceType.RESOURCE, ref=r
                )
                for r in user_group_ref
            ]
        if define_user_pass:
            users = []
            for unpw in define_user_pass or []:
                username, password = unpw.split('%', 1)
                users.append({'name': username, 'password': password})
            user_group_settings += [
                resources.UserGroupSource(
                    source_type=UserGroupSourceType.INLINE,
                    values=resources.UserGroupSettings(
                        users=users,
                        groups=[],
                    ),
                )
            ]

        pspec = resources.WrappedPlacementSpec.wrap(
            PlacementSpec.from_string(placement)
        )
        cluster = resources.Cluster(
            cluster_id=cluster_id,
            auth_mode=auth_mode,
            domain_settings=domain_settings,
            user_group_settings=user_group_settings,
            custom_dns=custom_dns,
            placement=pspec,
        )
        return self._handler.apply([cluster]).one()

    @cli.SMBCommand('cluster rm', perm='rw')
    def cluster_rm(self, cluster_id: str) -> handler.Result:
        """Remove an smb cluster"""
        cluster = resources.RemovedCluster(cluster_id=cluster_id)
        return self._handler.apply([cluster]).one()

    @cli.SMBCommand('share ls', perm='r')
    def share_ls(self, cluster_id: str) -> List[str]:
        """List smb shares in a cluster by ID"""
        return [
            shid
            for cid, shid in self._handler.share_ids()
            if cid == cluster_id
        ]

    @cli.SMBCommand('share create', perm='rw')
    def share_create(
        self,
        cluster_id: str,
        share_id: str,
        cephfs_volume: str,
        path: str,
        # plain old 'name' conflicts with builtin options to the `ceph` command.
        # use `share_name` to avoid having to `ceph -- smb share create ...`.
        share_name: str = '',
        subvolume: str = '',
        readonly: bool = False,
    ) -> handler.Result:
        """Create an smb share"""
        share = resources.Share(
            cluster_id=cluster_id,
            share_id=share_id,
            name=share_name,
            readonly=readonly,
            cephfs=resources.CephFSStorage(
                volume=cephfs_volume,
                path=path,
                subvolume=subvolume,
            ),
        )
        return self._handler.apply([share]).one()

    @cli.SMBCommand('share rm', perm='rw')
    def share_rm(self, cluster_id: str, share_id: str) -> handler.Result:
        """Remove an smb share"""
        share = resources.RemovedShare(
            cluster_id=cluster_id, share_id=share_id
        )
        return self._handler.apply([share]).one()

    @cli.SMBCommand('show', perm='r')
    def show(self, resource_names: Optional[List[str]] = None) -> Simplified:
        """Show resources fetched from the local config store based on resource
        type or resource type and id(s).
        """
        if not resource_names:
            resources = self._handler.all_resources()
        else:
            try:
                resources = self._handler.matching_resources(resource_names)
            except handler.InvalidResourceMatch as err:
                raise cli.InvalidInputValue(str(err)) from err
        if len(resources) == 1:
            return resources[0].to_simplified()
        return {'resources': [r.to_simplified() for r in resources]}

    @cli.SMBCommand('dump cluster-config', perm='r')
    def dump_config(self, cluster_id: str) -> Dict[str, Any]:
        """DEBUG: Generate an example configuration"""
        # TODO: Remove this command prior to release
        return self._handler.generate_config(cluster_id)

    @cli.SMBCommand('dump service-spec', perm='r')
    def dump_service_spec(self, cluster_id: str) -> Dict[str, Any]:
        """DEBUG: Generate an example smb service spec"""
        # TODO: Remove this command prior to release
        return dict(
            self._handler.generate_smb_service_spec(cluster_id).to_json()
        )

    @cli.SMBCommand('dump everything', perm='r')
    def dump_everything(self) -> Dict[str, Any]:
        """DEBUG: Show me everything"""
        # TODO: Remove this command prior to release
        everything: Dict[str, Any] = {}
        everything['PUBLIC'] = {}
        log.warning('dumping PUBLIC')
        for key in self._public_store:
            e = self._public_store[key]
            log.warning('dumping e: %s %r', e.uri, e.full_key)
            everything['PUBLIC'][e.uri] = e.get()
        log.warning('dumping PRIV')
        everything['PRIV'] = {}
        for key in self._priv_store:
            e = self._priv_store[key]
            log.warning('dumping e: %s %r', e.uri, e.full_key)
            everything['PRIV'][e.uri] = e.get()
        log.warning('dumping INTERNAL')
        everything['INTERNAL'] = {}
        for key in self._internal_store:
            e = self._internal_store[key]
            log.warning('dumping e: %s %r', e.uri, e.full_key)
            everything['INTERNAL'][e.uri] = e.get()
        return everything

    def submit_smb_spec(self, spec: SMBSpec) -> None:
        """Submit a new or updated smb spec object to ceph orchestration."""
        completion = self.apply_smb(spec)
        orchestrator.raise_if_exception(completion)

    def remove_smb_service(self, service_name: str) -> None:
        completion = self.remove_service(service_name)
        orchestrator.raise_if_exception(completion)
