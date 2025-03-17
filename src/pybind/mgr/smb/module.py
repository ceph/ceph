from typing import TYPE_CHECKING, Any, List, Optional, cast

import logging

import orchestrator
from ceph.deployment.service_spec import PlacementSpec, SMBSpec
from mgr_module import MgrModule, Option, OptionLevel
from mgr_util import CephFSEarmarkResolver

from . import (
    cli,
    fs,
    handler,
    mon_store,
    rados_store,
    resources,
    results,
    sqlite_store,
    utils,
)
from .enums import (
    AuthMode,
    InputPasswordFilter,
    JoinSourceType,
    PasswordFilter,
    ShowResults,
    SMBClustering,
    UserGroupSourceType,
)
from .proto import AccessAuthorizer, ConfigStore, Simplified

if TYPE_CHECKING:
    import sqlite3

log = logging.getLogger(__name__)


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS: List[Option] = [
        Option(
            'update_orchestration',
            type='bool',
            default=True,
            desc='automatically update orchestration when smb resources are changed',
        ),
        Option(
            'internal_store_backend',
            level=OptionLevel.DEV,
            type='str',
            default='',
            desc='set internal store backend. for develoment and testing only',
        ),
    ]

    def __init__(self, *args: str, **kwargs: Any) -> None:
        internal_store = kwargs.pop('internal_store', None)
        priv_store = kwargs.pop('priv_store', None)
        public_store = kwargs.pop('public_store', None)
        path_resolver = kwargs.pop('path_resolver', None)
        authorizer = kwargs.pop('authorizer', None)
        uo = kwargs.pop('update_orchestration', None)
        earmark_resolver = kwargs.pop('earmark_resolver', None)
        super().__init__(*args, **kwargs)
        if internal_store is not None:
            self._internal_store = internal_store
            log.info('Using internal_store passed to class: {internal_store}')
        else:
            self._internal_store = self._backend_store(
                self.internal_store_backend
            )
        self._priv_store = priv_store or mon_store.MonKeyConfigStore(self)
        # self._public_store = public_store or mon_store.MonKeyConfigStore(self)
        self._public_store = (
            public_store or rados_store.RADOSConfigStore.init(self)
        )
        path_resolver = path_resolver or fs.CachingCephFSPathResolver(self)
        earmark_resolver = earmark_resolver or CephFSEarmarkResolver(self)
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
            orch=self._orch_backend(enable_orch=uo),
            earmark_resolver=earmark_resolver,
        )

    def _backend_store(self, store_conf: str = '') -> ConfigStore:
        # Store conf is meant for devs, maybe testers to experiment with
        # certain backend options at run time. This is not meant to be
        # a formal or friendly interface.
        name = 'default'
        opts = {}
        if store_conf:
            parts = [v.strip() for v in store_conf.split(';')]
            assert parts
            name = parts[0]
            opts = dict(p.split('=', 1) for p in parts[1:])
        if name == 'default':
            log.info('Using default backend: sqlite3 with mirroring')
            mc_store = mon_store.ModuleConfigStore(self)
            db_store = sqlite_store.mgr_sqlite3_db_with_mirroring(
                self, mc_store, opts
            )
            return db_store
        if name == 'mon':
            log.info('Using specified backend: module config internal store')
            return mon_store.ModuleConfigStore(self)
        if name == 'db':
            log.info('Using specified backend: mgr pool sqlite3 db')
            return sqlite_store.mgr_sqlite3_db(self, opts)
        raise ValueError(f'invalid internal store: {name}')

    def _orch_backend(
        self, enable_orch: Optional[bool] = None
    ) -> Optional['Module']:
        if enable_orch is not None:
            log.info('smb orchestration argument supplied: %r', enable_orch)
            return self if enable_orch else None
        if self.update_orchestration:
            log.warning('smb orchestration enabled by module')
            return self
        log.warning('smb orchestration is disabled')
        return None

    @property
    def update_orchestration(self) -> bool:
        return cast(
            bool,
            self.get_module_option('update_orchestration', True),
        )

    @property
    def internal_store_backend(self) -> str:
        return cast(
            str,
            self.get_module_option('internal_store_backend', ''),
        )

    def _apply_res(
        self,
        resource_input: List[resources.SMBResource],
        create_only: bool = False,
        password_filter: InputPasswordFilter = InputPasswordFilter.NONE,
        password_filter_out: Optional[PasswordFilter] = None,
    ) -> results.ResultGroup:
        in_pf = password_filter.to_password_filter()  # update enum type
        # use the same filtering as the input unless expliclitly set
        out_pf = password_filter_out if password_filter_out else in_pf
        if in_pf is not PasswordFilter.NONE:
            in_op = (in_pf, PasswordFilter.NONE)
            log.debug('Password filtering for resource apply: %r', in_op)
            resource_input = [r.convert(in_op) for r in resource_input]
        all_results = self._handler.apply(
            resource_input, create_only=create_only
        )
        if out_pf is not PasswordFilter.NONE:
            # we need to apply the conversion filter to the output
            # resources in the results - otherwise we would show raw
            # passwords - this will be the inverse of the filter applied to
            # the input
            out_op = (PasswordFilter.NONE, out_pf)
            log.debug('Password filtering for smb apply output: %r', in_op)
            all_results = all_results.convert_results(out_op)
        return all_results

    @cli.SMBCommand('apply', perm='rw')
    def apply_resources(
        self,
        inbuf: str,
        password_filter: InputPasswordFilter = InputPasswordFilter.NONE,
        password_filter_out: Optional[PasswordFilter] = None,
    ) -> results.ResultGroup:
        """Create, update, or remove smb configuration resources based on YAML
        or JSON specs
        """
        try:
            return self._apply_res(
                resources.load_text(inbuf),
                password_filter=password_filter,
                password_filter_out=password_filter_out,
            )
        except resources.InvalidResourceError as err:
            # convert the exception into a result and return it as the only
            # item in the result group
            return results.ResultGroup(
                [results.InvalidResourceResult(err.resource_data, str(err))]
            )

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
        clustering: Optional[SMBClustering] = None,
        public_addrs: Optional[List[str]] = None,
        password_filter: InputPasswordFilter = InputPasswordFilter.NONE,
        password_filter_out: Optional[PasswordFilter] = None,
    ) -> results.Result:
        """Create an smb cluster"""
        domain_settings = None
        user_group_settings = None
        to_apply: List[resources.SMBResource] = []

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
                rname = utils.rand_name(cluster_id)
                join_sources.append(
                    resources.JoinSource(
                        source_type=JoinSourceType.RESOURCE,
                        ref=rname,
                    )
                )
                to_apply.append(
                    resources.JoinAuth(
                        auth_id=rname,
                        auth=resources.JoinAuthValues(
                            username=username,
                            password=password,
                        ),
                        linked_to_cluster=cluster_id,
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
            rname = utils.rand_name(cluster_id)
            user_group_settings.append(
                resources.UserGroupSource(
                    source_type=UserGroupSourceType.RESOURCE, ref=rname
                )
            )
            to_apply.append(
                resources.UsersAndGroups(
                    users_groups_id=rname,
                    values=resources.UserGroupSettings(
                        users=users,
                        groups=[],
                    ),
                    linked_to_cluster=cluster_id,
                )
            )

        c_public_addrs = []
        if public_addrs:
            for pa in public_addrs:
                pa_arr = pa.split('%', 1)
                address = pa_arr[0]
                destination = pa_arr[1] if len(pa_arr) > 1 else None
                c_public_addrs.append(
                    resources.ClusterPublicIPAssignment(
                        address=address, destination=destination
                    )
                )

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
            clustering=clustering,
            public_addrs=c_public_addrs,
        )
        to_apply.append(cluster)
        return self._apply_res(
            to_apply,
            create_only=True,
            password_filter=password_filter,
            password_filter_out=password_filter_out,
        ).squash(cluster)

    @cli.SMBCommand('cluster rm', perm='rw')
    def cluster_rm(
        self,
        cluster_id: str,
        password_filter: PasswordFilter = PasswordFilter.NONE,
    ) -> results.Result:
        """Remove an smb cluster"""
        cluster = resources.RemovedCluster(cluster_id=cluster_id)
        return self._apply_res(
            [cluster], password_filter_out=password_filter
        ).one()

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
    ) -> results.Result:
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
        return self._apply_res([share], create_only=True).one()

    @cli.SMBCommand('share rm', perm='rw')
    def share_rm(self, cluster_id: str, share_id: str) -> results.Result:
        """Remove an smb share"""
        share = resources.RemovedShare(
            cluster_id=cluster_id, share_id=share_id
        )
        return self._apply_res([share]).one()

    @cli.SMBCommand("show", perm="r")
    def show(
        self,
        resource_names: Optional[List[str]] = None,
        results: ShowResults = ShowResults.COLLAPSED,
        password_filter: PasswordFilter = PasswordFilter.NONE,
    ) -> Simplified:
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
        if password_filter is not PasswordFilter.NONE:
            op = (PasswordFilter.NONE, password_filter)
            log.debug('Password filtering for smb show: %r', op)
            resources = [r.convert(op) for r in resources]
        if len(resources) == 1 and results is ShowResults.COLLAPSED:
            return resources[0].to_simplified()
        return {"resources": [r.to_simplified() for r in resources]}

    def submit_smb_spec(self, spec: SMBSpec) -> None:
        """Submit a new or updated smb spec object to ceph orchestration."""
        completion = self.apply_smb(spec)
        orchestrator.raise_if_exception(completion)

    def remove_smb_service(self, service_name: str) -> None:
        completion = self.remove_service(service_name)
        orchestrator.raise_if_exception(completion)

    def maybe_upgrade(self, db: 'sqlite3.Connection', version: int) -> None:
        # Our db tables are self managed by our abstraction layer, via a store
        # class, not directly by the mgr module. Disable the default behavior
        # of the mgr module schema loader and use our internal_store class.
        if not isinstance(self._internal_store, sqlite_store.SqliteStore):
            return
        log.debug('Preparing db tables')
        self._internal_store.prepare(db.cursor())
