import json
import logging
from typing import TYPE_CHECKING, Iterator, Optional, Dict, Any

from ceph.deployment.service_spec import PlacementSpec, ServiceSpec, HostPlacementSpec
from cephadm.schedule import HostAssignment
import rados

from mgr_module import NFS_POOL_NAME
from orchestrator import OrchestratorError, DaemonDescription

if TYPE_CHECKING:
    from .module import CephadmOrchestrator

LAST_MIGRATION = 5

logger = logging.getLogger(__name__)


class Migrations:
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr = mgr

        # Why having a global counter, instead of spec versions?
        #
        # for the first migration:
        # The specs don't change in (this) migration. but the scheduler here.
        # Adding the version to the specs at this time just felt wrong to me.
        #
        # And the specs are only another part of cephadm which needs potential upgrades.
        # We have the cache, the inventory, the config store, the upgrade (imagine changing the
        # upgrade code, while an old upgrade is still in progress), naming of daemons,
        # fs-layout of the daemons, etc.
        if self.mgr.migration_current is None:
            self.set(LAST_MIGRATION)

        v = mgr.get_store('nfs_migration_queue')
        self.nfs_migration_queue = json.loads(v) if v else []

        # for some migrations, we don't need to do anything except for
        # incrementing migration_current.
        # let's try to shortcut things here.
        self.migrate(True)

    def set(self, val: int) -> None:
        self.mgr.set_module_option('migration_current', val)
        self.mgr.migration_current = val

    def is_migration_ongoing(self) -> bool:
        return self.mgr.migration_current != LAST_MIGRATION

    def verify_no_migration(self) -> None:
        if self.is_migration_ongoing():
            # this is raised in module.serve()
            raise OrchestratorError(
                "cephadm migration still ongoing. Please wait, until the migration is complete.")

    def migrate(self, startup: bool = False) -> None:
        if self.mgr.migration_current == 0:
            if self.migrate_0_1():
                self.set(1)

        if self.mgr.migration_current == 1:
            if self.migrate_1_2():
                self.set(2)

        if self.mgr.migration_current == 2 and not startup:
            if self.migrate_2_3():
                self.set(3)

        if self.mgr.migration_current == 3:
            if self.migrate_3_4():
                self.set(4)

        if self.mgr.migration_current == 4:
            if self.migrate_4_5():
                self.set(5)

    def migrate_0_1(self) -> bool:
        """
        Migration 0 -> 1
        New scheduler that takes PlacementSpec as the bound and not as recommendation.
        I.e. the new scheduler won't suggest any new placements outside of the hosts
        specified by label etc.

        Which means, we have to make sure, we're not removing any daemons directly after
        upgrading to the new scheduler.

        There is a potential race here:
        1. user updates his spec to remove daemons
        2. mgr gets upgraded to new scheduler, before the old scheduler removed the daemon
        3. now, we're converting the spec to explicit placement, thus reverting (1.)
        I think this is ok.
        """

        def interesting_specs() -> Iterator[ServiceSpec]:
            for s in self.mgr.spec_store.all_specs.values():
                if s.unmanaged:
                    continue
                p = s.placement
                if p is None:
                    continue
                if p.count is None:
                    continue
                if not p.hosts and not p.host_pattern and not p.label:
                    continue
                yield s

        def convert_to_explicit(spec: ServiceSpec) -> None:
            existing_daemons = self.mgr.cache.get_daemons_by_service(spec.service_name())
            placements, to_add, to_remove = HostAssignment(
                spec=spec,
                hosts=self.mgr.inventory.all_specs(),
                unreachable_hosts=self.mgr.cache.get_unreachable_hosts(),
                daemons=existing_daemons,
            ).place()

            # We have to migrate, only if the new scheduler would remove daemons
            if len(placements) >= len(existing_daemons):
                return

            def to_hostname(d: DaemonDescription) -> HostPlacementSpec:
                if d.hostname in old_hosts:
                    return old_hosts[d.hostname]
                else:
                    assert d.hostname
                    return HostPlacementSpec(d.hostname, '', '')

            old_hosts = {h.hostname: h for h in spec.placement.hosts}
            new_hosts = [to_hostname(d) for d in existing_daemons]

            new_placement = PlacementSpec(
                hosts=new_hosts,
                count=spec.placement.count
            )

            new_spec = ServiceSpec.from_json(spec.to_json())
            new_spec.placement = new_placement

            logger.info(f"Migrating {spec.one_line_str()} to explicit placement")

            self.mgr.spec_store.save(new_spec)

        specs = list(interesting_specs())
        if not specs:
            return True  # nothing to do. shortcut

        if not self.mgr.cache.daemon_cache_filled():
            logger.info("Unable to migrate yet. Daemon Cache still incomplete.")
            return False

        for spec in specs:
            convert_to_explicit(spec)

        return True

    def migrate_1_2(self) -> bool:
        """
        After 15.2.4, we unified some service IDs: MONs, MGRs etc no longer have a service id.
        Which means, the service names changed:

        mon.foo -> mon
        mgr.foo -> mgr

        This fixes the data structure consistency
        """
        bad_specs = {}
        for name, spec in self.mgr.spec_store.all_specs.items():
            if name != spec.service_name():
                bad_specs[name] = (spec.service_name(), spec)

        for old, (new, old_spec) in bad_specs.items():
            if new not in self.mgr.spec_store.all_specs:
                spec = old_spec
            else:
                spec = self.mgr.spec_store.all_specs[new]
            spec.unmanaged = True
            self.mgr.spec_store.save(spec)
            self.mgr.spec_store.finally_rm(old)

        return True

    def migrate_2_3(self) -> bool:
        if self.nfs_migration_queue:
            from nfs.cluster import create_ganesha_pool

            create_ganesha_pool(self.mgr)
            for service_id, pool, ns in self.nfs_migration_queue:
                if pool != '.nfs':
                    self.migrate_nfs_spec(service_id, pool, ns)
            self.nfs_migration_queue = []
            self.mgr.log.info('Done migrating all NFS services')
        return True

    def migrate_nfs_spec(self, service_id: str, pool: str, ns: Optional[str]) -> None:
        renamed = False
        if service_id.startswith('ganesha-'):
            service_id = service_id[8:]
            renamed = True

        self.mgr.log.info(
            f'Migrating nfs.{service_id} from legacy pool {pool} namespace {ns}'
        )

        # read exports
        ioctx = self.mgr.rados.open_ioctx(pool)
        if ns is not None:
            ioctx.set_namespace(ns)
        object_iterator = ioctx.list_objects()
        exports = []
        while True:
            try:
                obj = object_iterator.__next__()
                if obj.key.startswith('export-'):
                    self.mgr.log.debug(f'reading {obj.key}')
                    exports.append(obj.read().decode())
            except StopIteration:
                break
        self.mgr.log.info(f'Found {len(exports)} exports for legacy nfs.{service_id}')

        # copy grace file
        if service_id != ns:
            try:
                grace = ioctx.read("grace")
                new_ioctx = self.mgr.rados.open_ioctx(NFS_POOL_NAME)
                new_ioctx.set_namespace(service_id)
                new_ioctx.write_full("grace", grace)
                self.mgr.log.info('Migrated nfs-ganesha grace file')
            except rados.ObjectNotFound:
                self.mgr.log.debug('failed to read old grace file; skipping')

        if renamed and f'nfs.ganesha-{service_id}' in self.mgr.spec_store:
            # rename from nfs.ganesha-* to nfs.*.  This will destroy old daemons and
            # deploy new ones.
            self.mgr.log.info(f'Replacing nfs.ganesha-{service_id} with nfs.{service_id}')
            spec = self.mgr.spec_store[f'nfs.ganesha-{service_id}'].spec
            self.mgr.spec_store.rm(f'nfs.ganesha-{service_id}')
            spec.service_id = service_id
            self.mgr.spec_store.save(spec, True)

            # We have to remove the old daemons here as well, otherwise we'll end up with a port conflict.
            daemons = [d.name()
                       for d in self.mgr.cache.get_daemons_by_service(f'nfs.ganesha-{service_id}')]
            self.mgr.log.info(f'Removing old nfs.ganesha-{service_id} daemons {daemons}')
            self.mgr.remove_daemons(daemons)
        else:
            # redeploy all ganesha daemons to ensures that the daemon
            # cephx are correct AND container configs are set up properly
            daemons = [d.name() for d in self.mgr.cache.get_daemons_by_service(f'nfs.{service_id}')]
            self.mgr.log.info(f'Removing old nfs.{service_id} daemons {daemons}')
            self.mgr.remove_daemons(daemons)

            # re-save service spec (without pool and namespace properties!)
            spec = self.mgr.spec_store[f'nfs.{service_id}'].spec
            self.mgr.spec_store.save(spec)

        # import exports
        for export in exports:
            ex = ''
            for line in export.splitlines():
                if (
                        line.startswith('        secret_access_key =')
                        or line.startswith('        user_id =')
                ):
                    continue
                ex += line + '\n'
            self.mgr.log.debug(f'importing export: {ex}')
            ret, out, err = self.mgr.mon_command({
                'prefix': 'nfs export apply',
                'cluster_id': service_id
            }, inbuf=ex)
            if ret:
                self.mgr.log.warning(f'Failed to migrate export ({ret}): {err}\nExport was:\n{ex}')
        self.mgr.log.info(f'Done migrating nfs.{service_id}')

    def migrate_3_4(self) -> bool:
        # We can't set any host with the _admin label, but we're
        # going to warn when calling `ceph orch host rm...`
        if 'client.admin' not in self.mgr.keys.keys:
            self.mgr._client_keyring_set(
                entity='client.admin',
                placement='label:_admin',
            )
        return True

    def migrate_4_5(self) -> bool:
        registry_url = self.mgr.get_module_option('registry_url')
        registry_username = self.mgr.get_module_option('registry_username')
        registry_password = self.mgr.get_module_option('registry_password')
        if registry_url and registry_username and registry_password:

            registry_credentials = {'url': registry_url,
                                    'username': registry_username, 'password': registry_password}
            self.mgr.set_store('registry_credentials', json.dumps(registry_credentials))

            self.mgr.set_module_option('registry_url', None)
            self.mgr.check_mon_command({
                'prefix': 'config rm',
                'who': 'mgr',
                'key': 'mgr/cephadm/registry_url',
            })
            self.mgr.set_module_option('registry_username', None)
            self.mgr.check_mon_command({
                'prefix': 'config rm',
                'who': 'mgr',
                'key': 'mgr/cephadm/registry_username',
            })
            self.mgr.set_module_option('registry_password', None)
            self.mgr.check_mon_command({
                'prefix': 'config rm',
                'who': 'mgr',
                'key': 'mgr/cephadm/registry_password',
            })

            self.mgr.log.info('Done migrating registry login info')
        return True


def queue_migrate_nfs_spec(mgr: "CephadmOrchestrator", spec_dict: Dict[Any, Any]) -> None:
    """
    After 16.2.5 we dropped the NFSServiceSpec pool and namespace properties.
    Queue up a migration to process later, once we are sure that RADOS is available
    and so on.
    """
    service_id = spec_dict['spec']['service_id']
    args = spec_dict['spec'].get('spec', {})
    pool = args.pop('pool', 'nfs-ganesha')
    ns = args.pop('namespace', service_id)
    queued = mgr.get_store('nfs_migration_queue') or '[]'
    ls = json.loads(queued)
    ls.append([service_id, pool, ns])
    mgr.set_store('nfs_migration_queue', json.dumps(ls))
    mgr.log.info(f'Queued nfs.{service_id} for migration')
