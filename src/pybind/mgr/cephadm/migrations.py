import json
import re
import logging
from typing import TYPE_CHECKING, Iterator, Optional, Dict, Any, List

from ceph.deployment.service_spec import PlacementSpec, ServiceSpec, HostPlacementSpec, RGWSpec, CertificateSource
from cephadm.schedule import HostAssignment
from cephadm.utils import SpecialHostLabels
import rados
from mgr_util import parse_combined_pem_file, get_cert_issuer_info
from cephadm.tlsobject_types import CertKeyPair

from mgr_module import NFS_POOL_NAME
from orchestrator import OrchestratorError, DaemonDescription

if TYPE_CHECKING:
    from .module import CephadmOrchestrator

LAST_MIGRATION = 9

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
        self.set_sane_migration_current()

        v = mgr.get_store('nfs_migration_queue')
        self.nfs_migration_queue = json.loads(v) if v else []

        r = mgr.get_store('rgw_migration_queue')
        self.rgw_migration_queue = json.loads(r) if r else []

        r = mgr.get_store('rgw_ssl_migration_queue')
        self.rgw_ssl_migration_queue = json.loads(r) if r else []

        # for some migrations, we don't need to do anything except for
        # incrementing migration_current.
        # let's try to shortcut things here.
        self.migrate(True)

    def set(self, val: int) -> None:
        self.mgr.set_module_option('migration_current', val)
        self.mgr.migration_current = val

    def set_sane_migration_current(self) -> None:
        # migration current should always be an integer
        # between 0 and LAST_MIGRATION (inclusive) in order to
        # actually carry out migration. If we find
        # it is None or too high of a value here we should
        # set it to some sane value
        mc: Optional[int] = self.mgr.migration_current
        if mc is None:
            logger.info('Found migration_current of "None". Setting to last migration.')
            self.set(LAST_MIGRATION)
            return

        if mc > LAST_MIGRATION:
            logger.error(f'Found migration_current of {mc} when max should be {LAST_MIGRATION}. Setting back to 0.')
            # something has gone wrong and caused migration_current
            # to be higher than it should be able to be. Best option
            # we have here is to just set it back to 0
            self.set(0)

    def is_migration_ongoing(self) -> bool:
        self.set_sane_migration_current()
        mc: Optional[int] = self.mgr.migration_current
        return mc is None or mc < LAST_MIGRATION

    def verify_no_migration(self) -> None:
        if self.is_migration_ongoing():
            # this is raised in module.serve()
            raise OrchestratorError(
                "cephadm migration still ongoing. Please wait, until the migration is complete.")

    def migrate(self, startup: bool = False) -> None:
        if self.mgr.migration_current == 0:
            logger.info('Running migration 0 -> 1')
            if self.migrate_0_1():
                self.set(1)

        if self.mgr.migration_current == 1:
            logger.info('Running migration 1 -> 2')
            if self.migrate_1_2():
                self.set(2)

        if self.mgr.migration_current == 2 and not startup:
            logger.info('Running migration 2 -> 3')
            if self.migrate_2_3():
                self.set(3)

        if self.mgr.migration_current == 3:
            logger.info('Running migration 3 -> 4')
            if self.migrate_3_4():
                self.set(4)

        if self.mgr.migration_current == 4:
            logger.info('Running migration 4 -> 5')
            if self.migrate_4_5():
                self.set(5)

        if self.mgr.migration_current == 5:
            logger.info('Running migration 5 -> 6')
            if self.migrate_5_6():
                self.set(6)

        if self.mgr.migration_current == 6:
            logger.info('Running migration 6 -> 7')
            if self.migrate_6_7():
                self.set(7)

        if self.mgr.migration_current == 7:
            logger.info('Running migration 7 -> 8')
            if self.migrate_7_8():
                self.set(8)

        if self.mgr.migration_current == 8:
            logger.info('Running migration 8 -> 9')
            if self.migrate_8_9():
                self.set(9)

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
                draining_hosts=self.mgr.cache.get_draining_hosts(),
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
                placement=f'label:{SpecialHostLabels.ADMIN}',
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

    def migrate_rgw_spec(self, spec: Dict[Any, Any]) -> Optional[RGWSpec]:
        """ Migrate an old rgw spec to the new format."""
        new_spec = spec.copy()
        field_content: List[str] = re.split(' +', new_spec['spec']['rgw_frontend_type'])
        valid_spec = False
        if 'beast' in field_content:
            new_spec['spec']['rgw_frontend_type'] = 'beast'
            field_content.remove('beast')
            valid_spec = True
        elif 'civetweb' in field_content:
            new_spec['spec']['rgw_frontend_type'] = 'civetweb'
            field_content.remove('civetweb')
            valid_spec = True
        else:
            # Error: Should not happen as that would be an invalid RGW spec. In that case
            # we keep the spec as it, mark it as unmanaged to avoid the daemons being deleted
            # and raise a health warning so the user can fix the issue manually later.
            self.mgr.log.error("Cannot migrate RGW spec, bad rgw_frontend_type value: {spec['spec']['rgw_frontend_type']}.")

        if valid_spec:
            new_spec['spec']['rgw_frontend_extra_args'] = []
            new_spec['spec']['rgw_frontend_extra_args'].extend(field_content)

        return RGWSpec.from_json(new_spec)

    def rgw_spec_needs_migration(self, spec: Dict[Any, Any]) -> bool:
        if 'spec' not in spec:
            # if users allowed cephadm to set up most of the
            # attributes, it's possible there is no "spec" section
            # inside the spec. In that case, no migration is needed
            return False
        return 'rgw_frontend_type' in spec['spec'] \
            and spec['spec']['rgw_frontend_type'] is not None \
            and spec['spec']['rgw_frontend_type'].strip() not in ['beast', 'civetweb']

    def migrate_5_6(self) -> bool:
        """
        Migration 5 -> 6

        Old RGW spec used to allow 'bad' values on the rgw_frontend_type field. For example
        the following value used to be valid:

          rgw_frontend_type: "beast endpoint=10.16.96.54:8043 tcp_nodelay=1"

        As of 17.2.6 release, these kind of entries are not valid anymore and a more strict check
        has been added to validate this field.

        This migration logic detects this 'bad' values and tries to transform them to the new
        valid format where rgw_frontend_type field can only be either 'beast' or 'civetweb'.
        Any extra arguments detected on rgw_frontend_type field will be parsed and passed in the
        new spec field rgw_frontend_extra_args.
        """
        logger.info(f'Starting rgw migration (queue length is {len(self.rgw_migration_queue)})')
        for s in self.rgw_migration_queue:
            spec = s['spec']
            if self.rgw_spec_needs_migration(spec):
                rgw_spec = self.migrate_rgw_spec(spec)
                if rgw_spec is not None:
                    logger.info(f"Migrating {spec} to new RGW with extra args format {rgw_spec}")
                    self.mgr.spec_store.save(rgw_spec)
            else:
                logger.info(f"No Migration is needed for rgw spec: {spec}")

        self.rgw_migration_queue = []
        return True

    def migrate_6_7(self) -> bool:
        # start by placing certs/keys from rgw, iscsi, and ingress specs into cert store
        for spec in self.mgr.spec_store.all_specs.values():
            if spec.service_type in ['rgw', 'ingress', 'iscsi']:
                logger.info(f'Migrating certs/keys for {spec.service_name()} spec to cert store')
                self.mgr.spec_store._save_certs_and_keys(spec)

        # Grafana certs are stored based on the host they are placed on
        grafana_cephadm_signed_certs = True
        for grafana_daemon in self.mgr.cache.get_daemons_by_type('grafana'):
            logger.info(f'Checking for cert/key for {grafana_daemon.name()}')
            hostname = grafana_daemon.hostname
            assert hostname is not None  # for mypy
            grafana_cert_path = f'{hostname}/grafana_crt'
            grafana_key_path = f'{hostname}/grafana_key'
            grafana_cert = self.mgr.get_store(grafana_cert_path)
            grafana_key = self.mgr.get_store(grafana_key_path)
            if grafana_cert:
                (org, cn) = get_cert_issuer_info(grafana_cert)
                if org == 'Ceph':
                    logger.info(f'Migrating {grafana_daemon.name()}/{hostname} cert/key to cert store (as cephadm-signed certs)')
                    self.mgr.cert_mgr.register_self_signed_cert_key_pair('grafana')
                    self.mgr.cert_mgr.save_self_signed_cert_key_pair('grafana', CertKeyPair(grafana_cert, grafana_key), host=hostname)
                else:
                    logger.info(f'Migrating {grafana_daemon.name()}/{hostname} cert/key to cert store (as custom-certs)')
                    grafana_cephadm_signed_certs = False
                    self.mgr.cert_mgr.save_cert('grafana_ssl_cert', grafana_cert, host=hostname)
                    self.mgr.cert_mgr.save_key('grafana_ssl_key', grafana_key, host=hostname)

        if not grafana_cephadm_signed_certs:
            # Update the spec to specify the right certificate source
            grafana_spec = self.mgr.spec_store['grafana'].spec
            grafana_spec.certificate_source = CertificateSource.REFERENCE.value
            self.mgr.spec_store.save(grafana_spec)

        # NOTE: prometheus, alertmanager, and node-exporter certs were not stored
        # and appeared to just be generated at daemon deploy time if secure_monitoring_stack
        # was set to true. Therefore we have nothing to migrate for those daemons
        return True

    def migrate_7_8(self) -> bool:
        logger.info(f'Starting rgw SSL/TLS migration (queue length is {len(self.rgw_ssl_migration_queue)})')
        for s in self.rgw_ssl_migration_queue:

            svc_spec = s['spec']  # this is the RGWspec

            if 'spec' not in svc_spec:
                logger.info(f"No SSL/TLS fields migration is needed for rgw spec: {svc_spec}")
                continue

            cert_field = svc_spec['spec'].get('rgw_frontend_ssl_certificate')
            if not cert_field:
                logger.info(f"No SSL/TLS fields migration is needed for rgw spec: {svc_spec}")
                continue

            cert_str = '\n'.join(cert_field) if isinstance(cert_field, list) else cert_field
            ssl_cert, ssl_key = parse_combined_pem_file(cert_str)
            new_spec = svc_spec.copy()
            new_spec['spec'].update({
                'rgw_frontend_ssl_certificate': None,
                'certificate_source': CertificateSource.INLINE.value,
                'ssl_cert': ssl_cert,
                'ssl_key': ssl_key,
            })

            logger.info(f"Migrating {svc_spec} to new RGW SSL/TLS format {new_spec}")
            self.mgr.spec_store.save(RGWSpec.from_json(new_spec))

        self.rgw_ssl_migration_queue = []
        return True

    def migrate_8_9(self) -> bool:
        """
        Replace Promtail with Alloy.

        - If mgr daemons are still being upgraded, return True WITHOUT bumping migration_current.
        - Mark Promtail service unmanaged so cephadm won't redeploy it.
        - Remove Promtail daemons to free ports.
        - Deploy Alloy with Promtail's placement.
        - Once Alloy is confirmed deployed, remove Promtail service spec.
        """
        try:
            target_digests = getattr(self.mgr.upgrade.upgrade_state, "target_digests", [])
            active_mgr_digests = self.mgr.get_active_mgr_digests()

            if target_digests:
                if not any(d in target_digests for d in active_mgr_digests):
                    logger.info(
                        "Promtail -> Alloy migration: mgr daemons still upgrading. "
                        "Marking as complete without bumping migration_current."
                    )
                    return False

            promtail_spec = self.mgr.spec_store.active_specs.get("promtail")
            if not promtail_spec:
                logger.info("Promtail -> Alloy migration: no Promtail \
                    service found, nothing to do.")
                return True

            if not promtail_spec.unmanaged:
                logger.info("Promtail -> Alloy migration: marking promtail unmanaged")
                self.mgr.spec_store.set_unmanaged("promtail", True)

            daemons = self.mgr.cache.get_daemons()
            promtail_daemons = [d for d in daemons if d.daemon_type == "promtail"]
            if promtail_daemons:
                promtail_names = [d.name() for d in promtail_daemons]
                logger.info(f"Promtail -> Alloy migration: removing daemons {promtail_names}")
                self.mgr.remove_daemons(promtail_names)

            daemons = self.mgr.cache.get_daemons()
            if any(d.daemon_type == "promtail" for d in daemons):
                logger.info(
                    "Promtail -> Alloy migration: promtail daemons still present, "
                    "skipping Alloy deployment until next run."
                )
                return False

            alloy_spec = ServiceSpec(
                service_type="alloy",
                service_id="alloy",
                placement=promtail_spec.placement
            )

            logger.info("Promtail -> Alloy migration: deploying Alloy service")
            self.mgr.apply_alloy(alloy_spec)

            logger.info("Promtail -> Alloy migration: removing promtail service spec")
            self.mgr.remove_service("promtail")

            logger.info("Promtail -> Alloy migration completed successfully.")
            return True

        except Exception as e:
            logger.error(f"Promtail -> Alloy migration failed: {e}")
            return False


def queue_migrate_rgw_spec(mgr: "CephadmOrchestrator", spec_dict: Dict[Any, Any]) -> None:
    """
    As aprt of 17.2.6 a stricter RGW spec validation has been added so the field
    rgw_frontend_type cannot be used to pass rgw-frontends parameters.
    """
    service_id = spec_dict['spec']['service_id']
    queued = mgr.get_store('rgw_migration_queue') or '[]'
    ls = json.loads(queued)
    ls.append(spec_dict)
    mgr.set_store('rgw_migration_queue', json.dumps(ls))
    logger.info(f'Queued rgw.{service_id} for migration')


def queue_migrate_rgw_ssl_spec(mgr: "CephadmOrchestrator", spec_dict: Dict[Any, Any]) -> None:
    service_id = spec_dict['spec']['service_id']
    queued = mgr.get_store('rgw_ssl_migration_queue') or '[]'
    ls = json.loads(queued)
    ls.append(spec_dict)
    mgr.set_store('rgw_ssl_migration_queue', json.dumps(ls))
    logger.info(f'Queued rgw.{service_id} for TLS migration')


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
