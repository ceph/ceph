# -*- coding: utf-8 -*-

import json
import logging

import rados
from mgr_module import CommandResult
from mgr_util import get_most_recent_rate, get_time_series_rates, name_to_config_section

from .. import mgr
from ..model.rgw import EncryptionConfig, EncryptionTypes, KmipConfig, \
    KmsConfig, KmsProviders, S3Config, VaultConfig

try:
    from typing import Any, Dict, List, Optional, Union
except ImportError:
    pass  # For typing only

logger = logging.getLogger('ceph_service')


class SendCommandError(rados.Error):
    def __init__(self, err, prefix, argdict, errno):
        self.prefix = prefix
        self.argdict = argdict
        super(SendCommandError, self).__init__(err, errno)


# pylint: disable=R0904
class CephService(object):

    OSD_FLAG_NO_SCRUB = 'noscrub'
    OSD_FLAG_NO_DEEP_SCRUB = 'nodeep-scrub'

    PG_STATUS_SCRUBBING = 'scrubbing'
    PG_STATUS_DEEP_SCRUBBING = 'deep'

    SCRUB_STATUS_DISABLED = 'Disabled'
    SCRUB_STATUS_ACTIVE = 'Active'
    SCRUB_STATUS_INACTIVE = 'Inactive'

    @classmethod
    def get_service_map(cls, service_name):
        service_map = {}  # type: Dict[str, dict]
        for server in mgr.list_servers():
            for service in server['services']:
                if service['type'] == service_name:
                    if server['hostname'] not in service_map:
                        service_map[server['hostname']] = {
                            'server': server,
                            'services': []
                        }
                    inst_id = service['id']
                    metadata = mgr.get_metadata(service_name, inst_id)
                    status = mgr.get_daemon_status(service_name, inst_id)
                    service_map[server['hostname']]['services'].append({
                        'id': inst_id,
                        'type': service_name,
                        'hostname': server['hostname'],
                        'metadata': metadata,
                        'status': status
                    })
        return service_map

    @classmethod
    def get_service_list(cls, service_name):
        service_map = cls.get_service_map(service_name)
        return [svc for _, svcs in service_map.items() for svc in svcs['services']]

    @classmethod
    def get_service_data_by_metadata_id(cls,
                                        service_type: str,
                                        metadata_id: str) -> Optional[Dict[str, Any]]:
        for server in mgr.list_servers():
            for service in server['services']:
                if service['type'] == service_type:
                    metadata = mgr.get_metadata(service_type, service['id'])
                    if metadata_id == metadata['id']:
                        return {
                            'id': metadata['id'],
                            'service_map_id': str(service['id']),
                            'type': service_type,
                            'hostname': server['hostname'],
                            'metadata': metadata
                        }
        return None

    @classmethod
    def get_service(cls, service_type: str, metadata_id: str) -> Optional[Dict[str, Any]]:
        svc_data = cls.get_service_data_by_metadata_id(service_type, metadata_id)
        if svc_data:
            svc_data['status'] = mgr.get_daemon_status(svc_data['type'], svc_data['service_map_id'])
        return svc_data

    @classmethod
    def get_service_perf_counters(cls, service_type: str, service_id: str) -> Dict[str, Any]:
        schema_dict = mgr.get_perf_schema(service_type, service_id)
        schema = schema_dict["{}.{}".format(service_type, service_id)]
        counters = []
        for key, value in sorted(schema.items()):
            counter = {'name': str(key), 'description': value['description']}
            # pylint: disable=W0212
            if mgr._stattype_to_str(value['type']) == 'counter':
                counter['value'] = cls.get_rate(
                    service_type, service_id, key)
                counter['unit'] = mgr._unit_to_str(value['units'])
            else:
                counter['value'] = mgr.get_latest(
                    service_type, service_id, key)
                counter['unit'] = ''
            counters.append(counter)

        return {
            'service': {
                'type': service_type,
                'id': str(service_id)
            },
            'counters': counters
        }

    @classmethod
    def get_pool_list(cls, application=None):
        osd_map = mgr.get('osd_map')
        if not application:
            return osd_map['pools']
        return [pool for pool in osd_map['pools']
                if application in pool.get('application_metadata', {})]

    @classmethod
    def get_pool_list_with_stats(cls, application=None):
        # pylint: disable=too-many-locals
        pools = cls.get_pool_list(application)

        pools_w_stats = []

        pg_summary = mgr.get("pg_summary")
        pool_stats = mgr.get_updated_pool_stats()

        for pool in pools:
            pool['pg_status'] = pg_summary['by_pool'][pool['pool'].__str__()]
            stats = pool_stats[pool['pool']]
            s = {}

            for stat_name, stat_series in stats.items():
                rates = get_time_series_rates(stat_series)
                s[stat_name] = {
                    'latest': stat_series[0][1],
                    'rate': get_most_recent_rate(rates),
                    'rates': rates
                }
            pool['stats'] = s
            pools_w_stats.append(pool)
        return pools_w_stats

    @classmethod
    def get_erasure_code_profiles(cls):
        def _serialize_ecp(name, ecp):
            def serialize_numbers(key):
                value = ecp.get(key)
                if value is not None:
                    ecp[key] = int(value)

            ecp['name'] = name
            serialize_numbers('k')
            serialize_numbers('m')
            return ecp

        ret = []
        for name, ecp in mgr.get('osd_map').get('erasure_code_profiles', {}).items():
            ret.append(_serialize_ecp(name, ecp))
        return ret

    @classmethod
    def get_pool_name_from_id(cls, pool_id):
        # type: (int) -> Union[str, None]
        return mgr.rados.pool_reverse_lookup(pool_id)

    @classmethod
    def get_pool_by_attribute(cls, attribute, value):
        # type: (str, Any) -> Union[dict, None]
        pool_list = cls.get_pool_list()
        for pool in pool_list:
            if attribute in pool and pool[attribute] == value:
                return pool
        return None

    @classmethod
    def get_encryption_config(cls, daemon_name: str) -> Dict[str, Any]:
        full_daemon_name = f'rgw.{daemon_name}'

        encryption_config = EncryptionConfig(kms=None, s3=None)

        # Fetch configuration for KMS
        if EncryptionTypes.KMS.value:
            vault_config_data: Optional[Dict[str, Any]] = _get_conf_keys(
                list(VaultConfig._fields), VaultConfig.required_fields(),
                EncryptionTypes.KMS.value, KmsProviders.VAULT.value, full_daemon_name
            )

            kmip_config_data: Optional[Dict[str, Any]] = _get_conf_keys(
                list(KmipConfig._fields), KmipConfig.required_fields(),
                EncryptionTypes.KMS.value, KmsProviders.KMIP.value, full_daemon_name
            )

            kms_config: List[KmsConfig] = []

            if vault_config_data:
                vault_config_data = _set_defaults_in_encryption_configs(
                    vault_config_data, EncryptionTypes.KMS.value, KmsProviders.VAULT.value
                )
                kms_config.append(KmsConfig(vault=VaultConfig(**vault_config_data)))

            if kmip_config_data:
                kmip_config_data = _set_defaults_in_encryption_configs(
                    kmip_config_data, EncryptionTypes.KMS.value, KmsProviders.KMIP.value
                )
                kms_config.append(KmsConfig(kmip=KmipConfig(**kmip_config_data)))

            if kms_config:
                encryption_config = encryption_config._replace(kms=kms_config)

        # Fetch configuration for S3
        if EncryptionTypes.S3.value:
            s3_config_data: Optional[Dict[str, Any]] = _get_conf_keys(
                list(VaultConfig._fields), VaultConfig.required_fields(),
                EncryptionTypes.S3.value, KmsProviders.VAULT.value, full_daemon_name
            )

            s3_config: List[S3Config] = []
            if s3_config_data:
                s3_config_data = _set_defaults_in_encryption_configs(
                    s3_config_data, EncryptionTypes.S3.value, KmsProviders.VAULT.value
                )
                s3_config.append(S3Config(vault=VaultConfig(**s3_config_data)))

                encryption_config = encryption_config._replace(s3=s3_config)

        return encryption_config.to_dict()

    @classmethod
    def set_encryption_config(cls, encryption_type: str, kms_provider: str,
                              config: Union[VaultConfig, KmipConfig],
                              daemon_name: str) -> None:
        full_daemon_name = 'rgw.' + daemon_name

        config_dict = config._asdict() if isinstance(config, (VaultConfig, KmipConfig)) else config

        if isinstance(config_dict, dict):
            if kms_provider == KmsProviders.VAULT.value:
                config = VaultConfig(**config_dict)
            elif kms_provider == KmsProviders.KMIP.value:
                config = KmipConfig(**config_dict)

        for field in config._fields:
            value = getattr(config, field)
            if value is None:
                continue

            if isinstance(value, bool):
                value = str(value).lower()

            key = _generate_key(encryption_type, kms_provider, field)

            CephService.send_command('mon', 'config set',
                                     who=name_to_config_section(full_daemon_name),
                                     name=key, value=value)

    @classmethod
    def set_multisite_config(cls, realm_name, zonegroup_name, zone_name, daemon_name):
        full_daemon_name = 'rgw.' + daemon_name

        KMS_CONFIG = [
            ['rgw_realm', realm_name],
            ['rgw_zonegroup', zonegroup_name],
            ['rgw_zone', zone_name]
        ]

        for (key, value) in KMS_CONFIG:
            if value == 'null':
                continue
            CephService.send_command('mon', 'config set',
                                     who=name_to_config_section(full_daemon_name),
                                     name=key, value=value)
        return {}

    @classmethod
    def get_realm_tokens(cls):
        tokens_info = mgr.remote('rgw', 'get_realm_tokens')
        return tokens_info

    @classmethod
    def import_realm_token(cls, realm_token, zone_name, port, placement_spec):
        tokens_info = mgr.remote('rgw', 'import_realm_token', zone_name=zone_name,
                                 realm_token=realm_token, port=port, placement=placement_spec,
                                 start_radosgw=True)
        return tokens_info

    @classmethod
    def get_pool_pg_status(cls, pool_name):
        # type: (str) -> dict
        pool = cls.get_pool_by_attribute('pool_name', pool_name)
        if pool is None:
            return {}
        return mgr.get("pg_summary")['by_pool'][pool['pool'].__str__()]

    @staticmethod
    def send_command(srv_type, prefix, srv_spec='', to_json=True, inbuf='', **kwargs):
        # type: (str, str, Optional[str], bool, str, Any) -> Any
        """
        :type prefix: str
        :param srv_type: mon |
        :param kwargs: will be added to argdict
        :param srv_spec: typically empty. or something like "<fs_id>:0"
        :param to_json: if true return as json format

        :raises PermissionError: See rados.make_ex
        :raises ObjectNotFound: See rados.make_ex
        :raises IOError: See rados.make_ex
        :raises NoSpace: See rados.make_ex
        :raises ObjectExists: See rados.make_ex
        :raises ObjectBusy: See rados.make_ex
        :raises NoData: See rados.make_ex
        :raises InterruptedOrTimeoutError: See rados.make_ex
        :raises TimedOut: See rados.make_ex
        :raises ValueError: return code != 0
        """
        argdict = {
            "prefix": prefix,
        }
        if to_json:
            argdict["format"] = "json"
        argdict.update({k: v for k, v in kwargs.items() if v is not None})
        result = CommandResult("")
        mgr.send_command(result, srv_type, srv_spec, json.dumps(argdict), "", inbuf=inbuf)
        r, outb, outs = result.wait()
        if r != 0:
            logger.error("send_command '%s' failed. (r=%s, outs=\"%s\", kwargs=%s)", prefix, r,
                         outs, kwargs)

            raise SendCommandError(outs, prefix, argdict, r)

        try:
            return json.loads(outb or outs)
        except Exception:  # pylint: disable=broad-except
            return outb

    @staticmethod
    def _get_smart_data_by_device(device):
        # type: (dict) -> Dict[str, dict]
        # Check whether the device is associated with daemons.
        if 'daemons' in device and device['daemons']:
            dev_smart_data: Dict[str, Any] = {}

            # Get a list of all OSD daemons on all hosts that are 'up'
            # because SMART data can not be retrieved from daemons that
            # are 'down' or 'destroyed'.
            osd_tree = CephService.send_command('mon', 'osd tree')
            osd_daemons_up = [
                node['name'] for node in osd_tree.get('nodes', {})
                if node.get('status') == 'up'
            ]

            # All daemons on the same host can deliver SMART data,
            # thus it is not relevant for us which daemon we are using.
            # NOTE: the list may contain daemons that are 'down' or 'destroyed'.
            for daemon in device['daemons']:
                svc_type, svc_id = daemon.split('.', 1)
                if 'osd' in svc_type:
                    if daemon not in osd_daemons_up:
                        continue
                    try:
                        dev_smart_data = CephService.send_command(
                            svc_type, 'smart', svc_id, devid=device['devid'])
                    except SendCommandError as error:
                        logger.warning(str(error))
                        # Try to retrieve SMART data from another daemon.
                        continue
                elif 'mon' in svc_type:
                    try:
                        dev_smart_data = CephService.send_command(
                            svc_type, 'device query-daemon-health-metrics', who=daemon)
                    except SendCommandError as error:
                        logger.warning(str(error))
                        # Try to retrieve SMART data from another daemon.
                        continue
                else:
                    dev_smart_data = {}

                CephService.log_dev_data_error(dev_smart_data)

                break

            return dev_smart_data
        logger.warning('[SMART] No daemons associated with device ID "%s"',
                       device['devid'])
        return {}

    @staticmethod
    def log_dev_data_error(dev_smart_data):
        for dev_id, dev_data in dev_smart_data.items():
            if 'error' in dev_data:
                logger.warning(
                    '[SMART] Error retrieving smartctl data for device ID "%s": %s',
                    dev_id, dev_data)

    @staticmethod
    def get_devices_by_host(hostname):
        # type: (str) -> dict
        return CephService.send_command('mon',
                                        'device ls-by-host',
                                        host=hostname)

    @staticmethod
    def get_devices_by_daemon(daemon_type, daemon_id):
        # type: (str, str) -> dict
        return CephService.send_command('mon',
                                        'device ls-by-daemon',
                                        who='{}.{}'.format(
                                            daemon_type, daemon_id))

    @staticmethod
    def get_smart_data_by_host(hostname):
        # type: (str) -> dict
        """
        Get the SMART data of all devices on the given host, regardless
        of the daemon (osd, mon, ...).
        :param hostname: The name of the host.
        :return: A dictionary containing the SMART data of every device
          on the given host. The device name is used as the key in the
          dictionary.
        """
        devices = CephService.get_devices_by_host(hostname)
        smart_data = {}  # type: dict
        if devices:
            for device in devices:
                if device['devid'] not in smart_data:
                    smart_data.update(
                        CephService._get_smart_data_by_device(device))
        else:
            logger.debug('[SMART] could not retrieve device list from host %s', hostname)
        return smart_data

    @staticmethod
    def get_smart_data_by_daemon(daemon_type, daemon_id):
        # type: (str, str) -> Dict[str, dict]
        """
        Get the SMART data of the devices associated with the given daemon.
        :param daemon_type: The daemon type, e.g. 'osd' or 'mon'.
        :param daemon_id: The daemon identifier.
        :return: A dictionary containing the SMART data of every device
          associated with the given daemon. The device name is used as the
          key in the dictionary.
        """
        devices = CephService.get_devices_by_daemon(daemon_type, daemon_id)
        smart_data = {}  # type: Dict[str, dict]
        if devices:
            for device in devices:
                if device['devid'] not in smart_data:
                    smart_data.update(
                        CephService._get_smart_data_by_device(device))
        else:
            msg = '[SMART] could not retrieve device list from daemon with type %s and ' +\
                'with ID %s'
            logger.debug(msg, daemon_type, daemon_id)
        return smart_data

    @classmethod
    def get_rates(cls, svc_type, svc_name, path):
        """
        :return: the derivative of mgr.get_counter()
        :rtype: list[tuple[int, float]]"""
        data = mgr.get_counter(svc_type, svc_name, path)[path]
        return get_time_series_rates(data)

    @classmethod
    def get_rate(cls, svc_type, svc_name, path):
        """returns most recent rate"""
        return get_most_recent_rate(cls.get_rates(svc_type, svc_name, path))

    @classmethod
    def get_client_perf(cls):
        pools_stats = mgr.get('osd_pool_stats')['pool_stats']

        io_stats = {
            'read_bytes_sec': 0,
            'read_op_per_sec': 0,
            'write_bytes_sec': 0,
            'write_op_per_sec': 0,
        }
        recovery_stats = {'recovering_bytes_per_sec': 0}

        for pool_stats in pools_stats:
            client_io = pool_stats['client_io_rate']
            for stat in list(io_stats.keys()):
                if stat in client_io:
                    io_stats[stat] += client_io[stat]

            client_recovery = pool_stats['recovery_rate']
            for stat in list(recovery_stats.keys()):
                if stat in client_recovery:
                    recovery_stats[stat] += client_recovery[stat]

        client_perf = io_stats.copy()
        client_perf.update(recovery_stats)

        return client_perf

    @classmethod
    def get_scrub_status(cls):
        enabled_flags = mgr.get('osd_map')['flags_set']
        if cls.OSD_FLAG_NO_SCRUB in enabled_flags or cls.OSD_FLAG_NO_DEEP_SCRUB in enabled_flags:
            return cls.SCRUB_STATUS_DISABLED

        grouped_pg_statuses = mgr.get('pg_summary')['all']
        for grouped_pg_status in grouped_pg_statuses.keys():
            if len(grouped_pg_status.split(cls.PG_STATUS_SCRUBBING)) > 1 \
                    or len(grouped_pg_status.split(cls.PG_STATUS_DEEP_SCRUBBING)) > 1:
                return cls.SCRUB_STATUS_ACTIVE

        return cls.SCRUB_STATUS_INACTIVE

    @classmethod
    def get_pg_info(cls):
        pg_summary = mgr.get('pg_summary')
        object_stats = {stat: pg_summary['pg_stats_sum']['stat_sum'][stat] for stat in [
            'num_objects', 'num_object_copies', 'num_objects_degraded',
            'num_objects_misplaced', 'num_objects_unfound']}

        pgs_per_osd = 0.0
        total_osds = len(pg_summary['by_osd'])
        if total_osds > 0:
            total_pgs = 0.0
            for _, osd_pg_statuses in pg_summary['by_osd'].items():
                for _, pg_amount in osd_pg_statuses.items():
                    total_pgs += pg_amount

            pgs_per_osd = total_pgs / total_osds

        return {
            'object_stats': object_stats,
            'statuses': pg_summary['all'],
            'pgs_per_osd': pgs_per_osd,
        }


def _generate_key(encryption_type: str, backend: str, config_name: str):
    if encryption_type == EncryptionTypes.KMS.value:
        return f'rgw_crypt_{backend}_{config_name}'

    if encryption_type == EncryptionTypes.S3.value:
        return f'rgw_crypt_sse_s3_{backend}_{config_name}'
    return None


def _get_conf_keys(fields: List[str], req_fields: List[str], encryption_type: str,
                   backend: str, daemon_name: str) -> Dict[str, Any]:
    config: Dict[str, Any] = {}

    for field in fields:
        key = _generate_key(encryption_type, backend, field)
        try:
            value = CephService.send_command('mon', 'config get',
                                             who=name_to_config_section(daemon_name), key=key)
            if field in req_fields and not (isinstance(value, str) and value.strip()):
                config = {}
                break

            config[field] = value.strip() if isinstance(value, str) else value
        except Exception as e:  # pylint: disable=broad-except
            logger.exception('Error %s while fetching configuration for %s', e, key)
    return config


def _set_defaults_in_encryption_configs(config: Dict[str, Any],
                                        encryption_type: str,
                                        kms_provider: str) -> Dict[str, Any]:
    config['backend'] = kms_provider
    config['encryption_type'] = encryption_type
    config['unique_id'] = f'{encryption_type}-{kms_provider}'
    return config
