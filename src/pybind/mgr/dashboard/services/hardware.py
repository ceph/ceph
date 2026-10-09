
from typing import Any, Dict, List, Optional

from ..exceptions import DashboardException
from ..services.orchestrator import OrchClient

STATUS_OK = 'OK'
STATUS_WARNING = 'Warning'
STATUS_CRITICAL = 'Critical'
STATUS_UNKNOWN = 'Unknown'

# Maps node-proxy status.health values (case-insensitive) to API health strings.
# Redfish uses: "OK", "Warning", "Critical"
_HEALTH_MAP = {
    'ok': STATUS_OK,
    'warning': STATUS_WARNING,
    'critical': STATUS_CRITICAL,
}

# All category names match node-proxy keys directly in the API response.
_CATEGORIES = ['storage', 'processors', 'memory', 'network', 'power', 'fans', 'temperatures']


class HardwareService(object):

    @staticmethod
    def get_summary(categories: Optional[List[str]] = None,
                    hostname: Optional[List[str]] = None):
        total_count = {'total': 0, 'ok': 0, 'warn': 0, 'critical': 0}

        output: Dict[str, Any] = {
            'total': {
                'category': {},
                'total': {}
            },
            'host': {
                'flawed': 0
            }
        }

        def _get_health(component: Any) -> str:
            if not isinstance(component, dict):
                return STATUS_UNKNOWN
            status_val = component.get('status', {})
            if isinstance(status_val, dict):
                return status_val.get('health', STATUS_UNKNOWN)
            if isinstance(status_val, str):
                return status_val
            return STATUS_UNKNOWN

        def count_by_health(data: dict) -> Dict[str, int]:
            counts = {'ok': 0, 'warn': 0, 'critical': 0}
            for node in data.values():
                for system in node.values():
                    for component in system.values():
                        health = _get_health(component)
                        if health == STATUS_OK:
                            counts['ok'] += 1
                        elif health == STATUS_WARNING:
                            counts['warn'] += 1
                        elif health == STATUS_CRITICAL:
                            counts['critical'] += 1
            return counts

        def count_total(data: dict) -> int:
            return sum(
                len(component)
                for system in data.values()
                for component in system.values()
            )

        categories = HardwareService.validate_categories(categories)

        orch_hardware_instance = OrchClient.instance().hardware
        for category in categories:
            data = orch_hardware_instance.common(category, hostname)
            health_counts = count_by_health(data)
            category_total = {
                'total': count_total(data),
                'ok': health_counts['ok'],
                'warn': health_counts['warn'],
                'critical': health_counts['critical']
            }

            for host, systems in data.items():
                output['host'].setdefault(host, {'flawed': False})
                if not output['host'][host]['flawed']:
                    for system in systems.values():
                        if any(_get_health(comp) != STATUS_OK
                               for comp in system.values()):
                            output['host'][host]['flawed'] = True
                            break

            output['total']['category'].setdefault(category, {})
            output['total']['category'][category] = category_total

            total_count['total'] += category_total['total']
            total_count['ok'] += category_total['ok']
            total_count['warn'] += category_total['warn']
            total_count['critical'] += category_total['critical']

        output['total']['total'] = total_count

        output['host']['flawed'] = sum(
            1 for host in output['host']
            if host != 'flawed' and output['host'][host]['flawed']
        )

        return output

    @staticmethod
    def _get_component_health(component: Any) -> str:
        """Extract the Redfish health string from a component dict, normalised to our constants."""
        if not isinstance(component, dict):
            return STATUS_UNKNOWN
        status_val = component.get('status', {})
        raw = ''
        if isinstance(status_val, dict):
            raw = status_val.get('health', '')
        elif isinstance(status_val, str):
            raw = status_val
        return _HEALTH_MAP.get(raw.lower(), STATUS_UNKNOWN)

    @staticmethod
    def _count_category(category_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Given one category's data for a single host
        (shape: sys_id -> component_id -> component_dict),
        return { health, total, ok, warning, critical }.

        health = worst across all components:  critical > warning > ok > unknown
        """
        counts = {STATUS_OK: 0, STATUS_WARNING: 0, STATUS_CRITICAL: 0}
        unknown_count = 0
        for sys_components in category_data.values():
            for component in sys_components.values():
                h = HardwareService._get_component_health(component)
                if h in counts:
                    counts[h] += 1
                else:
                    # STATUS_UNKNOWN — still counts toward total
                    unknown_count += 1

        total = sum(counts.values()) + unknown_count

        # Derive worst health: critical > warning > ok.
        if counts[STATUS_CRITICAL] > 0:
            worst = STATUS_CRITICAL
        elif counts[STATUS_WARNING] > 0:
            worst = STATUS_WARNING
        elif total > 0:
            worst = STATUS_OK
        else:
            worst = STATUS_UNKNOWN

        return {
            'health': worst,
            'total': total,
            'ok': counts[STATUS_OK],
            'warning': counts[STATUS_WARNING],
            'critical': counts[STATUS_CRITICAL],
        }

    @staticmethod
    def _extract_firmware(firmware_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Flatten firmware inventory into { component_name: version }.
        The key in firmware_data is an opaque Redfish ID; we use the
        human-readable 'name' field instead.
        Skips entries with unknown/empty versions.
        """
        result: Dict[str, str] = {}
        for fw_info in firmware_data.values():
            if not isinstance(fw_info, dict):
                continue
            name = fw_info.get('name', '')
            version = fw_info.get('version', '')
            if name and version and version != 'unknown':
                result[name] = version
        return result

    @staticmethod
    def _summarise_host(host_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build the per-host summary dict from a single host's fullreport blob.

        Shape of host_data (from NodeProxyCache):
            host_data['status'][category][sys_id][component_id] = { status, ... }
            host_data['firmware'][fw_id] = { name, version, ... }
            host_data['sn']   = serial number string
            host_data['host'] = hostname string

        """
        status = host_data.get('status', {})

        category_summaries: Dict[str, Any] = {}
        for cat in _CATEGORIES:
            cat_data = status.get(cat, {})
            category_summaries[cat] = HardwareService._count_category(cat_data)

        # Overall host health = worst health across all categories.
        health_rank = {STATUS_CRITICAL: 3, STATUS_WARNING: 2, STATUS_OK: 1, STATUS_UNKNOWN: 0}
        worst = max(
            (s['health'] for s in category_summaries.values()),
            key=lambda h: health_rank.get(h, 0),
            default=STATUS_UNKNOWN
        )

        firmware = HardwareService._extract_firmware(
            host_data.get('firmware', host_data.get('firmwares', {}))
        )

        return {
            'hostname': host_data.get('host', ''),
            'sn': host_data.get('sn', ''),
            'health': worst,
            **category_summaries,
            'firmware': firmware,
        }

    @staticmethod
    def get_hosts(page: int = 1, per_page: int = 10) -> Dict[str, Any]:
        """
        GET /api/hardware/hosts — paginated list of hosts with hardware summary.

        :param page:     1-based page number
        :param per_page: hosts per page (default 10)
        """
        hardware = OrchClient.instance().hardware

        all_hosts = hardware.list_hosts()
        total = len(all_hosts)

        # Clamp page/per_page to valid range.
        page = max(1, page)
        per_page = max(1, per_page)
        start = (page - 1) * per_page
        end = start + per_page
        page_hosts = all_hosts[start:end]

        # Fetch and summarise each host in the page.
        # TOCTOU: a host may be removed between list_hosts() and fullreport().
        # Skip empty reports rather than emitting phantom entries.
        hosts = []
        for hostname in page_hosts:
            report = hardware.fullreport(hostname=hostname)
            host_blob = report.get(hostname)
            if not host_blob:
                continue
            hosts.append(HardwareService._summarise_host(host_blob))

        return {
            'hosts': hosts,
            'total': total,
            'page': page,
            'per_page': per_page,
        }

    @staticmethod
    def _flatten_category(category_data: Dict[str, Any], fields: List[str],
                          include_id: bool = False) -> List[Dict[str, Any]]:
        """
        Flatten a node-proxy category blob into a list of component dicts.

        Input shape: sys_id -> component_id -> { field: value, status: { health, state } }
        Output: [ { chassis_id, [id,] <fields...>, health, state }, ... ]

        :param category_data: one category's data for a single host
        :param fields:        list of field names to extract from each component
        :param include_id:    if True, include the Redfish component_id as 'id'
                              (useful for processors/memory where socket ID is meaningful)
        """
        result = []
        for chassis_id, components in category_data.items():
            for comp_id, component in components.items():
                if not isinstance(component, dict):
                    continue
                entry: Dict[str, Any] = {'chassis_id': chassis_id}
                if include_id:
                    entry['id'] = comp_id
                for field in fields:
                    if field in component:
                        entry[field] = component[field]
                status = component.get('status', {})
                if isinstance(status, dict):
                    entry['health'] = _HEALTH_MAP.get(
                        status.get('health', '').lower(), STATUS_UNKNOWN
                    )
                    entry['state'] = status.get('state', '')
                else:
                    # Absent or malformed status — default to unknown.
                    entry['health'] = STATUS_UNKNOWN
                    entry['state'] = ''
                result.append(entry)
        return result

    @staticmethod
    def get_host_detail(hostname: str) -> Dict[str, Any]:
        """
        GET /api/hardware/hosts/{hostname} — full per-component detail for one host.
        """
        hardware = OrchClient.instance().hardware

        report = hardware.fullreport(hostname=hostname)
        host_data = report.get(hostname)
        if not host_data:
            raise DashboardException(
                msg=f"Host '{hostname}' has no node-proxy data",
                http_status_code=404,
                component='Hardware'
            )
        status = host_data.get('status', {})

        return {
            'hostname': host_data.get('host', hostname),
            'sn': host_data.get('sn', ''),

            'storage': HardwareService._flatten_category(
                status.get('storage', {}),
                ['slot', 'description', 'model', 'protocol', 'serial_number',
                 'capacity_bytes', 'firmware_version']
            ),

            'fans': HardwareService._flatten_category(
                status.get('fans', {}),
                ['name', 'physical_context', 'reading', 'reading_units']
            ),
            'temperatures': HardwareService._flatten_category(
                status.get('temperatures', {}),
                ['name', 'physical_context', 'reading', 'reading_units']
            ),
            # include_id=True: cpu.socket.1, cpu.socket.2 etc. identifies the physical socket.
            'processors': HardwareService._flatten_category(
                status.get('processors', {}),
                ['description', 'model', 'manufacturer', 'total_cores', 'total_threads',
                 'processor_type'],
                include_id=True
            ),
            # include_id=True: dimm.socket.a1 etc. identifies the physical DIMM slot.
            'memory': HardwareService._flatten_category(
                status.get('memory', {}),
                ['description', 'memory_device_type', 'capacity_mi_b'],
                include_id=True
            ),
            # include_id=True: oslogicalnetwork.2 etc. identifies the NIC interface.
            'network': HardwareService._flatten_category(
                status.get('network', {}),
                ['name', 'description', 'speed_mbps'],
                include_id=True
            ),
            'power': HardwareService._flatten_category(
                status.get('power', {}),
                ['name', 'model', 'manufacturer']
            ),
            'firmware': HardwareService._extract_firmware(
                host_data.get('firmware', host_data.get('firmwares', {}))
            ),
        }

    @staticmethod
    def validate_categories(categories: Optional[List[str]]) -> List[str]:
        categories_list = ['memory', 'storage', 'processors',
                           'network', 'power', 'fans', 'temperatures']

        if isinstance(categories, str):
            categories = [categories]
        elif categories is None:
            categories = categories_list
        elif not isinstance(categories, list):
            raise DashboardException(msg=f'{categories} is not a list',
                                     component='Hardware')
        if not all(item in categories_list for item in categories):
            raise DashboardException(
                msg=f'Invalid category, there is no {categories}',
                component='Hardware'
            )

        return categories
