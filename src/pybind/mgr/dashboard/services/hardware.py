

from typing import Any, Dict, List, Optional

from ..exceptions import DashboardException
from ..services.orchestrator import OrchClient

STATUS_OK = 'OK'
STATUS_WARNING = 'Warning'
STATUS_CRITICAL = 'Critical'
STATUS_UNKNOWN = 'Unknown'


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
    def get_compression() -> Dict[str, Any]:
        """
        GET /api/hardware/compression -- cluster-wide FCM hardware compression stats.

        Aggregates FCM data from node-proxy cache across all hosts and drives.
        FCM data shape: status['fcm'][sys_id][device] = {
            compression_ratio, savings_bytes, phy_util_percent, log_util_percent, ...
        }

        compression_ratio, savings_bytes and efficiency_percent are gated on
        total physical used > 100 GiB to avoid misleading values from metadata
        on fresh/small clusters (mirrors Grafana dashboard behaviour).
        """
        MIN_PHY_UTIL_BYTES = 107_374_182_400  # 100 GiB

        fcm_data = OrchClient.instance().hardware.common('fcm')

        total_phy_util_bytes = 0
        total_log_util_bytes = 0
        total_phy_size_bytes = 0
        fcm_drive_count = 0

        for host_fcm in fcm_data.values():
            for sys_drives in host_fcm.values():
                if not isinstance(sys_drives, dict):
                    continue
                for drive in sys_drives.values():
                    if not isinstance(drive, dict):
                        continue
                    fcm_drive_count += 1
                    total_phy_util_bytes += drive.get('phy_util_bytes') or 0
                    total_log_util_bytes += drive.get('log_util_bytes') or 0
                    total_phy_size_bytes += drive.get('phy_size_bytes') or 0

        if fcm_drive_count == 0:
            return {
                'fcm_drive_count': 0,
                'phy_util_percent': None,
                'log_util_percent': None,
                'savings_bytes': None,
                'compression_ratio': None,
                'efficiency_percent': None,
            }

        phy_util_percent = round(
            (total_phy_util_bytes / total_phy_size_bytes) * 100, 2
        ) if total_phy_size_bytes else None

        # Gate ratio/savings/efficiency on minimum physical utilisation.
        if total_phy_util_bytes < MIN_PHY_UTIL_BYTES:
            return {
                'fcm_drive_count': fcm_drive_count,
                'phy_util_percent': phy_util_percent,
                'log_util_percent': None,
                'savings_bytes': None,
                'compression_ratio': None,
                'efficiency_percent': None,
            }

        compression_ratio = (
            round(total_log_util_bytes / total_phy_util_bytes, 2)
            if total_phy_util_bytes > 0 else None
        )
        savings_bytes = total_log_util_bytes - total_phy_util_bytes
        efficiency_percent = (
            round(100 * (1 - total_phy_util_bytes / total_log_util_bytes), 2)
            if total_log_util_bytes > 0 else None
        )
        log_util_percent = (
            round((total_log_util_bytes / total_phy_size_bytes) * 100, 2)
            if total_phy_size_bytes else None
        )

        return {
            'fcm_drive_count': fcm_drive_count,
            'phy_util_percent': phy_util_percent,
            'log_util_percent': log_util_percent,
            'savings_bytes': savings_bytes,
            'compression_ratio': compression_ratio,
            'efficiency_percent': efficiency_percent,
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
