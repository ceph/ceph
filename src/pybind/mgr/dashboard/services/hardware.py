

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
