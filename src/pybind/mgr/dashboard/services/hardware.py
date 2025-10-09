

from typing import Any, Dict, List, Optional

from ..exceptions import DashboardException
from ..services.orchestrator import OrchClient


class HardwareService(object):

    @staticmethod
    def get_summary(categories: Optional[List[str]] = None,
                    hostname: Optional[List[str]] = None):
        total_count = {'total': 0, 'ok': 0, 'error': 0}

        output: Dict[str, Any] = {
            'total': {
                'category': {},
                'total': {}
            },
            'host': {
                'flawed': 0
            }
        }

        def count_ok(data: dict) -> int:
            return sum(
                component.get("status", {}).get("health") == "OK"
                for node in data.values()
                for system in node.values()
                for component in system.values()
            )

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
            category_total = {
                'total': count_total(data),
                'ok': count_ok(data),
                'error': 0
            }

            for host, systems in data.items():
                output['host'].setdefault(host, {'flawed': False})
                if not output['host'][host]['flawed']:
                    for system in systems.values():
                        if any(dimm['status']['health'] != 'OK' for dimm in system.values()):
                            output['host'][host]['flawed'] = True
                            break

            category_total['error'] = max(0, category_total['total'] - category_total['ok'])
            output['total']['category'].setdefault(category, {})
            output['total']['category'][category] = category_total

            total_count['total'] += category_total['total']
            total_count['ok'] += category_total['ok']
            total_count['error'] += category_total['error']

        output['total']['total'] = total_count

        output['host']['flawed'] = sum(1 for host in output['host']
                                       if host != 'flawed' and output['host'][host]['flawed'])

        return output

    @staticmethod
    def validate_categories(categories: Optional[List[str]]) -> List[str]:
        categories_list = ['memory', 'storage', 'processors',
                           'network', 'power', 'fans']

        if isinstance(categories, str):
            categories = [categories]
        elif categories is None:
            categories = categories_list
        elif not isinstance(categories, list):
            raise DashboardException(msg=f'{categories} is not a list',
                                     component='Hardware')
        if not all(item in categories_list for item in categories):
            raise DashboardException(msg=f'Invalid category, there is no {categories}',
                                     component='Hardware')

        return categories
