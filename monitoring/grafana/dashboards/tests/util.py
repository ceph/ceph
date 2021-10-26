import json
from pathlib import Path
from typing import Any, Dict, Tuple, Union

from termcolor import cprint

UNITS = ['ms', 's', 'm', 'h', 'd', 'w', 'y']


def resolve_time_and_unit(time: str) -> Union[Tuple[int, str], Tuple[None, None]]:
    """
    Divide time with its unit and return a tuple like (10, 'm')
    Return None if its and invalid prometheus time
    Valid units are inside UNITS.
    """
    if time[-1] in UNITS:
        return int(time[:-1]), time[-1]
    if time[-2:] in UNITS:
        return int(time[:-2]), time[-2:]
    return None, None


def get_dashboards_data() -> Dict[str, Any]:
    data: Dict[str, Any] = {'queries': {}, 'variables': {}, 'stats': {}}
    for file in Path(__file__).parent.parent.glob('*.json'):
        with open(file, 'r') as f:
            dashboard_data = json.load(f)
            data['stats'][str(file)] = {'total': 0, 'tested': 0}
            add_dashboard_queries(data, dashboard_data, str(file))
            add_dashboard_variables(data, dashboard_data)
    return data


def add_dashboard_queries(data: Dict[str, Any], dashboard_data: Dict[str, Any], path: str) -> None:
    """
    Grafana panels can have more than one target/query, in order to identify each
    query in the panel we append the "legendFormat" of the target to the panel name.
    format: panel_name-legendFormat
    """
    if 'panels' not in dashboard_data:
        return
    for panel in dashboard_data['panels']:
        if (
                'title' in panel
                and 'targets' in panel
                and len(panel['targets']) > 0
                and 'expr' in panel['targets'][0]
        ):
            for target in panel['targets']:
                title = panel['title']
                legend_format = target['legendFormat'] if 'legendFormat' in target else ""
                query_id = title + '-' + legend_format
                if query_id in data['queries']:
                    # NOTE: If two or more panels have the same name and legend it
                    # might suggest a refactoring is needed or add something else
                    # to identify each query.
                    cprint((f'WARNING: Query in panel "{title}" with legend "{legend_format}"'
                                       ' already exists'), 'yellow')
                data['queries'][query_id] = {'query': target['expr'], 'path': path}
                data['stats'][path]['total'] += 1


def add_dashboard_variables(data: Dict[str, Any], dashboard_data: Dict[str, Any]) -> None:
    if 'templating' not in dashboard_data or 'list' not in dashboard_data['templating']:
        return
    for variable in dashboard_data['templating']['list']:
        if 'name' in variable:
            data['variables'][variable['name']] = 'UNSET VARIABLE'
