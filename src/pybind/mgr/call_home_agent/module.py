"""
IBM Ceph Call Home Agent
"""

from typing import List, Any, Tuple, Dict, Optional, Set, Callable
from datetime import datetime
import json
import requests
import asyncio
import os

from mgr_module import Option, CLIReadCommand, MgrModule, HandleCommandResult, NotifyType

from .config import get_settings
from .options import CHES_ENDPOINT, INTERVAL_INVENTORY_REPORT_MINUTES, INTERVAL_PERFORMANCE_REPORT_MINUTES

# Module with the functions to be used in telemetry and in CCHA for retrieving information from the ceph cluster
from .dataCollectors import inventory

reports_header = {
    "agent": "RedHat_Marina_firmware_agent",
    "api_key": "",
    "private_key": "",
    "target_space": "dev",
    "asset": "ceph",
    "asset_id": "",
    "asset_type": "RedHatMarine",
    "asset_vendor": "IBM",
    "asset_virtual_id": "",
    "country_code": "",
    "event_id": "",
    "event_time": "",
    "event_time_ms": 0,
    "local_event_time": "",
    "software_level": {
        "name": "ceph_software",
        "vrmf": "8.3.0.1"
    },
    "type": "eccnext_apisv1s",
    "version": "1.0.0.1",
    "analytics_event_source_type": "asset_event",
    "analytics_type": "ceph",
    "analytics_instance": "",
    "analytics_virtual_id": "",
    "analytics_group": "Storage",
    "analytics_category": "RedHatMarine",
    "events": []
}

# TODO: report functions providing the json paylod should be imported from the data collectors module


def report_inventory() -> str:
    """
    Produce the inventory report
    """
    dt = datetime.timestamp(datetime.now())
    # Set timestamps
    reports_header['event_time'] = datetime.fromtimestamp(dt).strftime("%Y-%m-%d %H:%M:%S")
    reports_header['event_time_ms'] = str(int(dt))
    reports_header['local_event_time'] = datetime.fromtimestamp(dt).strftime("%a %b %d %H:%M:%S %Z")

    # Set event id
    reports_header['event_id'] = "IBM_chc_event_RedHatMarine_ceph_{}_daily_report_{}".format(
                                            reports_header['asset_virtual_id'],
                                            reports_header['event_time_ms'])

    # return json.dumps({**reports_header, **inventory()}) <---- TODO ADD INVENTORY (from datacollector) as event
    return json.dumps(reports_header)


def report_performance() -> str:
    """
    TODO: temporally uses inventory report
    """
    return report_inventory()


class Report:
    def __init__(self, name: str, fn: Callable[[], str], url: str, minutes_interval: int):
        self.name = name                       # name of the report
        self.fn = fn                           # function used to retrieve the data
        self.url = url                         # url to send the report
        self.interval = minutes_interval * 60  # interval to send the report (seconds)

    def print(self) -> str:
        if reports_header['private_key'] and reports_header['api_key']:
            return self.fn()
        else:
            raise Exception('Not able to print <%s> report. Identification keys are not available' % self.name)

    def send(self) -> str:
        fail_reason = ""
        resp = None
        if reports_header['private_key'] and reports_header['api_key']:
            try:
                resp = requests.post(url=self.url,
                                     headers={'accept': 'application/json', 'content-type': 'application/json'},
                                     data=self.fn())
                resp.raise_for_status()
            except Exception as e:
                explanation = resp.content if resp else ''
                fail_reason = 'Failed to send <%s> to <%s>: %s \n%s' % (self.name, self.url, str(e), explanation)
        else:
            fail_reason = 'Not able to send <%s> report. Identification keys are not available' % self.name
        return fail_reason


class CallHomeAgent(MgrModule):
    MODULE_OPTIONS: List[Option] = [
        Option(
            name='ches_endpoint',
            type='str',
            default=CHES_ENDPOINT,
            desc='Call Home Event streamer end point'
        ),
        Option(
            name='interval_inventory_report_minutes',
            type='int',
            default=INTERVAL_INVENTORY_REPORT_MINUTES,
            desc='Time frequency for the inventory report'
        ),
        Option(
            name='interval_performance_report_minutes',
            type='int',
            default=INTERVAL_PERFORMANCE_REPORT_MINUTES,
            desc='Time frequency for the performance report'
        ),
        Option(
            name='customer_email',
            type='str',
            default='',
            desc='Customer contact email'
        ),
        Option(
            name='country_code',
            type='str',
            default='',
            desc='Customer country code'
        )
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(CallHomeAgent, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown()
        self.run = True

        # Init module options
        # Env vars (if they exist) have preference over module options
        self.ches_url = str(os.environ.get('CHA_CHES_ENDPOINT', self.get_module_option('ches_endpoint')))
        self.interval_performance_minutes = int(
            os.environ.get('CHA_INTERVAL_INVENTORY_REPORT_MINUTES',
                           self.get_module_option('interval_inventory_report_minutes')))  # type: ignore
        self.interval_inventory_minutes = int(
            os.environ.get('CHA_INTERVAL_PERFORMANCE_REPORT_MINUTES',
                           self.get_module_option('interval_performance_report_minutes')))  # type: ignore
        self.customer_email = os.environ.get('CHA_CUSTOMER_EMAIL', self.get_module_option('customer_email'))
        self.country_code = os.environ.get('CHA_COUNTRY_CODE', self.get_module_option('country_code'))
        # Health checks
        self.health_checks: Dict[str, Dict[str, Any]] = dict()

        # configure common report headers
        self._configure_headers()

        # Prepare reports
        self.reports = {'inventory': Report('inventory', report_inventory,
                                            self.ches_url,
                                            self.interval_performance_minutes),
                        'performance': Report('performance', report_performance,
                                              self.ches_url,
                                              self.interval_inventory_minutes)
                        }

    async def report_task(self, rpt: Report) -> None:
        """
            Coroutine for sending the report passed as parameter
        """
        self.log.info('Launched task for <%s> report each %s seconds)' % (rpt.name, rpt.interval))
        while self.run:
            try:
                self.log.info('Sending <%s> report to <%s>' % (rpt.name, rpt.url))
                send_error = rpt.send()
            except Exception as ex:
                send_error = str(ex)

            if send_error:
                self.log.error(send_error)
                self.health_checks.update({
                    'CCHA_ERROR_SENDING_REPORT': {
                        'severity': 'error',
                        'summary': 'Ceph Call Home Agent manager module: error sending <%s> report to IBM Storage '
                                   'Insights systems' % rpt.name,
                        'detail': [send_error]
                    }
                })
            else:
                self.health_checks = {}
                self.log.info('Successfully sent <%s> report to <%s>' % (rpt.name, rpt.url))

            self.set_health_checks(self.health_checks)
            await asyncio.sleep(rpt.interval)

    def launch_coroutines(self) -> None:
        """
         Launch module coroutines (reports or any other async task)
        """
        loop = asyncio.new_event_loop()  # type: ignore
        try:
            for rptName, rpt in self.reports.items():
                t = loop.create_task(self.report_task(rpt))
            loop.run_forever()
        except Exception as ex:
            self.log.error(ex)

    def serve(self) -> None:
        """
            TODO:
            - Register instance in CHES???: TODO
            - Launch ccha web server: TODO
            - Launch coroutines report tasks
        """
        self.log.info('Starting IBM Ceph Call Home Agent')

        # Launch coroutines for the reports
        self.launch_coroutines()

        self.log.info('IBM Call home agent finished')

    def shutdown(self) -> None:
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Stopping IBM Ceph Call Home Agent')
        self.run = False

    def notify(self, notify_type: NotifyType, tag: str) -> None:
        """
            TODO:
            Way to detect changes in
            osd_map, mon_map, fs_map, mon_status, health, pg_summary, command, service_map
            Generate an "inventory change report" and send to CHES changes endpoint
        """
        pass

    def config_notify(self) -> None:
        """
        This method is called whenever one of our config options is changed.
        """
        # This is some boilerplate that stores MODULE_OPTIONS in a class
        # member, so that, for instance, the 'emphatic' option is always
        # available as 'self.emphatic'.
        for opt in self.MODULE_OPTIONS:
            pass

    def _configure_headers(self) -> None:
        id_data = {'private_key': b'', 'api_key': b''}
        try:
            id_data = get_settings()
            self.health_checks = {}
        except Exception as ex:
            self.log.error('Error getting encrypted identification keys: %s. '
                           'Provide keys and restart Ceph Call Home module', ex)
            self.health_checks.update({
                'CCHA_ID_KEYS_NOT_AVAILABLE': {
                    'severity': 'error',
                    'summary': 'Ceph Call Home Agent manager module: The private identification keys needed to connect '
                               'with storage insights system are not available',
                    'detail': ['Provide the right keys and restart the Ceph Call Home manager module']
                }
            })
        self.set_health_checks(self.health_checks)

        cluster_config = self.get('config')
        reports_header['private_key'] = id_data['private_key'].decode('utf-8')  # type: ignore
        reports_header['api_key'] = id_data['api_key'].decode('utf-8')  # type: ignore
        reports_header['asset_id'] = cluster_config['fsid']
        reports_header['asset_virtual_id'] =  cluster_config['fsid']
        reports_header['country_code'] = self.get_module_option('country_code')
        reports_header['analytics_instance'] = cluster_config['fsid']
        reports_header['analytics_virtual_id'] = cluster_config['fsid']

    def alert_worker(self, alerts: str) -> None:
        """
            TODO:
            send the alerts to the ches alert endpoint
        """

    @CLIReadCommand('callhome print report')
    def print_report_cmd(self, report_name: str) -> Tuple[int, str, str]:
        """
            Prints the report requested.
            Example:
                ceph callhome print report inventory
            Available reports: inventory
        """
        return HandleCommandResult(stdout=f'report:, {self.reports[report_name].print()}')

    @CLIReadCommand('callhome send report')
    def send_report_cmd(self, report_name: str) -> Tuple[int, str, str]:
        """
            Command for sending the report requested.
        """
        send_error = self.reports[report_name].send()
        if send_error:
            return HandleCommandResult(stdout=send_error)
        else:
            return HandleCommandResult(stdout=f'{report_name} report successfully sent')

    # Temporal Data collectors (they will be imported from a manager scope module library)
    def ccha_web_server(self) -> None:
        """
            TODO:
            - configure https web server
            - Initialize route endpoints.
                In first phase we will have only the alert receiver for processing alert manager notifications
                - https://<mgr_host>:<port>/ccha_receiver
                  it calls ccha.alertWorker
            - start web server
        """

