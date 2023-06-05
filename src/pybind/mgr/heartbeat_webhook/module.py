import logging
import json
from mgr_module import MgrModule, Option, HandleCommandResult, CLIReadCommand
from typing import Any, List, TYPE_CHECKING
from threading import Event
import urllib3


class HeartbeatWebhook(MgrModule):
    MODULE_OPTIONS = [
        Option(
            name='interval',
            type='secs',
            default=60,
            desc='How frequently to reexamine health status',
            runtime=True),
        Option(
            name='webhook_url',
            type='string',
            default='',
            desc='Url to send webhooks to',
            runtime=True),
        Option(
            name='send_on_warn',
            type='bool',
            default=False,
            desc="Continue sending webhooks if the cluster health is HEALTH_WARN")
    ]
    NATIVE_OPTIONS: List[str] = [
    ]
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(HeartbeatWebhook, self).__init__(*args, **kwargs)

        self.run = True
        self.event = Event()
        
        self.config_notify()

        self.log.info("Initializing heartbeat_webhook module")

        self.http = urllib3.PoolManager()

        if TYPE_CHECKING:
            self.interval = 60
            self.webhook_url = ''

    def config_notify(self) -> None:
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))
        # Do the same for the native options.
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))

    def _get_fsid(self) -> str:
        mon_map = self.get('mon_map')
        return mon_map['fsid']

    @CLIReadCommand('heartbeat_webhook test')
    def webhook_test(self):
        status = json.loads(self.get('health')['json'])
        webhook_data = {'ceph_status': status['status'], 'fsid': self._get_fsid(), 'origin': 'Ceph mgr heartbeat_webhook module'}
        response = self.http.request('POST',
                                    self.webhook_url,
                                    body = json.dumps(webhook_data),
                                    headers = {'Content-Type': 'application/json'},
                                    retries = False)
        stdout = f"Response code: {response.status} Response data: {response.data}"
        return HandleCommandResult(retval=0, stdout=stdout, stderr='')
        
    def _check(self) -> None:
        # If we don't have a mon connection, we will likely have stale data that we shouldn't trust
        if self.have_mon_connection():
            status = json.loads(self.get('health')['json'])
            webhook_data = {'ceph_status': status['status'], 'fsid': self._get_fsid(), 'origin': 'Ceph mgr heartbeat_webhook module'}
            self.log.debug(f"Found status: {status}")
            if status['status'] == "HEALTH_OK":
                self.log.info("Cluster status is 'HEALTH_OK', sending status webhook")
                self._send_webhook(webhook_data)
                
            elif status['status'] == "HEALTH_WARN":
                if self.send_on_warn:
                    self.log.info(f"Cluster status is 'HEALTH_WARN' but send_on_warn option is set, sending status webhook.")
                    self._send_webhook(webhook_data)
                else:
                     self.log.info(f"Cluster status is 'HEALTH_WARN', not sending status webhook")
        else:
            self.log.error("Not sending webhook, mgr is disconnected from mons")

    def _send_webhook(self, webhook_data: dict) -> None:
                response = self.http.request('POST',
                                        self.webhook_url,
                                        body = json.dumps(webhook_data),
                                        headers = {'Content-Type': 'application/json'},
                                        retries = False)
                if response.status >= 200 and response.status < 400:
                     self.log.info("Webhook sent successfuly")
                else:
                    self.log.error(f"Error sending webhook: {response.data}")

    def serve(self) -> None:
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.info("Starting")
        while self.run:
            self._check()
            self.log.debug(f"Sleeping for {self.interval} seconds")
            self.event.wait(self.interval)
            self.event.clear()

    def shutdown(self) -> None:
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()
