
"""
A simple cluster health alerting module.
"""

from mgr_module import CLIReadCommand, HandleCommandResult, MgrModule, Option
from email.utils import formatdate, make_msgid
from threading import Event
from typing import Any, Optional, Dict, List, TYPE_CHECKING, Union
import json
import smtplib


class Alerts(MgrModule):
    MODULE_OPTIONS = [
        Option(
            name='interval',
            type='secs',
            default=60,
            desc='How frequently to reexamine health status',
            runtime=True),
        # smtp
        Option(
            name='smtp_host',
            default='',
            desc='SMTP server',
            runtime=True),
        Option(
            name='smtp_destination',
            default='',
            desc='Email address to send alerts to',
            runtime=True),
        Option(
            name='smtp_port',
            type='int',
            default=465,
            desc='SMTP port',
            runtime=True),
        Option(
            name='smtp_ssl',
            type='bool',
            default=True,
            desc='Use SSL to connect to SMTP server',
            runtime=True),
        Option(
            name='smtp_user',
            default='',
            desc='User to authenticate as',
            runtime=True),
        Option(
            name='smtp_password',
            default='',
            desc='Password to authenticate with',
            runtime=True),
        Option(
            name='smtp_sender',
            default='',
            desc='SMTP envelope sender',
            runtime=True),
        Option(
            name='smtp_from_name',
            default='Ceph',
            desc='Email From: name',
            runtime=True)
    ]

    # These are "native" Ceph options that this module cares about.
    NATIVE_OPTIONS: List[str] = [
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Alerts, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown()
        self.run = True
        self.event = Event()

        # ensure config options members are initialized; see config_notify()
        self.config_notify()

        self.log.info("Init")

        if TYPE_CHECKING:
            self.interval = 60
            self.smtp_host = ''
            self.smtp_destination = ''
            self.smtp_port = 0
            self.smtp_ssl = True
            self.smtp_user = ''
            self.smtp_password = ''
            self.smtp_sender = ''
            self.smtp_from_name = ''

    def config_notify(self) -> None:
        """
        This method is called whenever one of our config options is changed.
        """
        # This is some boilerplate that stores MODULE_OPTIONS in a class
        # member, so that, for instance, the 'emphatic' option is always
        # available as 'self.emphatic'.
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

    @CLIReadCommand('alerts send')
    def send(self) -> HandleCommandResult:
        """
        (re)send alerts immediately
        """
        status = json.loads(self.get('health')['json'])
        self._send_alert(status, {})
        return HandleCommandResult()

    def _diff(self, last: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        for code, alert in new.get('checks', {}).items():
            self.log.debug('new code %s alert %s' % (code, alert))
            if code not in last.get('checks', {}):
                if 'new' not in d:
                    d['new'] = {}
                d['new'][code] = alert
            elif (alert['summary'].get('count', 0)
                  > last['checks'][code]['summary'].get('count', 0)):
                if 'updated' not in d:
                    d['updated'] = {}
                d['updated'][code] = alert
        for code, alert in last.get('checks', {}).items():
            self.log.debug('old code %s alert %s' % (code, alert))
            if code not in new.get('checks', {}):
                if 'cleared' not in d:
                    d['cleared'] = {}
                d['cleared'][code] = alert
        return d

    def _send_alert(self, status: Dict[str, Any], diff: Dict[str, Any]) -> None:
        checks = {}
        if self.smtp_host:
            r = self._send_alert_smtp(status, diff)
            if r:
                for code, alert in r.items():
                    checks[code] = alert
        else:
            self.log.warning('Alert is not sent because smtp_host is not configured')
        self.set_health_checks(checks)

    def serve(self) -> None:
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.info("Starting")
        last_status: Dict[str, Any] = {}
        while self.run:
            # Do some useful background work here.
            new_status = json.loads(self.get('health')['json'])
            if new_status != last_status:
                self.log.debug('last_status %s' % last_status)
                self.log.debug('new_status %s' % new_status)
                diff = self._diff(last_status,
                                  new_status)
                self.log.debug('diff %s' % diff)
                if diff:
                    self._send_alert(new_status, diff)
                last_status = new_status

            self.log.debug('Sleeping for %s seconds', self.interval)
            self.event.wait(self.interval or 60)
            self.event.clear()

    def shutdown(self) -> None:
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    # SMTP
    def _smtp_format_alert(self, code: str, alert: Dict[str, Any]) -> str:
        r = '[{sev}] {code}: {summary}\n'.format(
            code=code,
            sev=alert['severity'].split('_')[1],
            summary=alert['summary']['message'])
        for detail in alert['detail']:
            r += '        {message}\n'.format(
                message=detail['message'])
        return r

    def _send_alert_smtp(self,
                         status: Dict[str, Any],
                         diff: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # message
        self.log.debug('_send_alert_smtp')
        message = ('From: {from_name} <{sender}>\n'
                   'Subject: {status}\n'
                   'To: {target}\n'
                   'Message-Id: {message_id}\n'
                   'Date: {date}\n'
                   '\n'
                   '{status}\n'.format(
                       sender=self.smtp_sender,
                       from_name=self.smtp_from_name,
                       status=status['status'],
                       target=self.smtp_destination,
                       message_id=make_msgid(),
                       date=formatdate()))

        if 'new' in diff:
            message += ('\n--- New ---\n')
            for code, alert in diff['new'].items():
                message += self._smtp_format_alert(code, alert)
        if 'updated' in diff:
            message += ('\n--- Updated ---\n')
            for code, alert in diff['updated'].items():
                message += self._smtp_format_alert(code, alert)
        if 'cleared' in diff:
            message += ('\n--- Cleared ---\n')
            for code, alert in diff['cleared'].items():
                message += self._smtp_format_alert(code, alert)

        message += ('\n\n=== Full health status ===\n')
        for code, alert in status['checks'].items():
            message += self._smtp_format_alert(code, alert)

        self.log.debug('message: %s' % message)

        # send
        try:
            if self.smtp_ssl:
                server: Union[smtplib.SMTP_SSL, smtplib.SMTP] = \
                    smtplib.SMTP_SSL(self.smtp_host, self.smtp_port)
            else:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            if self.smtp_password:
                server.login(self.smtp_user, self.smtp_password)
            server.sendmail(self.smtp_sender, self.smtp_destination, message)
            server.quit()
        except Exception as e:
            return {
                'ALERTS_SMTP_ERROR': {
                    'severity': 'warning',
                    'summary': 'unable to send alert email',
                    'count': 1,
                    'detail': [str(e)]
                }
            }
        self.log.debug('Sent email to %s' % self.smtp_destination)
        return None
