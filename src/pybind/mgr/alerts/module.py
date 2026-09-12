
"""
A simple cluster health alerting module.
"""

from mgr_module import HandleCommandResult, MgrModule, Option
from email.utils import formatdate, make_msgid
from threading import Event
from typing import Any, Optional, Dict, List, TYPE_CHECKING, Union
from urllib.parse import urlparse
import http.client
import json
import smtplib
import ssl

from .cli import AlertsCLICommand


PAGERDUTY_SEVERITY_MAP = {
    'HEALTH_ERR': 'critical',
    'HEALTH_WARN': 'warning',
    'HEALTH_OK': 'info',
}


class Alerts(MgrModule):
    CLICommand = AlertsCLICommand
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
            desc='Email address to send alerts to, use commas to separate multiple',
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
            runtime=True),
        # pagerduty
        Option(
            name='pagerduty_routing_key',
            default='',
            desc='PagerDuty Events API v2 integration/routing key',
            runtime=True),
        Option(
            name='pagerduty_url',
            default='https://events.pagerduty.com/v2/enqueue',
            desc='PagerDuty Events API v2 endpoint URL',
            runtime=True),
        Option(
            name='pagerduty_source',
            default='',
            desc='PagerDuty event source (defaults to cluster fsid)',
            runtime=True),
        Option(
            name='pagerduty_component',
            default='ceph',
            desc='PagerDuty event component field',
            runtime=True),
        Option(
            name='pagerduty_timeout',
            type='secs',
            default=10,
            desc='Timeout for PagerDuty API requests',
            runtime=True),
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
            self.pagerduty_routing_key = ''
            self.pagerduty_url = ''
            self.pagerduty_source = ''
            self.pagerduty_component = ''
            self.pagerduty_timeout = 10

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

    @AlertsCLICommand.Read('alerts send')
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
        if self.pagerduty_routing_key:
            r = self._send_alert_pagerduty(status, diff)
            if r:
                for code, alert in r.items():
                    checks[code] = alert
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
            context = ssl.create_default_context()
            if self.smtp_ssl:
                server: Union[smtplib.SMTP_SSL, smtplib.SMTP] = \
                    smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=context)
            else:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            if self.smtp_password:
                server.login(self.smtp_user, self.smtp_password)
            server.sendmail(self.smtp_sender, self.smtp_destination.split(','), message)
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

    # PagerDuty
    def _pagerduty_get_source(self) -> str:
        if self.pagerduty_source:
            return self.pagerduty_source
        try:
            return self.get('mon_map')['fsid']
        except Exception:
            return 'ceph'

    def _pagerduty_dedup_key(self, source: str, code: str) -> str:
        if source:
            return '{s}/{c}'.format(s=source, c=code)
        return code

    def _pagerduty_severity(self, alert: Dict[str, Any]) -> str:
        return PAGERDUTY_SEVERITY_MAP.get(alert.get('severity', ''), 'warning')

    def _pagerduty_enqueue(self, payload: Dict[str, Any]) -> None:
        url = self.pagerduty_url or 'https://events.pagerduty.com/v2/enqueue'
        parsed = urlparse(url)
        host = parsed.netloc or parsed.path
        path = parsed.path if parsed.netloc else '/v2/enqueue'
        if not path:
            path = '/v2/enqueue'
        timeout = self.pagerduty_timeout or 10
        conn: Union[http.client.HTTPSConnection, http.client.HTTPConnection]
        if parsed.scheme == 'http':
            conn = http.client.HTTPConnection(host, timeout=timeout)
        else:
            conn = http.client.HTTPSConnection(host, timeout=timeout)
        try:
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }
            conn.request('POST', path, json.dumps(payload), headers)
            res = conn.getresponse()
            data = res.read()
            body = data.decode(errors='replace')
            if res.status >= 300:
                raise RuntimeError(
                    'PagerDuty API returned {s}: {b}'.format(s=res.status, b=body))
            self.log.debug('PagerDuty response: %s', body)
        finally:
            conn.close()

    def _pagerduty_trigger(self,
                           code: str,
                           alert: Dict[str, Any],
                           source: str) -> None:
        payload = {
            'routing_key': self.pagerduty_routing_key,
            'event_action': 'trigger',
            'dedup_key': self._pagerduty_dedup_key(source, code),
            'payload': {
                'summary': '[{code}] {msg}'.format(
                    code=code,
                    msg=alert.get('summary', {}).get('message', code)),
                'source': source,
                'severity': self._pagerduty_severity(alert),
                'component': self.pagerduty_component or 'ceph',
                'class': code,
                'custom_details': {
                    'code': code,
                    'severity': alert.get('severity', ''),
                    'count': alert.get('summary', {}).get('count', 0),
                    'detail': [d.get('message', '')
                               for d in alert.get('detail', [])],
                },
            },
        }
        self._pagerduty_enqueue(payload)

    def _pagerduty_resolve(self, code: str, source: str) -> None:
        payload = {
            'routing_key': self.pagerduty_routing_key,
            'event_action': 'resolve',
            'dedup_key': self._pagerduty_dedup_key(source, code),
        }
        self._pagerduty_enqueue(payload)

    def _send_alert_pagerduty(self,
                              status: Dict[str, Any],
                              diff: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.log.debug('_send_alert_pagerduty')
        source = self._pagerduty_get_source()
        # An empty diff means this is an on-demand `alerts send`; re-trigger
        # every currently-active check so PagerDuty state matches the cluster.
        if not diff:
            diff = {'new': dict(status.get('checks', {}))}
        errors: List[str] = []
        for code, alert in diff.get('new', {}).items():
            try:
                self._pagerduty_trigger(code, alert, source)
            except Exception as e:
                self.log.exception('PagerDuty trigger failed for %s', code)
                errors.append('trigger {c}: {e}'.format(c=code, e=e))
        for code, alert in diff.get('updated', {}).items():
            try:
                self._pagerduty_trigger(code, alert, source)
            except Exception as e:
                self.log.exception('PagerDuty update failed for %s', code)
                errors.append('update {c}: {e}'.format(c=code, e=e))
        for code, alert in diff.get('cleared', {}).items():
            try:
                self._pagerduty_resolve(code, source)
            except Exception as e:
                self.log.exception('PagerDuty resolve failed for %s', code)
                errors.append('resolve {c}: {e}'.format(c=code, e=e))
        if errors:
            return {
                'ALERTS_PAGERDUTY_ERROR': {
                    'severity': 'warning',
                    'summary': 'unable to send one or more PagerDuty events',
                    'count': len(errors),
                    'detail': errors,
                }
            }
        return None
