import json
import smtplib
from typing import Any, Dict, List, Sequence
from unittest import mock

import pytest

from alerts.module import Alerts


def _check(code: str, message: str, count: int = 1,
           details: Sequence[str] = ()) -> Dict[str, Any]:
    return {
        code: {
            'severity': 'HEALTH_WARN',
            'summary': {'message': message, 'count': count},
            'detail': [{'message': d} for d in details],
            'muted': False,
        }
    }


def _health(*checks: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for c in checks:
        merged.update(c)
    return {
        'status': 'HEALTH_WARN' if merged else 'HEALTH_OK',
        'checks': merged,
        'mutes': [],
    }


HEALTHY = _health()
OSD_DOWN = _check('OSD_DOWN', '1 osds down', details=['osd.0 is down'])
OSD_DOWN_2 = _check('OSD_DOWN', '2 osds down', count=2,
                    details=['osd.0 is down', 'osd.1 is down'])


@pytest.fixture()
def module():
    m = Alerts('alerts', 0, 0)
    # the module is not running in serve(), so set_health_checks() only
    # records what it was given
    m.set_health_checks = mock.MagicMock()
    yield m


def _configure(m: Alerts, **opts: Any) -> None:
    for k, v in opts.items():
        m.set_module_option(k, v)
    m.config_notify()


def _run_serve(m: Alerts, statuses: List[Dict[str, Any]]) -> None:
    """
    Run serve() once for every status in statuses, as if the health had
    changed (or not) between two wakeups of the loop.
    """
    it = iter(statuses)
    current = [next(it)]

    def get(what: str) -> Dict[str, str]:
        assert what == 'health'
        return {'json': json.dumps(current[0])}

    def wait(timeout: float) -> bool:
        try:
            current[0] = next(it)
        except StopIteration:
            m.shutdown()
        return True

    with mock.patch.object(m, 'get', side_effect=get), \
            mock.patch.object(m.event, 'wait', side_effect=wait):
        m.serve()


def _sent_message(smtp: mock.MagicMock) -> str:
    server = smtp.return_value
    server.sendmail.assert_called_once()
    return server.sendmail.call_args[0][2]


@mock.patch('alerts.module.smtplib.SMTP')
@mock.patch('alerts.module.smtplib.SMTP_SSL')
def test_health_change_sends_alert(smtp_ssl, smtp, module):
    _configure(module,
               smtp_host='smtp.example.com',
               smtp_port=2465,
               smtp_sender='ceph@example.com',
               smtp_destination='a@example.com,b@example.com')

    _run_serve(module, [_health(OSD_DOWN)])

    smtp.assert_not_called()
    smtp_ssl.assert_called_once_with('smtp.example.com', 2465,
                                     context=mock.ANY)
    server = smtp_ssl.return_value
    # no password configured, so no login
    server.login.assert_not_called()
    sender, targets, _ = server.sendmail.call_args[0]
    assert sender == 'ceph@example.com'
    assert targets == ['a@example.com', 'b@example.com']
    message = _sent_message(smtp_ssl)
    assert 'Subject: HEALTH_WARN\n' in message
    assert '--- New ---\n[WARN] OSD_DOWN: 1 osds down\n' in message
    assert '        osd.0 is down\n' in message
    assert '--- Cleared ---' not in message
    server.quit.assert_called_once()
    module.set_health_checks.assert_called_once_with({})


@mock.patch('alerts.module.smtplib.SMTP_SSL')
def test_unchanged_health_sends_nothing(smtp_ssl, module):
    _configure(module, smtp_host='smtp.example.com')

    _run_serve(module, [_health(OSD_DOWN), _health(OSD_DOWN)])

    # only the transition from "no status yet" to OSD_DOWN is reported
    assert smtp_ssl.return_value.sendmail.call_count == 1


@mock.patch('alerts.module.smtplib.SMTP_SSL')
def test_cleared_and_updated_checks(smtp_ssl, module):
    _configure(module, smtp_host='smtp.example.com')
    server = smtp_ssl.return_value

    _run_serve(module, [_health(OSD_DOWN), _health(OSD_DOWN_2), HEALTHY])

    assert server.sendmail.call_count == 3
    updated = server.sendmail.call_args_list[1][0][2]
    assert '--- Updated ---\n[WARN] OSD_DOWN: 2 osds down\n' in updated
    assert '--- New ---' not in updated
    cleared = server.sendmail.call_args_list[2][0][2]
    assert 'Subject: HEALTH_OK\n' in cleared
    assert '--- Cleared ---\n[WARN] OSD_DOWN: 2 osds down\n' in cleared


def test_diff_ignores_lower_count(module):
    # a check whose count goes down is neither new, updated nor cleared
    assert module._diff(_health(OSD_DOWN_2), _health(OSD_DOWN)) == {}


@mock.patch('alerts.module.smtplib.SMTP_SSL')
def test_alerts_send_command(smtp_ssl, module):
    _configure(module, smtp_host='smtp.example.com')

    with mock.patch.object(module, 'get',
                           return_value={'json': json.dumps(_health(OSD_DOWN))}):
        r = module._handle_command(None, {'prefix': 'alerts send'})

    assert r.retval == 0
    message = _sent_message(smtp_ssl)
    # "alerts send" has no diff, it resends the full status only
    assert '--- New ---' not in message
    assert '=== Full health status ===\n[WARN] OSD_DOWN: 1 osds down\n' in message


@mock.patch('alerts.module.smtplib.SMTP')
@mock.patch('alerts.module.smtplib.SMTP_SSL')
def test_plain_smtp_with_login(smtp_ssl, smtp, module):
    _configure(module,
               smtp_host='smtp.example.com',
               smtp_port=25,
               smtp_ssl=False,
               smtp_user='ceph',
               smtp_password='secret')

    _run_serve(module, [_health(OSD_DOWN)])

    smtp_ssl.assert_not_called()
    smtp.assert_called_once_with('smtp.example.com', 25)
    smtp.return_value.login.assert_called_once_with('ceph', 'secret')
    smtp.return_value.sendmail.assert_called_once()


@mock.patch('alerts.module.smtplib.SMTP')
@mock.patch('alerts.module.smtplib.SMTP_SSL')
def test_missing_smtp_host(smtp_ssl, smtp, module):
    _run_serve(module, [_health(OSD_DOWN)])

    smtp_ssl.assert_not_called()
    smtp.assert_not_called()
    # nothing is raised either, so any earlier ALERTS_SMTP_ERROR is cleared
    module.set_health_checks.assert_called_once_with({})


@pytest.mark.parametrize('failure', [
    ConnectionRefusedError(111, 'Connection refused'),
    OSError('[Errno -2] Name or service not known'),
])
@mock.patch('alerts.module.smtplib.SMTP_SSL')
def test_unreachable_smtp_server(smtp_ssl, failure, module):
    _configure(module, smtp_host='smtp.invalid')
    smtp_ssl.side_effect = failure

    _run_serve(module, [_health(OSD_DOWN)])

    module.set_health_checks.assert_called_once()
    checks = module.set_health_checks.call_args[0][0]
    assert list(checks) == ['ALERTS_SMTP_ERROR']
    assert checks['ALERTS_SMTP_ERROR']['severity'] == 'warning'
    assert checks['ALERTS_SMTP_ERROR']['detail'] == [str(failure)]


@mock.patch('alerts.module.smtplib.SMTP_SSL')
def test_rejected_login(smtp_ssl, module):
    _configure(module, smtp_host='smtp.example.com',
               smtp_user='ceph', smtp_password='wrong')
    smtp_ssl.return_value.login.side_effect = \
        smtplib.SMTPAuthenticationError(535, b'authentication failed')

    _run_serve(module, [_health(OSD_DOWN)])

    smtp_ssl.return_value.sendmail.assert_not_called()
    checks = module.set_health_checks.call_args[0][0]
    assert 'ALERTS_SMTP_ERROR' in checks
    assert 'authentication failed' in checks['ALERTS_SMTP_ERROR']['detail'][0]


@mock.patch('alerts.module.smtplib.SMTP_SSL')
def test_error_cleared_after_successful_send(smtp_ssl, module):
    _configure(module, smtp_host='smtp.example.com')
    smtp_ssl.side_effect = [ConnectionRefusedError(111, 'Connection refused'),
                            mock.DEFAULT]

    _run_serve(module, [_health(OSD_DOWN), _health(OSD_DOWN_2)])

    calls = module.set_health_checks.call_args_list
    assert len(calls) == 2
    assert 'ALERTS_SMTP_ERROR' in calls[0][0][0]
    assert calls[1][0][0] == {}
