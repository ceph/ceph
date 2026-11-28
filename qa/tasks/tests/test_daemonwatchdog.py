"""
Unit tests for daemonwatchdog.py, run from the qa directory:

pytest tasks/tests/test_daemonwatchdog.py
"""
from types import SimpleNamespace

from teuthology.orchestra.daemon.cephadmunit import CephadmUnit
from tasks import daemonwatchdog


class SystemdRemote:
    """
    A remote whose systemd units are active from systemctl start until
    systemctl stop; systemctl status fails for a unit that is not active.
    """

    def __init__(self):
        self.active = set()

    def sh(self, cmd):
        action, unit = cmd.split()[-2:]
        if action == 'start':
            self.active.add(unit)
        elif action == 'stop':
            self.active.discard(unit)

    def run(self, args, **kwargs):
        if isinstance(args, str):
            return SimpleNamespace(
                returncode=0 if args.split()[-1] in self.active else 3)
        return SimpleNamespace(wait=lambda: None)


class Daemons:
    def __init__(self, *daemons):
        self.daemons = daemons

    def iter_daemons_of_role(self, type_, cluster=None):
        return [d for d in self.daemons if d.type_ == type_]


def osd(remote, id_):
    return CephadmUnit(remote, 'ceph.osd', id_, fsid='fsid', started=True)


def barks(monkeypatch, *daemons):
    ctx = SimpleNamespace(
        config={'watchdog': {'daemon_timeout': -1}},
        daemons=Daemons(*daemons),
        ceph={'ceph': SimpleNamespace(thrashers=[], watched_processes=[])})
    watchdog = daemonwatchdog.DaemonWatchdog(ctx, {'cluster': 'ceph'})
    reasons = []
    monkeypatch.setattr(watchdog, 'bark', reasons.append)
    monkeypatch.setattr(daemonwatchdog, 'sleep', lambda t: watchdog.stop())
    watchdog.watch()
    return reasons


def test_stopped_cephadm_daemon_is_not_failed(monkeypatch):
    remote = SystemdRemote()
    remote.sh('sudo systemctl start ceph-fsid@osd.0')
    stopped = osd(remote, '0')
    stopped.stop()
    assert barks(monkeypatch, stopped) == []


def test_failed_cephadm_daemon_barks(monkeypatch):
    failed = osd(SystemdRemote(), '0')
    assert barks(monkeypatch, failed) == ['Daemon ceph.osd.0 has failed']
