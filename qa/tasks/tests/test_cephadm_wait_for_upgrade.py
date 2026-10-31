"""Unit tests for the cephadm.wait_for_upgrade teuthology task (cluster is simulated)."""
import json
from io import StringIO
from types import SimpleNamespace
from unittest import mock

from teuthology.exceptions import CommandFailedError

import tasks.cephadm as cephadm

TARGET = 'quay.ceph.io/ceph-ci/ceph@sha256:b308'


class FakeCluster:
    """Scripted cluster: state(t) -> (status, ps, versions) or exception."""
    def __init__(self, state_fn):
        self.state_fn = state_fn
        self.t = 0.0
        self.calls = []

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.t += s

    def shell(self, ctx, cluster_name, remote, args, check_status=True, stdout=None, **kw):
        self.calls.append(args)
        st = self.state_fn(self.t)
        if isinstance(st, Exception):
            if check_status:
                raise st
            return SimpleNamespace(stdout=StringIO(''))
        status, ps, versions = st
        cmd = args[1:]
        if cmd[:3] == ['orch', 'upgrade', 'status']:
            out = status
        elif cmd[:2] == ['orch', 'ps']:
            out = ps
        elif cmd[:1] == ['versions']:
            out = versions
        else:
            out = {}
        if stdout is not None:
            stdout.write(json.dumps(out))
        return SimpleNamespace(stdout=stdout)


def run(state_fn, **config):
    fc = FakeCluster(state_fn)
    ctx = SimpleNamespace(ceph={'ceph': SimpleNamespace(bootstrap_remote='r')})
    with mock.patch.object(cephadm, '_shell', fc.shell), \
            mock.patch.object(cephadm.time, 'monotonic', fc.monotonic), \
            mock.patch.object(cephadm.time, 'sleep', fc.sleep):
        try:
            cephadm.wait_for_upgrade(ctx, config)
            return fc, None
        except RuntimeError as e:
            return fc, str(e)


def st(in_progress=True, message='', progress='0/10', ps=(), versions=None):
    return ({'in_progress': in_progress, 'target_image': TARGET, 'message': message,
             'progress': progress, 'services_complete': []},
            list(ps), versions or {'overall': {'v19': 10}})


def dd(name, image, status_desc='running'):
    return {'daemon_name': name, 'container_image_name': image,
            'container_image_digests': None,
            'status': -1 if status_desc == 'error' else 1, 'status_desc': status_desc}


def test_replay_job_659562_mon_a_dead_fails_fast():
    # mon.a redeployed on target at t=0 and dead; upgrade loops on mon.c ok-to-stop
    ps = [dd('mon.a', TARGET, 'error'), dd('mon.b', 'squid'), dd('mon.c', 'squid')]
    fc, err = run(lambda t: st(progress='2/23', ps=ps))
    assert err and 'mon.a' in err and 'error state' in err
    assert fc.t <= 330, fc.t          # ~5 min instead of 6h


def test_old_image_daemon_in_error_is_not_blamed_on_upgrade():
    ps = [dd('osd.3', 'squid', 'error')]
    fc, err = run(lambda t: st(progress=f'{int(t // 60)}/100', ps=ps,
                               versions={'overall': {'x': int(t // 60)}}),
                  timeout=1200)
    assert err and 'did not finish within 1200s' in err


def test_transient_daemon_error_within_grace_is_ok():
    def f(t):
        if t < 200:
            return st(progress=f'{int(t)}/10', ps=[dd('mon.a', TARGET, 'error')])
        if t < 400:
            return st(progress=f'{int(t)}/10', ps=[dd('mon.a', TARGET)])
        return st(in_progress=False)
    fc, err = run(f)
    assert err is None


def test_normal_upgrade_completes():
    def f(t):
        done = int(t // 60)
        if done >= 10:
            return st(in_progress=False)
        return st(progress=f'{done}/10', versions={'overall': {'a': 10 - done, 'b': done}})
    fc, err = run(f)
    assert err is None and 600 <= fc.t <= 660


def test_error_message_fails_immediately():
    fc, err = run(lambda t: st(message='Error: UPGRADE_FAILED_PULL: Upgrade: failed to pull target image'))
    assert err and 'UPGRADE_FAILED_PULL' in err and fc.t == 0


def test_stall_fails_after_stall_timeout():
    fc, err = run(lambda t: st(progress='5/10'))
    assert err and 'no progress for 1800s' in err
    assert 1800 <= fc.t <= 1830


def test_transient_command_failure_is_retried():
    def f(t):
        if 60 <= t < 150:
            return CommandFailedError('ceph orch upgrade status', 1)
        if t >= 300:
            return st(in_progress=False)
        return st(progress=f'{int(t)}/10')
    fc, err = run(f)
    assert err is None


def test_persistent_command_failure_hits_stall_timeout():
    fc, err = run(lambda t: CommandFailedError('ceph orch upgrade status', 1), stall_timeout=600)
    assert err and 'no progress for 600s' in err


def test_hard_timeout():
    fc, err = run(lambda t: st(progress=f'{int(t)}/100000'), timeout=3600)
    assert err and 'did not finish within 3600s' in err and 3600 <= fc.t <= 3630


def test_failure_dumps_debug_state():
    fc, err = run(lambda t: st(message='Error: boom'))
    dumped = [c[1:] for c in fc.calls[-4:]]
    assert dumped == [['orch', 'upgrade', 'status'], ['orch', 'ps'], ['versions'], ['health', 'detail']]
