"""
Shared helpers for the orch/cephadm/services/* suites.

Each service suite lives in its own package next to this module
(services/mgmt_gateway, ...) and builds on ServicesTestCase. The tests run
against a real cluster and check what clients actually get from the daemons:
HTTP status codes, redirects, TLS and client-certificate behaviour. They go
through probe.py, which runs inside ``cephadm shell`` so the test nodes only
need the Ceph container image.

The helper names follow tasks.cephadm_cases.certmgr.base on purpose, so the
two can be merged later without touching the tests.
"""

import json
import logging
import os
import time
from io import StringIO
from typing import Any, Callable, Dict, List, Sequence, Tuple

from tasks.cephadm import _shell
from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)

WORKDIR = '/tmp/cephadm-services-qa'
PROBE = f'{WORKDIR}/probe.py'
ROOT_CA_FILE = f'{WORKDIR}/cephadm-root-ca.crt'
ROOT_CA = 'cephadm_root_ca_cert'

# Spec fields that are never compared after an apply: 'orch ls --export' may
# mask them, and they are not what a test asserts on.
SECRET_FIELDS = ('secret', 'password', 'key', 'cert')


class CommandFailed(Exception):
    """A ceph/cephadm-shell command exited non-zero when it was expected to succeed."""


class CommandResult:
    def __init__(self, args: List[str], rc: int, out: str, err: str) -> None:
        self.args = args
        self.rc = rc
        self.out = out
        self.err = err

    def __repr__(self) -> str:
        cmd = ' '.join(a if len(a) < 60 else a[:24] + '...' for a in self.args)
        return f'<rc={self.rc} cmd={cmd!r} err={self.err.strip()[-500:]!r}>'


class ServicesTestCase(MgrTestCase):
    """Shared helpers. Not collected on its own (no test_ methods)."""

    MGRS_REQUIRED = 1

    # populated by setUpClass
    remote: Any = None
    hosts: Dict[str, str] = {}
    host_names: List[str] = []

    @classmethod
    def _mgr(cls) -> Any:
        assert cls.mgr_cluster is not None
        return cls.mgr_cluster

    @classmethod
    def job_ctx(cls) -> Any:
        """The teuthology context (job config, cluster remotes, vip...)."""
        assert cls.ctx is not None
        return cls.ctx

    # ------------------------------------------------------------------
    # class setup
    # ------------------------------------------------------------------
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.remote = cls._mgr().admin_remote
        cls.remote.run(args=['sudo', 'rm', '-rf', WORKDIR])
        cls.remote.run(args=['sudo', 'mkdir', '-p', WORKDIR])
        with open(os.path.join(os.path.dirname(__file__), 'probe.py')) as f:
            cls.remote.write_file(PROBE, f.read(), sudo=True)
        cls._wait_orch_ready()
        cls.hosts = cls._host_addrs()
        cls.host_names = sorted(cls.hosts)
        root_ca = cls._ceph_cls('orch', 'certmgr', 'cert', 'get', ROOT_CA).out
        cls.remote.write_file(ROOT_CA_FILE, root_ca, sudo=True)

    # ------------------------------------------------------------------
    # command execution
    # ------------------------------------------------------------------
    @classmethod
    def _run_cls(cls, args: List[str], check: bool = True, timeout: int = 300) -> CommandResult:
        proc = _shell(cls.ctx, 'ceph', cls._mgr().admin_remote,
                      ['timeout', str(timeout)] + args,
                      extra_cephadm_args=['-m', f'{WORKDIR}:{WORKDIR}:z'],
                      stdout=StringIO(), stderr=StringIO(), check_status=False)
        res = CommandResult(args, proc.exitstatus, proc.stdout.getvalue(), proc.stderr.getvalue())
        log.debug('%r', res)
        if check and res.rc != 0:
            raise CommandFailed(f'command failed: {res!r}')
        return res

    @classmethod
    def _ceph_cls(cls, *args: str, check: bool = True) -> CommandResult:
        return cls._run_cls(['ceph'] + list(args), check=check)

    @classmethod
    def _ceph_json_cls(cls, *args: str) -> Any:
        out = cls._ceph_cls(*args, '--format', 'json').out.strip()
        return json.loads(out) if out else None

    def run_cmd(self, args: List[str], check: bool = True) -> CommandResult:
        return self._run_cls(args, check=check)

    def ceph(self, *args: str, check: bool = True) -> CommandResult:
        return self._ceph_cls(*args, check=check)

    def ceph_json(self, *args: str) -> Any:
        return self._ceph_json_cls(*args)

    def orch(self, *args: str, check: bool = True) -> CommandResult:
        return self.ceph('orch', *args, check=check)

    @classmethod
    def _probe_cls(cls, *args: str) -> Any:
        return json.loads(cls._run_cls(['python3', PROBE] + list(args)).out)

    def probe(self, *args: str) -> Any:
        return self._probe_cls(*args)

    def read(self, path: str) -> str:
        return self.remote.sh(['sudo', 'cat', path])

    @classmethod
    def _write_cls(cls, path: str, content: str) -> str:
        mode = '0600' if 'PRIVATE KEY' in content else '0644'
        cls.remote.write_file(path, content, sudo=True, mode=mode)
        return path

    def write(self, path: str, content: str) -> str:
        return self._write_cls(path, content)

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    @classmethod
    def _http_many_cls(cls, reqs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run several requests in one cephadm shell call (see probe.py)."""
        if not reqs:
            return []
        path = cls._write_cls(f'{WORKDIR}/_batch.json', json.dumps(list(reqs)))
        return cls._probe_cls('batch', path)

    def http_many(self, reqs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self._http_many_cls(reqs)

    def http(self, url: str, **kwargs: Any) -> Dict[str, Any]:
        return self.http_many([dict(url=url, **kwargs)])[0]

    @staticmethod
    def rejected_without_client_cert(res: Dict[str, Any]) -> bool:
        """The server refused a client that did not present a certificate.

        Go servers (prometheus, alertmanager, node-exporter) abort the TLS
        session, so no HTTP status comes back. nginx answers 400 instead.
        Anything else, including 401, means the request got past TLS."""
        if res['status'] == 0:
            return True
        return res['status'] == 400 and 'certificate' in res['body'].lower()

    # ------------------------------------------------------------------
    # cluster helpers
    # ------------------------------------------------------------------
    @classmethod
    def _wait_orch_ready(cls, timeout: int = 180) -> None:
        cls.wait_until_true(
            lambda: cls._ceph_cls('orch', 'status', check=False).rc == 0,
            timeout=timeout)

    @classmethod
    def _host_addrs(cls) -> Dict[str, str]:
        hosts = json.loads(cls._ceph_cls('orch', 'host', 'ls', '--format', 'json').out)
        return {h['hostname']: h['addr'] for h in hosts}

    @classmethod
    def remote_for(cls, host: str) -> Any:
        """The teuthology remote behind a cephadm host name."""
        for remote in cls.job_ctx().cluster.remotes.keys():
            if host in (remote.shortname, remote.hostname) \
                    or remote.hostname.split('.')[0] == host:
                return remote
        raise KeyError(f'no remote for host {host}')

    @classmethod
    def host_names_for(cls, host: str) -> List[str]:
        """Every name a client may use for this host (short, FQDN, address)."""
        remote = cls.remote_for(host)
        names = {host, cls.hosts[host]}
        for flag in ('--short', '--fqdn'):
            out = remote.sh(['hostname', flag], check_status=False).strip()
            if out:
                names.add(out)
        return sorted(names)

    @classmethod
    def enable_mgr_module(cls, module: str, timeout: int = 300) -> None:
        """Enable a mgr module and wait until it publishes its service URL.

        MgrTestCase.setup_mgrs() disables every module but cephadm, so a
        suite that needs e.g. the dashboard turns it back on here."""
        cls._ceph_cls('mgr', 'module', 'enable', module)
        cls.wait_until_true(
            lambda: module in (cls._ceph_json_cls('mgr', 'services') or {}),
            timeout=timeout, period=10)

    @classmethod
    def mgr_option(cls, module: str, option: str) -> str:
        return cls._ceph_cls('config', 'get', 'mgr', f'mgr/{module}/{option}').out.strip()

    @classmethod
    def set_opt(cls, name: str, value: Any) -> None:
        cls._ceph_cls('config', 'set', 'mgr', f'mgr/cephadm/{name}', str(value).lower())

    @classmethod
    def kick(cls) -> None:
        """Wake up the cephadm serve loop and refresh its daemon cache."""
        cls._ceph_cls('orch', 'ps', '--refresh', check=False)

    @classmethod
    def poll(cls, what: str, fn: Callable[[], bool], timeout: int = 600,
             period: int = 10, kick: bool = False) -> None:
        deadline = time.time() + timeout
        while True:
            if kick:
                cls.kick()
            try:
                if fn():
                    return
            except (CommandFailed, KeyError, IndexError, ValueError, TypeError) as e:
                # transient: mgr failover, daemon restarting, partial JSON.
                # Test assertions inside fn() are not caught and fail at once.
                log.info('poll %s: not yet (%s)', what, e)
            if time.time() > deadline:
                raise AssertionError(f'timed out after {timeout}s waiting for {what}')
            time.sleep(period)

    def hold(self, what: str, fn: Callable[[], bool], seconds: int = 90,
             period: int = 15, kick: bool = True) -> None:
        """Assert fn() stays true for the whole window."""
        deadline = time.time() + seconds
        while time.time() < deadline:
            if kick:
                self.kick()
            self.assertTrue(fn(), f'{what} stopped holding')
            time.sleep(period)

    def health_checks(self) -> Dict[str, Any]:
        return self.ceph_json('health', 'detail').get('checks', {})

    @classmethod
    def orch_ls(cls, service_name: str, refresh: bool = False) -> List[Dict[str, Any]]:
        args = ['orch', 'ls', '--service_name', service_name, '--format', 'json']
        if refresh:
            args.append('--refresh')
        out = cls._ceph_cls(*args).out.strip()
        # 'orch ls' prints this plain-text line even when JSON is requested
        if not out or out == 'No services reported':
            return []
        return json.loads(out)

    @classmethod
    def service_running(cls, service_name: str) -> bool:
        ls = cls.orch_ls(service_name, refresh=True)
        if not ls:
            return False
        status = ls[0].get('status', {})
        return status.get('size', 0) > 0 and status.get('running', 0) >= status['size']

    @classmethod
    def wait_service(cls, service_name: str, timeout: int = 900) -> None:
        cls.poll(f'{service_name} running', lambda: cls.service_running(service_name),
                 timeout=timeout, period=15)

    @classmethod
    def daemons(cls, service_name: str) -> List[Dict[str, Any]]:
        return cls._ceph_json_cls('orch', 'ps', '--service_name', service_name) or []

    @classmethod
    def daemons_of_type(cls, daemon_type: str) -> List[Dict[str, Any]]:
        return cls._ceph_json_cls('orch', 'ps', '--daemon_type', daemon_type) or []

    @classmethod
    def rm_service(cls, service_name: str, timeout: int = 600) -> None:
        cls._ceph_cls('orch', 'rm', service_name, check=False)  # fails when already absent
        cls.poll(f'{service_name} removed',
                 lambda: not cls.daemons(service_name) and not cls.orch_ls(service_name),
                 timeout=timeout, period=15, kick=True)

    @classmethod
    def apply(cls, name: str, spec: Dict[str, Any], check: bool = True) -> CommandResult:
        # JSON is valid YAML and avoids any quoting issue with embedded files
        path = cls._write_cls(f'{WORKDIR}/{name}.json', json.dumps(spec))
        res = cls._ceph_cls('orch', 'apply', '-i', path, check=check)
        # The exit code is not enough: a spec rejected by validation exits 0.
        # Confirm the spec was stored, and on an update that the stored
        # values are the applied ones (the previous spec is still there).
        if check:
            stored = cls.stored_spec(cls.spec_service_name(spec))
            if not stored:
                raise CommandFailed(f'spec not stored after apply: {res!r}')
            have = stored.get('spec', {})
            for field, want in spec.get('spec', {}).items():
                if any(s in field for s in SECRET_FIELDS) or field not in have:
                    continue
                if have[field] != want:
                    raise CommandFailed(
                        f'stored {field} is {have[field]!r}, expected {want!r}: {res!r}')
        return res

    @classmethod
    def stored_spec(cls, service_name: str) -> Dict[str, Any]:
        out = cls._ceph_cls('orch', 'ls', '--service_name', service_name,
                            '--export', '--format', 'json').out.strip()
        if not out or out == 'No services reported':
            return {}
        specs = json.loads(out)
        return specs[0] if specs else {}

    @staticmethod
    def spec_service_name(spec: Dict[str, Any]) -> str:
        sid = spec.get('service_id')
        return f"{spec['service_type']}.{sid}" if sid else str(spec['service_type'])

    def label_hosts(self, label: str, hosts: Sequence[str]) -> None:
        for host in hosts:
            self.orch('host', 'label', 'add', host, label)

    def unlabel_hosts(self, label: str) -> None:
        for host in self.host_names:
            self.orch('host', 'label', 'rm', host, label, check=False)

    def fail_mgr_and_wait(self) -> None:
        old = self._mgr().get_active_id()
        self._mgr().mgr_fail(old)
        self.wait_until_true(
            lambda: self._mgr().get_active_id() not in ('', old)
            and self._mgr().get_mgr_map().get('available', False),
            timeout=120)
        self._wait_orch_ready()

    # ------------------------------------------------------------------
    # composite checks
    # ------------------------------------------------------------------
    def wait_checks(self, what: str, checks: Callable[[], List[Tuple[str, bool]]],
                    timeout: int = 900, period: int = 20) -> None:
        """Poll a set of named checks until all pass; on timeout, say which failed."""
        failed: List[str] = []

        def ok() -> bool:
            failed[:] = [name for name, passed in checks() if not passed]
            if failed:
                log.info('%s: still failing: %s', what, ', '.join(failed))
            return not failed
        try:
            self.poll(what, ok, timeout=timeout, period=period, kick=True)
        except AssertionError:
            raise AssertionError(f'timed out after {timeout}s waiting for {what}; '
                                 f'failing checks: {", ".join(failed) or "unknown"}')

    def hold_checks(self, what: str, checks: Callable[[], List[Tuple[str, bool]]],
                    seconds: int = 120, period: int = 20) -> None:
        def ok() -> bool:
            failed = [name for name, passed in checks() if not passed]
            if failed:
                log.error('%s: failing: %s', what, ', '.join(failed))
            return not failed
        self.hold(what, ok, seconds=seconds, period=period)
