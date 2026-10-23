"""
Integration tests for the cephadm certificate manager (certmgr).

The unit tests in src/pybind/mgr/cephadm/tests/test_certmgr.py cover the
store logic in isolation. These tests exercise certmgr against a real cluster:
the CLI, the periodic certificate check and its health reporting, how each
certificate source (cephadm-signed, inline, reference) reaches the daemons, and
what a TLS client connecting to those daemons actually receives.

Certificates are built with pki.py, which runs inside ``cephadm shell`` so
that the only requirement on the test nodes is the Ceph container image itself.

This module holds the helpers shared by every facet. Test classes, one module
per suite facet:

  test_cli.py                 TestCertMgrCLI: store semantics, input
                              validation, persistence
  test_health.py              TestCertMgrHealth: CEPHADM_CERT_WARNING /
                              CEPHADM_CERT_ERROR reporting
  test_service_scope.py       TestCertMgrServiceScope: RGW (service scope)
                              through all certificate sources
  test_host_scope.py          TestCertMgrHostScope: grafana (host scope);
                              TestCertMgrHostScopeReference: grafana with
                              per-host user certs (own facet, known issue)
  test_global_scope.py        TestCertMgrGlobalScope: mgmt-gateway (global
                              scope)
  test_renewal.py             TestCertMgrRenewal: automated rotation of
                              cephadm-signed certificates
"""

import json
import logging
import os
import time
from io import StringIO
from typing import Any, Callable, Dict, List, Optional

from tasks.cephadm import _shell
from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)

WORKDIR = '/tmp/certmgr-qa'
PKI = f'{WORKDIR}/certmgr_pki.py'
ROOT_CA = 'cephadm_root_ca_cert'
ROOT_CA_KEY = 'cephadm_root_ca_key'
CERT_HEALTH_CODES = ('CEPHADM_CERT_WARNING', 'CEPHADM_CERT_ERROR')
MODULE_OPTS = (
    'certificate_check_debug_mode',
    'certificate_automated_rotation_enabled',
    'certificate_check_period',
    'certificate_duration_days',
    'certificate_renewal_threshold_days',
)


class CommandFailed(Exception):
    """A ceph/cephadm-shell command exited non-zero when it was expected to succeed."""


class CommandResult:
    def __init__(self, args: List[str], rc: int, out: str, err: str) -> None:
        self.args = args
        self.rc = rc
        self.out = out
        self.err = err

    def __repr__(self) -> str:
        # PEM arguments are long; keep the command readable in failure output
        cmd = ' '.join(a if len(a) < 60 else a[:24] + '...' for a in self.args)
        return f'<rc={self.rc} cmd={cmd!r} err={self.err.strip()[-500:]!r}>'


class CertMgrTestCase(MgrTestCase):
    """Shared helpers. Not collected on its own (no test_ methods)."""

    MGRS_REQUIRED = 1

    # populated by setUpClass
    remote: Any = None
    hosts: Dict[str, str] = {}
    host_names: List[str] = []
    host: str = ''

    @classmethod
    def _mgr(cls) -> Any:
        assert cls.mgr_cluster is not None
        return cls.mgr_cluster

    # ------------------------------------------------------------------
    # class setup
    # ------------------------------------------------------------------
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.remote = cls._mgr().admin_remote
        cls.remote.run(args=['sudo', 'rm', '-rf', WORKDIR])
        cls.remote.run(args=['sudo', 'mkdir', '-p', f'{WORKDIR}/ca'])
        with open(os.path.join(os.path.dirname(__file__), 'pki.py')) as f:
            cls.remote.write_file(PKI, f.read(), sudo=True)
        cls._wait_orch_ready()
        cls.hosts = cls._host_addrs()
        cls.host_names = sorted(cls.hosts)
        # A two-level test PKI: servers present leaf + intermediate, clients
        # trust the root only, so a verified handshake proves the chain is sent.
        cls._pki_cls('ca', f'{WORKDIR}/ca/root', '--cn', 'certmgr QA root CA')
        cls._pki_cls('ca', f'{WORKDIR}/ca/int', '--cn', 'certmgr QA intermediate CA',
                     '--parent', f'{WORKDIR}/ca/root')
        cls._save_root_ca()

    @classmethod
    def tearDownClass(cls) -> None:
        for opt in MODULE_OPTS:
            cls._ceph_cls('config', 'rm', 'mgr', f'mgr/cephadm/{opt}', check=False)
        super().tearDownClass()

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
    def _pki_cls(cls, *args: str) -> Dict[str, Any]:
        return json.loads(cls._run_cls(['python3', PKI] + list(args)).out)

    def run_cmd(self, args: List[str], check: bool = True) -> CommandResult:
        return self._run_cls(args, check=check)

    def ceph(self, *args: str, check: bool = True) -> CommandResult:
        return self._ceph_cls(*args, check=check)

    def ceph_json(self, *args: str) -> Any:
        out = self.ceph(*args, '--format', 'json').out.strip()
        return json.loads(out) if out else None

    def orch(self, *args: str, check: bool = True) -> CommandResult:
        return self.ceph('orch', *args, check=check)

    def pki(self, *args: str) -> Dict[str, Any]:
        return self._pki_cls(*args)

    def assert_fails(self, args: List[str], *needles: str) -> CommandResult:
        res = self.ceph(*args, check=False)
        self.assertNotEqual(res.rc, 0, f'expected failure: {res!r}')
        # a rejected input must be reported, not crash the command handler
        self.assertNotIn('Traceback', res.err, f'handler crashed: {res!r}')
        text = (res.err + res.out).lower()
        for needle in needles:
            self.assertIn(needle.lower(), text, f'{needle!r} not in error output of {res!r}')
        return res

    def read(self, path: str) -> str:
        return self.remote.sh(['sudo', 'cat', path])

    def write(self, path: str, content: str) -> str:
        mode = '0600' if 'PRIVATE KEY' in content else '0644'
        self.remote.write_file(path, content, sudo=True, mode=mode)
        return path

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
    def _save_root_ca(cls) -> None:
        pem = cls._ceph_cls('orch', 'certmgr', 'cert', 'get', ROOT_CA).out
        cls._mgr().admin_remote.write_file(f'{WORKDIR}/cephadm-root-ca.crt', pem, sudo=True)

    def set_opt(self, name: str, value: Any) -> None:
        self.ceph('config', 'set', 'mgr', f'mgr/cephadm/{name}', str(value).lower())

    def kick(self) -> None:
        """Wake up the cephadm serve loop (it runs the certificate check)."""
        self.orch('ps', '--refresh', check=False)

    def poll(self, what: str, fn: Callable[[], bool], timeout: int = 600,
             period: int = 10, kick: bool = False) -> None:
        deadline = time.time() + timeout
        while True:
            if kick:
                self.kick()
            try:
                if fn():
                    return
            except (CommandFailed, KeyError, IndexError, ValueError) as e:
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

    def wait_health(self, code: str, present: bool = True, timeout: int = 600,
                    detail: Optional[str] = None) -> None:
        def ok() -> bool:
            checks = self.health_checks()
            if not present:
                return code not in checks
            if code not in checks:
                return False
            if detail is None:
                return True
            return any(detail in d['message'] for d in checks[code].get('detail', []))
        self.poll(f'health {code} present={present} detail={detail}', ok,
                  timeout=timeout, kick=True)

    def orch_ls(self, service_name: str, refresh: bool = False) -> List[Dict[str, Any]]:
        args = ['orch', 'ls', '--service_name', service_name, '--format', 'json']
        if refresh:
            args.append('--refresh')
        out = self.ceph(*args).out.strip()
        # 'orch ls' prints this plain-text line even when JSON is requested
        if not out or out == 'No services reported':
            return []
        return json.loads(out)

    def service_running(self, service_name: str) -> bool:
        ls = self.orch_ls(service_name, refresh=True)
        if not ls:
            return False
        status = ls[0].get('status', {})
        return status.get('size', 0) > 0 and status.get('running', 0) >= status['size']

    def wait_service(self, service_name: str, timeout: int = 900) -> None:
        self.poll(f'{service_name} running', lambda: self.service_running(service_name),
                  timeout=timeout, period=15)

    def daemons(self, service_name: str) -> List[Dict[str, Any]]:
        return self.ceph_json('orch', 'ps', '--service_name', service_name) or []

    def rm_service(self, service_name: str, timeout: int = 600) -> None:
        self.orch('rm', service_name, check=False)  # fails when already absent
        self.poll(f'{service_name} removed',
                  lambda: not self.daemons(service_name) and not self.orch_ls(service_name),
                  timeout=timeout, period=15, kick=True)

    def apply(self, name: str, spec: Dict[str, Any], check: bool = True) -> CommandResult:
        # JSON is valid YAML, and it avoids any quoting issue with PEM blocks
        path = self.write(f'{WORKDIR}/{name}.json', json.dumps(spec))
        res = self.ceph('orch', 'apply', '-i', path, check=check)
        # The exit code is not enough: a spec rejected by validation exits 0
        # (SpecValidationError carries a negative errno, the CLI wrapper
        # negates it again, and ceph.in only treats ret < 0 as an error).
        # Confirm the spec was stored before anyone waits on the service.
        # Checking that the service exists is not enough on an update (the
        # previous spec is still there), so compare the TLS fields as well.
        if check:
            stored = self.stored_spec(self.spec_service_name(spec))
            if not stored:
                raise CommandFailed(f'spec not stored after apply: {res!r}')
            want = spec.get('spec', {})
            have = stored.get('spec', {})
            if 'certificate_source' in want \
                    and have.get('certificate_source') != want['certificate_source']:
                raise CommandFailed(
                    f"stored certificate_source is {have.get('certificate_source')!r}, "
                    f"expected {want['certificate_source']!r}: {res!r}")
            if 'ssl_cert' in want and (
                    not have.get('ssl_cert')
                    or self.fingerprint_of(have['ssl_cert'])
                    != self.fingerprint_of(want['ssl_cert'])):
                raise CommandFailed(f'stored ssl_cert is not the applied one: {res!r}')
        return res

    def stored_spec(self, service_name: str) -> Dict[str, Any]:
        out = self.ceph('orch', 'ls', '--service_name', service_name,
                        '--export', '--format', 'json').out.strip()
        if not out or out == 'No services reported':
            return {}
        specs = json.loads(out)
        return specs[0] if specs else {}

    @staticmethod
    def spec_service_name(spec: Dict[str, Any]) -> str:
        sid = spec.get('service_id')
        return f"{spec['service_type']}.{sid}" if sid else str(spec['service_type'])

    # ------------------------------------------------------------------
    # certificate helpers
    # ------------------------------------------------------------------
    def leaf(self, name: str, cn: str = 'certmgr-qa', ca: str = 'int',
             dns: Optional[List[str]] = None, ips: Optional[List[str]] = None,
             days: int = 365, not_before: int = -1, key_type: str = 'rsa',
             passphrase: Optional[str] = None) -> Dict[str, Any]:
        prefix = f'{WORKDIR}/{name}'
        args = ['leaf', prefix, '--ca', f'{WORKDIR}/ca/{ca}', '--cn', cn,
                '--days', str(days), '--not-before', str(not_before),
                '--key-type', key_type]
        for d in dns or []:
            args += ['--dns', d]
        for ip in ips or []:
            args += ['--ip', ip]
        if passphrase:
            args += ['--passphrase', passphrase]
        info = self.pki(*args)
        info.update({
            'crt': f'{prefix}.crt',
            'chain': f'{prefix}.chain.crt',
            'key': f'{prefix}.key',
            'full': f'{prefix}.full.pem',
        })
        return info

    def cert_info_pem(self, pem: str) -> Dict[str, Any]:
        path = self.write(f'{WORKDIR}/_probe.crt', pem)
        return self.pki('info', path)

    def issued_by_cephadm_root_ca(self, pem: str) -> bool:
        """True if the cephadm root CA key directly signed this certificate.
        Not chain validation: cephadm signs its leaf certs with the root CA
        itself. Chains are checked with verified TLS handshakes instead."""
        path = self.write(f'{WORKDIR}/_verify.crt', pem)
        res = self.pki('verify', path, '--ca', f'{WORKDIR}/cephadm-root-ca.crt')
        return bool(res['signed_by_ca'])

    def served(self, host: str, port: int, cafile: Optional[str] = None) -> Dict[str, Any]:
        args = ['fetch', self.hosts[host], str(port)]
        if cafile:
            args += ['--cafile', cafile]
        return self.pki(*args)

    def wait_served(self, host: str, port: int, predicate: Callable[[Dict[str, Any]], bool],
                    what: str, timeout: int = 600) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        def ok() -> bool:
            result.clear()
            result.update(self.served(host, port))
            return bool(result.get('ok')) and predicate(result)
        self.poll(f'{what} on {host}:{port}', ok, timeout=timeout, period=15)
        return result

    def pem_file(self, name: str, *parts: str) -> str:
        """Concatenate PEM files (or literal PEM text) into one input file."""
        blobs = [p if p.lstrip().startswith('-----') else self.read(p) for p in parts]
        return self.write(f'{WORKDIR}/{name}.pem', ''.join(b if b.endswith('\n') else b + '\n'
                                                           for b in blobs))

    def cert_key_set_args(self, consumer: str, pem_path: str) -> List[str]:
        # Always '-i': the ceph CLI hands its argv to librados conf_parse_argv,
        # which consumes '--key <value>' as the client's cephx secret, so a
        # PEM passed with '--key' never reaches the mgr.
        return ['orch', 'certmgr', 'cert-key', 'set', consumer, '-i', pem_path]

    def set_pair(self, consumer: str, leaf: Dict[str, Any], service: Optional[str] = None,
                 host: Optional[str] = None, cert_name: Optional[str] = None,
                 pem: Optional[str] = None, force: bool = False,
                 check: bool = True) -> CommandResult:
        """cert-key set from a PEM bundle (default: key + leaf + intermediates)."""
        args = self.cert_key_set_args(consumer, pem or leaf['full'])
        if service:
            args += ['--service-name', service]
        if host:
            args += ['--hostname', host]
        if cert_name:
            args += ['--cert-name', cert_name]
        if force:
            args += ['--force']
        return self.ceph(*args, check=check)

    def _get_obj(self, kind: str, name: str, service: Optional[str],
                 host: Optional[str]) -> str:
        args = ['orch', 'certmgr', kind, 'get', name, '--no-exception-when-missing']
        if service:
            args += ['--service-name', service]
        if host:
            args += ['--hostname', host]
        res = self.ceph(*args, check=False)
        if res.rc == 0:
            return res.out.strip()
        # cephadm-signed names are registered on first use and may be unknown
        # to a freshly loaded store; for these tests that is the same as absent
        if 'unknown tls object name' in res.err.lower():
            return ''
        raise CommandFailed(f'command failed: {res!r}')

    def get_cert(self, name: str, service: Optional[str] = None,
                 host: Optional[str] = None) -> str:
        return self._get_obj('cert', name, service, host)

    def get_key(self, name: str, service: Optional[str] = None,
                host: Optional[str] = None) -> str:
        return self._get_obj('key', name, service, host)

    def cert_ls(self, *extra: str) -> Dict[str, Any]:
        return self.ceph_json('orch', 'certmgr', 'cert', 'ls', *extra) or {}

    def rm_cert_key(self, cert_name: str, service: Optional[str] = None,
                    host: Optional[str] = None) -> None:
        target: List[str] = []
        if service:
            target += ['--service-name', service]
        if host:
            target += ['--hostname', host]
        key_name = cert_name.replace('_cert', '_key')
        # rm is allowed to fail when the object is absent, and 'key rm' even
        # reports success for a missing key, so check the result by reading back
        self.ceph('orch', 'certmgr', 'cert', 'rm', cert_name, *target, check=False)
        self.ceph('orch', 'certmgr', 'key', 'rm', key_name, *target, check=False)
        self.assertEqual(self.get_cert(cert_name, service=service, host=host), '',
                         f'cleanup left {cert_name} ({service or host or "global"}) behind')
        self.assertEqual(self.get_key(key_name, service=service, host=host), '',
                         f'cleanup left {key_name} ({service or host or "global"}) behind')

    def fingerprint_of(self, pem: str) -> str:
        return str(self.cert_info_pem(pem)['fingerprint'])

    def fail_mgr_and_wait(self) -> None:
        old = self._mgr().get_active_id()
        self._mgr().mgr_fail(old)
        self.wait_until_true(
            lambda: self._mgr().get_active_id() not in ('', old)
            and self._mgr().get_mgr_map().get('available', False),
            timeout=120)
        self._wait_orch_ready()
