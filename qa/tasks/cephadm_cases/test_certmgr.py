"""
Integration tests for the cephadm certificate manager (certmgr).

The unit tests in src/pybind/mgr/cephadm/tests/test_certmgr.py cover the
store logic in isolation. These tests exercise certmgr against a real cluster:
the CLI, the periodic certificate check and its health reporting, how each
certificate source (cephadm-signed, inline, reference) reaches the daemons, and
what a TLS client connecting to those daemons actually receives.

Certificates are built with certmgr_pki.py, which runs inside ``cephadm
shell`` so that the only requirement on the test nodes is the Ceph container
image itself.

Test classes, one per suite facet:

  TestCertMgrCLI         store semantics, input validation, persistence
  TestCertMgrHealth      CEPHADM_CERT_WARNING / CEPHADM_CERT_ERROR reporting
  TestCertMgrServiceScope  RGW (service scope) through all certificate sources
  TestCertMgrHostGlobalScope  grafana (host scope) and mgmt-gateway (global)
  TestCertMgrRenewal     automated rotation of cephadm-signed certificates
"""

import json
import logging
import os
import time
from io import StringIO
from typing import Any, Callable, Dict, List, Optional, Tuple

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
        with open(os.path.join(os.path.dirname(__file__), 'certmgr_pki.py')) as f:
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
        if check and not self.orch_ls(self.spec_service_name(spec)):
            raise CommandFailed(f'spec not stored after apply: {res!r}')
        return res

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


class TestCertMgrCLI(CertMgrTestCase):
    """certmgr store semantics through the CLI. No TLS daemons are deployed."""

    MGRS_REQUIRED = 2
    SVC = 'rgw.cli-qa'

    def tearDown(self) -> None:
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        self.rm_cert_key('grafana_ssl_cert', host=self.host_names[0])
        self.rm_cert_key('mgmt_gateway_ssl_cert')
        super().tearDown()

    def test_bindings_expose_every_consumer_with_its_scope(self) -> None:
        bindings = self.ceph_json('orch', 'certmgr', 'bindings', 'ls')
        expected = {
            'service': {'rgw': 'rgw_ssl_cert', 'ingress': 'ingress_ssl_cert',
                        'iscsi': 'iscsi_ssl_cert', 'nvmeof': 'nvmeof_ssl_cert',
                        'nfs': 'nfs_ssl_cert', 'smb': 'smb_ssl_cert'},
            'host': {'grafana': 'grafana_ssl_cert', 'oauth2-proxy': 'oauth2_proxy_ssl_cert'},
            'global': {'mgmt-gateway': 'mgmt_gateway_ssl_cert'},
        }
        for scope, consumers in expected.items():
            self.assertIn(scope, bindings)
            for consumer, cert_name in consumers.items():
                self.assertIn(consumer, bindings[scope], f'{consumer} missing in {scope} scope')
                self.assertIn(cert_name, bindings[scope][consumer]['certs'])
                self.assertIn(cert_name.replace('_cert', '_key'),
                              bindings[scope][consumer]['keys'])
        # services that need a CA bundle register it next to the leaf
        self.assertIn('nfs_ssl_ca_cert', bindings['service']['nfs']['certs'])
        self.assertIn('smb_ssl_ca_cert', bindings['service']['smb']['certs'])
        self.assertIn('nvmeof_root_ca_cert', bindings['service']['nvmeof']['certs'])

    def test_root_ca_is_listed_and_is_a_ca(self) -> None:
        ls = self.cert_ls()
        self.assertIn(ROOT_CA, ls, 'root CA must be listed even without --include-cephadm-signed')
        self.assertEqual(ls[ROOT_CA]['scope'], 'global')
        info = self.cert_info_pem(self.get_cert(ROOT_CA))
        self.assertTrue(info['is_ca'])
        self.assertGreater(info['remaining_days'], 9 * 365)
        self.assertNotIn(ROOT_CA_KEY, self.ceph_json('orch', 'certmgr', 'key', 'ls'))
        self.assertNotIn(ROOT_CA_KEY, self.ceph_json('orch', 'certmgr', 'key', 'ls',
                                                     '--include-cephadm-generated-keys'))

    def test_root_ca_is_not_editable(self) -> None:
        root_fp = self.fingerprint_of(self.get_cert(ROOT_CA))
        other = self.leaf('not-a-root', ca='root')
        res = self.ceph('orch', 'certmgr', 'cert', 'set', ROOT_CA,
                        '--cert', self.read(other['crt']), check=False)
        self.assertNotEqual(res.rc, 0, res)
        self.assertEqual(self.fingerprint_of(self.get_cert(ROOT_CA)), root_fp)

    def test_set_get_roundtrip_service_scope(self) -> None:
        leaf = self.leaf('cli-rt', dns=['s3.cli-qa.test'])
        self.set_pair('rgw', leaf, service=self.SVC)
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.assertEqual(self.get_key('rgw_ssl_key', service=self.SVC).strip(),
                         self.read(leaf['key']).strip())
        ls = self.cert_ls()
        self.assertEqual(ls['rgw_ssl_cert']['scope'], 'service')
        entry = ls['rgw_ssl_cert']['certificates'][self.SVC]
        self.assertTrue(entry['contains_chain'])
        self.assertIn('rgw_ssl_key', self.ceph_json('orch', 'certmgr', 'key', 'ls'))

    def test_cert_ls_filters(self) -> None:
        leaf = self.leaf('cli-filter')
        self.set_pair('rgw', leaf, service=self.SVC)
        by_name = self.cert_ls('--filter-by', 'name=rgw*')
        self.assertIn('rgw_ssl_cert', by_name)
        self.assertNotIn(ROOT_CA, by_name)
        self.assertIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'scope=service,status=valid'))
        self.assertNotIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'status=expired'))
        self.assertNotIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'scope=host'))
        self.assertIn('rgw_ssl_cert', self.cert_ls('--filter-by', f'service={self.SVC}'))
        signed_by_cephadm = self.cert_ls('--filter-by', 'signed-by=cephadm')
        self.assertNotIn('rgw_ssl_cert', signed_by_cephadm)
        self.assertIn(ROOT_CA, signed_by_cephadm)

    def test_scope_targets_are_enforced(self) -> None:
        leaf = self.leaf('cli-scope')
        self.assert_fails(self.cert_key_set_args('rgw', leaf['full']), '--service-name')
        self.assert_fails(self.cert_key_set_args('grafana', leaf['full']), '--hostname')
        self.assert_fails(self.cert_key_set_args('no-such-service', leaf['full']),
                          'bindings ls')
        # host and global scope succeed with the right target
        self.set_pair('grafana', leaf, host=self.host_names[0])
        self.set_pair('mgmt-gateway', leaf)
        self.assertTrue(self.get_cert('grafana_ssl_cert', host=self.host_names[0]))
        self.assertTrue(self.get_cert('mgmt_gateway_ssl_cert'))

    def test_consumer_with_several_certs_needs_cert_name(self) -> None:
        leaf = self.leaf('cli-multi')
        self.assert_fails(self.cert_key_set_args('nvmeof', leaf['full'])
                          + ['--service-name', 'nvmeof.cli-qa'], '--cert-name')

    def test_pem_inputs_accepted(self) -> None:
        for key_type in ('rsa', 'rsa-pkcs1', 'ec'):
            for layout in ('key+leaf', 'key+leaf+intermediate', 'leaf+intermediate+key'):
                with self.subTest(key_type=key_type, layout=layout):
                    leaf = self.leaf(f'cli-{key_type}-{len(layout)}', key_type=key_type)
                    parts = {'key+leaf': (leaf['key'], leaf['crt']),
                             'key+leaf+intermediate': (leaf['key'], leaf['chain']),
                             'leaf+intermediate+key': (leaf['chain'], leaf['key'])}[layout]
                    pem = self.pem_file(f'cli-{key_type}-{len(layout)}-in', *parts)
                    self.set_pair('rgw', leaf, service=self.SVC, pem=pem)
                    stored = self.get_cert('rgw_ssl_cert', service=self.SVC)
                    self.assertEqual(self.fingerprint_of(stored), leaf['fingerprint'])
                    self.assertNotIn('PRIVATE KEY', stored,
                                     'key material must be split out of the certificate')
                    self.assertEqual(self.get_key('rgw_ssl_key', service=self.SVC).strip(),
                                     self.read(leaf['key']).strip())

    def test_invalid_inputs_rejected(self) -> None:
        good = self.leaf('cli-good')
        other_key = f'{WORKDIR}/cli-other.key'
        self.pki('key', other_key)
        expired = self.leaf('cli-expired', not_before=-30, days=10)
        expiring = self.leaf('cli-expiring', days=5)
        encrypted = self.leaf('cli-encrypted', passphrase='secret')
        cases: List[Tuple[str, str, Tuple[str, ...]]] = [
            ('mismatched key', self.pem_file('cli-mismatch', other_key, good['crt']),
             ('does not match',)),
            ('expired', expired['full'], ('expired',)),
            ('expiring', expiring['full'], ('about to expire',)),
            ('encrypted key', encrypted['full'], ('encrypted',)),
            ('two private keys', self.pem_file('cli-two-keys', good['key'], other_key,
                                               good['crt']), ('private key',)),
            ('key without cert', good['key'], ('no CERTIFICATE blocks',)),
            ('cert without key', good['chain'], ('private key are both required',)),
            ('corrupt certificate block', self.pem_file(
                'cli-corrupt', good['key'],
                '-----BEGIN CERTIFICATE-----\n' + ('A' * 64 + '\n') * 5
                + '-----END CERTIFICATE-----\n'), ('failed to parse',)),
        ]
        for label, pem, needles in cases:
            with self.subTest(case=label):
                self.assert_fails(self.cert_key_set_args('rgw', pem)
                                  + ['--service-name', self.SVC], *needles)
                self.assertEqual(self.get_cert('rgw_ssl_cert', service=self.SVC), '',
                                 f'{label}: nothing may be stored after a rejected input')
        self.assert_fails(['orch', 'certmgr', 'cert', 'set', 'rgw_ssl_cert',
                           '--service-name', self.SVC, '-i', good['full']], 'cert-key set')

    def test_force_without_debug_mode_does_not_bypass_validation(self) -> None:
        expired = self.leaf('cli-force', not_before=-30, days=10)
        self.set_opt('certificate_check_debug_mode', False)
        res = self.set_pair('rgw', expired, service=self.SVC, force=True, check=False)
        self.assertNotEqual(res.rc, 0, res)

    def test_separate_cert_and_key_set(self) -> None:
        leaf = self.leaf('cli-separate')
        self.ceph('orch', 'certmgr', 'cert', 'set', 'rgw_ssl_cert',
                  '--service-name', self.SVC, '-i', leaf['chain'])
        self.ceph('orch', 'certmgr', 'key', 'set', 'rgw_ssl_key',
                  '--service-name', self.SVC, '-i', leaf['key'])
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.assertIn('All certificates are valid',
                      '\n'.join(self.ceph_json('orch', 'certmgr', 'cert', 'check')))

    def test_rm_and_missing_objects(self) -> None:
        leaf = self.leaf('cli-rm')
        self.set_pair('rgw', leaf, service=self.SVC)
        self.ceph('orch', 'certmgr', 'cert', 'rm', 'rgw_ssl_cert', '--service-name', self.SVC)
        self.ceph('orch', 'certmgr', 'key', 'rm', 'rgw_ssl_key', '--service-name', self.SVC)
        self.assertEqual(self.get_cert('rgw_ssl_cert', service=self.SVC), '')
        self.assertEqual(self.get_key('rgw_ssl_key', service=self.SVC), '')
        self.assert_fails(['orch', 'certmgr', 'cert', 'get', 'rgw_ssl_cert',
                           '--service-name', self.SVC], 'no secret found')
        self.assert_fails(['orch', 'certmgr', 'cert', 'rm', 'rgw_ssl_cert',
                           '--service-name', self.SVC], 'cannot delete')
        self.assert_fails(['orch', 'certmgr', 'cert', 'rm', 'no_such_cert'], 'cannot delete')
        self.assert_fails(['orch', 'certmgr', 'cert', 'get', 'rgw_ssl_cert'],
                          'need service name')

    def test_generate_certificates(self) -> None:
        for module in ('dashboard', 'prometheus'):
            with self.subTest(module=module):
                out = json.loads(self.orch('certmgr', 'generate-certificates', module).out)
                self.assertIn('PRIVATE KEY', out['key'])
                self.assertTrue(self.issued_by_cephadm_root_ca(out['cert']))
                info = self.cert_info_pem(out['cert'])
                self.assertFalse(info['is_ca'])
                if module == 'dashboard':
                    self.assertIn('dashboard_servers', info['dns_names'])
        self.assert_fails(['orch', 'certmgr', 'generate-certificates', 'nope'], 'nope')

    def test_store_survives_reload_and_mgr_failover(self) -> None:
        leaf = self.leaf('cli-persist')
        self.set_pair('rgw', leaf, service=self.SVC)
        root_fp = self.fingerprint_of(self.get_cert(ROOT_CA))
        self.assertEqual(self.orch('certmgr', 'reload').out.strip(), 'OK')
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.fail_mgr_and_wait()
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.assertEqual(self.fingerprint_of(self.get_cert(ROOT_CA)), root_fp,
                         'root CA must not be regenerated on failover')


class TestCertMgrHealth(CertMgrTestCase):
    """Health reporting for user-provided certificates. No TLS daemons."""

    SVC = 'rgw.health-qa'

    def setUp(self) -> None:
        super().setUp()
        # debug mode: check on every serve loop and let --force store bad certs
        self.set_opt('certificate_check_debug_mode', True)
        self.set_opt('certificate_check_period', 1)
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        for code in CERT_HEALTH_CODES:
            self.wait_health(code, present=False, timeout=300)

    def tearDown(self) -> None:
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        self.set_opt('certificate_check_period', 1)
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.set_opt('certificate_check_debug_mode', True)
        for code in CERT_HEALTH_CODES:
            self.wait_health(code, present=False, timeout=300)
        super().tearDown()

    def cert_check(self) -> str:
        return '\n'.join(self.ceph_json('orch', 'certmgr', 'cert', 'check'))

    def test_expiring_user_cert_raises_warning(self) -> None:
        leaf = self.leaf('health-expiring', days=15)
        self.set_pair('rgw', leaf, service=self.SVC, force=True)
        self.wait_health('CEPHADM_CERT_WARNING', detail='rgw_ssl_cert')
        self.assertIn('about to expire', self.cert_check())
        self.assertIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'status=expiring'))
        # user certificates are never renewed by cephadm
        self.hold('user cert untouched',
                  lambda: self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC))
                  == leaf['fingerprint'], seconds=60)
        good = self.leaf('health-fixed')
        self.set_pair('rgw', good, service=self.SVC)
        self.wait_health('CEPHADM_CERT_WARNING', present=False)

    def test_expired_user_cert_raises_error(self) -> None:
        leaf = self.leaf('health-expired', not_before=-30, days=10)
        self.set_pair('rgw', leaf, service=self.SVC, force=True)
        self.wait_health('CEPHADM_CERT_ERROR', detail='rgw_ssl_cert')
        self.assertIn('has expired', self.cert_check())
        self.assertIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'status=expired'))
        self.set_pair('rgw', self.leaf('health-renewed'), service=self.SVC)
        self.wait_health('CEPHADM_CERT_ERROR', present=False)

    def test_mismatched_key_raises_error(self) -> None:
        leaf = self.leaf('health-mismatch')
        self.set_pair('rgw', leaf, service=self.SVC)
        other_key = f'{WORKDIR}/health-other.key'
        self.pki('key', other_key)
        self.ceph('orch', 'certmgr', 'key', 'set', 'rgw_ssl_key',
                  '--service-name', self.SVC, '-i', other_key)
        self.wait_health('CEPHADM_CERT_ERROR', detail='rgw_ssl_cert')
        self.assertIn('not valid', self.cert_check())
        # 'cert ls' status describes the certificate alone and stays 'valid'
        # here; the cert/key pair check is what the health check reports
        self.ceph('orch', 'certmgr', 'key', 'set', 'rgw_ssl_key',
                  '--service-name', self.SVC, '-i', leaf['key'])
        self.wait_health('CEPHADM_CERT_ERROR', present=False)

    def test_missing_key_raises_error(self) -> None:
        leaf = self.leaf('health-nokey')
        self.set_pair('rgw', leaf, service=self.SVC)
        self.ceph('orch', 'certmgr', 'key', 'rm', 'rgw_ssl_key', '--service-name', self.SVC)
        self.wait_health('CEPHADM_CERT_ERROR', detail='rgw_ssl_cert')
        self.assertIn('missing key', self.cert_check())

    def test_renewal_threshold_applies_to_user_certs(self) -> None:
        leaf = self.leaf('health-threshold', days=45)
        self.set_pair('rgw', leaf, service=self.SVC)
        self.hold('45-day cert is fine with a 30-day threshold',
                  lambda: 'CEPHADM_CERT_WARNING' not in self.health_checks(), seconds=60)
        self.set_opt('certificate_renewal_threshold_days', 60)
        self.wait_health('CEPHADM_CERT_WARNING', detail='rgw_ssl_cert')
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.wait_health('CEPHADM_CERT_WARNING', present=False)

    def test_check_period_zero_disables_checks(self) -> None:
        self.set_opt('certificate_check_period', 0)
        leaf = self.leaf('health-disabled', not_before=-30, days=10)
        self.set_pair('rgw', leaf, service=self.SVC, force=True)
        # note: 'certmgr cert check' refreshes health itself, so it is not used here
        self.hold('no certificate health reported while checks are disabled',
                  lambda: 'CEPHADM_CERT_ERROR' not in self.health_checks(), seconds=120)
        self.set_opt('certificate_check_period', 1)
        self.wait_health('CEPHADM_CERT_ERROR', detail='rgw_ssl_cert')

    def test_health_detail_lists_every_bad_cert(self) -> None:
        svc2 = 'rgw.health-qa2'
        try:
            self.set_pair('rgw', self.leaf('health-a', not_before=-30, days=10),
                          service=self.SVC, force=True)
            self.set_pair('rgw', self.leaf('health-b', not_before=-30, days=10),
                          service=svc2, force=True)
            self.wait_health('CEPHADM_CERT_ERROR', detail=self.SVC)
            self.wait_health('CEPHADM_CERT_ERROR', detail=svc2)
        finally:
            self.rm_cert_key('rgw_ssl_cert', service=svc2)


class TestCertMgrServiceScope(CertMgrTestCase):
    """RGW end to end: what clients see for each certificate source."""

    SVC_ID = 'certqa'
    SVC = f'rgw.{SVC_ID}'
    PORT = 8443

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.host = cls.host_names[0]

    def tearDown(self) -> None:
        self.rm_service(self.SVC)
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        super().tearDown()

    def spec(self, source: str, **extra: Any) -> Dict[str, Any]:
        spec: Dict[str, Any] = {'ssl': True, 'certificate_source': source,
                                'rgw_frontend_port': self.PORT}
        spec.update(extra)
        return {'service_type': 'rgw', 'service_id': self.SVC_ID,
                'placement': {'hosts': [self.host]}, 'spec': spec}

    def deploy(self, source: str, **extra: Any) -> None:
        self.apply(self.SVC, self.spec(source, **extra))
        self.wait_service(self.SVC)

    def change_tls(self, source: str, **extra: Any) -> None:
        """Change the TLS config of the running RGW service.

        Workaround: when an RGW TLS dependency changes (cert hash, source)
        cephadm only *reconfigures* the daemon (RgwService has no
        choose_next_action override). Reconfig rewrites rgw/cert/<svc>, but
        RGW reads it only at startup, so the old cert stays in service.
        Redeploy explicitly until that is fixed; the DNM reproducers cover
        the unassisted case."""
        self.apply(self.SVC, self.spec(source, **extra))
        self.orch('redeploy', self.SVC)
        self.wait_service(self.SVC)

    def cephadm_signed_name(self) -> str:
        return f'cephadm-signed_{self.SVC}_cert'

    def assert_verified_chain(self) -> None:
        res = self.served(self.host, self.PORT, cafile=f'{WORKDIR}/ca/root/ca.crt')
        self.assertTrue(res['ok'], f'handshake trusting only the QA root failed: {res}')

    def test_cephadm_signed(self) -> None:
        self.deploy('cephadm-signed', custom_sans=['s3.certqa.test'])
        served = self.wait_served(self.host, self.PORT, lambda r: True, 'rgw TLS')
        self.assertTrue(self.issued_by_cephadm_root_ca(served['pem']))
        self.assertIn(self.hosts[self.host], served['ip_addresses'])
        self.assertIn('s3.certqa.test', served['dns_names'])
        verified = self.served(self.host, self.PORT, cafile=f'{WORKDIR}/cephadm-root-ca.crt')
        self.assertTrue(verified['ok'], verified)
        # stored as a host-scoped cephadm-signed object, hidden by default
        name = self.cephadm_signed_name()
        self.assertEqual(self.fingerprint_of(self.get_cert(name, host=self.host)),
                         served['fingerprint'])
        self.assertNotIn(name, self.cert_ls())
        ls = self.cert_ls('--include-cephadm-signed')
        self.assertIn(self.host, ls[name]['certificates'])
        self.assertIn(name, self.cert_ls('--filter-by', 'signed-by=cephadm'))
        # cephadm-signed objects are host-scoped and owned by cephadm
        pem = self.pem_file('rgw-cephadm-signed',
                            self.get_key(f'cephadm-signed_{self.SVC}_key', host=self.host),
                            self.get_cert(name, host=self.host))
        self.assert_fails(self.cert_key_set_args('rgw', pem)
                          + ['--hostname', self.host, '--cert-name', name], 'not editable')

    def test_inline(self) -> None:
        leaf = self.leaf('rgw-inline', dns=['s3.certqa.test'], ips=[self.hosts[self.host]])
        self.deploy('inline', ssl_cert=self.read(leaf['chain']), ssl_key=self.read(leaf['key']))
        served = self.wait_served(self.host, self.PORT,
                                  lambda r: r['fingerprint'] == leaf['fingerprint'],
                                  'inline certificate')
        self.assertEqual(served['subject'], leaf['subject'])
        self.assert_verified_chain()
        ls = self.cert_ls('--filter-by', 'signed-by=user')
        self.assertIn(self.SVC, ls['rgw_ssl_cert']['certificates'])
        # inline certificates belong to the spec, not to certmgr
        self.assert_fails(['orch', 'certmgr', 'cert-key', 'set', 'rgw', '--service-name',
                           self.SVC, '-i', self.leaf('rgw-inline-2')['full']], 'not editable')
        # a new inline cert in the spec replaces the stored and the served one
        leaf2 = self.leaf('rgw-inline-3', ips=[self.hosts[self.host]])
        self.change_tls('inline', ssl_cert=self.read(leaf2['chain']),
                        ssl_key=self.read(leaf2['key']))
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] == leaf2['fingerprint'],
                         'updated inline certificate')
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf2['fingerprint'])

    def test_reference_requires_certificate_first(self) -> None:
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        res = self.apply(self.SVC, self.spec('reference'), check=False)
        self.assertNotEqual(res.rc, 0, res)
        self.assertIn('cert-key set', res.err + res.out)
        self.assertFalse(self.daemons(self.SVC))

    def test_reference(self) -> None:
        leaf = self.leaf('rgw-ref', ips=[self.hosts[self.host]])
        self.set_pair('rgw', leaf, service=self.SVC)
        self.deploy('reference')
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] == leaf['fingerprint'],
                         'referenced certificate')
        self.assert_verified_chain()
        # removing the service keeps user-provided reference objects
        self.rm_service(self.SVC)
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.assertTrue(self.get_key('rgw_ssl_key', service=self.SVC))

    def test_source_switches_garbage_collect(self) -> None:
        name = self.cephadm_signed_name()
        self.deploy('cephadm-signed')
        self.poll('cephadm-signed cert stored',
                  lambda: bool(self.get_cert(name, host=self.host)), timeout=300)

        leaf = self.leaf('rgw-gc-inline', ips=[self.hosts[self.host]])
        self.change_tls('inline', ssl_cert=self.read(leaf['chain']),
                        ssl_key=self.read(leaf['key']))
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] == leaf['fingerprint'], 'inline after switch')
        self.poll('cephadm-signed cert removed after switching to inline',
                  lambda: self.get_cert(name, host=self.host) == '', timeout=300, kick=True)

        # inline -> reference without a certmgr entry: inline objects are
        # dropped and the apply is refused until a certificate is provided
        res = self.apply(self.SVC, self.spec('reference'), check=False)
        self.assertNotEqual(res.rc, 0, res)
        self.assertEqual(self.get_cert('rgw_ssl_cert', service=self.SVC), '')

        ref = self.leaf('rgw-gc-ref', ips=[self.hosts[self.host]])
        self.set_pair('rgw', ref, service=self.SVC)
        self.change_tls('reference')
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] == ref['fingerprint'], 'reference after switch')

        self.change_tls('cephadm-signed')
        self.wait_served(self.host, self.PORT,
                         lambda r: self.issued_by_cephadm_root_ca(r['pem']),
                         'cephadm-signed after switch')
        # reference objects are user data and survive the switch
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         ref['fingerprint'])

    def test_service_removal_cleans_cephadm_signed_and_inline(self) -> None:
        name = self.cephadm_signed_name()
        self.deploy('cephadm-signed')
        self.poll('cephadm-signed cert stored',
                  lambda: bool(self.get_cert(name, host=self.host)), timeout=300)
        self.rm_service(self.SVC)
        self.poll('cephadm-signed cert removed with the service',
                  lambda: self.get_cert(name, host=self.host) == '', timeout=300, kick=True)

        leaf = self.leaf('rgw-rm-inline', ips=[self.hosts[self.host]])
        self.deploy('inline', ssl_cert=self.read(leaf['chain']), ssl_key=self.read(leaf['key']))
        self.rm_service(self.SVC)
        self.poll('inline cert removed with the service',
                  lambda: self.get_cert('rgw_ssl_cert', service=self.SVC) == '',
                  timeout=300, kick=True)


class TestCertMgrHostGlobalScope(CertMgrTestCase):
    """grafana (host scope) and mgmt-gateway (global scope)."""

    GRAFANA_PORT = 3000
    GW_PORT = 9876
    GW_INTERNAL_PORT = 29443

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.host = cls.host_names[0]

    def tearDown(self) -> None:
        self.rm_service('grafana')
        self.rm_service('mgmt-gateway')
        self.rm_cert_key('mgmt_gateway_ssl_cert')
        super().tearDown()

    def grafana_spec(self, hosts: List[str], **spec: Any) -> Dict[str, Any]:
        return {'service_type': 'grafana', 'placement': {'hosts': hosts}, 'spec': spec}

    def gateway_spec(self, **spec: Any) -> Dict[str, Any]:
        base = {'port': self.GW_PORT, 'enable_health_check_endpoint': True}
        base.update(spec)
        return {'service_type': 'mgmt-gateway', 'placement': {'hosts': [self.host]},
                'spec': base}

    def test_grafana_cephadm_signed_per_host(self) -> None:
        """Host scope: each grafana daemon gets its own cert, bound to its host,
        and the certs go away with the daemons. (grafana takes cephadm-signed
        or reference certs only; reference is in the open-issue reproducers.)"""
        hosts = self.host_names[:2]
        name = 'cephadm-signed_grafana_cert'
        self.apply('grafana', self.grafana_spec(hosts))
        self.wait_service('grafana')
        fingerprints = set()
        for host in hosts:
            with self.subTest(host=host):
                served = self.wait_served(host, self.GRAFANA_PORT, lambda r: True,
                                          'grafana TLS')
                self.assertTrue(self.issued_by_cephadm_root_ca(served['pem']))
                self.assertIn(self.hosts[host], served['ip_addresses'])
                for other in set(hosts) - {host}:
                    self.assertNotIn(self.hosts[other], served['ip_addresses'])
                self.assertIn('grafana_servers', served['dns_names'])
                self.assertEqual(self.fingerprint_of(self.get_cert(name, host=host)),
                                 served['fingerprint'])
                fingerprints.add(served['fingerprint'])
        self.assertEqual(len(fingerprints), len(hosts), 'hosts must not share a certificate')
        ls = self.cert_ls('--include-cephadm-signed', '--filter-by', 'name=cephadm-signed_grafana*')
        self.assertEqual(sorted(ls[name]['certificates']), sorted(hosts))
        self.rm_service('grafana')
        for host in hosts:
            self.poll(f'{name} removed for {host}',
                      lambda: self.get_cert(name, host=host) == '', timeout=300, kick=True)

    def test_mgmt_gateway_cephadm_signed(self) -> None:
        self.apply('mgmt-gateway', self.gateway_spec())
        self.wait_service('mgmt-gateway')
        external = self.wait_served(self.host, self.GW_PORT, lambda r: True, 'gateway TLS')
        self.assertTrue(self.issued_by_cephadm_root_ca(external['pem']))
        internal = self.wait_served(self.host, self.GW_INTERNAL_PORT, lambda r: True,
                                    'gateway internal TLS')
        self.assertTrue(self.issued_by_cephadm_root_ca(internal['pem']))

    def test_mgmt_gateway_reference_global(self) -> None:
        leaf = self.leaf('gw-ref', ips=[self.hosts[self.host]])
        self.set_pair('mgmt-gateway', leaf)
        ls = self.cert_ls()
        self.assertEqual(ls['mgmt_gateway_ssl_cert']['scope'], 'global')
        self.apply('mgmt-gateway', self.gateway_spec(certificate_source='reference'))
        self.wait_service('mgmt-gateway')
        self.wait_served(self.host, self.GW_PORT,
                         lambda r: r['fingerprint'] == leaf['fingerprint'], 'gateway reference')
        res = self.served(self.host, self.GW_PORT, cafile=f'{WORKDIR}/ca/root/ca.crt')
        self.assertTrue(res['ok'], res)

    def test_mgmt_gateway_inline(self) -> None:
        leaf = self.leaf('gw-inline', ips=[self.hosts[self.host]])
        self.apply('mgmt-gateway', self.gateway_spec(certificate_source='inline',
                                                     ssl_cert=self.read(leaf['chain']),
                                                     ssl_key=self.read(leaf['key'])))
        self.wait_service('mgmt-gateway')
        self.wait_served(self.host, self.GW_PORT,
                         lambda r: r['fingerprint'] == leaf['fingerprint'],
                         'gateway inline cert')
        res = self.served(self.host, self.GW_PORT, cafile=f'{WORKDIR}/ca/root/ca.crt')
        self.assertTrue(res['ok'], f'gateway must send the intermediate: {res}')


class TestCertMgrRenewal(CertMgrTestCase):
    """Automated rotation of cephadm-signed certificates, observed on grafana
    (a non-Ceph daemon, so a reconfig restarts it and reloads the cert)."""

    PORT = 3000

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.host = cls.host_names[0]
        cls._ceph_cls('orch', 'apply', 'grafana', '--placement', cls.host)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._ceph_cls('orch', 'rm', 'grafana', check=False)
        super().tearDownClass()

    def setUp(self) -> None:
        """Every test starts from the same settled state, whatever ran before:
        default policy, grafana up, the served cert is the stored one and no
        certificate health check is raised."""
        super().setUp()
        self.reset_policy()
        self.wait_service('grafana')
        self.poll('grafana serves the stored cephadm-signed cert',
                  lambda: bool(self.stored())
                  and self.served_fp() == self.fingerprint_of(self.stored()),
                  timeout=600, kick=True)
        for code in CERT_HEALTH_CODES:
            self.wait_health(code, present=False, timeout=300)

    def tearDown(self) -> None:
        self.reset_policy()
        super().tearDown()

    def reset_policy(self) -> None:
        # threshold before rotation: re-enabling rotation while a test's raised
        # threshold is still set would renew the cert on the way out
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.set_opt('certificate_check_debug_mode', True)
        self.set_opt('certificate_automated_rotation_enabled', True)
        self.ceph('config', 'rm', 'mgr', 'mgr/cephadm/certificate_duration_days')
        self.orch('certmgr', 'reload')

    def stored(self) -> str:
        return self.get_cert('cephadm-signed_grafana_cert', host=self.host)

    def served_fp(self) -> str:
        res = self.served(self.host, self.PORT)
        return str(res['fingerprint']) if res.get('ok') else ''

    def short_lived_cert(self) -> Dict[str, Any]:
        """Replace grafana's cert with a fresh 90-day one (the minimum duration)."""
        self.set_opt('certificate_duration_days', 90)
        # newly generated certs pick up the duration after a store reload
        # (setting the option alone does not change newly generated certs)
        self.orch('certmgr', 'reload')
        # The cert being replaced may itself be a 90-day one (left by an
        # earlier test), so wait for a *different* cert, not just a short one.
        previous = self.fingerprint_of(self.stored())
        self.rm_cert_key('cephadm-signed_grafana_cert', host=self.host)
        self.orch('redeploy', 'grafana')
        self.poll('a new cert in the store',
                  lambda: self.stored() != ''
                  and self.fingerprint_of(self.stored()) != previous,
                  timeout=600, kick=True)
        new = self.fingerprint_of(self.stored())
        served = self.wait_served(self.host, self.PORT,
                                  lambda r: r['fingerprint'] == new, 'new grafana cert')
        self.assertLessEqual(served['validity_days'], 91, served)
        return served

    def test_short_lived_cert_is_issued(self) -> None:
        served = self.short_lived_cert()
        self.assertTrue(self.issued_by_cephadm_root_ca(served['pem']))
        self.assertEqual(self.fingerprint_of(self.stored()), served['fingerprint'])

    def test_expiring_cert_is_renewed_and_served(self) -> None:
        before = self.short_lived_cert()
        self.set_opt('certificate_renewal_threshold_days', 90)  # 89 days left < 90
        renewed = self.wait_served(self.host, self.PORT,
                                   lambda r: r['fingerprint'] != before['fingerprint'],
                                   'renewed grafana certificate')
        self.assertTrue(self.issued_by_cephadm_root_ca(renewed['pem']))
        self.assertEqual(renewed['dns_names'], before['dns_names'])
        self.assertEqual(renewed['ip_addresses'], before['ip_addresses'])
        self.assertNotIn('CEPHADM_CERT_WARNING', self.health_checks(),
                         'auto-renewed certificates must not raise a warning')
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.poll('served cert matches the store once renewals stop',
                  lambda: self.served_fp() == self.fingerprint_of(self.stored()),
                  timeout=300, kick=True)
        settled = self.served_fp()
        self.hold('no renewal once the cert is outside the threshold',
                  lambda: self.served_fp() == settled, seconds=90)

    def test_rotation_disabled_warns_instead(self) -> None:
        before = self.short_lived_cert()
        self.set_opt('certificate_automated_rotation_enabled', False)
        self.set_opt('certificate_renewal_threshold_days', 90)
        self.wait_health('CEPHADM_CERT_WARNING', detail='cephadm-signed_grafana_cert')
        self.assertEqual(self.served_fp(), before['fingerprint'])
        self.assertIn('about to expire',
                      '\n'.join(self.ceph_json('orch', 'certmgr', 'cert', 'check')))
        self.set_opt('certificate_automated_rotation_enabled', True)
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] != before['fingerprint'],
                         'renewal once rotation is re-enabled')
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.wait_health('CEPHADM_CERT_WARNING', present=False)

    def test_invalid_cert_is_regenerated(self) -> None:
        before = self.fingerprint_of(self.stored())  # == served, see setUp
        bad_key = f'{WORKDIR}/renewal-bad.key'
        self.pki('key', bad_key)
        self.ceph('orch', 'certmgr', 'key', 'set', 'cephadm-signed_grafana_key',
                  '--hostname', self.host, '-i', bad_key)
        self.poll('store holds a regenerated cert',
                  lambda: self.stored() != ''
                  and self.fingerprint_of(self.stored()) != before,
                  timeout=600, kick=True)
        regenerated = self.fingerprint_of(self.stored())
        after = self.wait_served(self.host, self.PORT,
                                 lambda r: r['fingerprint'] == regenerated,
                                 'regenerated grafana certificate')
        self.assertTrue(self.issued_by_cephadm_root_ca(after['pem']))
        self.assertIn('All certificates are valid',
                      '\n'.join(self.ceph_json('orch', 'certmgr', 'cert', 'check')))
        self.wait_health('CEPHADM_CERT_ERROR', present=False)
