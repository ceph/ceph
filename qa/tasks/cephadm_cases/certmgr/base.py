"""
Integration tests for the cephadm certificate manager (certmgr).

The unit tests in src/pybind/mgr/cephadm/tests/test_certmgr.py cover the
store logic in isolation. These tests exercise certmgr against a real cluster:
the CLI, the periodic certificate check and its health reporting, how each
certificate source (cephadm-signed, inline, reference) reaches the daemons, and
what a TLS client connecting to those daemons actually receives.

Certificates are built with pki.py, which runs inside ``cephadm shell`` so
that the only requirement on the test nodes is the Ceph container image itself.

This module holds the certmgr-specific helpers shared by every facet (the
generic ones come from tasks.cephadm_cases.base). Test classes, one module
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
from typing import Any, Callable, Dict, List, Optional

from tasks.cephadm_cases.base import (  # noqa: F401 (re-exported for the tests)
    CephadmTestCase, CommandFailed, CommandResult, ROOT_CA, ROOT_CA_FILE_NAME,
)

log = logging.getLogger(__name__)

WORKDIR = '/tmp/certmgr-qa'
PKI = f'{WORKDIR}/certmgr_pki.py'
ROOT_CA_KEY = 'cephadm_root_ca_key'
CERT_HEALTH_CODES = ('CEPHADM_CERT_WARNING', 'CEPHADM_CERT_ERROR')
MODULE_OPTS = (
    'certificate_check_debug_mode',
    'certificate_automated_rotation_enabled',
    'certificate_check_period',
    'certificate_duration_days',
    'certificate_renewal_threshold_days',
)


class CertMgrTestCase(CephadmTestCase):
    """Shared helpers. Not collected on its own (no test_ methods)."""

    WORKDIR = WORKDIR

    # populated by setUpClass
    host: str = ''

    # ------------------------------------------------------------------
    # class setup
    # ------------------------------------------------------------------
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.remote.run(args=['sudo', 'mkdir', '-p', f'{WORKDIR}/ca'])
        cls.install_file(cls.local_file(__file__, 'pki.py'), PKI.rsplit('/', 1)[1])
        # A two-level test PKI: servers present leaf + intermediate, clients
        # trust the root only, so a verified handshake proves the chain is sent.
        cls._pki_cls('ca', f'{WORKDIR}/ca/root', '--cn', 'certmgr QA root CA')
        cls._pki_cls('ca', f'{WORKDIR}/ca/int', '--cn', 'certmgr QA intermediate CA',
                     '--parent', f'{WORKDIR}/ca/root')

    @classmethod
    def tearDownClass(cls) -> None:
        for opt in MODULE_OPTS:
            cls._ceph_cls('config', 'rm', 'mgr', f'mgr/cephadm/{opt}', check=False)
        super().tearDownClass()

    # ------------------------------------------------------------------
    # pki.py
    # ------------------------------------------------------------------
    @classmethod
    def _pki_cls(cls, *args: str) -> Dict[str, Any]:
        return json.loads(cls._run_cls(['python3', PKI] + list(args)).out)

    def pki(self, *args: str) -> Dict[str, Any]:
        return self._pki_cls(*args)

    # ------------------------------------------------------------------
    # apply(): checking an updated spec is the one stored
    # ------------------------------------------------------------------
    @classmethod
    def check_stored_spec(cls, spec: Dict[str, Any], stored: Dict[str, Any],
                          res: CommandResult) -> None:
        # Checking that the service exists is not enough on an update (the
        # previous spec is still there), so compare the TLS fields.
        want = spec.get('spec', {})
        have = stored.get('spec', {})
        if 'certificate_source' in want \
                and have.get('certificate_source') != want['certificate_source']:
            raise CommandFailed(
                f"stored certificate_source is {have.get('certificate_source')!r}, "
                f"expected {want['certificate_source']!r}: {res!r}")
        if 'ssl_cert' in want and (
                not have.get('ssl_cert')
                or cls.fingerprint_of(have['ssl_cert'])
                != cls.fingerprint_of(want['ssl_cert'])):
            raise CommandFailed(f'stored ssl_cert is not the applied one: {res!r}')

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

    @classmethod
    def cert_info_pem(cls, pem: str) -> Dict[str, Any]:
        path = cls._write_cls(f'{WORKDIR}/_probe.crt', pem)
        return cls._pki_cls('info', path)

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

    @classmethod
    def fingerprint_of(cls, pem: str) -> str:
        return str(cls.cert_info_pem(pem)['fingerprint'])
