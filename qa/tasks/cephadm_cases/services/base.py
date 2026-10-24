"""
Shared helpers for the orch/cephadm/services/* suites.

Each service suite lives in its own package next to this module
(services/mgmt_gateway, ...) and builds on ServicesTestCase, which adds to
tasks.cephadm_cases.base what checking a service from a client's side
needs. The tests check what clients actually get from the daemons: HTTP
status codes, redirects, TLS and client-certificate behaviour. They go
through probe.py, which runs inside ``cephadm shell`` so the test nodes
only need the Ceph container image.
"""

import json
import logging
from typing import Any, Dict, List, Sequence

from tasks.cephadm_cases.base import (  # noqa: F401 (re-exported for the tests)
    CephadmTestCase, CommandFailed, CommandResult, ROOT_CA, ROOT_CA_FILE_NAME,
)

log = logging.getLogger(__name__)

WORKDIR = '/tmp/cephadm-services-qa'
PROBE = f'{WORKDIR}/probe.py'
ROOT_CA_FILE = f'{WORKDIR}/{ROOT_CA_FILE_NAME}'

# Spec fields that are never compared after an apply: 'orch ls --export' may
# mask them, and they are not what a test asserts on.
SECRET_FIELDS = ('secret', 'password', 'key', 'cert')


class ServicesTestCase(CephadmTestCase):
    """Shared helpers. Not collected on its own (no test_ methods)."""

    WORKDIR = WORKDIR
    # probe results are nested JSON; a missing level while a daemon restarts
    # shows up as TypeError
    POLL_TRANSIENT = CephadmTestCase.POLL_TRANSIENT + (TypeError,)
    # the tests check settings cephadm writes when it deploys a service
    # (dashboard API URLs, standby behaviour, SSO); the only option they set
    # themselves, secure_monitoring_stack, is reset by the tests using it
    RESET_CONFIG_AFTER_TEST = False

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.install_file(cls.local_file(__file__, 'probe.py'), PROBE.rsplit('/', 1)[1])

    # ------------------------------------------------------------------
    # probe.py
    # ------------------------------------------------------------------
    @classmethod
    def _probe_cls(cls, *args: str) -> Any:
        return json.loads(cls._run_cls(['python3', PROBE] + list(args)).out)

    def probe(self, *args: str) -> Any:
        return self._probe_cls(*args)

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
    # apply(): checking an updated spec is the one stored
    # ------------------------------------------------------------------
    @classmethod
    def check_stored_spec(cls, spec: Dict[str, Any], stored: Dict[str, Any],
                          res: CommandResult) -> None:
        # on an update the previous spec is still there: compare every field
        # the test set, except secrets
        have = stored.get('spec', {})
        for field, want in spec.get('spec', {}).items():
            if any(s in field for s in SECRET_FIELDS) or field not in have:
                continue
            if have[field] != want:
                raise CommandFailed(
                    f'stored {field} is {have[field]!r}, expected {want!r}: {res!r}')
