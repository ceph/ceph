"""
cephadm integration tests for fullchain PEM ingest (Ceph issue #75710).

Covers cephadm-side consumers of ceph.deployment.tls_utils:
  - CertMgr.save_cert_key_from_pem  — high-level ingest helper
  - module.cert_store_set_pair      — CLI entry point
  - module.cert_store_set_cert      — cert-only CLI entry point
  - CephadmService._get_certificates_from_spec — service-spec entry point

Pure parser/detector unit tests for ceph.deployment.tls_utils itself live in
src/python-common/ceph/tests/test_tls_utils.py — this file only tests
cephadm's own ingest/orchestration logic built on top of that parser.
"""

import logging

import pytest
from unittest import mock

from ceph.deployment.service_spec import RGWSpec

# Import the wait() helper used throughout cephadm tests to unwrap OrchResult
from .fixtures import wait
from .pem_fixtures import (
    LEAF_CERT,
    INTERMEDIATE_CA_CERT,
    ROOT_CA_CERT,
    LEAF_KEY_PKCS8,
    EC_CERT,
    UNRELATED_KEY_PKCS1,
    fullchain_pkcs1,
    fullchain_pkcs8,
    fullchain_ec,
    cert_chain_only,
    multi_block_no_cert_no_key,
    encrypted_fullchain,
)


# ===========================================================================
# Tests: CertMgr.save_cert_key_from_pem — high-level ingest method
# ===========================================================================

class TestCertMgrSaveCertKeyFromPem:
    """Tests for the new CertMgr.save_cert_key_from_pem ingest helper."""

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_pkcs1_split_on_ingest(self, _set_store, cephadm_module):
        """Fullchain PEM (PKCS#1 key + chain) is split and both halves stored."""
        cm = cephadm_module.cert_mgr
        blob = fullchain_pkcs1()

        cert_chain, key = cm.save_cert_key_from_pem(
            'rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.myzone'
        )

        # cert_chain must not contain any key material
        assert '-----BEGIN RSA PRIVATE KEY-----' not in cert_chain
        assert '-----BEGIN PRIVATE KEY-----' not in cert_chain
        # key must not contain any cert material
        assert '-----BEGIN CERTIFICATE-----' not in key
        # Both halves persisted
        stored_cert = cm.get_cert('rgw_ssl_cert', service_name='rgw.myzone')
        stored_key = cm.get_key('rgw_ssl_key', service_name='rgw.myzone')
        assert stored_cert is not None and '-----BEGIN CERTIFICATE-----' in stored_cert
        assert stored_key is not None and '-----BEGIN RSA PRIVATE KEY-----' in stored_key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_pkcs8_split_on_ingest(self, _set_store, cephadm_module):
        cm = cephadm_module.cert_mgr
        blob = fullchain_pkcs8()
        cert_chain, key = cm.save_cert_key_from_pem(
            'grafana_ssl_cert', 'grafana_ssl_key', blob, host='node1'
        )
        assert '-----BEGIN CERTIFICATE-----' in cert_chain
        assert '-----BEGIN PRIVATE KEY-----' in key

        # grafana cert/key are host-scoped, so fullchain ingest must preserve
        # the host target when saving the split cert/key halves.
        stored_cert = cm.get_cert('grafana_ssl_cert', host='node1')
        stored_key = cm.get_key('grafana_ssl_key', host='node1')
        assert stored_cert == cert_chain
        assert stored_key == key
        assert cm.get_cert('grafana_ssl_cert', host='node2') is None
        assert cm.get_key('grafana_ssl_key', host='node2') is None

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_ec_split_on_ingest(self, _set_store, cephadm_module):
        cm = cephadm_module.cert_mgr
        blob = fullchain_ec()
        cert_chain, key = cm.save_cert_key_from_pem(
            'mgmt_gateway_ssl_cert', 'mgmt_gateway_ssl_key', blob
        )
        assert EC_CERT.strip() in cert_chain
        assert '-----BEGIN EC PRIVATE KEY-----' in key

        # mgmt-gateway cert/key are global-scoped, so there is no service/host
        # target to preserve; the split cert/key halves must be retrievable globally.
        stored_cert = cm.get_cert('mgmt_gateway_ssl_cert')
        stored_key = cm.get_key('mgmt_gateway_ssl_key')
        assert stored_cert == cert_chain
        assert stored_key == key
        assert cm.get_cert(
            'mgmt_gateway_ssl_cert', service_name='ignored', host='ignored'
        ) == cert_chain
        assert cm.get_key(
            'mgmt_gateway_ssl_key', service_name='ignored', host='ignored'
        ) == key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_cert_chain_only_no_key_stored(self, _set_store, cephadm_module):
        """Chain-only PEM (no key block) — cert saved, key store untouched."""
        cm = cephadm_module.cert_mgr
        blob = cert_chain_only()
        cert_chain, key = cm.save_cert_key_from_pem(
            'nfs_ssl_cert', 'nfs_ssl_key', blob, service_name='nfs.foo'
        )
        assert key == ''
        stored_cert = cm.get_cert('nfs_ssl_cert', service_name='nfs.foo')
        assert stored_cert is not None

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_single_leaf_cert_only_saves_cert_and_does_not_store_key(
        self, _set_store, cephadm_module
    ):
        """A single PEM certificate with no private key is saved as a cert-only
        value and must not create an empty key object.

        This covers the single-certificate branch in save_cert_key_from_pem(),
        which does not go through the fullchain/multi-block split path.
        """
        cm = cephadm_module.cert_mgr

        cert_chain, key = cm.save_cert_key_from_pem(
            'rgw_ssl_cert', 'rgw_ssl_key', LEAF_CERT, service_name='rgw.single-cert-only'
        )

        assert cert_chain == LEAF_CERT
        assert key == ''

        stored_cert = cm.get_cert('rgw_ssl_cert', service_name='rgw.single-cert-only')
        stored_key = cm.get_key('rgw_ssl_key', service_name='rgw.single-cert-only')
        assert stored_cert == LEAF_CERT
        assert stored_key is None

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_with_crlf_line_endings_split_on_ingest(
        self, _set_store, cephadm_module
    ):
        """Windows/CRLF-formatted PEM bundles are accepted and normalized."""
        cm = cephadm_module.cert_mgr
        blob = fullchain_pkcs8().replace('\n', '\r\n')

        cert_chain, key = cm.save_cert_key_from_pem(
            'rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.crlf'
        )

        assert '-----BEGIN CERTIFICATE-----' in cert_chain
        assert '-----BEGIN PRIVATE KEY-----' not in cert_chain
        assert '-----BEGIN PRIVATE KEY-----' in key
        assert '-----BEGIN CERTIFICATE-----' not in key

        stored_cert = cm.get_cert('rgw_ssl_cert', service_name='rgw.crlf')
        stored_key = cm.get_key('rgw_ssl_key', service_name='rgw.crlf')
        assert stored_cert == cert_chain
        assert stored_key == key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_with_openssl_bag_attributes_strips_non_pem_metadata(
        self, _set_store, cephadm_module
    ):
        """OpenSSL/pkcs12-style metadata around PEM blocks is stripped."""
        cm = cephadm_module.cert_mgr
        blob = '\n'.join([
            'Bag Attributes',
            '    friendlyName: rgw-key',
            '    localKeyID: 01 02 03 04',
            LEAF_KEY_PKCS8,
            'Bag Attributes',
            '    friendlyName: rgw-leaf',
            'subject=CN = test.example.com',
            'issuer=CN = Test Intermediate CA',
            LEAF_CERT,
            'Bag Attributes',
            '    friendlyName: rgw-intermediate',
            INTERMEDIATE_CA_CERT,
            ROOT_CA_CERT,
        ])

        cert_chain, key = cm.save_cert_key_from_pem(
            'rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.bag-attrs'
        )

        assert LEAF_CERT.strip() in cert_chain
        assert INTERMEDIATE_CA_CERT.strip() in cert_chain
        assert ROOT_CA_CERT.strip() in cert_chain

        # Non-PEM metadata from OpenSSL/PKCS#12 exports is not part of the
        # certificate/key material and should not be preserved in certmgr storage.
        for metadata in [
            'Bag Attributes',
            'friendlyName',
            'localKeyID',
            'subject=',
            'issuer=',
        ]:
            assert metadata not in cert_chain
            assert metadata not in key

        assert '-----BEGIN PRIVATE KEY-----' in key
        assert '-----BEGIN CERTIFICATE-----' not in key

        stored_cert = cm.get_cert('rgw_ssl_cert', service_name='rgw.bag-attrs')
        stored_key = cm.get_key('rgw_ssl_key', service_name='rgw.bag-attrs')
        assert stored_cert == cert_chain
        assert stored_key == key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_key_mismatch_raises(self, _set_store, cephadm_module):
        """Mismatched key/cert in a fullchain blob must raise, not silently store garbage."""
        from ceph.deployment.tls_utils import SSLConfigException
        cm = cephadm_module.cert_mgr
        blob = "\n".join([UNRELATED_KEY_PKCS1, LEAF_CERT])
        with pytest.raises(SSLConfigException, match='does not match'):
            cm.save_cert_key_from_pem('rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.x')

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_encrypted_private_key_raises(self, _set_store, cephadm_module):
        from ceph.deployment.tls_utils import SSLConfigException
        cm = cephadm_module.cert_mgr
        with pytest.raises(SSLConfigException, match='Encrypted private keys are not supported'):
            cm.save_cert_key_from_pem(
                'rgw_ssl_cert', 'rgw_ssl_key', encrypted_fullchain(), service_name='rgw.x'
            )

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_preserves_intermediate_certs_in_chain(self, _set_store, cephadm_module):
        """All intermediate CA certs must be retained in the stored cert chain."""
        cm = cephadm_module.cert_mgr
        blob = fullchain_pkcs1()   # leaf + intermediate + root
        cert_chain, _ = cm.save_cert_key_from_pem(
            'rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.full'
        )
        assert LEAF_CERT.strip() in cert_chain
        assert INTERMEDIATE_CA_CERT.strip() in cert_chain
        assert ROOT_CA_CERT.strip() in cert_chain

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_leaf_cert_is_first_in_chain(self, _set_store, cephadm_module):
        """Leaf cert (first CERTIFICATE block) must appear before intermediate certs."""
        cm = cephadm_module.cert_mgr
        blob = fullchain_pkcs1()
        cert_chain, _ = cm.save_cert_key_from_pem(
            'rgw_ssl_cert', 'rgw_ssl_key', blob, service_name='rgw.order'
        )
        leaf_pos = cert_chain.index('test.example.com') if 'test.example.com' in cert_chain else \
            cert_chain.index(LEAF_CERT[:40])
        intermediate_pos = cert_chain.index(INTERMEDIATE_CA_CERT[:40])
        assert leaf_pos < intermediate_pos


# ===========================================================================
# Tests: SpecStore._save_certs_and_keys — RGW inventory/spec-store path
# ===========================================================================

class TestRgwInventoryFullchainPem:
    """RGW rgw_frontend_ssl_certificate ingest through SpecStore._save_certs_and_keys."""

    def _mock_cert_mgr_saves(self, cephadm_module):
        cephadm_module.cert_mgr.save_cert = mock.Mock()
        cephadm_module.cert_mgr.save_key = mock.Mock()

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_rgw_fullchain_string_is_split_and_saved(self, _set_store, cephadm_module):
        """RGW inventory/spec-store path splits a fullchain PEM string into cert and key."""
        spec = RGWSpec(
            service_id='realm.zone',
            rgw_frontend_ssl_certificate=fullchain_pkcs1(),
        )

        self._mock_cert_mgr_saves(cephadm_module)

        cephadm_module.spec_store._save_certs_and_keys(spec)

        cephadm_module.cert_mgr.save_cert.assert_called_once()
        cert_args, cert_kwargs = cephadm_module.cert_mgr.save_cert.call_args

        assert cert_args[0] == 'rgw_ssl_cert'
        assert '-----BEGIN CERTIFICATE-----' in cert_args[1]
        assert '-----BEGIN RSA PRIVATE KEY-----' not in cert_args[1]
        assert LEAF_CERT.strip() in cert_args[1]
        assert INTERMEDIATE_CA_CERT.strip() in cert_args[1]
        assert ROOT_CA_CERT.strip() in cert_args[1]
        assert cert_kwargs == {
            'service_name': spec.service_name(),
            'user_made': True,
        }

        cephadm_module.cert_mgr.save_key.assert_called_once()
        key_args, key_kwargs = cephadm_module.cert_mgr.save_key.call_args

        assert key_args[0] == 'rgw_ssl_key'
        assert '-----BEGIN RSA PRIVATE KEY-----' in key_args[1]
        assert '-----BEGIN CERTIFICATE-----' not in key_args[1]
        assert key_kwargs == {
            'service_name': spec.service_name(),
            'user_made': True,
        }

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_rgw_fullchain_list_is_joined_split_and_saved(self, _set_store, cephadm_module):
        """Legacy/list-style RGW cert input is joined, parsed, split, and saved."""
        spec = RGWSpec(
            service_id='realm.zone',
            rgw_frontend_ssl_certificate=[
                LEAF_KEY_PKCS8,
                LEAF_CERT,
                INTERMEDIATE_CA_CERT,
                ROOT_CA_CERT,
            ],
        )

        self._mock_cert_mgr_saves(cephadm_module)

        cephadm_module.spec_store._save_certs_and_keys(spec)

        cephadm_module.cert_mgr.save_cert.assert_called_once()
        cert_args, cert_kwargs = cephadm_module.cert_mgr.save_cert.call_args

        assert cert_args[0] == 'rgw_ssl_cert'
        assert cert_args[1].count('-----BEGIN CERTIFICATE-----') == 3
        assert '-----BEGIN PRIVATE KEY-----' not in cert_args[1]
        assert cert_kwargs == {
            'service_name': spec.service_name(),
            'user_made': True,
        }

        cephadm_module.cert_mgr.save_key.assert_called_once()
        key_args, key_kwargs = cephadm_module.cert_mgr.save_key.call_args

        assert key_args[0] == 'rgw_ssl_key'
        assert '-----BEGIN PRIVATE KEY-----' in key_args[1]
        assert '-----BEGIN CERTIFICATE-----' not in key_args[1]
        assert key_kwargs == {
            'service_name': spec.service_name(),
            'user_made': True,
        }

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_rgw_cert_chain_without_key_is_rejected(self, _set_store, cephadm_module, caplog):
        """RGW fullchain-style cert input without an embedded key is not saved."""
        cert_chain = cert_chain_only()
        spec = RGWSpec(
            service_id='realm.zone',
            rgw_frontend_ssl_certificate=cert_chain,
        )

        self._mock_cert_mgr_saves(cephadm_module)

        with caplog.at_level(logging.ERROR, logger='cephadm.inventory'):
            cephadm_module.spec_store._save_certs_and_keys(spec)

        cephadm_module.cert_mgr.save_cert.assert_not_called()
        cephadm_module.cert_mgr.save_key.assert_not_called()

        assert 'Cannot parse the rgw certificate: missing certificate or key.' in caplog.text
        assert '-----BEGIN CERTIFICATE-----' not in caplog.text
        assert cert_chain not in caplog.text

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_rgw_bad_fullchain_is_rejected_without_leaking_key_material(
        self,
        _set_store,
        cephadm_module,
        caplog,
    ):
        """Parser errors are logged without leaking raw PEM/private-key material."""
        bad_fullchain = encrypted_fullchain()
        spec = RGWSpec(
            service_id='realm.zone',
            rgw_frontend_ssl_certificate=bad_fullchain,
        )

        self._mock_cert_mgr_saves(cephadm_module)

        with caplog.at_level(logging.ERROR, logger='cephadm.inventory'):
            cephadm_module.spec_store._save_certs_and_keys(spec)

        cephadm_module.cert_mgr.save_cert.assert_not_called()
        cephadm_module.cert_mgr.save_key.assert_not_called()

        assert 'Cannot parse RGW certificate PEM:' in caplog.text
        assert 'Encrypted private keys are not supported' in caplog.text
        assert 'Cannot parse the rgw certificate: missing certificate or key.' in caplog.text

        assert '-----BEGIN ENCRYPTED PRIVATE KEY-----' not in caplog.text
        assert '-----BEGIN PRIVATE KEY-----' not in caplog.text
        assert '-----BEGIN RSA PRIVATE KEY-----' not in caplog.text
        assert bad_fullchain not in caplog.text


# ===========================================================================
# Tests: cert_store_set_pair CLI — fullchain auto-detection
# ===========================================================================

class TestCertStoreSetPairFullchain:
    """cert_store_set_pair must auto-split a fullchain PEM passed as 'cert'."""

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_pkcs1_via_cli(self, _set_store, cephadm_module):
        """Full e2e: fullchain blob → cert_store_set_pair → stored separately."""
        blob = fullchain_pkcs1()
        # cert_store_set_pair validates with verify_tls; mock that check away
        with mock.patch.object(cephadm_module.cert_mgr, 'check_certificate_state') as mock_check:
            from cephadm.cert_mgr import CertInfo
            mock_check.return_value = CertInfo(
                'rgw_ssl_cert', 'rgw.zone1', user_made=True,
                is_valid=True, is_close_to_expiration=False, days_to_expiration=300
            )
            result = cephadm_module.cert_store_set_pair(
                cert=blob,
                key='',           # no separate key — it's embedded in the blob
                consumer='rgw',
                service_name='rgw.zone1',
            )
        # cert_store_set_pair is decorated with @handle_orch_error → unwrap via wait()
        assert wait(cephadm_module, result) == 'Certificate/key pair set correctly'
        stored_cert = cephadm_module.cert_mgr.get_cert('rgw_ssl_cert', service_name='rgw.zone1')
        stored_key = cephadm_module.cert_mgr.get_key('rgw_ssl_key', service_name='rgw.zone1')
        assert stored_cert is not None and '-----BEGIN CERTIFICATE-----' in stored_cert
        assert '-----BEGIN RSA PRIVATE KEY-----' not in stored_cert   # key must NOT be in cert store
        assert stored_key is not None and '-----BEGIN RSA PRIVATE KEY-----' in stored_key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_generated_cert_via_cli_uses_real_validation(
        self, _set_store, cephadm_module
    ):
        """Fullchain blob generated at test runtime is split, validated with
        the real check_certificate_state()/verify_tls path, and stored separately.

        This avoids relying on static fixture certificates, which can become
        flaky as they age or if their validity window changes.
        """
        generated = cephadm_module.cert_mgr.generate_cert(
            host_fqdn='rgw.real-validation.test',
            node_ip='127.0.0.1',
        )
        cert, key = generated[0], generated[1]
        blob = key + cert

        with mock.patch.object(
            cephadm_module.cert_mgr,
            'check_certificate_state',
            wraps=cephadm_module.cert_mgr.check_certificate_state,
        ) as mock_check:
            result = cephadm_module.cert_store_set_pair(
                cert=blob,
                key='',
                consumer='rgw',
                service_name='rgw.real-validation',
            )

        assert wait(cephadm_module, result) == 'Certificate/key pair set correctly'
        mock_check.assert_called_once()

        stored_cert = cephadm_module.cert_mgr.get_cert(
            'rgw_ssl_cert', service_name='rgw.real-validation'
        )
        stored_key = cephadm_module.cert_mgr.get_key(
            'rgw_ssl_key', service_name='rgw.real-validation'
        )

        assert stored_cert is not None
        assert stored_key is not None
        assert stored_cert == cert
        assert stored_key == key
        assert '-----BEGIN PRIVATE KEY-----' not in stored_cert
        assert '-----BEGIN CERTIFICATE-----' not in stored_key

        cert_info = cephadm_module.cert_mgr.check_certificate_state(
            'rgw_ssl_cert', 'rgw.real-validation', stored_cert, stored_key
        )
        assert cert_info.is_valid
        assert not cert_info.is_close_to_expiration

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_rejected_when_separate_key_also_provided(self, _set_store, cephadm_module):
        """Passing a fullchain blob AND a separate --key is an error.

        handle_orch_error re-raises OrchestratorError immediately (it is a
        user-facing validation error, not a deferred async failure), so the
        exception is raised by cert_store_set_pair() itself — no wait() needed.
        """
        from orchestrator import OrchestratorError
        blob = fullchain_pkcs1()
        with pytest.raises(OrchestratorError, match='fullchain PEM.*embedded private key'):
            cephadm_module.cert_store_set_pair(
                cert=blob,
                key=LEAF_KEY_PKCS8,  # conflicting extra key
                consumer='rgw',
                service_name='rgw.zone1',
            )

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_encrypted_private_key_rejected_via_cli(self, _set_store, cephadm_module):
        from orchestrator import OrchestratorError
        with pytest.raises(OrchestratorError, match='Encrypted private keys are not supported'):
            cephadm_module.cert_store_set_pair(
                cert=encrypted_fullchain(),
                key='',
                consumer='rgw',
                service_name='rgw.zone1',
            )

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_duplicate_embedded_private_key_rejected_via_cli(
        self, _set_store, cephadm_module
    ):
        """Parser duplicate-key errors are propagated through cert_store_set_pair.

        The detailed parser matrix lives in test_tls_utils.py; this test only
        verifies that one representative parser error is surfaced by the
        high-level cephadm CLI/backend path and nothing is stored.
        """
        from orchestrator import OrchestratorError

        duplicate_key_fullchain = '\n'.join([LEAF_KEY_PKCS8, LEAF_KEY_PKCS8, LEAF_CERT])

        with mock.patch.object(cephadm_module.cert_mgr, 'check_certificate_state') as mock_check:
            with pytest.raises(OrchestratorError, match='key'):
                cephadm_module.cert_store_set_pair(
                    cert=duplicate_key_fullchain,
                    key='',
                    consumer='rgw',
                    service_name='rgw.duplicate-key',
                )
            mock_check.assert_not_called()

        assert cephadm_module.cert_mgr.get_cert(
            'rgw_ssl_cert', service_name='rgw.duplicate-key'
        ) is None
        assert cephadm_module.cert_mgr.get_key(
            'rgw_ssl_key', service_name='rgw.duplicate-key'
        ) is None

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_cert_chain_only_with_separate_key_is_normalized(self, _set_store, cephadm_module):
        """A multi-block cert chain (no embedded key) + a separate --key is
        now routed through parse_tls_pem_bundle (contains_multiple_pem_blocks() check),
        not just contains_private_key(). For well-formed chains the stored
        output is unchanged; see test_malformed_multiblock_cert_rejected_via_cli
        below for the case where this actually changes behaviour.
        """
        with mock.patch.object(cephadm_module.cert_mgr, 'check_certificate_state') as mock_check:
            from cephadm.cert_mgr import CertInfo
            mock_check.return_value = CertInfo(
                'rgw_ssl_cert', 'rgw.chain-only', user_made=True,
                is_valid=True, is_close_to_expiration=False, days_to_expiration=300
            )
            result = cephadm_module.cert_store_set_pair(
                cert=cert_chain_only(),
                key=LEAF_KEY_PKCS8,
                consumer='rgw',
                service_name='rgw.chain-only',
            )
        assert wait(cephadm_module, result) == 'Certificate/key pair set correctly'
        stored_cert = cephadm_module.cert_mgr.get_cert(
            'rgw_ssl_cert', service_name='rgw.chain-only')
        stored_key = cephadm_module.cert_mgr.get_key('rgw_ssl_key', service_name='rgw.chain-only')
        # All three blocks (leaf + intermediate + root) must be preserved.
        assert stored_cert is not None and stored_cert.count('-----BEGIN CERTIFICATE-----') == 3
        assert stored_key == LEAF_KEY_PKCS8

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_malformed_multiblock_cert_rejected_via_cli(self, _set_store, cephadm_module):
        """A multi-block blob containing zero CERTIFICATE blocks (e.g. stray
        PUBLIC KEY blocks) must be rejected with a clear error instead of
        being silently passed through to check_certificate_state as if it
        were valid cert data. This is the concrete case where checking
        contains_multiple_pem_blocks() (in addition to contains_private_key()) changes
        behaviour: previously, since no PRIVATE KEY block is present, this
        blob skipped fullchain handling entirely and was forwarded unchanged.
        """
        from orchestrator import OrchestratorError
        with mock.patch.object(cephadm_module.cert_mgr, 'check_certificate_state') as mock_check:
            from cephadm.cert_mgr import CertInfo
            # If reached, this would incorrectly report the garbage as valid —
            # the fix must raise before check_certificate_state is ever called.
            mock_check.return_value = CertInfo(
                'rgw_ssl_cert', 'rgw.malformed', user_made=True,
                is_valid=True, is_close_to_expiration=False, days_to_expiration=300
            )
            with pytest.raises(OrchestratorError, match='no CERTIFICATE blocks'):
                cephadm_module.cert_store_set_pair(
                    cert=multi_block_no_cert_no_key(),
                    key=LEAF_KEY_PKCS8,
                    consumer='rgw',
                    service_name='rgw.malformed',
                )
            mock_check.assert_not_called()

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_cert_only_no_key_anywhere_is_rejected(self, _set_store, cephadm_module):
        """A cert with no embedded key and no separate --key must be rejected
        by cert_store_set_pair itself. This check matters more now than it
        used to: orchestrator/module.py's 'orch certmgr cert-key set -i <file>'
        path forwards inbuf completely unparsed/unvalidated (key='') and
        relies entirely on this method to catch a missing key — there is no
        PEM-awareness left in orchestrator/module.py at all.
        """
        from orchestrator import OrchestratorError
        with pytest.raises(OrchestratorError, match='certificate and a private key are both required'):
            cephadm_module.cert_store_set_pair(
                cert=LEAF_CERT,
                key='',
                consumer='rgw',
                service_name='rgw.no-key',
            )

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_cert_chain_only_no_separate_key_is_rejected(self, _set_store, cephadm_module):
        """Same as above but with a multi-block cert chain (no embedded key)
        and still no separate key — must still be rejected, not silently
        stored with an empty key.
        """
        from orchestrator import OrchestratorError
        with pytest.raises(OrchestratorError, match='certificate and a private key are both required'):
            cephadm_module.cert_store_set_pair(
                cert=cert_chain_only(),
                key='',
                consumer='rgw',
                service_name='rgw.chain-no-key',
            )

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_garbage_with_no_key_is_rejected(self, _set_store, cephadm_module):
        """Plain non-PEM garbage forwarded with an empty key (e.g. via the
        'orch certmgr cert-key set -i <file>' path, which no longer validates
        PEM content itself) is rejected by the not-cert-or-not-key guard
        before ever reaching check_certificate_state.
        """
        from orchestrator import OrchestratorError
        with mock.patch.object(cephadm_module.cert_mgr, 'check_certificate_state') as mock_check:
            with pytest.raises(OrchestratorError, match='certificate and a private key are both required'):
                cephadm_module.cert_store_set_pair(
                    cert='this is not a PEM at all',
                    key='',
                    consumer='rgw',
                    service_name='rgw.garbage',
                )
            mock_check.assert_not_called()


# ===========================================================================
# Tests: cert_store_set_cert CLI — cert-only path must reject embedded keys
# ===========================================================================

class TestCertStoreSetCertFullchain:
    """cert_store_set_cert has no key parameter, so any embedded private key
    must be rejected outright rather than silently stored in the cert object.
    """

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_fullchain_with_embedded_key_rejected(self, _set_store, cephadm_module):
        """A combined PEM (key + cert) passed to the cert-only path must be
        rejected, not silently persisted with the key material intact.
        """
        from orchestrator import OrchestratorError
        with pytest.raises(OrchestratorError, match='private key material'):
            cephadm_module.cert_store_set_cert(
                cert_name='rgw_ssl_cert',
                cert=fullchain_pkcs1(),
                service_name='rgw.embedded-key',
            )
        # The key must never have reached storage under the cert name.
        stored_cert = cephadm_module.cert_mgr.get_cert(
            'rgw_ssl_cert', service_name='rgw.embedded-key')
        assert stored_cert is None

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_cert_chain_only_normalized(self, _set_store, cephadm_module):
        """A key-free multi-block cert chain is still accepted and normalised
        via parse_tls_pem_bundle (all blocks preserved).
        """
        with mock.patch.object(cephadm_module.cert_mgr, 'check_certificate_state') as mock_check:
            from cephadm.cert_mgr import CertInfo
            mock_check.return_value = CertInfo(
                'rgw_ssl_cert', 'rgw.chain-only-cert', user_made=True,
                is_valid=True, is_close_to_expiration=False, days_to_expiration=300
            )
            result = cephadm_module.cert_store_set_cert(
                cert_name='rgw_ssl_cert',
                cert=cert_chain_only(),
                service_name='rgw.chain-only-cert',
            )
        assert wait(cephadm_module, result) == 'Certificate for rgw_ssl_cert set correctly'
        stored_cert = cephadm_module.cert_mgr.get_cert(
            'rgw_ssl_cert', service_name='rgw.chain-only-cert')
        assert stored_cert is not None and stored_cert.count('-----BEGIN CERTIFICATE-----') == 3


# ===========================================================================
# Tests: _get_certificates_from_spec — service spec entry point
# ===========================================================================

class TestGetCertificatesFromSpecFullchain:
    """_get_certificates_from_spec must split fullchain PEM from service specs."""

    def _make_service(self, cephadm_module):
        """Return a minimal *concrete* CephadmService instance.

        CephadmService has ``TYPE`` as an ``@abstractproperty``, so we create a
        minimal subclass here rather than instantiating the base class directly.
        """
        from cephadm.services.cephadmservice import CephadmService

        class _MinimalService(CephadmService):
            @property
            def TYPE(self) -> str:  # type: ignore[override]
                return 'rgw'

        svc = object.__new__(_MinimalService)
        svc.mgr = cephadm_module
        return svc

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_spec_with_fullchain_pkcs1_split_correctly(self, _set_store, cephadm_module):
        """ssl_cert field contains a fullchain blob — key must be extracted."""
        from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
        svc = self._make_service(cephadm_module)

        # Fake spec object with fullchain in ssl_cert and no ssl_key
        spec = mock.MagicMock()
        spec.service_name.return_value = 'rgw.test'
        spec.ssl_cert = fullchain_pkcs1()
        spec.ssl_key = None

        daemon_spec = mock.MagicMock(spec=CephadmDaemonDeploySpec)
        daemon_spec.host = 'node1'

        result = svc._get_certificates_from_spec(
            svc_spec=spec,
            daemon_spec=daemon_spec,
            cert_attr='ssl_cert',
            key_attr='ssl_key',
            cert_name='rgw_ssl_cert',
            key_name='rgw_ssl_key',
        )

        # Must not return EMPTY_TLS_CREDENTIALS
        assert result.cert and result.key

        stored_cert = cephadm_module.cert_mgr.get_cert('rgw_ssl_cert', service_name='rgw.test')
        stored_key = cephadm_module.cert_mgr.get_key('rgw_ssl_key', service_name='rgw.test')

        assert stored_cert is not None and '-----BEGIN CERTIFICATE-----' in stored_cert
        assert '-----BEGIN RSA PRIVATE KEY-----' not in stored_cert
        assert stored_key is not None and '-----BEGIN RSA PRIVATE KEY-----' in stored_key

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_spec_fullchain_plus_key_field_returns_empty(self, _set_store, cephadm_module):
        """When both fullchain blob AND a separate key field are set, refuse with EMPTY."""
        from cephadm.tlsobject_types import EMPTY_TLS_CREDENTIALS
        svc = self._make_service(cephadm_module)

        spec = mock.MagicMock()
        spec.service_name.return_value = 'rgw.conflict'
        spec.ssl_cert = fullchain_pkcs1()
        spec.ssl_key = LEAF_KEY_PKCS8    # conflict!

        daemon_spec = mock.MagicMock()
        daemon_spec.host = 'node1'

        result = svc._get_certificates_from_spec(
            svc_spec=spec,
            daemon_spec=daemon_spec,
            cert_attr='ssl_cert',
            key_attr='ssl_key',
            cert_name='rgw_ssl_cert',
            key_name='rgw_ssl_key',
        )
        assert result == EMPTY_TLS_CREDENTIALS

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_spec_encrypted_private_key_returns_empty(self, _set_store, cephadm_module):
        from cephadm.tlsobject_types import EMPTY_TLS_CREDENTIALS
        svc = self._make_service(cephadm_module)

        spec = mock.MagicMock()
        spec.service_name.return_value = 'rgw.encrypted'
        spec.ssl_cert = encrypted_fullchain()
        spec.ssl_key = None

        daemon_spec = mock.MagicMock()
        daemon_spec.host = 'node1'

        result = svc._get_certificates_from_spec(
            svc_spec=spec,
            daemon_spec=daemon_spec,
            cert_attr='ssl_cert',
            key_attr='ssl_key',
            cert_name='rgw_ssl_cert',
            key_name='rgw_ssl_key',
        )
        assert result == EMPTY_TLS_CREDENTIALS

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_spec_plain_cert_plus_key_unchanged(self, _set_store, cephadm_module):
        """Plain (non-fullchain) cert + separate key still works exactly as before."""
        svc = self._make_service(cephadm_module)

        spec = mock.MagicMock()
        spec.service_name.return_value = 'rgw.plain'
        spec.ssl_cert = LEAF_CERT
        spec.ssl_key = LEAF_KEY_PKCS8

        daemon_spec = mock.MagicMock()
        daemon_spec.host = 'node1'

        result = svc._get_certificates_from_spec(
            svc_spec=spec,
            daemon_spec=daemon_spec,
            cert_attr='ssl_cert',
            key_attr='ssl_key',
            cert_name='rgw_ssl_cert',
            key_name='rgw_ssl_key',
        )

        assert result.cert == LEAF_CERT
        assert result.key == LEAF_KEY_PKCS8

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_spec_cert_chain_only_with_separate_key_is_normalized(self, _set_store, cephadm_module):
        """A multi-block cert chain (no embedded key) in the spec's cert field,
        with a separate key field set, is now routed through
        parse_tls_pem_bundle via the contains_multiple_pem_blocks() check, not just
        contains_private_key(). For well-formed chains the output is
        unchanged; see test_spec_malformed_multiblock_cert_returns_empty
        below for the case where this actually changes behaviour.
        """
        svc = self._make_service(cephadm_module)

        spec = mock.MagicMock()
        spec.service_name.return_value = 'rgw.chain-only-spec'
        spec.ssl_cert = cert_chain_only()
        spec.ssl_key = LEAF_KEY_PKCS8

        daemon_spec = mock.MagicMock()
        daemon_spec.host = 'node1'

        result = svc._get_certificates_from_spec(
            svc_spec=spec,
            daemon_spec=daemon_spec,
            cert_attr='ssl_cert',
            key_attr='ssl_key',
            cert_name='rgw_ssl_cert',
            key_name='rgw_ssl_key',
        )

        assert result.cert is not None and result.cert.count('-----BEGIN CERTIFICATE-----') == 3
        assert result.key == LEAF_KEY_PKCS8

    @mock.patch('cephadm.module.CephadmOrchestrator.set_store')
    def test_spec_malformed_multiblock_cert_returns_empty(self, _set_store, cephadm_module):
        """A multi-block spec cert field with zero CERTIFICATE blocks must be
        rejected (EMPTY_TLS_CREDENTIALS) rather than silently accepted as-is.
        Previously this slipped past fullchain handling entirely because
        contains_private_key() alone was checked, not contains_multiple_pem_blocks().
        """
        from cephadm.tlsobject_types import EMPTY_TLS_CREDENTIALS
        svc = self._make_service(cephadm_module)

        spec = mock.MagicMock()
        spec.service_name.return_value = 'rgw.malformed-spec'
        spec.ssl_cert = multi_block_no_cert_no_key()
        spec.ssl_key = LEAF_KEY_PKCS8

        daemon_spec = mock.MagicMock()
        daemon_spec.host = 'node1'

        result = svc._get_certificates_from_spec(
            svc_spec=spec,
            daemon_spec=daemon_spec,
            cert_attr='ssl_cert',
            key_attr='ssl_key',
            cert_name='rgw_ssl_cert',
            key_name='rgw_ssl_key',
        )
        assert result == EMPTY_TLS_CREDENTIALS
