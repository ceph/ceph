import pytest
from cephadm.registry import Registry


class TestParseWWWAuthenticate:
    """Tests for Registry.parse_www_authenticate()."""

    def test_standard_docker_registry_header(self):
        """Standard Docker Hub header with quoted values."""
        registry = Registry('docker.io')
        header = 'Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:ceph/ceph:pull"'
        realm, params = registry.parse_www_authenticate(header)
        assert realm == 'https://auth.docker.io/token'
        assert params == {
            'service': 'registry.docker.io',
            'scope': 'repository:ceph/ceph:pull',
        }

    def test_ibm_container_registry_header(self):
        """IBM ICR returns unquoted values with spaces after commas — the actual bug."""
        registry = Registry('cp.icr.io')
        header = 'Bearer realm=https://cp.icr.io/oauth/token, service=registry, scope=repository:cp/ibm-ceph/ceph-8-rhel9:pull'
        realm, params = registry.parse_www_authenticate(header)
        assert realm == 'https://cp.icr.io/oauth/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:cp/ibm-ceph/ceph-8-rhel9:pull',
        }

    def test_quoted_scope_with_comma(self):
        """Quoted values that contain commas (e.g. pull,push) must not be split."""
        registry = Registry('example.com')
        header = 'Bearer realm="https://auth.example.com/token",service="registry",scope="repository:ceph/ceph:pull,push"'
        realm, params = registry.parse_www_authenticate(header)
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:ceph/ceph:pull,push',
        }

    def test_mixed_quoted_and_unquoted_values(self):
        """Header mixing quoted and unquoted values."""
        registry = Registry('example.com')
        header = 'Bearer realm="https://auth.example.com/token",service=registry,scope="repository:ceph/ceph:pull"'
        realm, params = registry.parse_www_authenticate(header)
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:ceph/ceph:pull',
        }

    def test_missing_realm_raises_error(self):
        """Missing realm must raise ValueError."""
        registry = Registry('example.com')
        header = 'Bearer service="registry",scope="repository:ceph/ceph:pull"'
        with pytest.raises(ValueError, match='No realm found in WWW-Authenticate header'):
            registry.parse_www_authenticate(header)

    def test_no_bearer_prefix(self):
        """Header without 'Bearer ' prefix is still parseable."""
        registry = Registry('example.com')
        header = 'realm="https://auth.example.com/token",service="registry"'
        realm, params = registry.parse_www_authenticate(header)
        assert realm == 'https://auth.example.com/token'
        assert params == {'service': 'registry'}

    def test_quay_io_header(self):
        """Quay.io style header."""
        registry = Registry('quay.io')
        header = 'Bearer realm="https://quay.io/v2/auth",service="quay.io",scope="repository:ceph/ceph:pull"'
        realm, params = registry.parse_www_authenticate(header)
        assert realm == 'https://quay.io/v2/auth'
        assert params == {
            'service': 'quay.io',
            'scope': 'repository:ceph/ceph:pull',
        }
