import pytest
from cephadm.registry import Registry


class TestParseWWWAuthenticate:
    """Test suite for Registry.parse_www_authenticate() method."""

    def test_standard_docker_registry_header(self):
        """Test parsing standard Docker Hub WWW-Authenticate header with quoted values."""
        registry = Registry('docker.io')
        header = 'Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:ceph/ceph:pull"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.docker.io/token'
        assert params == {
            'service': 'registry.docker.io',
            'scope': 'repository:ceph/ceph:pull'
        }

    def test_ibm_container_registry_header_unquoted(self):
        """Test parsing IBM Container Registry header with unquoted values."""
        registry = Registry('cp.icr.io')
        header = 'Bearer realm=https://cp.icr.io/oauth/token, service=registry, scope=repository:cp/ibm-ceph/ceph-8-rhel9:pull'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://cp.icr.io/oauth/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:cp/ibm-ceph/ceph-8-rhel9:pull'
        }

    def test_quoted_value_with_comma(self):
        """Test parsing quoted values that contain commas (e.g., multiple scopes)."""
        registry = Registry('example.com')
        header = 'Bearer realm="https://auth.example.com/token",service="registry",scope="repository:ceph/ceph:pull,push"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:ceph/ceph:pull,push'
        }

    def test_mixed_quoted_and_unquoted_values(self):
        """Test parsing header with both quoted and unquoted values."""
        registry = Registry('example.com')
        header = 'Bearer realm="https://auth.example.com/token",service=registry,scope="repository:ceph/ceph:pull"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:ceph/ceph:pull'
        }

    def test_whitespace_around_equals(self):
        """Test parsing with optional whitespace around equals sign."""
        registry = Registry('example.com')
        header = 'Bearer realm = "https://auth.example.com/token" , service = "registry" , scope = "repository:ceph/ceph:pull"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:ceph/ceph:pull'
        }

    def test_unquoted_values_with_whitespace(self):
        """Test parsing unquoted values with whitespace separation."""
        registry = Registry('example.com')
        header = 'Bearer realm=https://auth.example.com/token, service=registry, scope=repository:ceph/ceph:pull'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:ceph/ceph:pull'
        }

    def test_missing_realm_raises_error(self):
        """Test that missing realm parameter raises ValueError."""
        registry = Registry('example.com')
        header = 'Bearer service="registry",scope="repository:ceph/ceph:pull"'
        
        with pytest.raises(ValueError, match='No realm found in WWW-Authenticate header'):
            registry.parse_www_authenticate(header)

    def test_additional_auth_parameters_preserved(self):
        """Test that additional authentication parameters are preserved."""
        registry = Registry('example.com')
        header = 'Bearer realm="https://auth.example.com/token",service="registry",scope="repository:ceph/ceph:pull",custom_param="value"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:ceph/ceph:pull',
            'custom_param': 'value'
        }

    def test_no_bearer_prefix(self):
        """Test parsing header without 'Bearer ' prefix."""
        registry = Registry('example.com')
        header = 'realm="https://auth.example.com/token",service="registry"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token'
        assert params == {'service': 'registry'}

    def test_empty_quoted_value(self):
        """Test parsing with empty quoted value."""
        registry = Registry('example.com')
        header = 'Bearer realm="https://auth.example.com/token",service="",scope="repository:ceph/ceph:pull"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': '',
            'scope': 'repository:ceph/ceph:pull'
        }

    def test_complex_scope_with_multiple_commas(self):
        """Test parsing scope with multiple comma-separated actions."""
        registry = Registry('example.com')
        header = 'Bearer realm="https://auth.example.com/token",service="registry",scope="repository:ceph/ceph:pull,push,delete"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:ceph/ceph:pull,push,delete'
        }

    def test_url_with_query_parameters(self):
        """Test parsing realm URL containing query parameters."""
        registry = Registry('example.com')
        header = 'Bearer realm="https://auth.example.com/token?client_id=docker",service="registry"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token?client_id=docker'
        assert params == {'service': 'registry'}

    def test_unquoted_url_with_path(self):
        """Test parsing unquoted realm URL with path components."""
        registry = Registry('example.com')
        header = 'Bearer realm=https://auth.example.com/v2/token, service=registry'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/v2/token'
        assert params == {'service': 'registry'}

    def test_parameter_order_independence(self):
        """Test that parameter order doesn't affect parsing."""
        registry = Registry('example.com')
        header = 'Bearer scope="repository:ceph/ceph:pull",realm="https://auth.example.com/token",service="registry"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.example.com/token'
        assert params == {
            'service': 'registry',
            'scope': 'repository:ceph/ceph:pull'
        }

    def test_real_world_docker_hub_header(self):
        """Test with actual Docker Hub WWW-Authenticate header format."""
        registry = Registry('docker.io')
        header = 'Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/alpine:pull"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://auth.docker.io/token'
        assert params == {
            'service': 'registry.docker.io',
            'scope': 'repository:library/alpine:pull'
        }

    def test_real_world_quay_io_header(self):
        """Test with Quay.io style header."""
        registry = Registry('quay.io')
        header = 'Bearer realm="https://quay.io/v2/auth",service="quay.io",scope="repository:ceph/ceph:pull"'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://quay.io/v2/auth'
        assert params == {
            'service': 'quay.io',
            'scope': 'repository:ceph/ceph:pull'
        }

    def test_backward_compatibility_with_old_format(self):
        """Test backward compatibility with previously supported format."""
        registry = Registry('example.com')
        # This was the format that worked before the regression
        header = 'Bearer realm=https://cp.icr.io/oauth/token, service=registry, scope=repository:cp/ibm-ceph/ceph-8-rhel9:pull'
        
        realm, params = registry.parse_www_authenticate(header)
        
        assert realm == 'https://cp.icr.io/oauth/token'
        assert params['service'] == 'registry'
        assert params['scope'] == 'repository:cp/ibm-ceph/ceph-8-rhel9:pull'

# Made with Bob
