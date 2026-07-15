# -*- coding: utf-8 -*-
"""
Unit tests for ceph_secrets_types.py.
Placed at the same level as the module under test (src/pybind/mgr/).
"""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest

from ceph_secrets_types import (
    CephSecretException,
    CephSecretDataError,
    CephSecretNotFoundError,
    SecretScope,
    SecretRef,
    BadSecretURI,
    parse_secret_uri,
    parse_secret_path,
    validate_secret_namespace,
    SECRET_SCHEME,
    _validate_segment,
    _validate_custom_path,
    _quote_segment,
    _quote_custom_path,
)


# ============================================================
# Module constants
# ============================================================

class TestConstants:
    def test_secret_scheme(self):
        assert SECRET_SCHEME == "secret"


# ============================================================
# Exception hierarchy
# ============================================================

class TestExceptions:
    def test_base_is_exception(self):
        assert issubclass(CephSecretException, Exception)

    def test_data_error_inherits_base(self):
        assert issubclass(CephSecretDataError, CephSecretException)

    def test_not_found_inherits_base(self):
        assert issubclass(CephSecretNotFoundError, CephSecretException)

    def test_can_raise_and_catch_base(self):
        with pytest.raises(CephSecretException):
            raise CephSecretException("test")

    def test_can_raise_and_catch_data_error_as_base(self):
        with pytest.raises(CephSecretException):
            raise CephSecretDataError("data")

    def test_can_raise_and_catch_not_found_as_base(self):
        with pytest.raises(CephSecretException):
            raise CephSecretNotFoundError("gone")


# ============================================================
# _validate_segment
# ============================================================

class TestValidateSegment:
    @pytest.mark.parametrize("v", [
        "simple", "with-dashes", "under_score", "dot.dot", "a1b2",
        "UPPER", "Mixed123",
    ])
    def test_valid(self, v):
        _validate_segment("f", v)

    @pytest.mark.parametrize("bad", [
        "", " ", "has space", "a/b", "a:b", "star*",
    ])
    def test_invalid(self, bad):
        with pytest.raises(ValueError):
            _validate_segment("f", bad)

    def test_ends_with_dot_invalid(self):
        with pytest.raises(ValueError, match="must not end"):
            _validate_segment("f", "end.")

    def test_non_string_raises(self):
        with pytest.raises(ValueError, match="must be a string"):
            _validate_segment("f", None)


# ============================================================
# _validate_custom_path
# ============================================================

class TestValidateCustomPath:
    @pytest.mark.parametrize("p", [
        "single", "a/b", "a/b/c", "deep/path/here",
    ])
    def test_valid(self, p):
        _validate_custom_path(p)

    def test_empty_string(self):
        with pytest.raises(ValueError, match="must not be empty"):
            _validate_custom_path("")

    def test_empty_segment_in_middle(self):
        with pytest.raises(ValueError, match="empty segments"):
            _validate_custom_path("a//b")

    def test_trailing_slash(self):
        with pytest.raises(ValueError, match="empty segments"):
            _validate_custom_path("a/b/")

    def test_leading_slash_empty_first_segment(self):
        with pytest.raises(ValueError, match="empty segments"):
            _validate_custom_path("/a/b")

    def test_segment_with_bad_chars(self):
        with pytest.raises(ValueError):
            _validate_custom_path("a/b c/d")


# ============================================================
# _quote helpers
# ============================================================

class TestQuoteHelpers:
    def test_quote_segment_no_slash(self):
        # clean identifiers are unchanged
        assert _quote_segment("cephadm") == "cephadm"

    def test_quote_custom_path_preserves_slash(self):
        assert _quote_custom_path("a/b/c") == "a/b/c"


# ============================================================
# validate_secret_namespace
# ============================================================

class TestValidateSecretNamespace:
    def test_valid_namespace(self):
        validate_secret_namespace("cephadm")

    def test_empty_namespace(self):
        with pytest.raises(ValueError):
            validate_secret_namespace("")

    def test_namespace_with_space(self):
        with pytest.raises(ValueError):
            validate_secret_namespace("bad name")


# ============================================================
# SecretScope
# ============================================================

class TestSecretScope:
    def test_all_values(self):
        assert SecretScope.GLOBAL.value == "global"
        assert SecretScope.SERVICE.value == "service"
        assert SecretScope.HOST.value == "host"
        assert SecretScope.CUSTOM.value == "custom"

    def test_from_str_valid(self):
        for v in ("global", "service", "host", "custom"):
            scope = SecretScope.from_str(v)
            assert scope.value == v

    def test_from_str_invalid(self):
        with pytest.raises(CephSecretException, match="Invalid secret scope"):
            SecretScope.from_str("vault")

    def test_is_str_subclass(self):
        assert isinstance(SecretScope.GLOBAL, str)

    # validate_fields – global
    def test_global_valid(self):
        SecretScope.GLOBAL.validate_fields("", "mykey")

    def test_global_nonempty_target_fails(self):
        with pytest.raises(ValueError, match="target must be empty"):
            SecretScope.GLOBAL.validate_fields("target", "key")

    def test_global_empty_name_fails(self):
        with pytest.raises(ValueError):
            SecretScope.GLOBAL.validate_fields("", "")

    # validate_fields – service / host
    def test_service_valid(self):
        SecretScope.SERVICE.validate_fields("prometheus", "auth")

    def test_service_empty_target_fails(self):
        with pytest.raises(ValueError):
            SecretScope.SERVICE.validate_fields("", "auth")

    def test_host_valid(self):
        SecretScope.HOST.validate_fields("node1", "ssh_key")

    def test_host_empty_name_fails(self):
        with pytest.raises(ValueError):
            SecretScope.HOST.validate_fields("node1", "")

    # validate_fields – custom
    def test_custom_valid_flat(self):
        SecretScope.CUSTOM.validate_fields("", "some-key")

    def test_custom_valid_nested(self):
        SecretScope.CUSTOM.validate_fields("", "a/b/c")

    def test_custom_nonempty_target_fails(self):
        with pytest.raises(ValueError, match="target must be empty"):
            SecretScope.CUSTOM.validate_fields("badtarget", "key")

    def test_custom_empty_name_fails(self):
        with pytest.raises(ValueError):
            SecretScope.CUSTOM.validate_fields("", "")


# ============================================================
# SecretRef
# ============================================================

class TestSecretRef:
    def test_global_ref_construction(self):
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "pw")
        assert ref.namespace == "ns"
        assert ref.scope == SecretScope.GLOBAL
        assert ref.target == ""
        assert ref.name == "pw"

    def test_service_ref(self):
        ref = SecretRef("ns", SecretScope.SERVICE, "prom", "auth")
        assert ref.target == "prom"

    def test_host_ref(self):
        ref = SecretRef("ns", SecretScope.HOST, "n1", "k")
        assert ref.name == "k"

    def test_custom_ref(self):
        ref = SecretRef("ns", SecretScope.CUSTOM, "", "a/b/c")
        assert ref.name == "a/b/c"

    def test_is_frozen(self):
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "k")
        with pytest.raises(Exception):
            ref.name = "other"  # type: ignore[misc]

    def test_scope_coerced_from_string(self):
        ref = SecretRef("ns", "host", "node1", "k")
        assert ref.scope == SecretScope.HOST

    def test_invalid_scope_raises(self):
        with pytest.raises(ValueError):
            SecretRef("ns", "badscope", "", "k")

    def test_invalid_namespace_raises(self):
        with pytest.raises(ValueError):
            SecretRef("bad ns", SecretScope.GLOBAL, "", "k")

    def test_ident_global(self):
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "k")
        assert ref.ident() == ("ns", "global", "", "k")

    def test_ident_service(self):
        ref = SecretRef("ns", SecretScope.SERVICE, "t", "n")
        assert ref.ident() == ("ns", "service", "t", "n")

    def test_to_uri_global(self):
        ref = SecretRef("cephadm", SecretScope.GLOBAL, "", "pw")
        assert ref.to_uri() == "secret:/cephadm/global/pw"

    def test_to_uri_service(self):
        ref = SecretRef("ns", SecretScope.SERVICE, "prom", "auth")
        assert ref.to_uri() == "secret:/ns/service/prom/auth"

    def test_to_uri_host(self):
        ref = SecretRef("ns", SecretScope.HOST, "node1", "ssh")
        assert ref.to_uri() == "secret:/ns/host/node1/ssh"

    def test_to_uri_custom(self):
        ref = SecretRef("ns", SecretScope.CUSTOM, "", "a/b/c")
        assert ref.to_uri() == "secret:/ns/custom/a/b/c"

    def test_equality(self):
        r1 = SecretRef("ns", SecretScope.GLOBAL, "", "k")
        r2 = SecretRef("ns", SecretScope.GLOBAL, "", "k")
        assert r1 == r2

    def test_inequality_different_name(self):
        r1 = SecretRef("ns", SecretScope.GLOBAL, "", "k1")
        r2 = SecretRef("ns", SecretScope.GLOBAL, "", "k2")
        assert r1 != r2


# ============================================================
# BadSecretURI
# ============================================================

class TestBadSecretURI:
    def test_construction(self):
        b = BadSecretURI(raw="secret:/bad/scope/key", error="bad scope", namespace="ns")
        assert b.raw == "secret:/bad/scope/key"
        assert b.error == "bad scope"

    def test_to_uri_returns_raw(self):
        b = BadSecretURI(raw="secret:/bad", error="err", namespace="ns")
        assert b.to_uri() == "secret:/bad"

    def test_is_frozen(self):
        b = BadSecretURI(raw="x", error="e", namespace="n")
        with pytest.raises(Exception):
            b.raw = "y"  # type: ignore[misc]


# ============================================================
# parse_secret_uri
# ============================================================

class TestParseSecretUri:
    # --- valid inputs ---
    def test_global(self):
        ref = parse_secret_uri("secret:/ns/global/pw")
        assert ref.scope == SecretScope.GLOBAL
        assert ref.name == "pw"

    def test_service(self):
        ref = parse_secret_uri("secret:/ns/service/prom/auth")
        assert ref.scope == SecretScope.SERVICE
        assert ref.target == "prom"
        assert ref.name == "auth"

    def test_host(self):
        ref = parse_secret_uri("secret:/ns/host/node1/ssh")
        assert ref.target == "node1"
        assert ref.name == "ssh"

    def test_custom_flat(self):
        ref = parse_secret_uri("secret:/ns/custom/single")
        assert ref.scope == SecretScope.CUSTOM
        assert ref.name == "single"

    def test_custom_nested(self):
        ref = parse_secret_uri("secret:/ns/custom/a/b/c")
        assert ref.name == "a/b/c"

    # --- invalid inputs ---
    def test_wrong_scheme(self):
        with pytest.raises(CephSecretException, match="Not a secret uri"):
            parse_secret_uri("http://example.com/foo")

    def test_authority_rejected(self):
        with pytest.raises(CephSecretException, match="authority"):
            parse_secret_uri("secret://ns/global/key")

    def test_query_string_rejected(self):
        with pytest.raises(CephSecretException, match="query"):
            parse_secret_uri("secret:/ns/global/key?x=y")

    def test_fragment_rejected(self):
        with pytest.raises(CephSecretException, match="query"):
            parse_secret_uri("secret:/ns/global/key#frag")

    def test_percent_encoding_rejected(self):
        with pytest.raises(CephSecretException, match="percent-encoding"):
            parse_secret_uri("secret:/ns/global/my%2Dkey")

    def test_too_short(self):
        with pytest.raises(CephSecretException):
            parse_secret_uri("secret:/ns/global")

    def test_bad_scope(self):
        with pytest.raises(CephSecretException):
            parse_secret_uri("secret:/ns/vault/key")

    def test_non_string_input(self):
        with pytest.raises(CephSecretException, match="must be a string"):
            parse_secret_uri(42)  # type: ignore[arg-type]

    def test_global_with_extra_segment_rejected(self):
        with pytest.raises(CephSecretException):
            parse_secret_uri("secret:/ns/global/target/name")

    def test_service_missing_name(self):
        with pytest.raises(CephSecretException):
            parse_secret_uri("secret:/ns/service/prom")


# ============================================================
# parse_secret_path
# ============================================================

class TestParseSecretPath:
    # --- valid inputs ---
    def test_global(self):
        ref = parse_secret_path("cephadm/global/pw")
        assert ref.namespace == "cephadm"
        assert ref.scope == SecretScope.GLOBAL
        assert ref.name == "pw"

    def test_service(self):
        ref = parse_secret_path("ns/service/prom/auth")
        assert ref.target == "prom"
        assert ref.name == "auth"

    def test_host(self):
        ref = parse_secret_path("ns/host/node1/ssh_key")
        assert ref.name == "ssh_key"

    def test_custom_flat(self):
        ref = parse_secret_path("ns/custom/flat")
        assert ref.scope == SecretScope.CUSTOM
        assert ref.name == "flat"

    def test_custom_nested(self):
        ref = parse_secret_path("ns/custom/a/b/c")
        assert ref.name == "a/b/c"

    def test_leading_slash_stripped(self):
        ref = parse_secret_path("/ns/global/k")
        assert ref.namespace == "ns"

    # --- invalid inputs ---
    def test_empty_string(self):
        with pytest.raises(CephSecretException, match="empty"):
            parse_secret_path("")

    def test_non_string(self):
        with pytest.raises(CephSecretException):
            parse_secret_path(None)  # type: ignore[arg-type]

    def test_too_few_segments(self):
        with pytest.raises(CephSecretException, match="Use"):
            parse_secret_path("ns/global")

    def test_double_slash(self):
        with pytest.raises(CephSecretException):
            parse_secret_path("ns//global/key")

    def test_trailing_slash(self):
        with pytest.raises(CephSecretException, match="empty segment"):
            parse_secret_path("ns/global/key/")

    def test_bad_scope(self):
        with pytest.raises(CephSecretException):
            parse_secret_path("ns/badscope/key")

    def test_global_extra_segment(self):
        with pytest.raises(CephSecretException, match="global scope"):
            parse_secret_path("ns/global/target/name")

    def test_service_missing_name(self):
        with pytest.raises(CephSecretException, match="service"):
            parse_secret_path("ns/service/target")

    def test_host_missing_name(self):
        with pytest.raises(CephSecretException, match="host"):
            parse_secret_path("ns/host/target")


# ============================================================
# Whitespace / canonicality
# ============================================================

class TestParseSecretPathWhitespace:
    @pytest.mark.parametrize("path", [
        " ns/global/key",
        "ns/global/key ",
        "\tns/global/key",
        "ns/global/key\n",
    ])
    def test_rejects_outer_whitespace(self, path):
        with pytest.raises(CephSecretException):
            parse_secret_path(path)


class TestParseSecretUriWhitespace:
    def test_rejects_leading_whitespace(self):
        with pytest.raises(CephSecretException):
            parse_secret_uri(" secret:/ns/global/key")

    def test_rejects_trailing_whitespace(self):
        with pytest.raises(CephSecretException):
            parse_secret_uri("secret:/ns/global/key ")
