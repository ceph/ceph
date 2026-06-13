# -*- coding: utf-8 -*-
"""Tests for ceph_secrets.secret_mgr (SecretMgr)."""
from __future__ import annotations

import pytest

from ceph_secrets_types import (
    SecretScope, SecretRef,
    CephSecretException, CephSecretNotFoundError,
    BadSecretURI,
)


# ============================================================
# make_ref
# ============================================================

class TestMakeRef:
    def test_basic(self, secret_mgr):
        ref = secret_mgr.make_ref("ns", SecretScope.GLOBAL, "", "key")
        assert isinstance(ref, SecretRef)
        assert ref.namespace == "ns"

    def test_scope_as_string(self, secret_mgr):
        ref = secret_mgr.make_ref("ns", "host", "node1", "ssh_key")
        assert ref.scope == SecretScope.HOST

    def test_bad_scope_raises(self, secret_mgr):
        with pytest.raises(CephSecretException):
            secret_mgr.make_ref("ns", "badscope", "", "key")

    def test_bad_namespace_raises(self, secret_mgr):
        with pytest.raises(CephSecretException):
            secret_mgr.make_ref("bad ns!", SecretScope.GLOBAL, "", "key")


# ============================================================
# get / get_value
# ============================================================

class TestGet:
    def test_get_existing(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "pw")
        rec = secret_mgr.get(ref)
        assert rec.data == "s3cr3t"

    def test_get_missing_raises(self, secret_mgr):
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "ghost")
        with pytest.raises(CephSecretNotFoundError):
            secret_mgr.get(ref)

    def test_get_value(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "pw")
        assert secret_mgr.get_value(ref) == "s3cr3t"

    def test_get_value_opaque_string(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "creds", '{"u": "admin", "p": "pw"}')
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "creds")
        val = secret_mgr.get_value(ref)
        assert isinstance(val, str)
        assert val == '{"u": "admin", "p": "pw"}'

    def test_get_value_missing_raises(self, secret_mgr):
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "ghost")
        with pytest.raises(CephSecretNotFoundError):
            secret_mgr.get_value(ref)


# ============================================================
# set
# ============================================================

class TestSet:
    def test_set_global(self, secret_mgr):
        rec = secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", "data-v1")
        assert rec.metadata.version == 1
        assert rec.data == "data-v1"

    def test_set_service(self, secret_mgr):
        rec = secret_mgr.set("ns", SecretScope.SERVICE, "prom", "auth", "user-a")
        assert rec.target == "prom"

    def test_set_custom(self, secret_mgr):
        rec = secret_mgr.set("ns", SecretScope.CUSTOM, "", "a/b/c", "tok")
        assert rec.name == "a/b/c"

    def test_set_non_str_raises(self, secret_mgr):
        with pytest.raises(CephSecretException, match="string"):
            secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", {"not": "a-string"})  # type: ignore[arg-type]

    def test_set_empty_string_raises(self, secret_mgr):
        with pytest.raises(CephSecretException, match="must not be empty"):
            secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", "")

    def test_set_preserves_whitespace_and_newlines(self, secret_mgr):
        payload = "  secret-value\n"
        rec = secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", payload)
        assert rec.data == payload
        assert secret_mgr.get_value(SecretRef("ns", SecretScope.GLOBAL, "", "k")) == payload

    def test_set_increments_version(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", "data-v1")
        rec = secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", "data-v2")
        assert rec.metadata.version == 2

    def test_set_scope_string(self, secret_mgr):
        rec = secret_mgr.set("ns", "global", "", "k", "data-x")
        assert rec.scope == SecretScope.GLOBAL


# ============================================================
# rm
# ============================================================

class TestRm:
    def test_rm_existing(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", "data-x")
        assert secret_mgr.rm("ns", SecretScope.GLOBAL, "", "k") is True

    def test_rm_nonexistent(self, secret_mgr):
        assert secret_mgr.rm("ns", SecretScope.GLOBAL, "", "ghost") is False


# ============================================================
# ls
# ============================================================

class TestLs:
    def test_ls_empty(self, secret_mgr):
        assert secret_mgr.ls(namespace="ns") == []

    def test_ls_returns_records(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "a", "data-a")
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "b", "data-b")
        recs = secret_mgr.ls(namespace="ns")
        assert len(recs) == 2

    def test_ls_scope_filter(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "g", "data-g")
        secret_mgr.set("ns", SecretScope.SERVICE, "prom", "auth", "data-auth")
        recs = secret_mgr.ls(namespace="ns", scope="service")
        assert len(recs) == 1
        assert recs[0].scope == SecretScope.SERVICE


# ============================================================
# scan_refs / scan_unresolved_refs
# ============================================================

class TestScanRefs:
    def test_scan_simple_string(self, secret_mgr):
        obj = "secret:/ns/global/pw"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        uris = {r.to_uri() for r in refs}
        assert "secret:/ns/global/pw" in uris

    def test_scan_in_dict(self, secret_mgr):
        obj = {"key": "secret:/ns/global/pw"}
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        assert len(refs) == 1

    def test_scan_in_list(self, secret_mgr):
        obj = ["secret:/ns/global/pw", "secret:/ns/host/node1/ssh"]
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        assert len(refs) == 2

    def test_scan_nested(self, secret_mgr):
        obj = {"a": {"b": "secret:/ns/global/pw"}}
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        assert len(refs) == 1

    def test_scan_no_refs(self, secret_mgr):
        obj = {"plain": "value"}
        assert secret_mgr.scan_refs(obj, namespace="ns") == set()

    def test_scan_bad_uri_yields_bad_secret_uri(self, secret_mgr):
        obj = "secret:/ns/badscope/key"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        bad = [r for r in refs if isinstance(r, BadSecretURI)]
        assert len(bad) == 1

    def test_scan_unresolved_all_exist(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "x")
        obj = "secret:/ns/global/pw"
        unresolved = secret_mgr.scan_unresolved_refs(obj, namespace="ns")
        assert len(unresolved) == 0

    def test_scan_unresolved_missing(self, secret_mgr):
        obj = "secret:/ns/global/ghost"
        unresolved = secret_mgr.scan_unresolved_refs(obj, namespace="ns")
        assert len(unresolved) == 1

    def test_scan_unresolved_bad_uri_is_unresolved(self, secret_mgr):
        obj = "secret:/ns/badscope/key"
        unresolved = secret_mgr.scan_unresolved_refs(obj, namespace="ns")
        assert len(unresolved) == 1

    def test_scan_unresolved_embedded_uri_is_unresolved(self, secret_mgr):
        # Even though the referenced secret exists, an embedded (non-whole-value)
        # URI is never resolvable, so it must be reported as unresolved so that
        # pre-deploy validation catches it.
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "x")
        obj = {"auth": "Bearer secret:/ns/global/pw"}
        unresolved = secret_mgr.scan_unresolved_refs(obj, namespace="ns")
        assert len(unresolved) == 1


# ============================================================
# resolve_object
# ============================================================

class TestResolveObject:
    def test_resolve_secret(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        result = secret_mgr.resolve_object("secret:/ns/global/pw")
        assert result == "s3cr3t"

    def test_resolve_opaque_json_string(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "creds", '{"u": "a", "p": "b"}')
        result = secret_mgr.resolve_object("secret:/ns/global/creds")
        assert isinstance(result, str)
        assert result == '{"u": "a", "p": "b"}'

    def test_resolve_in_dict(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        result = secret_mgr.resolve_object({"password": "secret:/ns/global/pw"})
        assert result["password"] == "s3cr3t"

    def test_resolve_in_list(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "a", "x")
        result = secret_mgr.resolve_object(["secret:/ns/global/a", "plain"])
        assert result[0] == "x"
        assert result[1] == "plain"

    def test_resolve_in_tuple(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "t", "y")
        result = secret_mgr.resolve_object(("secret:/ns/global/t",))
        assert result == ("y",)

    def test_resolve_non_secret_string_unchanged(self, secret_mgr):
        result = secret_mgr.resolve_object("just a normal string")
        assert result == "just a normal string"

    def test_resolve_non_string_unchanged(self, secret_mgr):
        assert secret_mgr.resolve_object(42) == 42
        assert secret_mgr.resolve_object(None) is None

    def test_resolve_missing_secret_raises(self, secret_mgr):
        with pytest.raises(CephSecretException):
            secret_mgr.resolve_object("secret:/ns/global/ghost")

    def test_resolve_invalid_uri_raises(self, secret_mgr):
        with pytest.raises(CephSecretException):
            secret_mgr.resolve_object("secret:/ns/badscope/key")

    def test_resolve_edge_whitespace_tolerated(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        assert secret_mgr.resolve_object("  secret:/ns/global/pw  ") == "s3cr3t"

    def test_resolve_newline_whitespace_tolerated(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        assert secret_mgr.resolve_object("\nsecret:/ns/global/pw\n") == "s3cr3t"

    def test_resolve_ref_with_suffix_raises(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        with pytest.raises(CephSecretException):
            secret_mgr.resolve_object("secret:/ns/global/pw suffix")

    def test_resolve_embedded_uri_raises(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        with pytest.raises(CephSecretException, match="embedded"):
            secret_mgr.resolve_object("Bearer secret:/ns/global/pw")

    def test_resolve_embedded_uri_in_dict_raises(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        with pytest.raises(CephSecretException, match="embedded"):
            secret_mgr.resolve_object({"auth": "Bearer secret:/ns/global/pw"})

    def test_resolve_plain_string_whitespace_preserved(self, secret_mgr):
        # non-secret strings pass through byte-for-byte, including whitespace
        assert secret_mgr.resolve_object("  plain value  ") == "  plain value  "


# ============================================================
# scan_refs — edge cases
# ============================================================

class TestScanRefsEdgeCases:
    def test_embedded_refs_rejected_as_bad(self, secret_mgr):
        # A string that embeds URIs inside other text is NOT a whole-value
        # reference; it is surfaced as a single BadSecretURI, not parsed into
        # multiple SecretRefs.
        obj = "use secret:/ns/global/a and secret:/ns/global/b"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        assert len(refs) == 1
        bad = [r for r in refs if isinstance(r, BadSecretURI)]
        assert len(bad) == 1
        assert bad[0].raw == obj

    def test_duplicate_refs_deduped(self, secret_mgr):
        obj = ["secret:/ns/global/pw", "secret:/ns/global/pw"]
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        uris = [r.to_uri() for r in refs]
        assert uris.count("secret:/ns/global/pw") == 1

    def test_ref_followed_by_punctuation(self, secret_mgr):
        # "secret:/ns/global/pw," is a whole-value string that starts with the
        # prefix but is not a valid URI (trailing comma in name) → BadSecretURI.
        obj = "secret:/ns/global/pw,"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        bad = [r for r in refs if isinstance(r, BadSecretURI)]
        assert len(bad) == 1

    def test_whole_value_ref_with_edge_whitespace(self, secret_mgr):
        # Surrounding whitespace around an otherwise-clean URI is tolerated.
        obj = "  secret:/ns/global/pw  "
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        uris = {r.to_uri() for r in refs if isinstance(r, SecretRef)}
        assert "secret:/ns/global/pw" in uris

    def test_embedded_ref_in_dict_value_is_bad(self, secret_mgr):
        obj = {"auth": "Bearer secret:/ns/global/pw"}
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        bad = [r for r in refs if isinstance(r, BadSecretURI)]
        assert len(bad) == 1

    def test_ref_inside_tuple(self, secret_mgr):
        obj = ("secret:/ns/global/pw",)
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        assert len(refs) == 1

    def test_cross_namespace_ref_is_scanned(self, secret_mgr):
        """scan_refs finds refs regardless of namespace — namespace arg does not filter."""
        obj = "secret:/other/global/pw"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        uris = {r.to_uri() for r in refs}
        assert "secret:/other/global/pw" in uris
