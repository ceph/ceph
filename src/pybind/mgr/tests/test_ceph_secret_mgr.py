# -*- coding: utf-8 -*-
"""
Unit tests for ceph_secrets/secret_mgr.py
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

import pytest

from ceph_secrets_types import (
    SecretScope,
    SecretRef,
    BadSecretURI,
    CephSecretException,
    SECRET_URI_SCHEME,
)
from ceph_secrets.secret_store import SECRET_STORE_PREFIX
from ceph_secrets.secret_mgr import SecretMgr

MGR_LOGGER = 'ceph_secrets.secret_mgr'

class MockMgr:
    def __init__(self):
        self._store: Dict[str, str] = {}

    def get_store(self, key: str) -> Optional[str]:
        return self._store.get(key)

    def set_store(self, key: str, value: Optional[str]) -> None:
        if value is None:
            self._store.pop(key, None)
        else:
            self._store[key] = value

    def get_store_prefix(self, prefix: str) -> Dict[str, str]:
        return {k: v for k, v in self._store.items() if k.startswith(prefix)}

    def raw_keys(self) -> list:
        return list(self._store.keys())

    def inject_raw(self, key: str, value: str) -> None:
        self._store[key] = value


def make_mgr():
    mgr = MockMgr()
    return mgr, SecretMgr(mgr)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def populated_mgr():
    """SecretMgr with a few pre-loaded secrets across scopes."""
    raw, mgr = make_mgr()
    mgr.set("reg-creds",  {"password": "s"},           namespace="cephadm")
    mgr.set("tls-cert",   {"cert": "x"},               namespace="cephadm", scope=SecretScope.SERVICE, target="ingress")
    mgr.set("ssh-key",    {"key": "y"},                namespace="cephadm", scope=SecretScope.HOST,    target="host01")
    mgr.set("other-cred", {"token": "t"},              namespace="other")
    mgr.set("multi",      {"cert": "c", "key": "k"},   namespace="cephadm")
    return raw, mgr


# Canonical URIs for populated_mgr secrets
URI_REG   = f"{SECRET_URI_SCHEME}cephadm/global/reg-creds"
URI_TLS   = f"{SECRET_URI_SCHEME}cephadm/service/ingress/tls-cert"
URI_SSH   = f"{SECRET_URI_SCHEME}cephadm/host/host01/ssh-key"
URI_MULTI = f"{SECRET_URI_SCHEME}cephadm/global/multi"


# ---------------------------------------------------------------------------
# make_ref
# ---------------------------------------------------------------------------

def test_make_ref_global():
    _, mgr = make_mgr()
    ref = mgr.make_ref("ns", SecretScope.GLOBAL, name="myname", key="mykey")
    assert ref.namespace == "ns"
    assert ref.scope == SecretScope.GLOBAL
    assert ref.name == "myname"
    assert ref.key == "mykey"
    assert ref.target == ""


def test_make_ref_non_global_with_target():
    _, mgr = make_mgr()
    ref = mgr.make_ref("ns", SecretScope.SERVICE, target="ingress", name="tls", key="cert")
    assert ref.target == "ingress"
    assert ref.scope == SecretScope.SERVICE


def test_make_ref_returns_secret_ref_instance():
    _, mgr = make_mgr()
    ref = mgr.make_ref("ns", SecretScope.GLOBAL, name="n", key="k")
    assert isinstance(ref, SecretRef)


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------

def test_get_existing_returns_record(populated_mgr):
    _, mgr = populated_mgr
    ref = mgr.make_ref("cephadm", SecretScope.GLOBAL, name="reg-creds")
    rec = mgr.get(ref)
    assert rec.data == {"password": "s"}
    assert rec.namespace == "cephadm"


def test_get_missing_raises(populated_mgr):
    _, mgr = populated_mgr
    ref = mgr.make_ref("cephadm", SecretScope.GLOBAL, name="nonexistent")
    with pytest.raises(CephSecretException, match="Secret not found"):
        mgr.get(ref)


def test_get_wrong_namespace_raises(populated_mgr):
    _, mgr = populated_mgr
    ref = mgr.make_ref("wrong-ns", SecretScope.GLOBAL, name="reg-creds")
    with pytest.raises(CephSecretException, match="Secret not found"):
        mgr.get(ref)


def test_get_non_global_scope(populated_mgr):
    _, mgr = populated_mgr
    ref = mgr.make_ref("cephadm", SecretScope.SERVICE, target="ingress", name="tls-cert")
    rec = mgr.get(ref)
    assert rec.data == {"cert": "x"}
    assert rec.target == "ingress"


# ---------------------------------------------------------------------------
# get_value
# ---------------------------------------------------------------------------

def test_get_value_single_key_returns_scalar(populated_mgr):
    """Single-key secrets return the value directly, not the dict."""
    _, mgr = populated_mgr
    ref = mgr.make_ref("cephadm", SecretScope.GLOBAL, name="reg-creds")
    val = mgr.get_value(ref)
    assert val == "s"


def test_get_value_multi_key_no_key_returns_dict(populated_mgr):
    """Multi-key secrets without ?key= return the whole dict."""
    _, mgr = populated_mgr
    ref = mgr.make_ref("cephadm", SecretScope.GLOBAL, name="multi")
    val = mgr.get_value(ref)
    assert val == {"cert": "c", "key": "k"}


def test_get_value_explicit_key_returns_entry(populated_mgr):
    """?key= selects one entry from a multi-key secret."""
    _, mgr = populated_mgr
    ref = mgr.make_ref("cephadm", SecretScope.GLOBAL, name="multi", key="cert")
    assert mgr.get_value(ref) == "c"
    ref2 = mgr.make_ref("cephadm", SecretScope.GLOBAL, name="multi", key="key")
    assert mgr.get_value(ref2) == "k"


def test_get_value_explicit_key_missing_raises(populated_mgr):
    """?key= for a key that doesn't exist in data raises."""
    _, mgr = populated_mgr
    ref = mgr.make_ref("cephadm", SecretScope.GLOBAL, name="multi", key="nonexistent")
    with pytest.raises(CephSecretException, match="not present"):
        mgr.get_value(ref)


def test_get_value_secret_not_found_raises(populated_mgr):
    """get_value raises with 'Secret not found' when the secret itself is missing."""
    _, mgr = populated_mgr
    ref = mgr.make_ref("cephadm", SecretScope.GLOBAL, name="missing")
    with pytest.raises(CephSecretException, match="Secret not found"):
        mgr.get_value(ref)


def test_get_value_empty_data_returns_empty_dict():
    """A secret with no data keys returns {}."""
    _, mgr = make_mgr()
    mgr.set("empty", {}, namespace="ns")
    ref = mgr.make_ref("ns", SecretScope.GLOBAL, name="empty")
    assert mgr.get_value(ref) == {}


# ---------------------------------------------------------------------------
# set
# ---------------------------------------------------------------------------

def test_set_global_default_scope():
    _, mgr = make_mgr()
    rec = mgr.set("n", {"k": "v"}, namespace="ns")
    assert rec.scope == SecretScope.GLOBAL
    assert rec.target == ""
    assert rec.version == 1


def test_set_service_scope_requires_target():
    _, mgr = make_mgr()
    with pytest.raises(CephSecretException, match="target is required"):
        mgr.set("n", {}, namespace="ns", scope=SecretScope.SERVICE)


def test_set_host_scope_requires_target():
    _, mgr = make_mgr()
    with pytest.raises(CephSecretException, match="target is required"):
        mgr.set("n", {}, namespace="ns", scope=SecretScope.HOST)


def test_set_global_scope_rejects_target():
    _, mgr = make_mgr()
    with pytest.raises(CephSecretException, match="target must be empty"):
        mgr.set("n", {}, namespace="ns", scope=SecretScope.GLOBAL, target="oops")


def test_set_none_scope_defaults_to_global():
    _, mgr = make_mgr()
    rec = mgr.set("n", {"k": "v"}, namespace="ns", scope=None)
    assert rec.scope == SecretScope.GLOBAL


def test_set_increments_version_on_update():
    _, mgr = make_mgr()
    mgr.set("n", {"k": "v1"}, namespace="ns")
    rec2 = mgr.set("n", {"k": "v2"}, namespace="ns")
    assert rec2.version == 2
    assert rec2.data == {"k": "v2"}


def test_set_preserves_created_timestamp():
    _, mgr = make_mgr()
    rec1 = mgr.set("n", {"k": "v1"}, namespace="ns")
    rec2 = mgr.set("n", {"k": "v2"}, namespace="ns")
    assert rec1.created == rec2.created


@pytest.mark.parametrize("scope, target", [
    (SecretScope.SERVICE, "ingress"),
    (SecretScope.HOST,    "host01"),
])
def test_set_non_global_scopes(scope, target):
    _, mgr = make_mgr()
    rec = mgr.set("n", {"k": "v"}, namespace="ns", scope=scope, target=target)
    assert rec.scope == scope
    assert rec.target == target


def test_set_non_editable_blocks_update():
    _, mgr = make_mgr()
    mgr.set("n", {"k": "v"}, namespace="ns", editable=False)
    with pytest.raises(CephSecretException, match="not editable"):
        mgr.set("n", {"k": "new"}, namespace="ns")


def test_set_custom_secret_type_and_flags():
    _, mgr = make_mgr()
    rec = mgr.set("n", {}, namespace="ns",
                  secret_type="kubernetes.io/tls", user_made=False, editable=False)
    assert rec.secret_type == "kubernetes.io/tls"
    assert rec.user_made is False
    assert rec.editable is False


# ---------------------------------------------------------------------------
# rm
# ---------------------------------------------------------------------------

def test_rm_existing_returns_true(populated_mgr):
    _, mgr = populated_mgr
    assert mgr.rm("cephadm", SecretScope.GLOBAL, "", "reg-creds") is True


def test_rm_missing_returns_false(populated_mgr):
    _, mgr = populated_mgr
    assert mgr.rm("cephadm", SecretScope.GLOBAL, "", "nonexistent") is False


def test_rm_makes_get_raise(populated_mgr):
    _, mgr = populated_mgr
    mgr.rm("cephadm", SecretScope.GLOBAL, "", "reg-creds")
    ref = mgr.make_ref("cephadm", SecretScope.GLOBAL, name="reg-creds")
    with pytest.raises(CephSecretException, match="Secret not found"):
        mgr.get(ref)


def test_rm_is_idempotent(populated_mgr):
    _, mgr = populated_mgr
    assert mgr.rm("cephadm", SecretScope.GLOBAL, "", "reg-creds") is True
    assert mgr.rm("cephadm", SecretScope.GLOBAL, "", "reg-creds") is False


def test_rm_non_global_scope(populated_mgr):
    _, mgr = populated_mgr
    assert mgr.rm("cephadm", SecretScope.SERVICE, "ingress", "tls-cert") is True
    ref = mgr.make_ref("cephadm", SecretScope.SERVICE, target="ingress", name="tls-cert")
    with pytest.raises(CephSecretException, match="Secret not found"):
        mgr.get(ref)


# ---------------------------------------------------------------------------
# ls
# ---------------------------------------------------------------------------

def test_ls_no_filters_returns_all(populated_mgr):
    _, mgr = populated_mgr
    good, bad = mgr.ls()
    assert len(good) == 5
    assert bad == []


def test_ls_filter_namespace(populated_mgr):
    _, mgr = populated_mgr
    good, bad = mgr.ls(namespace="cephadm")
    assert len(good) == 4
    assert all(r.namespace == "cephadm" for r in good)
    assert bad == []


def test_ls_filter_namespace_and_scope(populated_mgr):
    _, mgr = populated_mgr
    good, bad = mgr.ls(namespace="cephadm", scope=SecretScope.SERVICE)
    assert len(good) == 1
    assert good[0].name == "tls-cert"


def test_ls_filter_namespace_scope_target(populated_mgr):
    _, mgr = populated_mgr
    good, bad = mgr.ls(namespace="cephadm", scope=SecretScope.HOST, target="host01")
    assert len(good) == 1
    assert good[0].name == "ssh-key"


def test_ls_default_scope_is_none_returns_all_scopes():
    """ls() with no args must return GLOBAL, SERVICE, and HOST records."""
    _, mgr = make_mgr()
    mgr.set("g", {}, namespace="ns")
    mgr.set("s", {}, namespace="ns", scope=SecretScope.SERVICE, target="t")
    mgr.set("h", {}, namespace="ns", scope=SecretScope.HOST,    target="t")
    good, _ = mgr.ls()
    scopes = {r.scope for r in good}
    assert SecretScope.GLOBAL  in scopes
    assert SecretScope.SERVICE in scopes
    assert SecretScope.HOST    in scopes


def test_ls_unknown_namespace_returns_empty(populated_mgr):
    _, mgr = populated_mgr
    assert mgr.ls(namespace="nonexistent") == ([], [])


def test_ls_returns_bad_records_for_corrupted_entries(populated_mgr):
    raw, mgr = populated_mgr
    raw.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/global/broken", "not-json{{{")
    good, bad = mgr.ls(namespace="cephadm")
    assert len(good) == 4
    assert len(bad) == 1
    assert "broken" in bad[0].raw_key


def test_ls_result_sorted_by_ident(populated_mgr):
    _, mgr = populated_mgr
    good, _ = mgr.ls()
    idents = [r.ident() for r in good]
    assert idents == sorted(idents)


# ---------------------------------------------------------------------------
# scan_refs
# ---------------------------------------------------------------------------

def test_scan_refs_finds_uri_in_string(populated_mgr):
    _, mgr = populated_mgr
    refs = mgr.scan_refs(URI_REG, namespace="cephadm")
    assert len(refs) == 1
    ref = next(iter(refs))
    assert isinstance(ref, SecretRef)
    assert ref.name == "reg-creds"


def test_scan_refs_finds_uri_in_nested_dict(populated_mgr):
    _, mgr = populated_mgr
    obj = {"config": {"password": URI_REG, "cert": URI_TLS}}
    refs = mgr.scan_refs(obj, namespace="cephadm")
    names = {r.name for r in refs if isinstance(r, SecretRef)}
    assert "reg-creds" in names
    assert "tls-cert" in names


def test_scan_refs_finds_uri_in_list(populated_mgr):
    _, mgr = populated_mgr
    refs = mgr.scan_refs([URI_REG, URI_SSH], namespace="cephadm")
    names = {r.name for r in refs if isinstance(r, SecretRef)}
    assert {"reg-creds", "ssh-key"} == names


def test_scan_refs_finds_uri_in_tuple(populated_mgr):
    _, mgr = populated_mgr
    refs = mgr.scan_refs((URI_REG,), namespace="cephadm")
    assert len(refs) == 1


def test_scan_refs_embedded_uri_in_string(populated_mgr):
    _, mgr = populated_mgr
    refs = mgr.scan_refs(f"prefix {URI_REG} suffix", namespace="cephadm")
    assert any(isinstance(r, SecretRef) and r.name == "reg-creds" for r in refs)


def test_scan_refs_multiple_uris_in_one_string(populated_mgr):
    """Multiple URIs in one string are all detected."""
    _, mgr = populated_mgr
    refs = mgr.scan_refs(f"{URI_REG} {URI_SSH}", namespace="cephadm")
    names = {r.name for r in refs if isinstance(r, SecretRef)}
    assert names == {"reg-creds", "ssh-key"}


def test_scan_refs_no_uri_returns_empty():
    _, mgr = make_mgr()
    assert mgr.scan_refs({"k": "v", "n": 42}, namespace="ns") == set()


def test_scan_refs_invalid_uri_returns_bad_secret_uri(caplog):
    _, mgr = make_mgr()
    bad_uri = f"{SECRET_URI_SCHEME}baduri"
    with caplog.at_level(logging.WARNING, logger=MGR_LOGGER):
        refs = mgr.scan_refs(bad_uri, namespace="ns")
    assert len(refs) == 1
    ref = next(iter(refs))
    assert isinstance(ref, BadSecretURI)
    assert ref.raw == bad_uri
    assert ref.namespace == "ns"
    assert any("Failed to parse" in r.message for r in caplog.records)


def test_scan_refs_skips_non_string_values():
    """Non-string leaves (int, bool, None) must not cause errors."""
    _, mgr = make_mgr()
    refs = mgr.scan_refs({"a": 1, "b": True, "c": None}, namespace="ns")
    assert refs == set()


def test_scan_refs_does_not_scan_dict_keys(populated_mgr):
    """Document: scan_refs scans dict values, not dict keys."""
    _, mgr = populated_mgr
    obj = {URI_REG: "value"}  # URI is a key, not a value
    refs = mgr.scan_refs(obj, namespace="cephadm")
    assert refs == set()


def test_scan_refs_uri_with_key_param(populated_mgr):
    _, mgr = populated_mgr
    uri_with_key = f"{URI_MULTI}?key=cert"
    refs = mgr.scan_refs(uri_with_key, namespace="cephadm")
    assert len(refs) == 1
    ref = next(iter(refs))
    assert isinstance(ref, SecretRef)
    assert ref.name == "multi"
    assert ref.key == "cert"


# ---------------------------------------------------------------------------
# scan_unresolved_refs
# ---------------------------------------------------------------------------

def test_scan_unresolved_refs_all_present(populated_mgr):
    _, mgr = populated_mgr
    obj = {"a": URI_REG, "b": URI_TLS}
    assert mgr.scan_unresolved_refs(obj, namespace="cephadm") == set()


def test_scan_unresolved_refs_missing_secret(populated_mgr):
    _, mgr = populated_mgr
    missing = f"{SECRET_URI_SCHEME}cephadm/global/no-such-secret"
    unresolved = mgr.scan_unresolved_refs(missing, namespace="cephadm")
    assert len(unresolved) == 1
    ref = next(iter(unresolved))
    assert isinstance(ref, SecretRef)
    assert ref.name == "no-such-secret"


def test_scan_unresolved_refs_bad_uri_is_always_unresolved():
    """A BadSecretURI (parse failure) is always unresolved regardless of store contents."""
    _, mgr = make_mgr()
    bad = f"{SECRET_URI_SCHEME}bad"
    unresolved = mgr.scan_unresolved_refs(bad, namespace="ns")
    assert len(unresolved) == 1
    assert isinstance(next(iter(unresolved)), BadSecretURI)


def test_scan_unresolved_refs_does_not_validate_key():
    """
    scan_unresolved_refs only checks the secret record exists.
    A ref with a nonexistent key is NOT reported as unresolved.
    Documents the intentional limitation noted in the docstring.
    """
    _, mgr = make_mgr()
    mgr.set("multi", {"cert": "x", "key": "y"}, namespace="ns")
    uri = f"{SECRET_URI_SCHEME}ns/global/multi?key=nonexistent"
    assert mgr.scan_unresolved_refs(uri, namespace="ns") == set()


def test_scan_unresolved_refs_mixed(populated_mgr):
    _, mgr = populated_mgr
    missing = f"{SECRET_URI_SCHEME}cephadm/global/missing"
    obj = {"present": URI_REG, "absent": missing}
    unresolved = mgr.scan_unresolved_refs(obj, namespace="cephadm")
    assert len(unresolved) == 1
    ref = next(iter(unresolved))
    assert ref.name == "missing"


# ---------------------------------------------------------------------------
# resolve_object
# ---------------------------------------------------------------------------

def test_resolve_object_exact_uri_single_key(populated_mgr):
    """Exact URI for a single-key secret resolves to the scalar value."""
    _, mgr = populated_mgr
    assert mgr.resolve_object(URI_REG) == "s"


def test_resolve_object_exact_uri_multi_key_no_key_param(populated_mgr):
    """Exact URI for a multi-key secret without ?key= resolves to the full dict."""
    _, mgr = populated_mgr
    assert mgr.resolve_object(URI_MULTI) == {"cert": "c", "key": "k"}


def test_resolve_object_exact_uri_with_key_param(populated_mgr):
    """Exact URI with ?key= selects a single value."""
    _, mgr = populated_mgr
    assert mgr.resolve_object(f"{URI_MULTI}?key=cert") == "c"


def test_resolve_object_embedded_uri_substituted(populated_mgr):
    """URI embedded inside a larger string gets replaced inline."""
    _, mgr = populated_mgr
    result = mgr.resolve_object(f"password is {URI_REG} end")
    assert result == "password is s end"


def test_resolve_object_embedded_uri_multi_key_raises(populated_mgr):
    """
    Embedded URI (inside a string) resolving to a dict raises --
    cannot substitute a dict into a string context.
    """
    _, mgr = populated_mgr
    with pytest.raises(CephSecretException, match="non-string"):
        mgr.resolve_object(f"value={URI_MULTI}")


def test_resolve_object_nested_dict(populated_mgr):
    """URIs inside nested dict values are all resolved."""
    _, mgr = populated_mgr
    obj = {"password": URI_REG, "nested": {"cert": URI_TLS}}
    result = mgr.resolve_object(obj)
    assert result["password"] == "s"
    assert result["nested"]["cert"] == "x"


def test_resolve_object_list(populated_mgr):
    """URIs in a list are all resolved."""
    _, mgr = populated_mgr
    result = mgr.resolve_object([URI_REG, URI_REG])
    assert result == ["s", "s"]


def test_resolve_object_tuple(populated_mgr):
    """resolve_object preserves tuple type."""
    _, mgr = populated_mgr
    result = mgr.resolve_object((URI_REG,))
    assert isinstance(result, tuple)
    assert result == ("s",)


def test_resolve_object_non_string_passthrough(populated_mgr):
    """Non-string, non-container values pass through unchanged."""
    _, mgr = populated_mgr
    assert mgr.resolve_object(42) == 42
    assert mgr.resolve_object(None) is None
    assert mgr.resolve_object(True) is True


def test_resolve_object_plain_string_passthrough(populated_mgr):
    """A plain string with no URI is returned unchanged."""
    _, mgr = populated_mgr
    assert mgr.resolve_object("just a normal string") == "just a normal string"


def test_resolve_object_invalid_uri_raises(populated_mgr):
    """A string that starts with the scheme but is malformed raises CephSecretException."""
    _, mgr = populated_mgr
    with pytest.raises(CephSecretException, match="Invalid secret URI"):
        mgr.resolve_object(f"{SECRET_URI_SCHEME}bad")


def test_resolve_object_missing_secret_raises(populated_mgr):
    """Resolving a well-formed URI whose secret doesn't exist raises CephSecretException."""
    _, mgr = populated_mgr
    missing = f"{SECRET_URI_SCHEME}cephadm/global/no-such"
    with pytest.raises(CephSecretException, match="Secret not found"):
        mgr.resolve_object(missing)


def test_resolve_object_missing_key_raises(populated_mgr):
    """Resolving with ?key= for a nonexistent key raises CephSecretException."""
    _, mgr = populated_mgr
    with pytest.raises(CephSecretException, match="not present"):
        mgr.resolve_object(f"{URI_MULTI}?key=nonexistent")


def test_resolve_object_does_not_mutate_input(populated_mgr):
    """resolve_object must not modify the original dict."""
    _, mgr = populated_mgr
    original = {"password": URI_REG}
    mgr.resolve_object(original)
    assert original == {"password": URI_REG}


def test_resolve_object_multiple_embedded_uris(populated_mgr):
    """Multiple URIs embedded in one string are all substituted."""
    _, mgr = populated_mgr
    # Use space as separator -- colon is not excluded by the URI regex and would
    # be consumed as part of the first URI, merging both tokens into one.
    result = mgr.resolve_object(f"{URI_REG} {URI_REG}")
    assert result == "s s"


# ---------------------------------------------------------------------------
# Round-trip: set -> get -> ls -> rm via SecretMgr
# ---------------------------------------------------------------------------

def test_round_trip_global():
    _, mgr = make_mgr()
    mgr.set("cred", {"token": "abc"}, namespace="ns")

    ref = mgr.make_ref("ns", SecretScope.GLOBAL, name="cred")
    rec = mgr.get(ref)
    assert rec.data == {"token": "abc"}
    assert rec.version == 1

    good, bad = mgr.ls(namespace="ns")
    assert len(good) == 1
    assert bad == []

    assert mgr.rm("ns", SecretScope.GLOBAL, "", "cred") is True
    with pytest.raises(CephSecretException, match="Secret not found"):
        mgr.get(ref)
    assert mgr.ls(namespace="ns") == ([], [])


def test_round_trip_service():
    _, mgr = make_mgr()
    mgr.set("tls", {"cert": "x", "key": "y"},
            namespace="ns", scope=SecretScope.SERVICE, target="ingress")

    ref = mgr.make_ref("ns", SecretScope.SERVICE, target="ingress", name="tls")
    rec = mgr.get(ref)
    assert rec.scope == SecretScope.SERVICE
    assert rec.target == "ingress"
    assert rec.data == {"cert": "x", "key": "y"}

    good, bad = mgr.ls(namespace="ns", scope=SecretScope.SERVICE, target="ingress")
    assert len(good) == 1
    assert bad == []


def test_resolve_after_set_and_rm():
    """Full lifecycle: set a secret, resolve it via URI, rm it, confirm resolution fails."""
    _, mgr = make_mgr()
    mgr.set("pw", {"value": "hunter2"}, namespace="ns")
    uri = f"{SECRET_URI_SCHEME}ns/global/pw"

    assert mgr.resolve_object(uri) == "hunter2"

    mgr.rm("ns", SecretScope.GLOBAL, "", "pw")
    with pytest.raises(CephSecretException, match="Secret not found"):
        mgr.resolve_object(uri)
