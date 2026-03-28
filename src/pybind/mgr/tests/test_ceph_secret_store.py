# -*- coding: utf-8 -*-
"""
Unit tests for ceph_secrets/secret_store.py
"""
from __future__ import annotations

import json
import logging
from typing import Dict, Optional

import pytest

from ceph_secrets_types import SecretScope, CephSecretException, CephSecretDataError
from ceph_secrets.secret_store import (
    SecretRecord,
    BadSecretRecord,
    SecretStoreMon,
    SECRET_STORE_PREFIX,
    _parse_ts,
)

STORE_LOGGER = 'ceph_secrets.secret_store'


# ---------------------------------------------------------------------------
# MockMgr -- in-memory stand-in for the ceph mgr KV store
# ---------------------------------------------------------------------------

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
        """Bypass SecretStoreMon validation to simulate corrupted / migrated entries."""
        self._store[key] = value


def make_store():
    mgr = MockMgr()
    return mgr, SecretStoreMon(mgr)


def good_records(results):
    """Return the good records from an ls() tuple."""
    good, _ = results
    return good


def bad_records(results):
    """Return the bad records from an ls() tuple."""
    _, bad = results
    return bad


# ---------------------------------------------------------------------------
# _parse_ts
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value, expected", [
    (None,          ""),
    ("",            ""),
    ("2024-01-01T00:00:00Z",  "2024-01-01T00:00:00Z"),  # ISO passthrough
    (0,             "1970-01-01T00:00:00Z"),              # epoch int
    (0.0,           "1970-01-01T00:00:00Z"),              # epoch float
    (1704067200,    "2024-01-01T00:00:00Z"),              # known epoch -> known ISO
])
def test_parse_ts(value, expected):
    assert _parse_ts(value) == expected


def test_parse_ts_invalid_type_logs_and_returns_empty(caplog):
    with caplog.at_level(logging.WARNING, logger=STORE_LOGGER):
        result = _parse_ts([1, 2, 3])
    assert result == ""
    assert any("Invalid timestamp type" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# SecretRecord.ident
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("kwargs, expected", [
    (
        dict(namespace="ns", scope=SecretScope.GLOBAL,  target="",       name="n",   version=1, secret_type="Opaque", data={}),
        ("ns", "global", "", "n"),
    ),
    (
        dict(namespace="ns", scope=SecretScope.SERVICE, target="ingress", name="tls", version=1, secret_type="Opaque", data={}),
        ("ns", "service", "ingress", "tls"),
    ),
    (
        dict(namespace="ns", scope=SecretScope.HOST,    target="host01",  name="ssh", version=1, secret_type="Opaque", data={}),
        ("ns", "host", "host01", "ssh"),
    ),
])
def test_secret_record_ident(kwargs, expected):
    assert SecretRecord(**kwargs).ident() == expected


# ---------------------------------------------------------------------------
# SecretRecord.to_json
# ---------------------------------------------------------------------------

def _make_rec(**overrides):
    defaults = dict(
        namespace="ns", scope=SecretScope.GLOBAL, target="", name="n",
        version=2, secret_type="Opaque", data={"k": "v"},
        created="2024-01-01T00:00:00Z", updated="2024-06-01T00:00:00Z",
    )
    defaults.update(overrides)
    return SecretRecord(**defaults)


def test_to_json_include_data_true():
    d = _make_rec().to_json(include_data=True)
    assert d["version"] == 2
    assert d["type"] == "Opaque"
    assert d["data"] == {"k": "v"}
    assert "keys" not in d


def test_to_json_include_data_false_exposes_sorted_keys():
    rec = _make_rec(data={"cert": "x", "key": "y", "a": "z"})
    d = rec.to_json(include_data=False)
    assert "data" not in d
    assert d["keys"] == ["a", "cert", "key"]  # sorted


def test_to_json_include_internal_true():
    rec = _make_rec(user_made=False, editable=False)
    d = rec.to_json(include_internal=True)
    assert d["user_made"] is False
    assert d["editable"] is False


def test_to_json_include_internal_false_by_default():
    d = _make_rec().to_json()
    assert "user_made" not in d
    assert "editable" not in d


def test_to_json_contains_timestamps():
    d = _make_rec().to_json()
    assert d["created"] == "2024-01-01T00:00:00Z"
    assert d["updated"] == "2024-06-01T00:00:00Z"


# ---------------------------------------------------------------------------
# SecretRecord.from_json
# ---------------------------------------------------------------------------

def test_from_json_full_payload():
    payload = {
        "version": 3,
        "type": "kubernetes.io/tls",
        "user_made": False,
        "editable": False,
        "created": "2024-01-01T00:00:00Z",
        "updated": "2024-06-01T00:00:00Z",
        "data": {"cert": "abc"},
    }
    rec = SecretRecord.from_json("ns", SecretScope.SERVICE, "ingress", "tls", payload)
    assert rec.version == 3
    assert rec.secret_type == "kubernetes.io/tls"
    assert rec.user_made is False
    assert rec.editable is False
    assert rec.data == {"cert": "abc"}
    assert rec.created == "2024-01-01T00:00:00Z"


def test_from_json_missing_fields_use_defaults():
    rec = SecretRecord.from_json("ns", SecretScope.GLOBAL, "", "n", {})
    assert rec.version == 1
    assert rec.secret_type == "Opaque"
    assert rec.user_made is True
    assert rec.editable is True
    assert rec.data == {}
    assert rec.created == ""
    assert rec.updated == ""


def test_from_json_legacy_epoch_timestamps():
    payload = {"created": 1704067200, "updated": 1704067200, "data": {}}
    rec = SecretRecord.from_json("ns", SecretScope.GLOBAL, "", "n", payload)
    assert rec.created == "2024-01-01T00:00:00Z"
    assert rec.updated == "2024-01-01T00:00:00Z"


def test_from_json_non_dict_data_raises():
    with pytest.raises(CephSecretDataError, match="must be a JSON object"):
        SecretRecord.from_json("ns", SecretScope.GLOBAL, "", "n", {"data": ["not", "a", "dict"]})


@pytest.mark.parametrize("bad_payload", [
    {"data": "a string"},
    {"data": 42},
    {"data": None},
    {"data": True},
])
def test_from_json_various_non_dict_data_types_raise(bad_payload):
    with pytest.raises((CephSecretDataError, TypeError)):
        SecretRecord.from_json("ns", SecretScope.GLOBAL, "", "n", bad_payload)


# ---------------------------------------------------------------------------
# SecretStoreMon._kv_key
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ns, scope, target, name, expected_suffix", [
    ("cephadm", SecretScope.GLOBAL,  "",        "reg-creds", "cephadm/global/reg-creds"),
    ("cephadm", SecretScope.SERVICE, "ingress",  "tls-cert",  "cephadm/service/ingress/tls-cert"),
    ("cephadm", SecretScope.HOST,    "host01",   "ssh-key",   "cephadm/host/host01/ssh-key"),
])
def test_kv_key_format(ns, scope, target, name, expected_suffix):
    _, store = make_store()
    assert store._kv_key(ns, scope, target, name) == f"{SECRET_STORE_PREFIX}{expected_suffix}"


@pytest.mark.parametrize("ns, scope, target, name, error_fragment", [
    ("ns/bad",  SecretScope.GLOBAL,  "",     "n",   "namespace"),
    ("ns",      SecretScope.GLOBAL,  "",     "a/b", "name"),
    ("ns",      SecretScope.SERVICE, "t/x",  "n",   "target"),
    ("ns",      SecretScope.HOST,    "h/h",  "n",   "target"),
])
def test_kv_key_rejects_slash_in_components(ns, scope, target, name, error_fragment):
    _, store = make_store()
    with pytest.raises(CephSecretDataError, match=error_fragment):
        store._kv_key(ns, scope, target, name)


def test_kv_key_global_produces_3_part_suffix():
    _, store = make_store()
    suffix = store._kv_key("ns", SecretScope.GLOBAL, "", "n")[len(SECRET_STORE_PREFIX):]
    assert suffix.count('/') == 2  # ns/global/name


def test_kv_key_non_global_produces_4_part_suffix():
    _, store = make_store()
    suffix = store._kv_key("ns", SecretScope.SERVICE, "tgt", "n")[len(SECRET_STORE_PREFIX):]
    assert suffix.count('/') == 3  # ns/service/tgt/name


# ---------------------------------------------------------------------------
# SecretStoreMon.get
# ---------------------------------------------------------------------------

def test_get_missing_returns_none():
    _, store = make_store()
    assert store.get("ns", SecretScope.GLOBAL, "", "missing") is None


def test_get_existing_returns_record():
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"})
    rec = store.get("ns", SecretScope.GLOBAL, "", "n")
    assert rec is not None
    assert rec.data == {"k": "v"}


def test_get_non_global_scope():
    _, store = make_store()
    store.set("ns", SecretScope.SERVICE, "ingress", "tls", {"cert": "abc"})
    rec = store.get("ns", SecretScope.SERVICE, "ingress", "tls")
    assert rec is not None
    assert rec.target == "ingress"
    assert rec.data == {"cert": "abc"}


def test_get_wrong_namespace_returns_none():
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"})
    assert store.get("other", SecretScope.GLOBAL, "", "n") is None


# ---------------------------------------------------------------------------
# SecretStoreMon.set
# ---------------------------------------------------------------------------

def test_set_new_record_starts_at_version_1():
    _, store = make_store()
    rec = store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"})
    assert rec.version == 1
    assert rec.data == {"k": "v"}
    assert rec.secret_type == "Opaque"
    assert rec.user_made is True
    assert rec.editable is True
    assert rec.created != ""
    assert rec.updated != ""


def test_set_update_increments_version():
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v1"})
    rec2 = store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v2"})
    assert rec2.version == 2
    assert rec2.data == {"k": "v2"}


def test_set_update_preserves_created_timestamp():
    _, store = make_store()
    rec1 = store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v1"})
    rec2 = store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v2"})
    assert rec1.created == rec2.created


def test_set_custom_type_and_flags():
    _, store = make_store()
    rec = store.set("ns", SecretScope.GLOBAL, "", "n", {},
                    secret_type="kubernetes.io/tls", user_made=False, editable=False)
    assert rec.secret_type == "kubernetes.io/tls"
    assert rec.user_made is False
    assert rec.editable is False


def test_set_non_editable_secret_raises():
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"}, editable=False)
    with pytest.raises(CephSecretException, match="not editable"):
        store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "new"})


def test_set_persists_with_internal_fields():
    """set() calls to_json(include_internal=True) so user_made/editable are stored."""
    mgr, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"}, user_made=False, editable=True)
    raw = mgr.get_store(f"{SECRET_STORE_PREFIX}ns/global/n")
    assert raw is not None
    payload = json.loads(raw)
    assert payload["user_made"] is False
    assert payload["editable"] is True
    assert payload["data"] == {"k": "v"}


def test_set_service_scope_stores_under_correct_key():
    mgr, store = make_store()
    store.set("ns", SecretScope.SERVICE, "ingress", "tls", {"cert": "x"})
    assert f"{SECRET_STORE_PREFIX}ns/service/ingress/tls" in mgr.raw_keys()


# ---------------------------------------------------------------------------
# SecretStoreMon.rm
# ---------------------------------------------------------------------------

def test_rm_existing_returns_true():
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"})
    assert store.rm("ns", SecretScope.GLOBAL, "", "n") is True


def test_rm_missing_returns_false():
    _, store = make_store()
    assert store.rm("ns", SecretScope.GLOBAL, "", "missing") is False


def test_rm_makes_get_return_none():
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"})
    store.rm("ns", SecretScope.GLOBAL, "", "n")
    assert store.get("ns", SecretScope.GLOBAL, "", "n") is None


def test_rm_is_idempotent():
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"})
    assert store.rm("ns", SecretScope.GLOBAL, "", "n") is True
    assert store.rm("ns", SecretScope.GLOBAL, "", "n") is False


def test_rm_does_not_affect_sibling_keys():
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "a", {"k": "1"})
    store.set("ns", SecretScope.GLOBAL, "", "b", {"k": "2"})
    store.rm("ns", SecretScope.GLOBAL, "", "a")
    assert store.get("ns", SecretScope.GLOBAL, "", "b") is not None


# ---------------------------------------------------------------------------
# SecretStoreMon.ls
# ---------------------------------------------------------------------------

@pytest.fixture
def populated_store():
    mgr, store = make_store()
    store.set("cephadm", SecretScope.GLOBAL,  "",        "reg-creds",  {"password": "s"})
    store.set("cephadm", SecretScope.SERVICE, "ingress", "tls-cert",   {"cert": "x"})
    store.set("cephadm", SecretScope.HOST,    "host01",  "ssh-key",    {"key": "y"})
    store.set("other",   SecretScope.GLOBAL,  "",        "other-cred", {"token": "t"})
    return mgr, store


def test_ls_no_filters_returns_all(populated_store):
    _, store = populated_store
    recs = store.ls()
    assert len(good_records(recs)) == 4
    assert len(bad_records(recs)) == 0


def test_ls_result_is_sorted_by_ident(populated_store):
    _, store = populated_store
    recs = good_records(store.ls())
    idents = [r.ident() for r in recs]
    assert idents == sorted(idents)


def test_ls_filter_namespace(populated_store):
    _, store = populated_store
    recs = good_records(store.ls(namespace="cephadm"))
    assert len(recs) == 3
    assert all(r.namespace == "cephadm" for r in recs)


def test_ls_filter_namespace_and_scope(populated_store):
    _, store = populated_store
    recs = good_records(store.ls(namespace="cephadm", scope=SecretScope.SERVICE))
    assert len(recs) == 1
    assert recs[0].name == "tls-cert"


def test_ls_filter_namespace_scope_and_target(populated_store):
    _, store = populated_store
    recs = good_records(store.ls(namespace="cephadm", scope=SecretScope.HOST, target="host01"))
    assert len(recs) == 1
    assert recs[0].name == "ssh-key"


def test_ls_unknown_namespace_returns_empty(populated_store):
    _, store = populated_store
    assert store.ls(namespace="nonexistent") == ([], [])


def test_ls_empty_store_returns_empty():
    _, store = make_store()
    assert store.ls() == ([], [])


def test_ls_scope_without_namespace_still_filters():
    """scope filter applied post-hoc when no namespace is given."""
    _, store = make_store()
    store.set("ns1", SecretScope.GLOBAL,  "",  "a", {})
    store.set("ns2", SecretScope.SERVICE, "t", "b", {})
    recs = good_records(store.ls(scope=SecretScope.GLOBAL))
    assert len(recs) == 1
    assert all(r.scope == SecretScope.GLOBAL for r in recs)


def test_ls_global_scope_target_filter_returns_empty():
    """
    Global records have tgt=''. ls(scope=GLOBAL, target='x') returns nothing
    because the post-hoc target filter eliminates all global records.
    """
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {})
    assert store.ls(namespace="ns", scope=SecretScope.GLOBAL, target="x") == ([], [])


# ---------------------------------------------------------------------------
# ls -- resilience to corrupted KV entries
# ---------------------------------------------------------------------------

def test_ls_invalid_json_returns_bad_record(populated_store):
    """Corrupted (non-JSON) entry is returned as BadSecretRecord, good entries unaffected."""
    mgr, store = populated_store
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/global/broken", "not-json{{{")
    recs = store.ls(namespace="cephadm")
    assert len(good_records(recs)) == 3
    bads = bad_records(recs)
    assert len(bads) == 1
    assert "broken" in bads[0].raw_key
    assert bads[0].namespace == "cephadm"
    assert bads[0].error != ""


def test_ls_non_dict_payload_returns_bad_record(populated_store):
    """A JSON array payload is not a valid secret and must be returned as BadSecretRecord."""
    mgr, store = populated_store
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/global/listval", json.dumps([1, 2, 3]))
    recs = store.ls(namespace="cephadm")
    assert len(good_records(recs)) == 3
    bads = bad_records(recs)
    assert len(bads) == 1
    assert "listval" in bads[0].raw_key
    assert "not a JSON object" in bads[0].error


def test_ls_bad_scope_returns_bad_record(populated_store):
    """An entry with an unrecognised scope string is returned as BadSecretRecord."""
    mgr, store = populated_store
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/badscope/entry", json.dumps({"data": {}}))
    recs = store.ls(namespace="cephadm")
    assert len(good_records(recs)) == 3
    bads = bad_records(recs)
    assert len(bads) == 1
    assert bads[0].namespace == "cephadm"
    assert "badscope" in bads[0].error or "scope" in bads[0].error


def test_ls_2_part_suffix_returns_bad_record(populated_store):
    """A 2-part suffix (missing name segment) is structurally invalid -> BadSecretRecord."""
    mgr, store = populated_store
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/global", json.dumps({"data": {}}))
    recs = store.ls(namespace="cephadm")
    assert len(good_records(recs)) == 3
    assert len(bad_records(recs)) == 1


def test_ls_5_part_suffix_returns_bad_record(populated_store):
    """A 5-part suffix cannot be written via the API but could appear from migration -> BadSecretRecord."""
    mgr, store = populated_store
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/service/a/b/c", json.dumps({"data": {}}))
    recs = store.ls(namespace="cephadm")
    assert len(good_records(recs)) == 3
    assert len(bad_records(recs)) == 1
    assert "unexpected key structure" in bad_records(recs)[0].error


def test_ls_3part_non_global_scope_returns_bad_record():
    """A 3-part key with non-global scope is structurally invalid -> BadSecretRecord."""
    mgr, store = make_store()
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}ns/service/orphan", json.dumps({"data": {}}))
    recs = store.ls()
    assert good_records(recs) == []
    bads = bad_records(recs)
    assert len(bads) == 1
    assert bads[0].namespace == "ns"
    assert "not global" in bads[0].error


def test_ls_4part_global_scope_returns_bad_record():
    """A 4-part key with global scope is structurally invalid -> BadSecretRecord."""
    mgr, store = make_store()
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}ns/global/extra/name", json.dumps({"data": {}}))
    recs = store.ls()
    assert good_records(recs) == []
    bads = bad_records(recs)
    assert len(bads) == 1
    assert bads[0].namespace == "ns"
    assert "global" in bads[0].error


# ---------------------------------------------------------------------------
# Round-trip: set -> get -> ls -> rm
# ---------------------------------------------------------------------------

def test_round_trip_global():
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "cred", {"token": "abc"})

    rec = store.get("ns", SecretScope.GLOBAL, "", "cred")
    assert rec is not None
    assert rec.data == {"token": "abc"}
    assert rec.version == 1

    good, bad = store.ls(namespace="ns")
    assert len(good) == 1
    assert bad == []
    assert good[0].ident() == rec.ident()

    assert store.rm("ns", SecretScope.GLOBAL, "", "cred") is True
    assert store.get("ns", SecretScope.GLOBAL, "", "cred") is None
    assert store.ls(namespace="ns") == ([], [])


def test_round_trip_service():
    _, store = make_store()
    store.set("ns", SecretScope.SERVICE, "ingress", "tls", {"cert": "x", "key": "y"})

    rec = store.get("ns", SecretScope.SERVICE, "ingress", "tls")
    assert rec.scope == SecretScope.SERVICE
    assert rec.target == "ingress"
    assert rec.data == {"cert": "x", "key": "y"}

    good, bad = store.ls(namespace="ns", scope=SecretScope.SERVICE, target="ingress")
    assert len(good) == 1
    assert bad == []
    assert good[0].ident() == rec.ident()


def test_multiple_set_calls_only_keep_latest():
    """set() overwrites in place; only the latest version lives in the store."""
    mgr, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"v": "1"})
    store.set("ns", SecretScope.GLOBAL, "", "n", {"v": "2"})
    store.set("ns", SecretScope.GLOBAL, "", "n", {"v": "3"})
    assert len(mgr.raw_keys()) == 1
    rec = store.get("ns", SecretScope.GLOBAL, "", "n")
    assert rec.version == 3
    assert rec.data == {"v": "3"}


def test_from_json_to_json_round_trip():
    """to_json(include_internal=True) -> from_json must be lossless."""
    original = _make_rec(version=5, user_made=False, editable=False,
                         data={"cert": "abc", "key": "xyz"})
    payload = original.to_json(include_data=True, include_internal=True)
    restored = SecretRecord.from_json(
        original.namespace, original.scope, original.target, original.name, payload
    )
    assert restored.version == original.version
    assert restored.secret_type == original.secret_type
    assert restored.data == original.data
    assert restored.user_made == original.user_made
    assert restored.editable == original.editable
    assert restored.created == original.created
    assert restored.updated == original.updated


# ---------------------------------------------------------------------------
# Additional tests to cover remaining paths
# ---------------------------------------------------------------------------

def test_kv_key_global_with_non_empty_target_raises():
    """_kv_key must reject a non-empty target when scope is GLOBAL."""
    _, store = make_store()
    with pytest.raises(CephSecretDataError, match="target must be empty"):
        store._kv_key("ns", SecretScope.GLOBAL, "should-be-empty", "n")


def test_get_non_dict_json_payload_raises():
    """get() must raise CephSecretDataError when the stored value is a JSON array."""
    mgr, store = make_store()
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}ns/global/n", json.dumps([1, 2, 3]))
    with pytest.raises(CephSecretDataError, match="expected a JSON object"):
        store.get("ns", SecretScope.GLOBAL, "", "n")


def test_get_invalid_json_raises():
    """get() must raise CephSecretDataError when the stored value is not valid JSON."""
    mgr, store = make_store()
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}ns/global/n", "not-json{{{")
    with pytest.raises(CephSecretDataError):
        store.get("ns", SecretScope.GLOBAL, "", "n")


def test_set_corrupted_existing_entry_raises():
    """set() must raise CephSecretException (not CephSecretDataError) when the existing entry is corrupted."""
    mgr, store = make_store()
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}ns/global/n", "not-json{{{")
    with pytest.raises(CephSecretException, match="corrupted"):
        store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"})


def test_ls_empty_segment_key_returns_bad_record():
    """A key with an empty path segment (e.g. double slash) must be returned as BadSecretRecord."""
    mgr, store = make_store()
    # inject a key with a double slash -- results in '' in parts after split
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}ns//global/n", json.dumps({"data": {}}))
    good, bad = store.ls()
    assert good == []
    assert len(bad) == 1
    assert bad[0].namespace == "ns"
    assert "empty path component" in bad[0].error


# ---------------------------------------------------------------------------
# Additional tests suggested by expert review
# ---------------------------------------------------------------------------

def test_kv_key_empty_namespace_accepted_but_produces_malformed_key():
    """
    _kv_key does not check for empty strings -- '/' not in '' passes the slash guard.
    This documents a known gap: empty namespace silently produces a malformed key.
    If _kv_key is hardened to reject empty strings, this test should be updated
    to expect CephSecretDataError instead.
    """
    _, store = make_store()
    key = store._kv_key("", SecretScope.GLOBAL, "", "n")
    # Malformed: double slash in the key
    assert "//" in key


def test_get_editable_flag_round_trips():
    """get() must return exactly what set() stored, including editable=False."""
    _, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v"}, editable=False)
    rec = store.get("ns", SecretScope.GLOBAL, "", "n")
    assert rec is not None
    assert rec.editable is False
    assert rec.user_made is True


def test_set_created_empty_in_legacy_record_replaced_with_now():
    """
    set() preserves existing.created via `existing.created or now`.
    If a legacy entry has created='', the falsy check means it gets replaced
    with the current timestamp rather than kept as ''.
    Documents current behaviour: empty created is treated as missing.
    """
    mgr, store = make_store()
    mgr.inject_raw(
        f"{SECRET_STORE_PREFIX}ns/global/n",
        json.dumps({"version": 1, "data": {}, "created": "", "updated": ""})
    )
    rec = store.set("ns", SecretScope.GLOBAL, "", "n", {"k": "v2"})
    assert rec.version == 2
    # created is '' in the stored record, so 'or now' kicks in -> non-empty timestamp
    assert rec.created != ""


def test_rm_non_global_scope():
    """rm() works correctly for service and host scope, not just global."""
    _, store = make_store()
    store.set("ns", SecretScope.SERVICE, "ingress", "tls", {"cert": "x"})
    store.set("ns", SecretScope.HOST, "host01", "ssh", {"key": "y"})

    assert store.rm("ns", SecretScope.SERVICE, "ingress", "tls") is True
    assert store.get("ns", SecretScope.SERVICE, "ingress", "tls") is None
    # host entry unaffected
    assert store.get("ns", SecretScope.HOST, "host01", "ssh") is not None

    assert store.rm("ns", SecretScope.HOST, "host01", "ssh") is True
    assert store.rm("ns", SecretScope.HOST, "host01", "ssh") is False


def test_ls_target_filter_without_scope():
    """
    ls(target='x') without a scope filter still applies target post-hoc.
    The target is not added to the KV prefix (scope is required for that),
    but matching entries are returned correctly.
    """
    _, store = make_store()
    store.set("ns", SecretScope.SERVICE, "ingress", "tls", {"cert": "x"})
    store.set("ns", SecretScope.HOST, "host01", "ssh", {"key": "y"})
    store.set("ns", SecretScope.GLOBAL, "", "cred", {"k": "v"})

    good, bad = store.ls(target="ingress")
    assert len(good) == 1
    assert good[0].name == "tls"
    assert bad == []


def test_ls_bad_record_with_unparseable_scope_leaks_through_scope_filter():
    """
    A bad record whose scope string cannot be parsed is caught before the caller
    scope filter runs. Without a namespace filter this bad record is visible even
    when the caller only asked for a specific scope.
    Documents current behaviour: scope filter is not applied to unparseable-scope entries.
    """
    mgr, store = make_store()
    store.set("ns", SecretScope.GLOBAL, "", "good", {})
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}ns/badscope/entry", json.dumps({"data": {}}))

    # Without namespace filter: bad record appears even with scope=SERVICE
    good, bad = store.ls(scope=SecretScope.SERVICE)
    assert good == []
    assert len(bad) == 1
    assert "badscope" in bad[0].error or "scope" in bad[0].error

    # With namespace filter: caller filters are applied to ns before bad_records.append
    good2, bad2 = store.ls(namespace="ns", scope=SecretScope.SERVICE)
    assert good2 == []
    assert bad2 == []


def test_ls_multiple_bad_records_sorted_by_namespace_then_key():
    """Multiple bad records are sorted by (namespace, raw_key)."""
    mgr, store = make_store()
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}zzz/badscope/b", json.dumps({"data": {}}))
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}aaa/badscope/a", json.dumps({"data": {}}))
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}aaa/badscope/c", json.dumps({"data": {}}))

    good, bad = store.ls()
    assert good == []
    assert len(bad) == 3
    assert bad[0].namespace == "aaa"
    assert bad[1].namespace == "aaa"
    assert bad[2].namespace == "zzz"
    # within same namespace, sorted by raw_key
    assert bad[0].raw_key < bad[1].raw_key


def test_from_json_invalid_version_string_raises():
    """
    from_json uses int(payload.get('version', 1)), so non-integer version strings raise.
    This documents that the exception is ValueError (not CephSecretDataError),
    which is inconsistent with other validation in from_json.
    """
    with pytest.raises(ValueError):
        SecretRecord.from_json("ns", SecretScope.GLOBAL, "", "n", {"version": "1.5", "data": {}})

    with pytest.raises(ValueError):
        SecretRecord.from_json("ns", SecretScope.GLOBAL, "", "n", {"version": "abc", "data": {}})


# ---------------------------------------------------------------------------
# ls -- filtering of bad records (must respect namespace/scope/target filters)
# ---------------------------------------------------------------------------

def test_ls_bad_records_are_filtered_by_namespace(populated_store):
    """
    Bad records should only be returned if they match the requested namespace,
    based on key-derived components.
    """
    mgr, store = populated_store

    # Bad record in OTHER namespace
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}other/global/broken", "not-json{{{")

    # Bad record in CEPHADM namespace
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/global/broken2", "not-json{{{")

    good, bad = store.ls(namespace="cephadm")
    # good: cephadm has 3 valid entries in populated_store
    assert len(good) == 3
    assert len(bad) == 1
    assert bad[0].namespace == "cephadm"
    assert "broken2" in bad[0].raw_key


def test_ls_bad_records_are_filtered_by_scope(populated_store):
    """
    When scope filter is provided, bad records that can be parsed enough to
    determine scope should be filtered out if scope doesn't match.
    """
    mgr, store = populated_store

    # Valid key structure, invalid JSON, SERVICE scope
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/service/ingress/broken", "not-json{{{")
    # Valid key structure, invalid JSON, GLOBAL scope
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/global/broken2", "not-json{{{")

    good, bad = store.ls(namespace="cephadm", scope=SecretScope.SERVICE)
    # Only 1 good SERVICE record in populated_store
    assert len(good) == 1
    assert good[0].scope == SecretScope.SERVICE

    # Only SERVICE bad record should be returned
    assert len(bad) == 1
    assert bad[0].namespace == "cephadm"
    assert "service/ingress/broken" in bad[0].raw_key


def test_ls_bad_records_are_filtered_by_target(populated_store):
    """
    When target filter is provided for non-global scopes, bad records should be
    filtered by the derived target component.
    """
    mgr, store = populated_store

    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/host/host01/broken", "not-json{{{")
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}cephadm/host/host02/broken2", "not-json{{{")

    good, bad = store.ls(namespace="cephadm", scope=SecretScope.HOST, target="host01")
    assert len(good) == 1
    assert good[0].target == "host01"

    assert len(bad) == 1
    assert bad[0].namespace == "cephadm"
    assert "host/host01/broken" in bad[0].raw_key


def test_ls_bad_records_with_unparseable_key_structure_do_not_leak_into_filtered_namespace():
    """
    If the key structure is so broken that namespace extraction is unreliable,
    ensure it doesn't leak into filtered results. We simulate a bad key that
    doesn't include a namespace segment after the prefix.
    """
    mgr, store = make_store()

    # suffix = "" -> parts = [""] -> triggers empty-path-component bad record
    mgr.inject_raw(f"{SECRET_STORE_PREFIX}", json.dumps({"data": {}}))

    # Asking for a namespace should not return this, because namespace isn't
    # meaningfully extractable/matching.
    good, bad = store.ls(namespace="cephadm")
    assert good == []
    assert bad == []


# ---------------------------------------------------------------------------
# ls -- cover additional prefix-building / post-hoc filtering paths
# ---------------------------------------------------------------------------

def test_ls_namespace_and_global_scope_prefix_path(populated_store):
    """
    Exercise prefix narrowing for namespace + GLOBAL scope explicitly.
    """
    _, store = populated_store
    good, bad = store.ls(namespace="cephadm", scope=SecretScope.GLOBAL)

    assert len(good) == 1
    assert bad == []
    assert good[0].scope == SecretScope.GLOBAL
    assert good[0].target == ""


def test_ls_namespace_and_target_without_scope_is_posthoc_filtered():
    """
    If target is provided without scope, prefix cannot include target.
    Ensure post-hoc filtering still works correctly.
    """
    mgr, store = make_store()
    store.set("ns", SecretScope.HOST, "h1", "a", {})
    store.set("ns", SecretScope.HOST, "h2", "b", {})
    store.set("ns", SecretScope.SERVICE, "svc1", "c", {})

    # target='h1' should only match host/h1/a
    good, bad = store.ls(namespace="ns", target="h1")
    assert bad == []
    assert len(good) == 1
    assert good[0].scope == SecretScope.HOST
    assert good[0].target == "h1"
    assert good[0].name == "a"
