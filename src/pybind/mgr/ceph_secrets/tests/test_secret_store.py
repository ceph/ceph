# -*- coding: utf-8 -*-
"""Tests for ceph_secrets.secret_store (SecretStoreMon + data model)."""
from __future__ import annotations

import pytest

# conftest injects stubs; just import production code after.
from ceph_secrets.secret_store import (
    SecretRecord,
    SecretMetadata,
    SecretPolicy,
    SECRET_STORE_PREFIX,
    SECRET_STORE_FORMAT_VERSION,
    _checked_ref,
    _checked_namespace,
    _epoch_key,
    _now_iso,
    _expect_bool,
    _expect_str,
    _expect_positive_int,
    _expect_object,
    _reject_unknown_keys,
    _require_keys,
)
from ceph_secrets_types import (
    SecretScope, SecretRef,
    CephSecretException, CephSecretDataError,
)


# ============================================================
# Helpers / primitive validators
# ============================================================

class TestPrimitiveHelpers:
    def test_now_iso_format(self):
        ts = _now_iso()
        import re
        assert re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$', ts)

    def test_expect_bool_ok(self):
        assert _expect_bool("f", True) is True
        assert _expect_bool("f", False) is False

    def test_expect_bool_bad(self):
        with pytest.raises(CephSecretDataError, match="must be a boolean"):
            _expect_bool("f", 1)

    def test_expect_str_ok(self):
        assert _expect_str("f", "hello") == "hello"

    def test_expect_str_bad(self):
        with pytest.raises(CephSecretDataError, match="must be a string"):
            _expect_str("f", 42)

    def test_expect_positive_int_ok(self):
        assert _expect_positive_int("f", 1) == 1
        assert _expect_positive_int("f", 99) == 99

    def test_expect_positive_int_zero(self):
        with pytest.raises(CephSecretDataError):
            _expect_positive_int("f", 0)

    def test_expect_positive_int_bool_rejected(self):
        with pytest.raises(CephSecretDataError):
            _expect_positive_int("f", True)

    def test_expect_object_ok(self):
        d = {"a": 1}
        assert _expect_object("o", d) is d

    def test_expect_object_bad(self):
        with pytest.raises(CephSecretDataError, match="must be a JSON object"):
            _expect_object("o", [1, 2])

    def test_reject_unknown_keys(self):
        with pytest.raises(CephSecretDataError, match="unknown field"):
            _reject_unknown_keys("L", {"a": 1, "z": 2}, {"a"})

    def test_require_keys_missing(self):
        with pytest.raises(CephSecretDataError, match="missing required"):
            _require_keys("L", {"a": 1}, {"a", "b"})

    def test_checked_namespace_valid(self):
        assert _checked_namespace("cephadm") == "cephadm"

    def test_checked_namespace_bad(self):
        with pytest.raises(CephSecretDataError):
            _checked_namespace("bad ns!")

    def test_epoch_key_format(self):
        assert _epoch_key("cephadm") == "secret_store/meta/cephadm/_epoch"

    def test_checked_ref_valid(self):
        ref = _checked_ref("ns", SecretScope.GLOBAL, "", "key")
        assert isinstance(ref, SecretRef)

    def test_checked_ref_bad_scope(self):
        with pytest.raises(CephSecretDataError):
            _checked_ref("ns", "bad", "", "key")


# ============================================================
# SecretMetadata
# ============================================================

class TestSecretMetadata:
    def test_defaults(self):
        m = SecretMetadata(version=1, created="2024-01-01T00:00:00Z", updated="2024-01-01T00:00:00Z")
        assert m.version == 1

    def test_to_json(self):
        m = SecretMetadata(version=2, created="2024-01-01T00:00:00Z", updated="2024-01-02T00:00:00Z")
        d = m.to_json()
        assert d["version"] == 2
        assert "created" in d
        assert "updated" in d

    def test_from_json_roundtrip(self):
        m = SecretMetadata(version=3, created="2024-01-01T00:00:00Z", updated="2024-01-02T00:00:00Z")
        m2 = SecretMetadata.from_json(m.to_json())
        assert m2.version == 3

    def test_from_json_missing_field(self):
        with pytest.raises(CephSecretDataError, match="missing required"):
            SecretMetadata.from_json({"version": 1, "created": "x"})

    def test_from_json_unknown_field(self):
        with pytest.raises(CephSecretDataError, match="unknown"):
            SecretMetadata.from_json({"version": 1, "created": "x", "updated": "y", "extra": 1})

    def test_version_zero_raises(self):
        with pytest.raises(CephSecretDataError):
            SecretMetadata(version=0, created="x", updated="y")

    def test_non_dict_raises(self):
        with pytest.raises(CephSecretDataError):
            SecretMetadata.from_json("not a dict")


# ============================================================
# SecretPolicy
# ============================================================

class TestSecretPolicy:
    def test_defaults(self):
        p = SecretPolicy()
        assert p.user_made is True
        assert p.editable is True

    def test_to_json(self):
        p = SecretPolicy(user_made=False, editable=True)
        d = p.to_json()
        assert d["user_made"] is False

    def test_from_json_roundtrip(self):
        p = SecretPolicy(user_made=True, editable=False)
        p2 = SecretPolicy.from_json(p.to_json())
        assert p2.editable is False

    def test_from_json_missing(self):
        with pytest.raises(CephSecretDataError):
            SecretPolicy.from_json({"user_made": True})

    def test_from_json_bad_type(self):
        with pytest.raises(CephSecretDataError, match="must be a boolean"):
            SecretPolicy.from_json({"user_made": 1, "editable": True})

    def test_non_bool_raises(self):
        with pytest.raises(CephSecretDataError):
            SecretPolicy(user_made="yes", editable=True)  # type: ignore[arg-type]


# ============================================================
# SecretRecord
# ============================================================

class TestSecretRecord:
    def _ref(self, scope=SecretScope.GLOBAL, target="", name="key"):
        return SecretRef("ns", scope, target, name)

    def _meta(self):
        return SecretMetadata(version=1, created="2024-01-01T00:00:00Z", updated="2024-01-01T00:00:00Z")

    def test_basic_construction(self):
        rec = SecretRecord(ref=self._ref(), metadata=self._meta(), data="s3cr3t")
        assert rec.namespace == "ns"
        assert rec.scope == SecretScope.GLOBAL
        assert rec.name == "key"

    def test_to_public_json_no_data(self):
        rec = SecretRecord(ref=self._ref(), metadata=self._meta(), data="s3cr3t")
        out = rec.to_public_json(include_data=False)
        assert "data" not in out
        assert "metadata" in out

    def test_to_public_json_with_data(self):
        rec = SecretRecord(ref=self._ref(), metadata=self._meta(), data="s3cr3t")
        out = rec.to_public_json(include_data=True)
        assert out["data"] == "s3cr3t"

    def test_to_public_json_with_ref(self):
        rec = SecretRecord(ref=self._ref(), metadata=self._meta(), data="x")
        out = rec.to_public_json(include_ref=True)
        assert out["ref"]["namespace"] == "ns"
        assert out["ref"]["scope"] == "global"

    def test_to_public_json_with_policy(self):
        rec = SecretRecord(ref=self._ref(), metadata=self._meta(), data="x")
        out = rec.to_public_json(include_policy=True)
        assert "policy" in out

    def test_to_store_json(self):
        rec = SecretRecord(ref=self._ref(), metadata=self._meta(), data="x")
        stored = rec.to_store_json()
        assert stored["format_version"] == SECRET_STORE_FORMAT_VERSION
        assert "data" in stored
        assert "policy" in stored

    def test_from_store_json_roundtrip(self):
        ref = self._ref()
        rec = SecretRecord(ref=ref, metadata=self._meta(), data="pw")
        rec2 = SecretRecord.from_store_json(ref, rec.to_store_json())
        assert rec2.data == "pw"
        assert rec2.metadata.version == 1

    def test_from_store_json_wrong_version(self):
        ref = self._ref()
        payload = {
            "format_version": 99,
            "metadata": {"version": 1, "created": "x", "updated": "y"},
            "policy": {"user_made": True, "editable": True},
            "data": "x",
        }
        with pytest.raises(CephSecretDataError, match="unsupported"):
            SecretRecord.from_store_json(ref, payload)

    def test_from_store_json_missing_key(self):
        ref = self._ref()
        with pytest.raises(CephSecretDataError, match="missing required"):
            SecretRecord.from_store_json(ref, {"format_version": 1, "metadata": {}})

    def test_bad_ref_type_raises(self):
        with pytest.raises(CephSecretDataError, match="must be a SecretRef"):
            SecretRecord(ref="not-a-ref", metadata=self._meta(), data="x")  # type: ignore[arg-type]

    def test_bad_metadata_type_raises(self):
        with pytest.raises(CephSecretDataError):
            SecretRecord(ref=self._ref(), metadata={"version": 1}, data="x")  # type: ignore[arg-type]

    def test_bad_data_type_raises(self):
        with pytest.raises(CephSecretDataError):
            SecretRecord(ref=self._ref(), metadata=self._meta(), data={"not": "a-string"})  # type: ignore[arg-type]

    def test_empty_data_raises(self):
        with pytest.raises(CephSecretDataError, match="must not be empty"):
            SecretRecord(ref=self._ref(), metadata=self._meta(), data="")

    def test_data_in_store_json(self):
        rec = SecretRecord(ref=self._ref(), metadata=self._meta(), data="myvalue")
        stored = rec.to_store_json()
        assert stored["data"] == "myvalue"

    def test_ident(self):
        ref = self._ref()
        rec = SecretRecord(ref=ref, metadata=self._meta(), data="x")
        assert rec.ident() == ("ns", "global", "", "key")


# ============================================================
# SecretStoreMon – epoch
# ============================================================

class TestSecretStoreMon_Epoch:
    def test_initial_epoch_is_zero(self, store):
        assert store.get_epoch("cephadm") == 0

    def test_bump_epoch(self, store):
        assert store.bump_epoch("cephadm") == 1
        assert store.bump_epoch("cephadm") == 2

    def test_namespace_isolation(self, store):
        store.bump_epoch("cephadm")
        store.bump_epoch("cephadm")
        assert store.get_epoch("rook") == 0

    def test_epoch_key_stored_correctly(self, mgr, store):
        store.bump_epoch("myns")
        assert mgr.get_store("secret_store/meta/myns/_epoch") == "1"

    def test_corrupted_epoch_returns_zero(self, mgr, store):
        mgr.set_store("secret_store/meta/ns/_epoch", "notanumber")
        assert store.get_epoch("ns") == 0


# ============================================================
# SecretStoreMon – CRUD
# ============================================================

class TestSecretStoreMon_Crud:
    def test_set_and_get_global(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "pw", "secret")
        rec = store.get("ns", SecretScope.GLOBAL, "", "pw")
        assert rec is not None
        assert rec.data == "secret"

    def test_get_missing_returns_none(self, store):
        assert store.get("ns", SecretScope.GLOBAL, "", "noexist") is None

    def test_set_increments_version(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-v1")
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-v2")
        rec = store.get("ns", SecretScope.GLOBAL, "", "k")
        assert rec.metadata.version == 2

    def test_set_preserves_created_timestamp(self, store):
        import time
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-v1")
        rec1 = store.get("ns", SecretScope.GLOBAL, "", "k")
        time.sleep(1)
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-v2")
        rec2 = store.get("ns", SecretScope.GLOBAL, "", "k")
        assert rec1.metadata.created == rec2.metadata.created
        assert rec2.metadata.updated != rec2.metadata.created

    def test_set_service_scope(self, store):
        store.set("ns", SecretScope.SERVICE, "prometheus", "auth", "admin")
        rec = store.get("ns", SecretScope.SERVICE, "prometheus", "auth")
        assert rec.data == "admin"

    def test_set_host_scope(self, store):
        store.set("ns", SecretScope.HOST, "node1", "ssh", "abc")
        rec = store.get("ns", SecretScope.HOST, "node1", "ssh")
        assert rec.data == "abc"

    def test_set_custom_scope(self, store):
        store.set("ns", SecretScope.CUSTOM, "", "a/b/c", "tok")
        rec = store.get("ns", SecretScope.CUSTOM, "", "a/b/c")
        assert rec.data == "tok"

    def test_set_bumps_epoch(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-v1")
        assert store.get_epoch("ns") == 1

    def test_set_non_str_data_raises(self, store):
        with pytest.raises(CephSecretException):
            store.set("ns", SecretScope.GLOBAL, "", "k", {"not": "a-string"})  # type: ignore[arg-type]

    def test_set_empty_data_raises(self, store):
        with pytest.raises(CephSecretException, match="must not be empty"):
            store.set("ns", SecretScope.GLOBAL, "", "k", "")

    def test_set_non_editable_raises(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-v1", editable=False)
        with pytest.raises(CephSecretException, match="not editable"):
            store.set("ns", SecretScope.GLOBAL, "", "k", "data-v2")

    def test_rm_existing(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-x")
        assert store.rm("ns", SecretScope.GLOBAL, "", "k") is True
        assert store.get("ns", SecretScope.GLOBAL, "", "k") is None

    def test_rm_nonexistent(self, store):
        assert store.rm("ns", SecretScope.GLOBAL, "", "ghost") is False

    def test_rm_bumps_epoch(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-x")
        epoch_after_set = store.get_epoch("ns")
        store.rm("ns", SecretScope.GLOBAL, "", "k")
        assert store.get_epoch("ns") == epoch_after_set + 1

    def test_rm_nonexistent_no_epoch_bump(self, store):
        assert store.get_epoch("ns") == 0
        store.rm("ns", SecretScope.GLOBAL, "", "ghost")
        assert store.get_epoch("ns") == 0

    def test_get_corrupted_json_raises(self, mgr, store):
        mgr.set_store(
            f"{SECRET_STORE_PREFIX}ns/global/badkey", "NOT JSON"
        )
        with pytest.raises(CephSecretDataError):
            store.get("ns", SecretScope.GLOBAL, "", "badkey")

    def test_set_with_bad_scope_string(self, store):
        with pytest.raises(CephSecretDataError):
            store.set("ns", "badscope", "", "k", "data-v1")


# ============================================================
# SecretStoreMon – ls
# ============================================================

class TestSecretStoreMon_Ls:
    def test_ls_empty(self, store):
        assert store.ls(namespace="ns") == []

    def test_ls_returns_all(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "k1", "data-1")
        store.set("ns", SecretScope.GLOBAL, "", "k2", "data-2")
        recs = store.ls(namespace="ns")
        assert len(recs) == 2

    def test_ls_filter_scope(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "g", "data-g")
        store.set("ns", SecretScope.SERVICE, "prom", "auth", "data-auth")
        recs = store.ls(namespace="ns", scope=SecretScope.GLOBAL)
        assert len(recs) == 1
        assert recs[0].scope == SecretScope.GLOBAL

    def test_ls_filter_target(self, store):
        store.set("ns", SecretScope.SERVICE, "prom", "a1", "data-a1")
        store.set("ns", SecretScope.SERVICE, "grafana", "a2", "data-a2")
        recs = store.ls(namespace="ns", scope=SecretScope.SERVICE, target="prom")
        assert len(recs) == 1
        assert recs[0].target == "prom"

    def test_ls_namespace_isolation(self, store):
        store.set("ns1", SecretScope.GLOBAL, "", "k", "data-ns1")
        store.set("ns2", SecretScope.GLOBAL, "", "k", "data-ns2")
        recs = store.ls(namespace="ns1")
        assert len(recs) == 1

    def test_ls_custom_scope(self, store):
        store.set("ns", SecretScope.CUSTOM, "", "a/b/c", "data-abc")
        store.set("ns", SecretScope.CUSTOM, "", "x/y", "data-xy")
        recs = store.ls(namespace="ns", scope=SecretScope.CUSTOM)
        assert len(recs) == 2

    def test_ls_sorted(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "zz", "data-zz")
        store.set("ns", SecretScope.GLOBAL, "", "aa", "data-aa")
        recs = store.ls(namespace="ns")
        names = [r.name for r in recs]
        assert names == sorted(names)

    def test_ls_invalid_namespace(self, store):
        with pytest.raises(CephSecretDataError):
            store.ls(namespace="bad ns!")

    def test_ls_scope_as_string(self, store):
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-v1")
        recs = store.ls(namespace="ns", scope="global")
        assert len(recs) == 1

    def test_ls_corrupted_json_raises(self, mgr, store):
        mgr.set_store(f"{SECRET_STORE_PREFIX}ns/global/bad", "NOT-JSON")
        with pytest.raises(CephSecretDataError):
            store.ls(namespace="ns")

    def test_ls_no_filter(self, store):
        store.set("ns1", SecretScope.GLOBAL, "", "a", "data-a")
        store.set("ns2", SecretScope.GLOBAL, "", "b", "data-b")
        recs = store.ls()
        assert len(recs) == 2

    def test_ls_kv_key_structure_validation(self, mgr, store):
        """A key with empty segments inside the prefix should raise CephSecretDataError."""
        mgr.set_store(f"{SECRET_STORE_PREFIX}ns//global/k", "{}")
        with pytest.raises(CephSecretDataError, match="empty path component"):
            store.ls(namespace="ns")


# ============================================================
# TestSecretStoreMon_Ls — malformed persisted-record cases
# ============================================================

class TestSecretStoreMon_Ls_Malformed:
    def test_ls_too_few_segments(self, mgr, store):
        mgr.set_store(f"{SECRET_STORE_PREFIX}ns/global", "{}")
        with pytest.raises(CephSecretDataError, match="unexpected key structure"):
            store.ls(namespace="ns")

    def test_ls_invalid_scope_in_key(self, mgr, store):
        mgr.set_store(f"{SECRET_STORE_PREFIX}ns/badscope/key", "{}")
        with pytest.raises(CephSecretDataError, match="invalid scope"):
            store.ls(namespace="ns")

    def test_ls_global_with_extra_segment(self, mgr, store):
        mgr.set_store(f"{SECRET_STORE_PREFIX}ns/global/target/name", "{}")
        with pytest.raises(CephSecretDataError, match="unexpected global key structure"):
            store.ls(namespace="ns")

    def test_ls_service_with_missing_segment(self, mgr, store):
        # service needs 4 parts (ns/service/target/name); 3 is too few
        mgr.set_store(f"{SECRET_STORE_PREFIX}ns/service/onlytarget", "{}")
        with pytest.raises(CephSecretDataError, match="unexpected targeted-scope key structure"):
            store.ls(namespace="ns")

    def test_ls_payload_not_object(self, mgr, store):
        mgr.set_store(f"{SECRET_STORE_PREFIX}ns/global/k", '["not", "object"]')
        with pytest.raises(CephSecretDataError, match="not a JSON object"):
            store.ls(namespace="ns")

    def test_ls_payload_valid_json_missing_fields(self, mgr, store):
        # Valid JSON object but missing required secret record fields
        mgr.set_store(f"{SECRET_STORE_PREFIX}ns/global/k", '{"unexpected": 1}')
        with pytest.raises(CephSecretDataError):
            store.ls(namespace="ns")


# ============================================================
# TestSecretStoreMon_Crud — editable=False allows rm by design
# ============================================================

class TestSecretStoreMon_EditableRm:
    def test_rm_non_editable_secret_is_allowed(self, store):
        """rm ignores editable — deletion is always permitted by design."""
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-v1", editable=False)
        result = store.rm("ns", SecretScope.GLOBAL, "", "k")
        assert result is True

    def test_set_non_editable_blocks_update(self, store):
        """Confirmed: set refuses to update a non-editable secret."""
        store.set("ns", SecretScope.GLOBAL, "", "k", "data-v1", editable=False)
        with pytest.raises(CephSecretException):
            store.set("ns", SecretScope.GLOBAL, "", "k", "data-v2")
