# -*- coding: utf-8 -*-
"""
Unit tests for ceph_secrets_client.py.
Placed at the same level as the module under test (src/pybind/mgr/).
"""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest
from typing import Any
from unittest.mock import MagicMock, call

from ceph_secrets_client import CephSecretsClient, MgrRemote
from ceph_secrets_types import SecretScope


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_mgr(return_value: Any = None, raise_exc: Exception = None) -> MagicMock:
    """Return a mock that satisfies the MgrRemote Protocol."""
    mgr = MagicMock()
    if raise_exc is not None:
        mgr.remote.side_effect = raise_exc
    else:
        mgr.remote.return_value = return_value
    return mgr


def _client(return_value: Any = None, raise_exc: Exception = None) -> CephSecretsClient:
    return CephSecretsClient(_make_mgr(return_value, raise_exc))


# ============================================================
# CephSecretsClient construction
# ============================================================

class TestConstruction:
    def test_default_module_name(self):
        mgr = MagicMock()
        client = CephSecretsClient(mgr)
        assert client.module == "ceph_secrets"

    def test_custom_module_name(self):
        mgr = MagicMock()
        client = CephSecretsClient(mgr, module="my_secrets")
        assert client.module == "my_secrets"

    def test_stores_mgr(self):
        mgr = MagicMock()
        client = CephSecretsClient(mgr)
        assert client.mgr is mgr


# ============================================================
# _remote error handling
# ============================================================

class TestRemoteErrorHandling:
    def test_passes_through_return_value(self):
        client = _client(return_value=42)
        assert client._remote("some_method", foo="bar") == 42

    def test_wraps_exception_as_runtime_error(self):
        client = _client(raise_exc=RuntimeError("module down"))
        with pytest.raises(RuntimeError, match="Cannot call secrets mgr-module"):
            client._remote("any_method")

    def test_exception_includes_module_name(self):
        mgr = _make_mgr(raise_exc=Exception("boom"))
        client = CephSecretsClient(mgr, module="my_secrets")
        with pytest.raises(RuntimeError, match="my_secrets"):
            client._remote("any_method")

    def test_exception_is_enabled_hint(self):
        client = _client(raise_exc=Exception("unavailable"))
        with pytest.raises(RuntimeError, match="enabled"):
            client._remote("any_method")


# ============================================================
# secret_get_epoch
# ============================================================

class TestSecretGetEpoch:
    def test_calls_correct_method(self):
        mgr = _make_mgr(return_value=5)
        client = CephSecretsClient(mgr)
        result = client.secret_get_epoch("cephadm")
        mgr.remote.assert_called_once_with("ceph_secrets", "secret_get_epoch", namespace="cephadm")
        assert result == 5

    def test_returns_zero_epoch(self):
        assert _client(return_value=0).secret_get_epoch("ns") == 0


# ============================================================
# secret_get
# ============================================================

class TestSecretGet:
    def test_calls_remote_correctly(self):
        mgr = _make_mgr(return_value={"metadata": {"version": 1}})
        client = CephSecretsClient(mgr)
        result = client.secret_get("ns", SecretScope.GLOBAL, "", "pw")
        mgr.remote.assert_called_once_with(
            "ceph_secrets", "secret_get",
            namespace="ns", scope=SecretScope.GLOBAL,
            target="", name="pw", reveal=False,
        )
        assert result["metadata"]["version"] == 1

    def test_reveal_kwarg_forwarded(self):
        mgr = _make_mgr(return_value={"metadata": {}, "data": "s3cr3t"})
        client = CephSecretsClient(mgr)
        client.secret_get("ns", SecretScope.GLOBAL, "", "pw", reveal=True)
        _, kwargs = mgr.remote.call_args
        assert kwargs["reveal"] is True

    def test_returns_none_when_not_found(self):
        assert _client(return_value=None).secret_get("ns", "global", "", "ghost") is None

    def test_scope_as_string(self):
        mgr = _make_mgr(return_value=None)
        client = CephSecretsClient(mgr)
        client.secret_get("ns", "host", "node1", "ssh")
        _, kwargs = mgr.remote.call_args
        assert kwargs["scope"] == "host"


# ============================================================
# secret_get_value
# ============================================================

class TestSecretGetValue:
    def test_calls_remote(self):
        mgr = _make_mgr(return_value="s3cr3t")
        client = CephSecretsClient(mgr)
        result = client.secret_get_value("ns", SecretScope.GLOBAL, "", "pw")
        mgr.remote.assert_called_once_with(
            "ceph_secrets", "secret_get_value",
            namespace="ns", scope=SecretScope.GLOBAL, target="", name="pw",
        )
        assert result == "s3cr3t"

    def test_returns_none_if_not_found(self):
        assert _client(return_value=None).secret_get_value("ns", "global", "", "ghost") is None

    def test_returns_opaque_string(self):
        payload = '{"u": "a", "p": "b"}'
        result = _client(return_value=payload).secret_get_value("ns", "global", "", "creds")
        assert result == payload

    def test_module_unreachable_raises(self):
        with pytest.raises(RuntimeError, match="Cannot call"):
            _client(raise_exc=Exception("down")).secret_get_value("ns", "global", "", "pw")


# secret_get_version
# ============================================================

class TestSecretGetVersion:
    def test_calls_remote(self):
        mgr = _make_mgr(return_value=3)
        client = CephSecretsClient(mgr)
        result = client.secret_get_version("ns", SecretScope.GLOBAL, "", "k")
        mgr.remote.assert_called_once_with(
            "ceph_secrets", "secret_get_version",
            namespace="ns", scope=SecretScope.GLOBAL, target="", name="k",
        )
        assert result == 3

    def test_returns_none_if_not_found(self):
        assert _client(return_value=None).secret_get_version("ns", "global", "", "ghost") is None


# ============================================================
# secret_get_versions
# ============================================================

class TestSecretGetVersions:
    def test_calls_remote_with_list(self):
        uris = ["secret:/ns/global/a", "secret:/ns/global/b"]
        expected = {"secret:/ns/global/a": 1, "secret:/ns/global/b": None}
        mgr = _make_mgr(return_value=expected)
        client = CephSecretsClient(mgr)
        result = client.secret_get_versions(uris)
        mgr.remote.assert_called_once_with("ceph_secrets", "secret_get_versions", uris=uris)
        assert result == expected

    def test_returns_empty_dict_for_empty_list(self):
        result = _client(return_value={}).secret_get_versions([])
        assert result == {}


# ============================================================
# secret_set
# ============================================================

class TestSecretSet:
    def test_calls_remote_with_defaults(self):
        expected = {"metadata": {"version": 1}}
        mgr = _make_mgr(return_value=expected)
        client = CephSecretsClient(mgr)
        result = client.secret_set("ns", SecretScope.GLOBAL, "", "pw", "x")
        mgr.remote.assert_called_once_with(
            "ceph_secrets", "secret_set",
            namespace="ns", scope=SecretScope.GLOBAL,
            target="", name="pw", data="x",
            user_made=True, editable=True,
        )
        assert result == expected

    def test_user_made_editable_overrides(self):
        mgr = _make_mgr(return_value={})
        client = CephSecretsClient(mgr)
        client.secret_set("ns", "global", "", "k", "x", user_made=False, editable=False)
        _, kwargs = mgr.remote.call_args
        assert kwargs["user_made"] is False
        assert kwargs["editable"] is False

    def test_raises_on_module_error(self):
        with pytest.raises(RuntimeError):
            _client(raise_exc=Exception("down")).secret_set("ns", "global", "", "k", "x")


# ============================================================
# secret_rm
# ============================================================

class TestSecretRm:
    def test_calls_remote(self):
        mgr = _make_mgr(return_value=True)
        client = CephSecretsClient(mgr)
        result = client.secret_rm("ns", SecretScope.GLOBAL, "", "k")
        mgr.remote.assert_called_once_with(
            "ceph_secrets", "secret_rm",
            namespace="ns", scope=SecretScope.GLOBAL, target="", name="k",
        )
        assert result is True

    def test_returns_false_when_not_found(self):
        assert _client(return_value=False).secret_rm("ns", "global", "", "ghost") is False

    def test_truthy_value_coerced_to_bool(self):
        # remote might return 1 instead of True
        assert _client(return_value=1).secret_rm("ns", "global", "", "k") is True

    def test_falsy_value_coerced_to_bool(self):
        assert _client(return_value=0).secret_rm("ns", "global", "", "k") is False


# ============================================================
# scan_unresolved_refs
# ============================================================

class TestScanUnresolvedRefs:
    def test_calls_remote(self):
        expected = ["secret:/ns/global/missing"]
        mgr = _make_mgr(return_value=expected)
        client = CephSecretsClient(mgr)
        result = client.scan_unresolved_refs({"k": "secret:/ns/global/missing"}, "ns")
        mgr.remote.assert_called_once_with(
            "ceph_secrets", "scan_unresolved_refs",
            obj={"k": "secret:/ns/global/missing"}, namespace="ns",
        )
        assert result == expected

    def test_returns_empty_when_all_resolved(self):
        assert _client(return_value=[]).scan_unresolved_refs({}, "ns") == []


# ============================================================
# scan_refs
# ============================================================

class TestScanRefs:
    def test_calls_remote(self):
        expected = ["secret:/ns/global/pw"]
        mgr = _make_mgr(return_value=expected)
        client = CephSecretsClient(mgr)
        result = client.scan_refs("secret:/ns/global/pw", "ns")
        mgr.remote.assert_called_once_with(
            "ceph_secrets", "scan_refs",
            obj="secret:/ns/global/pw", namespace="ns",
        )
        assert result == expected

    def test_returns_empty_for_no_refs(self):
        assert _client(return_value=[]).scan_refs("no refs here", "ns") == []


# ============================================================
# resolve_object
# ============================================================

class TestResolveObject:
    def test_calls_remote(self):
        mgr = _make_mgr(return_value={"password": "s3cr3t"})
        client = CephSecretsClient(mgr)
        result = client.resolve_object({"password": "secret:/ns/global/pw"})
        mgr.remote.assert_called_once_with(
            "ceph_secrets", "resolve_object",
            obj={"password": "secret:/ns/global/pw"},
        )
        assert result["password"] == "s3cr3t"

    def test_raises_on_unresolvable(self):
        with pytest.raises(RuntimeError):
            _client(raise_exc=Exception("missing")).resolve_object("secret:/ns/global/ghost")

    def test_returns_plain_value_unchanged(self):
        result = _client(return_value="plain").resolve_object("plain")
        assert result == "plain"
