# -*- coding: utf-8 -*-
"""Tests for ceph_secrets.module.Module — RPC surface and internal methods."""
from __future__ import annotations

import json
import pytest
from typing import Any
from unittest.mock import MagicMock, patch

from ceph_secrets.module import Module
from ceph_secrets.secret_store import SecretStoreMon
from ceph_secrets.secret_mgr import SecretMgr
from ceph_secrets_types import (
    SecretScope,
    SecretRef,
    CephSecretException,
    CephSecretDataError,
)


# ---------------------------------------------------------------------------
# Helper: build a Module without going through MgrModule.__init__
# ---------------------------------------------------------------------------

def _make_module(mgr_stub) -> Module:
    """Bypass MgrModule.__init__ and wire up manually."""
    mod = object.__new__(Module)
    mod.secret_mgr = SecretMgr(SecretStoreMon(mgr_stub))
    return mod


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Module.__init__ — backend selection
# ---------------------------------------------------------------------------

class TestModuleInit:
    def test_default_backend_initialises_secret_mgr(self, mgr):
        """Module.__init__ with default 'mon' backend wires up SecretMgr."""
        from mgr_module import MgrModule
        mod = object.__new__(Module)
        with patch.object(MgrModule, "__init__", lambda self, *a, **kw: None):
            with patch.object(MgrModule, "get_module_option", return_value="mon"):
                Module.__init__(mod, mgr)
        assert isinstance(mod.secret_mgr, SecretMgr)

    def test_unsupported_backend_raises(self, mgr):
        """Unknown backend name raises RuntimeError."""
        from mgr_module import MgrModule
        with patch.object(MgrModule, "__init__", lambda self, *a, **kw: None):
            with patch("ceph_secrets.module.BACKENDS", {}):
                with patch.object(MgrModule, "get_module_option", return_value="vault"):
                    with pytest.raises(RuntimeError, match="Unsupported secrets backend"):
                        Module.__init__(object.__new__(Module), mgr)

    def test_backend_init_failure_raises(self, mgr):
        """Backend constructor failure raises RuntimeError with helpful message."""
        from mgr_module import MgrModule
        broken_cls = MagicMock(side_effect=Exception("connection refused"))
        with patch.object(MgrModule, "__init__", lambda self, *a, **kw: None):
            with patch("ceph_secrets.module.BACKENDS", {"mon": broken_cls}):
                with patch.object(MgrModule, "get_module_option", return_value="mon"):
                    with pytest.raises(RuntimeError, match="Failed to initialize secrets backend"):
                        Module.__init__(object.__new__(Module), mgr)


# ---------------------------------------------------------------------------
# Module RPC surface
# ---------------------------------------------------------------------------

class TestModuleRPC:
    @pytest.fixture
    def mod(self, mgr):
        return _make_module(mgr)

    def test_secret_get_epoch_zero(self, mod):
        assert mod.secret_get_epoch("cephadm") == 0

    def test_secret_get_epoch_after_set(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "k", "data-x")
        assert mod.secret_get_epoch("ns") == 1

    def test_secret_set_and_get(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        result = mod.secret_get("ns", SecretScope.GLOBAL, "", "pw", reveal=True)
        assert result is not None
        assert result["data"] == "s3cr3t"

    def test_secret_get_without_reveal_hides_data(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        result = mod.secret_get("ns", SecretScope.GLOBAL, "", "pw", reveal=False)
        assert result is not None
        assert "data" not in result

    def test_secret_get_missing_returns_none(self, mod):
        assert mod.secret_get("ns", SecretScope.GLOBAL, "", "ghost") is None

    def test_secret_get_version_existing(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "k", "data-x")
        assert mod.secret_get_version("ns", SecretScope.GLOBAL, "", "k") == 1

    def test_secret_get_version_missing_returns_none(self, mod):
        assert mod.secret_get_version("ns", SecretScope.GLOBAL, "", "ghost") is None

    def test_secret_get_value_existing(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        assert mod.secret_get_value("ns", SecretScope.GLOBAL, "", "pw") == "s3cr3t"

    def test_secret_get_value_missing_returns_none(self, mod):
        assert mod.secret_get_value("ns", SecretScope.GLOBAL, "", "ghost") is None

    def test_secret_get_value_opaque_string(self, mod):
        # Any string is valid — including one that looks like JSON
        mod.secret_set("ns", SecretScope.GLOBAL, "", "creds", '{"u": "a", "p": "b"}')
        assert mod.secret_get_value("ns", SecretScope.GLOBAL, "", "creds") == '{"u": "a", "p": "b"}'

    def test_secret_get_value_corrupt_raises_data_error(self, mgr, mod):
        mgr.set_store("secret_store/v1/ns/global/bad", "{not json")
        with pytest.raises(CephSecretDataError):
            mod.secret_get_value("ns", SecretScope.GLOBAL, "", "bad")

    def test_secret_get_versions_batch(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "a", "data-a")
        mod.secret_set("ns", SecretScope.GLOBAL, "", "b", "data-b")
        result = mod.secret_get_versions([
            "secret:/ns/global/a",
            "secret:/ns/global/b",
            "secret:/ns/global/ghost",
        ])
        assert result["secret:/ns/global/a"] == 1
        assert result["secret:/ns/global/b"] == 1
        assert result["secret:/ns/global/ghost"] is None

    def test_secret_get_versions_skips_invalid_uris(self, mod):
        result = mod.secret_get_versions(["not-a-uri", "secret:/ns/badscope/key"])
        assert len(result) == 0

    def test_secret_get_versions_empty_list(self, mod):
        assert mod.secret_get_versions([]) == {}

    def test_secret_get_versions_corrupt_raises_data_error(self, mgr, mod):
        """Corruption must not be silently swallowed as a missing/invalid URI."""
        mgr.set_store("secret_store/v1/ns/global/bad", "{not json")
        with pytest.raises(CephSecretDataError):
            mod.secret_get_versions(["secret:/ns/global/bad"])

    def test_secret_set_returns_metadata(self, mod):
        result = mod.secret_set("ns", SecretScope.GLOBAL, "", "k", "data-x")
        assert "metadata" in result
        assert result["metadata"]["version"] == 1

    def test_secret_set_empty_data_raises(self, mod):
        with pytest.raises(CephSecretException, match="must not be empty"):
            mod.secret_set("ns", SecretScope.GLOBAL, "", "k", "")

    def test_secret_rm_existing(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "k", "data-x")
        assert mod.secret_rm("ns", SecretScope.GLOBAL, "", "k") is True

    def test_secret_rm_nonexistent(self, mod):
        assert mod.secret_rm("ns", SecretScope.GLOBAL, "", "ghost") is False

    def test_secret_ls_empty(self, mod):
        assert mod.secret_ls(namespace="ns") == {}

    def test_secret_ls_returns_records(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "a", "data-a")
        mod.secret_set("ns", SecretScope.GLOBAL, "", "b", "data-b")
        out = mod.secret_ls(namespace="ns")
        assert "ns/global/a" in out
        assert "ns/global/b" in out

    def test_secret_ls_scope_filter(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "g", "data-g")
        mod.secret_set("ns", SecretScope.SERVICE, "prom", "auth", "data-auth")
        out = mod.secret_ls(namespace="ns", scope="service")
        assert all("service" in k for k in out.keys())

    def test_secret_ls_with_target(self, mod):
        mod.secret_set("ns", SecretScope.SERVICE, "prom", "auth", "data-auth")
        out = mod.secret_ls(namespace="ns", scope="service", target="prom")
        assert "ns/service/prom/auth" in out

    def test_secret_ls_show_internals(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "k", "data-x")
        out = mod.secret_ls(namespace="ns", show_internals=True)
        rec = out["ns/global/k"]
        assert "policy" in rec

    def test_secret_ls_key_format_global(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "k", "data-x")
        out = mod.secret_ls(namespace="ns")
        assert "ns/global/k" in out
        for k in out:
            assert "//" not in k

    def test_scan_refs(self, mod):
        result = mod.scan_refs({"key": "secret:/ns/global/pw"}, namespace="ns")
        assert "secret:/ns/global/pw" in result

    def test_scan_unresolved_refs_all_missing(self, mod):
        result = mod.scan_unresolved_refs("secret:/ns/global/ghost", namespace="ns")
        assert "secret:/ns/global/ghost" in result

    def test_scan_unresolved_refs_exists(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "pw", "x")
        result = mod.scan_unresolved_refs("secret:/ns/global/pw", namespace="ns")
        assert result == []

    def test_resolve_object(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "pw", "s3cr3t")
        result = mod.resolve_object("secret:/ns/global/pw")
        assert result == "s3cr3t"


# ---------------------------------------------------------------------------
# Module internal ref-based methods
# ---------------------------------------------------------------------------

class TestModuleInternals:
    @pytest.fixture
    def mod(self, mgr):
        return _make_module(mgr)

    def _ref(self, ns="ns", scope=SecretScope.GLOBAL, target="", name="key"):
        return SecretRef(ns, scope, target, name)

    def test_secret_get_existing(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "key", "data-x")
        result = mod._secret_get(self._ref())
        assert result is not None

    def test_secret_get_not_found(self, mod):
        assert mod._secret_get(self._ref(name="ghost")) is None

    def test_secret_get_corrupt_raises_data_error(self, mgr, mod):
        mgr.set_store("secret_store/v1/ns/global/bad", "{not json")
        with pytest.raises(CephSecretDataError):
            mod._secret_get(self._ref(name="bad"))

    def test_secret_get_version_existing(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "key", "data-x")
        assert mod._secret_get_version(self._ref()) == 1

    def test_secret_get_version_not_found(self, mod):
        assert mod._secret_get_version(self._ref(name="ghost")) is None

    def test_secret_get_version_corrupt_raises_data_error(self, mgr, mod):
        mgr.set_store("secret_store/v1/ns/global/bad", "{not json")
        with pytest.raises(CephSecretDataError):
            mod._secret_get_version(self._ref(name="bad"))

    def test_secret_set_returns_metadata_without_data(self, mod):
        result = mod._secret_set(self._ref(), "data-x")
        assert "metadata" in result
        assert "data" not in result

    def test_secret_rm_existing(self, mod):
        mod._secret_set(self._ref(), "data-x")
        assert mod._secret_rm(self._ref()) is True

    def test_secret_rm_nonexistent(self, mod):
        assert mod._secret_rm(self._ref(name="ghost")) is False


# ---------------------------------------------------------------------------
# CLI handler logic (path-based dispatch)
# ---------------------------------------------------------------------------

class TestModuleCLIHandlers:
    @pytest.fixture
    def mod(self, mgr):
        return _make_module(mgr)

    def _ok(self, result: Any) -> Any:
        """Unwrap a Responder tuple (retcode, body, status) → parsed dict.
        Falls through unchanged when Responder is a no-op stub."""
        if isinstance(result, tuple):
            retcode, body, _ = result
            assert retcode == 0, f"unexpected error retcode {retcode}: {body}"
            return json.loads(body)
        return result

    def _assert_error(self, result: Any, match: str = "") -> None:
        """Assert a CLI call failed — either via a non-zero tuple or ErrorResponse.
        Never accepts a raw CephSecretException (would mean _handle_secret_errors broke)."""
        from object_format import ErrorResponse
        if isinstance(result, tuple):
            retcode, body, status = result
            assert retcode != 0, "expected non-zero retcode"
            if match:
                assert match in (body + status).lower(), \
                    f"expected {match!r} in response, got body={body!r} status={status!r}"
        elif isinstance(result, ErrorResponse):
            if match:
                assert match in str(result).lower()
        else:
            raise AssertionError(
                f"expected error tuple or ErrorResponse, got {type(result).__name__}: {result!r}"
            )

    def test_cli_get_by_path_found(self, mod):
        mod.secret_set("ns", SecretScope.GLOBAL, "", "key", "data-x")
        result = self._ok(mod._cli_secret_get_by_path(path="ns/global/key"))
        assert "metadata" in result

    def test_cli_get_by_path_not_found(self, mod):
        from object_format import ErrorResponse
        try:
            result = mod._cli_secret_get_by_path(path="ns/global/ghost")
            self._assert_error(result, match="not found")
        except ErrorResponse as e:
            assert "not found" in str(e).lower()

    def test_cli_get_by_path_bad_path(self, mod):
        from object_format import ErrorResponse
        try:
            result = mod._cli_secret_get_by_path(path="ns/badscope/key")
            self._assert_error(result)
        except ErrorResponse:
            pass

    def test_cli_get_corrupt_raises_data_error(self, mgr, mod):
        from object_format import ErrorResponse
        mgr.set_store("secret_store/v1/ns/global/bad", "{not json")
        # CephSecretDataError is a CephSecretException subclass so _handle_secret_errors
        # wraps it into ErrorResponse; Responder formats it as a non-zero tuple.
        try:
            result = mod._cli_secret_get_by_path(path="ns/global/bad")
            self._assert_error(result)
        except ErrorResponse:
            pass

    def test_cli_set_by_path(self, mod):
        result = self._ok(mod._cli_secret_set_by_path(
            path="ns/global/key",
            inbuf="s3cr3t"
        ))
        assert result["metadata"]["version"] == 1

    def test_cli_set_by_path_no_inbuf(self, mod):
        from object_format import ErrorResponse
        try:
            result = mod._cli_secret_set_by_path(path="ns/global/key", inbuf=None)
            self._assert_error(result, match="-i")
        except ErrorResponse as e:
            assert "-i" in str(e)

    def test_cli_set_by_path_empty_data(self, mod):
        from object_format import ErrorResponse
        try:
            result = mod._cli_secret_set_by_path(path="ns/global/key", inbuf="")
            self._assert_error(result, match="must not be empty")
        except ErrorResponse as e:
            assert "must not be empty" in str(e).lower()

    def test_cli_rm_by_path_existing(self, mod):
        mod._cli_secret_set_by_path(path="ns/global/key", inbuf='{"v": "x"}')
        result = self._ok(mod._cli_secret_rm_by_path(path="ns/global/key"))
        assert result["status"] == "removed"

    def test_cli_rm_by_path_not_found(self, mod):
        result = self._ok(mod._cli_secret_rm_by_path(path="ns/global/ghost"))
        assert result["status"] == "not_found"

    def test_cli_rm_by_path_bad_path(self, mod):
        from object_format import ErrorResponse
        try:
            result = mod._cli_secret_rm_by_path(path="ns/badscope/key")
            self._assert_error(result)
        except ErrorResponse:
            pass

    def test_cli_ls(self, mod):
        mod._cli_secret_set_by_path(path="ns/global/a", inbuf='{"v": "1"}')
        mod._cli_secret_set_by_path(path="ns/global/b", inbuf='{"v": "2"}')
        result = self._ok(mod._cli_secret_ls(namespace="ns"))
        assert "ns/global/a" in result
        assert "ns/global/b" in result

    def test_cli_get_with_reveal(self, mod):
        mod._cli_secret_set_by_path(path="ns/global/pw", inbuf="s3cr3t")
        result = self._ok(mod._cli_secret_get_by_path(path="ns/global/pw", reveal=True))
        assert result["data"] == "s3cr3t"

    def test_cli_get_without_reveal_hides_data(self, mod):
        mod._cli_secret_set_by_path(path="ns/global/pw", inbuf="s3cr3t")
        result = self._ok(mod._cli_secret_get_by_path(path="ns/global/pw", reveal=False))
        assert "data" not in result

    def _get_value(self, result: Any) -> str:
        """Unwrap a get-value tuple (retcode, body, status) → raw string."""
        if isinstance(result, tuple):
            retcode, body, _ = result
            assert retcode == 0, f"unexpected error retcode {retcode}: {body}"
            return body
        return result

    def test_cli_get_value_returns_raw_string(self, mod):
        mod._cli_secret_set_by_path(path="ns/global/pw", inbuf="s3cr3t")
        result = self._get_value(mod._cli_secret_get_value_by_path(path="ns/global/pw"))
        assert result == "s3cr3t"

    def test_cli_get_value_not_found_raises(self, mod):
        from object_format import ErrorResponse
        try:
            result = mod._cli_secret_get_value_by_path(path="ns/global/ghost")
            self._assert_error(result)
        except ErrorResponse:
            pass

    def test_cli_get_value_bad_path_raises(self, mod):
        from object_format import ErrorResponse
        try:
            result = mod._cli_secret_get_value_by_path(path="ns/badscope/key")
            self._assert_error(result)
        except ErrorResponse:
            pass

    def test_cli_get_value_opaque_json_string(self, mod):
        payload = '{"u": "a", "p": "b"}'
        mod._cli_secret_set_by_path(path="ns/global/creds", inbuf=payload)
        result = self._get_value(mod._cli_secret_get_value_by_path(path="ns/global/creds"))
        assert result == payload
