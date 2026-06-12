# -*- coding: utf-8 -*-
"""Tests for ceph_secrets.backends (registry of storage backends)."""
from __future__ import annotations

from ceph_secrets.backends import BACKENDS
from ceph_secrets.secret_store import SecretStoreMon


class TestBackendsRegistry:
    def test_mon_backend_registered(self):
        assert "mon" in BACKENDS

    def test_mon_maps_to_correct_class(self):
        assert BACKENDS["mon"] is SecretStoreMon

    def test_only_known_backends(self):
        # Update this test when new backends are added.
        assert set(BACKENDS.keys()) == {"mon"}

    def test_backend_is_instantiable(self, mgr):
        cls = BACKENDS["mon"]
        instance = cls(mgr)
        assert isinstance(instance, SecretStoreMon)
