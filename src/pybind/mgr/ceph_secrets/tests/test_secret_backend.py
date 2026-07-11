# -*- coding: utf-8 -*-
"""Tests for the SecretStorageBackend ABC and contract."""
from __future__ import annotations

import pytest
from abc import ABC

from ceph_secrets.secret_backend import SecretStorageBackend
from ceph_secrets_types import SecretScope


class TestSecretStorageBackend:
    def test_is_abstract(self):
        assert issubclass(SecretStorageBackend, ABC)

    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            SecretStorageBackend()  # type: ignore[abstract]

    def test_concrete_subclass_instantiates(self, store):
        """SecretStoreMon (the concrete impl) must satisfy the ABC."""
        assert isinstance(store, SecretStorageBackend)

    def test_abstract_methods_defined(self):
        abstract = SecretStorageBackend.__abstractmethods__
        assert 'get' in abstract
        assert 'set' in abstract
        assert 'rm' in abstract
        assert 'ls' in abstract
        assert 'get_epoch' in abstract
        assert 'bump_epoch' in abstract

    def test_partial_implementation_still_abstract(self):
        """Subclass missing any abstract method stays abstract."""
        class PartialImpl(SecretStorageBackend):
            def get(self, ns, scope, target, name):
                pass

            def set(self, ns, scope, target, name, data, user_made=True, editable=True):
                pass

            def rm(self, ns, scope, target, name):
                pass

            def ls(self, namespace=None, scope=None, target=None):
                pass

            def get_epoch(self, namespace):
                pass
            # missing bump_epoch

        with pytest.raises(TypeError):
            PartialImpl()  # type: ignore[abstract]

    def test_full_implementation_is_instantiable(self):

        class FullImpl(SecretStorageBackend):

            def get(self, ns, scope, target, name):
                return None

            def set(self, ns, scope, target, name, data, user_made=True, editable=True):
                pass

            def rm(self, ns, scope, target, name):
                return False

            def ls(self, namespace=None, scope=None, target=None):
                return []

            def get_epoch(self, namespace):
                return 0

            def bump_epoch(self, namespace):
                return 1

        impl = FullImpl()
        assert impl.get("ns", SecretScope.GLOBAL, "", "k") is None
        assert impl.get_epoch("ns") == 0
        assert impl.bump_epoch("ns") == 1
