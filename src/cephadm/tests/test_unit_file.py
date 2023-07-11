# Tests for various assorted utility functions found within cephadm
#
from unittest import mock

import functools
import io
import os
import sys

import pytest

from tests.fixtures import (
    import_cephadm,
    mock_docker,
    mock_podman,
    with_cephadm_ctx,
)

_cephadm = import_cephadm()


def test_docker_engine_requires_docker():
    ctx = _cephadm.CephadmContext()
    ctx.container_engine = mock_docker()
    r = _cephadm.get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
    assert 'Requires=docker.service' in r


def test_podman_engine_does_not_req_docker():
    ctx = _cephadm.CephadmContext()
    ctx.container_engine = mock_podman()
    r = _cephadm.get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
    assert 'Requires=docker.service' not in r
