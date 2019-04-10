import os
import sys
import asyncio
import json

import pytest

from rbench.async_client.async_client import AsyncClient
from rbench.async_client.response_handler import make_response_handler_from_str
from rbench.client_ctx import ctx, utils
from rgw import admin_socket

def test_python_version():
    import platform
    major, minor, _ = platform.python_version_tuple()
    if not (int(major) >= 3 and int(minor) >=5):
        pytest.exit('low python version', -2)

@pytest.fixture(scope="module")
def ctx_f():
    import os
    return ctx.make_ctx(os.environ.get('TEST_CONF'))

@pytest.fixture(scope="module")
def setup_buckets(ctx_f):
    client_ctx = ctx_f
    # Ensure that buckets are created before running other args
    yield utils.create_buckets(client_ctx.buckets, client_ctx.base_url,
                               client_ctx.auth_creds)
    # TODO: cleanup buckets here

def test_rgw_throttler(ctx_f, setup_buckets):
    client_ctx = ctx_f
    resp_handler = make_response_handler_from_str('simple')
    client = AsyncClient(response_handler = resp_handler,
                         auth_type = client_ctx.auth_type,
                         auth_creds = client_ctx.auth_creds
    )

    ev_loop = asyncio.get_event_loop()

    pc1 = ev_loop.run_until_complete(admin_socket.async_perf_dump(client_ctx.admin_sock_path))
    futures = [ asyncio.ensure_future(client.run(**kwargs)) for kwargs in client_ctx.arg_list ]

    ev_loop.run_until_complete(asyncio.gather(*futures))
    pc2 = ev_loop.run_until_complete(admin_socket.async_perf_dump(client_ctx.admin_sock_path))

    d = resp_handler.aggr_stats()

    # this assumes that NO other requests were present at the time, probably ok
    # for teuthology, otherwise make an acceptable range here
    assert d['denied'] == pc2['throttle'] - pc1['throttle']
