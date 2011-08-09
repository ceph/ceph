import argparse
from orchestra import cluster

from nose.tools import (
    eq_ as eq,
    assert_raises,
    )

from .. import misc


class FakeRemote(object):
    pass


def test_get_clients_simple():
    ctx = argparse.Namespace()
    remote = FakeRemote()
    ctx.cluster = cluster.Cluster(
        remotes=[
            (remote, ['client.0', 'client.1'])
            ],
        )
    g = misc.get_clients(ctx=ctx, roles=['client.1'])
    got = next(g)
    eq(len(got), 2)
    eq(got[0], ('1'))
    assert got[1] is remote
    assert_raises(StopIteration, next, g)
