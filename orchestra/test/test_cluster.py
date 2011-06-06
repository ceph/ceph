from nose.tools import eq_ as eq

import fudge
import nose

from .. import cluster, remote

from .util import assert_raises

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_init_empty():
    c = cluster.Cluster()
    eq(c.remotes, {})

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_init():
    r1 = fudge.Fake('Remote')
    r2 = fudge.Fake('Remote')
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['baz']),
            ],
        )
    r3 = fudge.Fake('Remote')
    c.add(r3, ['xyzzy', 'thud', 'foo'])
    eq(c.remotes, {
            r1: ['foo', 'bar'],
            r2: ['baz'],
            r3: ['xyzzy', 'thud', 'foo'],
            })

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_repr():
    r1 = remote.Remote('r1', ssh=fudge.Fake('SSH'))
    r2 = remote.Remote('r2', ssh=fudge.Fake('SSH'))
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['baz']),
            ],
        )
    eq(repr(c), "Cluster(remotes={Remote(name='r1'): ['foo', 'bar'], Remote(name='r2'): ['baz']})")

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_str():
    r1 = remote.Remote('r1', ssh=fudge.Fake('SSH'))
    r2 = remote.Remote('r2', ssh=fudge.Fake('SSH'))
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['baz']),
            ],
        )
    eq(str(c), "r1[foo,bar] r2[baz]")

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_run_all():
    r1 = fudge.Fake('Remote').has_attr(name='r1')
    ret1 = fudge.Fake('RemoteProcess')
    r1.expects('run').with_args(args=['test']).returns(ret1)
    r2 = fudge.Fake('Remote').has_attr(name='r2')
    ret2 = fudge.Fake('RemoteProcess')
    r2.expects('run').with_args(args=['test']).returns(ret2)
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['baz']),
            ],
        )
    got = c.run(args=['test'])
    eq(len(got), 2)
    eq(got, [ret1, ret2])
    # check identity not equality
    assert got[0] is ret1
    assert got[1] is ret2

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_only_one():
    r1 = fudge.Fake('r1')
    r2 = fudge.Fake('r2')
    r3 = fudge.Fake('r3')
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['bar']),
            (r3, ['foo']),
            ],
        )
    c_foo = c.only('foo')
    eq(c_foo.remotes, {r1: ['foo', 'bar'], r3: ['foo']})

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_only_two():
    r1 = fudge.Fake('r1')
    r2 = fudge.Fake('r2')
    r3 = fudge.Fake('r3')
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['bar']),
            (r3, ['foo']),
            ],
        )
    c_both = c.only('foo', 'bar')
    eq(c_both.remotes, {r1: ['foo', 'bar']})

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_only_none():
    r1 = fudge.Fake('r1')
    r2 = fudge.Fake('r2')
    r3 = fudge.Fake('r3')
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['bar']),
            (r3, ['foo']),
            ],
        )
    c_none = c.only('impossible')
    eq(c_none.remotes, {})

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_only_match():
    r1 = fudge.Fake('r1')
    r2 = fudge.Fake('r2')
    r3 = fudge.Fake('r3')
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['bar']),
            (r3, ['foo']),
            ],
        )
    c_foo = c.only('foo', lambda role: role.startswith('b'))
    eq(c_foo.remotes, {r1: ['foo', 'bar']})

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_exclude_one():
    r1 = fudge.Fake('r1')
    r2 = fudge.Fake('r2')
    r3 = fudge.Fake('r3')
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['bar']),
            (r3, ['foo']),
            ],
        )
    c_foo = c.exclude('foo')
    eq(c_foo.remotes, {r2: ['bar']})

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_exclude_two():
    r1 = fudge.Fake('r1')
    r2 = fudge.Fake('r2')
    r3 = fudge.Fake('r3')
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['bar']),
            (r3, ['foo']),
            ],
        )
    c_both = c.exclude('foo', 'bar')
    eq(c_both.remotes, {r2: ['bar'], r3: ['foo']})

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_exclude_none():
    r1 = fudge.Fake('r1')
    r2 = fudge.Fake('r2')
    r3 = fudge.Fake('r3')
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['bar']),
            (r3, ['foo']),
            ],
        )
    c_none = c.exclude('impossible')
    eq(c_none.remotes, {r1: ['foo', 'bar'], r2: ['bar'], r3: ['foo']})

@nose.with_setup(fudge.clear_expectations)
@fudge.with_fakes
def test_exclude_match():
    r1 = fudge.Fake('r1')
    r2 = fudge.Fake('r2')
    r3 = fudge.Fake('r3')
    c = cluster.Cluster(
        remotes=[
            (r1, ['foo', 'bar']),
            (r2, ['bar']),
            (r3, ['foo']),
            ],
        )
    c_foo = c.exclude('foo', lambda role: role.startswith('b'))
    eq(c_foo.remotes, {r2: ['bar'], r3: ['foo']})
