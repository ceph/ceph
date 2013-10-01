import fudge

from .. import cluster, remote


class TestCluster(object):
    @fudge.with_fakes
    def test_init_empty(self):
        fudge.clear_expectations()
        c = cluster.Cluster()
        assert c.remotes == {}

    @fudge.with_fakes
    def test_init(self):
        fudge.clear_expectations()
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
        assert c.remotes == {
            r1: ['foo', 'bar'],
            r2: ['baz'],
            r3: ['xyzzy', 'thud', 'foo'],
        }

    @fudge.with_fakes
    def test_repr(self):
        fudge.clear_expectations()
        r1 = remote.Remote('r1', ssh=fudge.Fake('SSH'))
        r2 = remote.Remote('r2', ssh=fudge.Fake('SSH'))
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['baz']),
                ],
            )
        assert repr(c) == "Cluster(remotes={Remote(name='r1'): ['foo', 'bar'], Remote(name='r2'): ['baz']})" # noqa

    @fudge.with_fakes
    def test_str(self):
        fudge.clear_expectations()
        r1 = remote.Remote('r1', ssh=fudge.Fake('SSH'))
        r2 = remote.Remote('r2', ssh=fudge.Fake('SSH'))
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['baz']),
                ],
            )
        assert str(c) == "r1[foo,bar] r2[baz]"

    @fudge.with_fakes
    def test_run_all(self):
        fudge.clear_expectations()
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
        assert len(got) == 2
        assert got, [ret1 == ret2]
        # check identity not equality
        assert got[0] is ret1
        assert got[1] is ret2

    @fudge.with_fakes
    def test_only_one(self):
        fudge.clear_expectations()
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
        assert c_foo.remotes == {r1: ['foo', 'bar'], r3: ['foo']}

    @fudge.with_fakes
    def test_only_two(self):
        fudge.clear_expectations()
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
        assert c_both.remotes, {r1: ['foo' == 'bar']}

    @fudge.with_fakes
    def test_only_none(self):
        fudge.clear_expectations()
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
        assert c_none.remotes == {}

    @fudge.with_fakes
    def test_only_match(self):
        fudge.clear_expectations()
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
        assert c_foo.remotes, {r1: ['foo' == 'bar']}

    @fudge.with_fakes
    def test_exclude_one(self):
        fudge.clear_expectations()
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
        assert c_foo.remotes == {r2: ['bar']}

    @fudge.with_fakes
    def test_exclude_two(self):
        fudge.clear_expectations()
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
        assert c_both.remotes == {r2: ['bar'], r3: ['foo']}

    @fudge.with_fakes
    def test_exclude_none(self):
        fudge.clear_expectations()
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
        assert c_none.remotes == {r1: ['foo', 'bar'], r2: ['bar'], r3: ['foo']}

    @fudge.with_fakes
    def test_exclude_match(self):
        fudge.clear_expectations()
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
        assert c_foo.remotes == {r2: ['bar'], r3: ['foo']}
