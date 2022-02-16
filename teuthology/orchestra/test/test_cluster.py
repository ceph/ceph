import pytest

from mock import patch, Mock

from teuthology.orchestra import cluster, remote, run


class TestCluster(object):
    def test_init_empty(self):
        c = cluster.Cluster()
        assert c.remotes == {}

    def test_init(self):
        r1 = Mock()
        r2 = Mock()
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['baz']),
                ],
            )
        r3 = Mock()
        c.add(r3, ['xyzzy', 'thud', 'foo'])
        assert c.remotes == {
            r1: ['foo', 'bar'],
            r2: ['baz'],
            r3: ['xyzzy', 'thud', 'foo'],
        }

    def test_repr(self):
        r1 = remote.Remote('r1', ssh=Mock())
        r2 = remote.Remote('r2', ssh=Mock())
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['baz']),
                ],
            )
        assert repr(c) == \
            "Cluster(remotes=[[Remote(name='r1'), ['foo', 'bar']], " \
            "[Remote(name='r2'), ['baz']]])"

    def test_str(self):
        r1 = remote.Remote('r1', ssh=Mock())
        r2 = remote.Remote('r2', ssh=Mock())
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['baz']),
                ],
            )
        assert str(c) == "r1[foo,bar] r2[baz]"

    def test_run_all(self):
        r1 = Mock(spec=remote.Remote)
        r1.configure_mock(name='r1')
        ret1 = Mock(spec=run.RemoteProcess)
        r1.run.return_value = ret1
        r2 = Mock(spec=remote.Remote)
        r2.configure_mock(name='r2')
        ret2 = Mock(spec=run.RemoteProcess)
        r2.run.return_value = ret2
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['baz']),
                ],
            )
        assert r1.run.called_once_with(args=['test'])
        assert r2.run.called_once_with(args=['test'])
        got = c.run(args=['test'])
        assert len(got) == 2
        assert got, [ret1 == ret2]
        # check identity not equality
        assert got[0] is ret1
        assert got[1] is ret2

    def test_only_one(self):
        r1 = Mock()
        r2 = Mock()
        r3 = Mock()
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['bar']),
                (r3, ['foo']),
                ],
            )
        c_foo = c.only('foo')
        assert c_foo.remotes == {r1: ['foo', 'bar'], r3: ['foo']}

    def test_only_two(self):
        r1 = Mock()
        r2 = Mock()
        r3 = Mock()
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['bar']),
                (r3, ['foo']),
                ],
            )
        c_both = c.only('foo', 'bar')
        assert c_both.remotes, {r1: ['foo' == 'bar']}

    def test_only_none(self):
        r1 = Mock()
        r2 = Mock()
        r3 = Mock()
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['bar']),
                (r3, ['foo']),
                ],
            )
        c_none = c.only('impossible')
        assert c_none.remotes == {}

    def test_only_match(self):
        r1 = Mock()
        r2 = Mock()
        r3 = Mock()
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['bar']),
                (r3, ['foo']),
                ],
            )
        c_foo = c.only('foo', lambda role: role.startswith('b'))
        assert c_foo.remotes, {r1: ['foo' == 'bar']}

    def test_exclude_one(self):
        r1 = Mock()
        r2 = Mock()
        r3 = Mock()
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['bar']),
                (r3, ['foo']),
                ],
            )
        c_foo = c.exclude('foo')
        assert c_foo.remotes == {r2: ['bar']}

    def test_exclude_two(self):
        r1 = Mock()
        r2 = Mock()
        r3 = Mock()
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['bar']),
                (r3, ['foo']),
                ],
            )
        c_both = c.exclude('foo', 'bar')
        assert c_both.remotes == {r2: ['bar'], r3: ['foo']}

    def test_exclude_none(self):
        r1 = Mock()
        r2 = Mock()
        r3 = Mock()
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['bar']),
                (r3, ['foo']),
                ],
            )
        c_none = c.exclude('impossible')
        assert c_none.remotes == {r1: ['foo', 'bar'], r2: ['bar'], r3: ['foo']}

    def test_exclude_match(self):
        r1 = Mock()
        r2 = Mock()
        r3 = Mock()
        c = cluster.Cluster(
            remotes=[
                (r1, ['foo', 'bar']),
                (r2, ['bar']),
                (r3, ['foo']),
                ],
            )
        c_foo = c.exclude('foo', lambda role: role.startswith('b'))
        assert c_foo.remotes == {r2: ['bar'], r3: ['foo']}

    def test_filter(self):
        r1 = Mock(_name='r1')
        r2 = Mock(_name='r2')
        def func(r):
            return r._name == "r1"
        c = cluster.Cluster(remotes=[
                (r1, ['foo']),
                (r2, ['bar']),
            ])
        assert c.filter(func).remotes == {
            r1: ['foo']
        }


class TestWriteFile(object):
    """ Tests for cluster.write_file """
    def setup(self):
        self.r1 = remote.Remote('r1', ssh=Mock())
        self.c = cluster.Cluster(
            remotes=[
                (self.r1, ['foo', 'bar']),
            ],
        )

    @patch("teuthology.orchestra.remote.RemoteShell.write_file")
    def test_write_file(self, m_write_file):
        self.c.write_file("filename", "content")
        m_write_file.assert_called_with("filename", "content")

    @patch("teuthology.orchestra.remote.RemoteShell.write_file")
    def test_fails_with_invalid_perms(self, m_write_file):
        with pytest.raises(ValueError):
            self.c.write_file("filename", "content", sudo=False, perms="invalid")

    @patch("teuthology.orchestra.remote.RemoteShell.write_file")
    def test_fails_with_invalid_owner(self, m_write_file):
        with pytest.raises(ValueError):
            self.c.write_file("filename", "content", sudo=False, owner="invalid")

    @patch("teuthology.orchestra.remote.RemoteShell.write_file")
    def test_with_sudo(self, m_write_file):
        self.c.write_file("filename", "content", sudo=True)
        m_write_file.assert_called_with("filename", "content", sudo=True, owner=None, mode=None)
