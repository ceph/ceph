import pytest
from pathlib import PurePosixPath
from fs import schedule


class TestScheduleIndex(object):

    in_parameters = [
        (['/'], ['/'], []),
        (['/a'], ['/a'], ['/']),
        (['/a'], ['/a'], ['/', '/bar']),
        (['/a/b'], ['/a/b'], ['/', '/a']),
        (['/a/b', '/'], ['/a/b', '/'], ['/a']),
        (['/a/b/c', '/a/d'], ['/a/b/c', '/a/d'], ['/a', '/', 'a/b']),
        (['/a/b', '/a/b/c/d'], ['/a/b', '/a/b/c/d'], ['/a', '/', 'a/b/c']),
        (['/' + '/'.join(['x' for _ in range(1000)])],
         ['/' + '/'.join(['x' for _ in range(1000)])], ['/a']),
    ]

    def test_index_ctor(self):
        s = schedule.ScheduleIndex()
        assert '/' in s.root

    @pytest.mark.parametrize("add,in_,not_in", in_parameters)
    def test_add_in(self, add, in_, not_in):
        if len(add[0]) > 999:
            pytest.xfail("will reach max recursion depth, deal with tail recursion to get this fixed")
        s = schedule.ScheduleIndex()
        [s.add(p) for p in add]
        for p in in_:
            assert p in s, '{} not found in schedule'.format(p)
        for p in not_in:
            assert p not in s, '{} found in schedule'.format(p)

    descend_parameters = [
        (['/'], '/', '/', []),
        (['/a'], '/a', 'a', ['/b']),
        (['/a/b'], '/a/b', 'b', ['/a/a']),
        (['/a/b', '/a/b/c'], '/a/b', 'b', ['/a/a']),
    ]

    @pytest.mark.parametrize("add,descend,pos,not_in", descend_parameters)
    def test_descend(self, add, descend, pos, not_in):
        # TODO improve this
        s = schedule.ScheduleIndex()
        [s.add(p) for p in add]
        prefix, result = s._descend(PurePosixPath(descend).parts, s.root)
        assert pos in result, '{} not in {}'.format(pos, result)
        for p in not_in:
            result = s._descend(PurePosixPath(p).parts, s.root)
            assert result == (False, False)

    def test_gather_subdirs(self):
        s = schedule.ScheduleIndex()
        s.add('/a/b')
        s.add('/a/b/c/d')
        ret = s._gather_subdirs(s.root['/'].children,
                                [],
                                PurePosixPath('/'))
        assert ret == [PurePosixPath('/a/b'), PurePosixPath('/a/b/c/d')]

    def test_get(self):
        s = schedule.ScheduleIndex()
        s.add('/a/b')
        s.add('/a/b/c/d')
        ret = s.get('/')
        assert ret == ['/a/b', '/a/b/c/d']
        ret = s.get('/a/b/c/')
        assert ret == ['/a/b/c/d']
