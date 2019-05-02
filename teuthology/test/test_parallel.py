from teuthology.parallel import parallel


def identity(item, input_set=None, remove=False):
    if input_set is not None:
        assert item in input_set
        if remove:
            input_set.remove(item)
    return item


class TestParallel(object):
    def test_basic(self):
        in_set = set(range(10))
        with parallel() as para:
            for i in in_set:
                para.spawn(identity, i, in_set, remove=True)
                assert para.any_spawned is True
            assert para.count == len(in_set)

    def test_result(self):
        in_set = set(range(10))
        with parallel() as para:
            for i in in_set:
                para.spawn(identity, i, in_set)
            for result in para:
                in_set.remove(result)

