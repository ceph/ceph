from nose.tools import eq_ as eq, assert_raises
from rados import Rados, ObjectExists, ObjectNotFound, ANONYMOUS_AUID, ADMIN_AUID

class TestPool(object):

    def setUp(self):
        self.rados = Rados(conffile='')
        self.rados.connect()

    def tearDown(self):
        self.rados.shutdown()

    def test_create(self):
        self.rados.create_pool('foo')
        self.rados.delete_pool('foo')

    def test_create_auid(self):
        self.rados.create_pool('foo', 100)
        assert self.rados.pool_exists('foo')
        self.rados.delete_pool('foo')

    def test_eexist(self):
        self.rados.create_pool('foo')
        assert_raises(ObjectExists, self.rados.create_pool, 'foo')

class TestIoctx(object):

    def setUp(self):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        self.ioctx = self.rados.open_ioctx('test_pool')

    def tearDown(self):
        self.ioctx.close()
        self.rados.delete_pool('test_pool')
        self.rados.shutdown()

    def test_change_auid(self):
        self.ioctx.change_auid(ANONYMOUS_AUID)
        self.ioctx.change_auid(ADMIN_AUID)

    def test_write(self):
        self.ioctx.write('abc', 'abc')
        eq(self.ioctx.read('abc'), 'abc')

    def test_write_full(self):
        self.ioctx.write('abc', 'abc')
        eq(self.ioctx.read('abc'), 'abc')
        self.ioctx.write_full('abc', 'd')
        eq(self.ioctx.read('abc'), 'd')

    def test_write_zeros(self):
        self.ioctx.write('abc', 'a\0b\0c')
        eq(self.ioctx.read('abc'), 'a\0b\0c')

    def test_list_objects_empty(self):
        eq(list(self.ioctx.list_objects()), [])

    def test_list_objects(self):
        self.ioctx.write('a', '')
        self.ioctx.write('b', 'foo')
        self.ioctx.write_full('c', 'bar')
        object_names = [obj.key for obj in self.ioctx.list_objects()]
        eq(sorted(object_names), ['a', 'b', 'c'])

    def test_xattrs(self):
        xattrs = dict(a='1', b='2', c='3', d='a\0b', e='\0')
        self.ioctx.write('abc', '')
        for key, value in xattrs.iteritems():
            self.ioctx.set_xattr('abc', key, value)
            eq(self.ioctx.get_xattr('abc', key), value)
        stored_xattrs = {}
        for key, value in self.ioctx.get_xattrs('abc'):
            stored_xattrs[key] = value
        eq(stored_xattrs, xattrs)

    def test_create_snap(self):
        assert_raises(ObjectNotFound, self.ioctx.remove_snap, 'foo')
        self.ioctx.create_snap('foo')
        self.ioctx.remove_snap('foo')

    def test_list_snaps_empty(self):
        eq(list(self.ioctx.list_snaps()), [])

    def test_list_snaps(self):
        snaps = ['snap1', 'snap2', 'snap3']
        for snap in snaps:
            self.ioctx.create_snap(snap)
        listed_snaps = [snap.name for snap in self.ioctx.list_snaps()]
        eq(snaps, listed_snaps)

    def test_lookup_snap(self):
        self.ioctx.create_snap('foo')
        snap = self.ioctx.lookup_snap('foo')
        eq(snap.name, 'foo')

    def test_snap_timestamp(self):
        self.ioctx.create_snap('foo')
        snap = self.ioctx.lookup_snap('foo')
        snap.get_timestamp()

    def test_remove_snap(self):
        self.ioctx.create_snap('foo')
        (snap,) = self.ioctx.list_snaps()
        eq(snap.name, 'foo')
        self.ioctx.remove_snap('foo')
        eq(list(self.ioctx.list_snaps()), [])
