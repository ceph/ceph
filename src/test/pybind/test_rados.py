from nose.tools import eq_ as eq, assert_raises
from rados import (Rados, Object, ObjectExists, ObjectNotFound,
                   ANONYMOUS_AUID, ADMIN_AUID)
import threading

class TestRados(object):

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
        self.rados.delete_pool('foo')

    def list_non_default_pools(self):
        pools = self.rados.list_pools()
        pools.remove('data')
        pools.remove('metadata')
        pools.remove('rbd')
        return set(pools)

    def test_list_pools(self):
        eq(set(), self.list_non_default_pools())
        self.rados.create_pool('foo')
        eq(set(['foo']), self.list_non_default_pools())
        self.rados.create_pool('bar')
        eq(set(['foo', 'bar']), self.list_non_default_pools())
        self.rados.create_pool('baz')
        eq(set(['foo', 'bar', 'baz']), self.list_non_default_pools())
        self.rados.delete_pool('foo')
        eq(set(['bar', 'baz']), self.list_non_default_pools())
        self.rados.delete_pool('baz')
        eq(set(['bar']), self.list_non_default_pools())
        self.rados.delete_pool('bar')
        eq(set(), self.list_non_default_pools())
        self.rados.create_pool('a' * 500)
        eq(set(['a' * 500]), self.list_non_default_pools())
        self.rados.delete_pool('a' * 500)

    def test_get_fsid(self):
        fsid = self.rados.get_fsid()
        eq(len(fsid), 36)

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

    def test_locator(self):
        self.ioctx.set_locator_key("bar")
        self.ioctx.write('foo', 'contents1')
        objects = [i for i in self.ioctx.list_objects()]
        eq(len(objects), 1)
        eq(self.ioctx.get_locator_key(), "bar")
        self.ioctx.set_locator_key("")
        objects[0].seek(0)
        objects[0].write("contents2")
        eq(self.ioctx.get_locator_key(), "")
        self.ioctx.set_locator_key("bar")
        contents = self.ioctx.read("foo")
        eq(contents, "contents2")
        eq(self.ioctx.get_locator_key(), "bar")
        objects[0].remove()
        objects = [i for i in self.ioctx.list_objects()]
        eq(objects, [])
        self.ioctx.set_locator_key("")

    def test_aio_write(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        comp = self.ioctx.aio_write("foo", "bar", 0, cb, cb)
        comp.wait_for_complete()
        comp.wait_for_safe()
        with lock:
            while count[0] < 2:
                lock.wait()
        eq(comp.get_return_value(), 0)
        contents = self.ioctx.read("foo")
        eq(contents, "bar")
        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_append(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        comp = self.ioctx.aio_write("foo", "bar", 0, cb, cb)
        comp2 = self.ioctx.aio_append("foo", "baz", cb, cb)
        comp.wait_for_complete()
        contents = self.ioctx.read("foo")
        eq(contents, "barbaz")
        with lock:
            while count[0] < 4:
                lock.wait()
        eq(comp.get_return_value(), 0)
        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_write_full(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        self.ioctx.aio_write("foo", "barbaz", 0, cb, cb)
        comp = self.ioctx.aio_write_full("foo", "bar", cb, cb)
        comp.wait_for_complete()
        comp.wait_for_safe()
        with lock:
            while count[0] < 2:
                lock.wait()
        eq(comp.get_return_value(), 0)
        contents = self.ioctx.read("foo")
        eq(contents, "bar")
        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_read(self):
        retval = [None]
        lock = threading.Condition()
        def cb(_, buf):
            with lock:
                retval[0] = buf
                lock.notify()
        self.ioctx.write("foo", "bar")
        self.ioctx.aio_read("foo", 3, 0, cb)
        with lock:
            while retval[0] is None:
                lock.wait()
        eq(retval[0], "bar")
        [i.remove() for i in self.ioctx.list_objects()]

class TestObject(object):

    def setUp(self):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        self.ioctx = self.rados.open_ioctx('test_pool')
        self.ioctx.write('foo', 'bar')
        self.object = Object(self.ioctx, 'foo')

    def tearDown(self):
        self.ioctx.close()
        self.rados.delete_pool('test_pool')
        self.rados.shutdown()

    def test_read(self):
        eq(self.object.read(3), 'bar')
        eq(self.object.read(100), '')

    def test_seek(self):
        self.object.write('blah')
        self.object.seek(0)
        eq(self.object.read(4), 'blah')
        self.object.seek(1)
        eq(self.object.read(3), 'lah')

    def test_write(self):
        self.object.write('barbaz')
        self.object.seek(0)
        eq(self.object.read(3), 'bar')
        eq(self.object.read(3), 'baz')
