from nose.tools import eq_ as eq, assert_raises
from rados import (Rados, Error, Object, ObjectExists, ObjectNotFound,
                   ANONYMOUS_AUID, ADMIN_AUID, LIBRADOS_ALL_NSPACES)
import threading
import json
import errno

def test_rados_init_error():
    assert_raises(Error, Rados, conffile='', rados_id='admin',
                  name='client.admin')
    assert_raises(Error, Rados, conffile='', name='invalid')
    assert_raises(Error, Rados, conffile='', name='bad.invalid')

def test_rados_init_type_error():
    assert_raises(TypeError, Rados, rados_id=u'admin')
    assert_raises(TypeError, Rados, rados_id=u'')
    assert_raises(TypeError, Rados, name=u'client.admin')
    assert_raises(TypeError, Rados, name=u'')
    assert_raises(TypeError, Rados, conffile=u'blah')
    assert_raises(TypeError, Rados, conffile=u'')
    assert_raises(TypeError, Rados, clusternaem=u'blah')
    assert_raises(TypeError, Rados, clustername=u'')

def test_rados_init():
    with Rados(conffile='', rados_id='admin'):
        pass
    with Rados(conffile='', name='client.admin'):
        pass
    with Rados(conffile='', name='client.admin'):
        pass
    with Rados(conffile='', name='client.admin'):
        pass

def test_ioctx_context_manager():
    with Rados(conffile='', rados_id='admin') as conn:
        with conn.open_ioctx('rbd') as ioctx:
            pass

class TestRados(object):

    def setUp(self):
        self.rados = Rados(conffile='')
        self.rados.conf_parse_env('FOO_DOES_NOT_EXIST_BLAHBLAH')
        self.rados.conf_parse_env()
        self.rados.connect()

        # Assume any pre-existing pools are the cluster's defaults
        self.default_pools = self.rados.list_pools()

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
        for p in self.default_pools:
            pools.remove(p)
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

    def test_get_pool_base_tier(self):
        self.rados.create_pool('foo')
        try:
            self.rados.create_pool('foo-cache')
            try:
                pool_id = self.rados.pool_lookup('foo')
                tier_pool_id = self.rados.pool_lookup('foo-cache')

                cmd = {"prefix":"osd tier add", "pool":"foo", "tierpool":"foo-cache", "force_nonempty":""}
                ret, buf, errs = self.rados.mon_command(json.dumps(cmd), '', timeout=30)
                eq(ret, 0)

                try:
                    cmd = {"prefix":"osd tier cache-mode", "pool":"foo-cache", "tierpool":"foo-cache", "mode":"readonly"}
                    ret, buf, errs = self.rados.mon_command(json.dumps(cmd), '', timeout=30)
                    eq(ret, 0)

                    eq(self.rados.wait_for_latest_osdmap(), 0)

                    eq(pool_id, self.rados.get_pool_base_tier(pool_id))
                    eq(pool_id, self.rados.get_pool_base_tier(tier_pool_id))
                finally:
                    cmd = {"prefix":"osd tier remove", "pool":"foo", "tierpool":"foo-cache"}
                    ret, buf, errs = self.rados.mon_command(json.dumps(cmd), '', timeout=30)
                    eq(ret, 0)
            finally:
                self.rados.delete_pool('foo-cache')
        finally:
            self.rados.delete_pool('foo')

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

    def test_append(self):
        self.ioctx.write('abc', 'a')
        self.ioctx.append('abc', 'b')
        self.ioctx.append('abc', 'c')
        eq(self.ioctx.read('abc'), 'abc')

    def test_write_zeros(self):
        self.ioctx.write('abc', 'a\0b\0c')
        eq(self.ioctx.read('abc'), 'a\0b\0c')

    def test_trunc(self):
        self.ioctx.write('abc', 'abc')
        self.ioctx.trunc('abc', 2)
        eq(self.ioctx.read('abc'), 'ab')
        size = self.ioctx.stat('abc')[0]
        eq(size, 2)

    def test_list_objects_empty(self):
        eq(list(self.ioctx.list_objects()), [])

    def test_list_objects(self):
        self.ioctx.write('a', '')
        self.ioctx.write('b', 'foo')
        self.ioctx.write_full('c', 'bar')
        self.ioctx.append('d', 'jazz')
        object_names = [obj.key for obj in self.ioctx.list_objects()]
        eq(sorted(object_names), ['a', 'b', 'c', 'd'])

    def test_list_ns_objects(self):
        self.ioctx.write('a', '')
        self.ioctx.write('b', 'foo')
        self.ioctx.write_full('c', 'bar')
        self.ioctx.append('d', 'jazz')
        self.ioctx.set_namespace("ns1")
        self.ioctx.write('ns1-a', '')
        self.ioctx.write('ns1-b', 'foo')
        self.ioctx.write_full('ns1-c', 'bar')
        self.ioctx.append('ns1-d', 'jazz')
        self.ioctx.append('d', 'jazz')
        self.ioctx.set_namespace(LIBRADOS_ALL_NSPACES)
        object_names = [(obj.nspace, obj.key) for obj in self.ioctx.list_objects()]
        eq(sorted(object_names), [('', 'a'), ('','b'), ('','c'), ('','d'),\
                ('ns1', 'd'), ('ns1', 'ns1-a'), ('ns1', 'ns1-b'),\
                ('ns1', 'ns1-c'), ('ns1', 'ns1-d')])

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

    def test_obj_xattrs(self):
        xattrs = dict(a='1', b='2', c='3', d='a\0b', e='\0')
        self.ioctx.write('abc', '')
        obj = list(self.ioctx.list_objects())[0]
        for key, value in xattrs.iteritems():
            obj.set_xattr(key, value)
            eq(obj.get_xattr(key), value)
        stored_xattrs = {}
        for key, value in obj.get_xattrs():
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
        eq(comp2.get_return_value(), 0)
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
        payload = "bar\000frob"
        self.ioctx.write("foo", payload)
        self.ioctx.aio_read("foo", len(payload), 0, cb)
        with lock:
            while retval[0] is None:
                lock.wait()
        eq(retval[0], payload)
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

class TestCommand(object):

    def setUp(self):
        self.rados = Rados(conffile='')
        self.rados.connect()

    def tearDown(self):
        self.rados.shutdown()

    def test_monmap_dump(self):

        # check for success and some plain output with epoch in it
        cmd = {"prefix":"mon dump"}
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), '', timeout=30)
        eq(ret, 0)
        assert len(buf) > 0
        assert('epoch' in buf)

        # JSON, and grab current epoch
        cmd['format'] = 'json'
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), '', timeout=30)
        eq(ret, 0)
        assert len(buf) > 0
        d = json.loads(buf)
        assert('epoch' in d)
        epoch = d['epoch']

        # assume epoch + 1000 does not exist; test for ENOENT
        cmd['epoch'] = epoch + 1000
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), '', timeout=30)
        eq(ret, -errno.ENOENT)
        eq(len(buf), 0)
        del cmd['epoch']

        # send to specific target by name
        target = d['mons'][0]['name']
        print target
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), '', timeout=30,
                                                target=target)
        eq(ret, 0)
        assert len(buf) > 0
        d = json.loads(buf)
        assert('epoch' in d)

        # and by rank
        target = d['mons'][0]['rank']
        print target
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), '', timeout=30,
                                                target=target)
        eq(ret, 0)
        assert len(buf) > 0
        d = json.loads(buf)
        assert('epoch' in d)

    def test_osd_bench(self):
        cmd = dict(prefix='bench', size=4096, count=8192)
        ret, buf, err = self.rados.osd_command(0, json.dumps(cmd), '',
                                               timeout=30)
        eq(ret, 0)
        assert len(err) > 0
        out = json.loads(err)
        eq(out['blocksize'], cmd['size'])
        eq(out['bytes_written'], cmd['count'])
