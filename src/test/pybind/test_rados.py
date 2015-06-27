from nose.tools import eq_ as eq, ok_ as ok, assert_raises
from rados import (Rados, Error, RadosStateError, Object, ObjectExists,
                   ObjectNotFound, ObjectBusy, requires, opt,
                   ANONYMOUS_AUID, ADMIN_AUID, LIBRADOS_ALL_NSPACES)
import time
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

class TestRequires(object):
    @requires(('foo', str), ('bar', int), ('baz', int))
    def _method_plain(self, foo, bar, baz):
        ok(isinstance(foo, str))
        ok(isinstance(bar, int))
        ok(isinstance(baz, int))
        return (foo, bar, baz)

    def test_method_plain(self):
        assert_raises(TypeError, self._method_plain, 42, 42, 42)
        assert_raises(TypeError, self._method_plain, '42', '42', '42')
        assert_raises(TypeError, self._method_plain, foo='42', bar='42', baz='42')
        eq(self._method_plain('42', 42, 42), ('42', 42, 42))
        eq(self._method_plain(foo='42', bar=42, baz=42), ('42', 42, 42))

    @requires(('opt_foo', opt(str)), ('opt_bar', opt(int)), ('baz', int))
    def _method_with_opt_arg(self, foo, bar, baz):
        ok(isinstance(foo, str) or foo is None)
        ok(isinstance(bar, int) or bar is None)
        ok(isinstance(baz, int))
        return (foo, bar, baz)

    def test_method_with_opt_args(self):
        assert_raises(TypeError, self._method_with_opt_arg, 42, 42, 42)
        assert_raises(TypeError, self._method_with_opt_arg, '42', '42', 42)
        assert_raises(TypeError, self._method_with_opt_arg, None, None, None)
        eq(self._method_with_opt_arg(None, 42, 42), (None, 42, 42))
        eq(self._method_with_opt_arg('42', None, 42), ('42', None, 42))
        eq(self._method_with_opt_arg(None, None, 42), (None, None, 42))


class TestRadosStateError(object):
    def _requires_configuring(self, rados):
        assert_raises(RadosStateError, rados.connect)

    def _requires_configuring_or_connected(self, rados):
        assert_raises(RadosStateError, rados.conf_read_file)
        assert_raises(RadosStateError, rados.conf_parse_argv, None)
        assert_raises(RadosStateError, rados.conf_parse_env)
        assert_raises(RadosStateError, rados.conf_get, 'opt')
        assert_raises(RadosStateError, rados.conf_set, 'opt', 'val')
        assert_raises(RadosStateError, rados.ping_monitor, 0)

    def _requires_connected(self, rados):
        assert_raises(RadosStateError, rados.pool_exists, 'foo')
        assert_raises(RadosStateError, rados.pool_lookup, 'foo')
        assert_raises(RadosStateError, rados.pool_reverse_lookup, 0)
        assert_raises(RadosStateError, rados.create_pool, 'foo')
        assert_raises(RadosStateError, rados.get_pool_base_tier, 0)
        assert_raises(RadosStateError, rados.delete_pool, 'foo')
        assert_raises(RadosStateError, rados.list_pools)
        assert_raises(RadosStateError, rados.get_fsid)
        assert_raises(RadosStateError, rados.open_ioctx, 'foo')
        assert_raises(RadosStateError, rados.mon_command, '', '')
        assert_raises(RadosStateError, rados.osd_command, 0, '', '')
        assert_raises(RadosStateError, rados.pg_command, '', '', '')
        assert_raises(RadosStateError, rados.wait_for_latest_osdmap)
        assert_raises(RadosStateError, rados.blacklist_add, '127.0.0.1/123', 0)

    def test_configuring(self):
        rados = Rados(conffile='')
        eq('configuring', rados.state)
        self._requires_connected(rados)

    def test_connected(self):
        rados = Rados(conffile='')
        with rados:
            eq('connected', rados.state)
            self._requires_configuring(rados)

    def test_shutdown(self):
        rados = Rados(conffile='')
        with rados:
            pass
        eq('shutdown', rados.state)
        self._requires_configuring(rados)
        self._requires_configuring_or_connected(rados)
        self._requires_connected(rados)


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

    def test_blacklist_add(self):
        self.rados.blacklist_add("1.2.3.4/123", 1)

class TestIoctx(object):

    def setUp(self):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        self.ioctx = self.rados.open_ioctx('test_pool')

    def tearDown(self):
        cmd = {"prefix":"osd unset", "key":"noup"}
        self.rados.mon_command(json.dumps(cmd), '')
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

    def _take_down_acting_set(self, pool, objectname):
        # find acting_set for pool:objectname and take it down; used to
        # verify that async reads don't complete while acting set is missing
        cmd = {
            "prefix":"osd map",
            "pool":pool,
            "object":objectname,
            "format":"json",
        }
        r, jsonout, _ = self.rados.mon_command(json.dumps(cmd), '')
        objmap = json.loads(jsonout)
        acting_set = objmap['acting']
        cmd = {"prefix":"osd set", "key":"noup"}
        r, _, _ = self.rados.mon_command(json.dumps(cmd), '')
        eq(r, 0)
        cmd = {"prefix":"osd down", "ids":[str(i) for i in acting_set]}
        r, _, _ = self.rados.mon_command(json.dumps(cmd), '')
        eq(r, 0)

        # wait for OSDs to acknowledge the down
        eq(self.rados.wait_for_latest_osdmap(), 0)

    def _let_osds_back_up(self):
        cmd = {"prefix":"osd unset", "key":"noup"}
        r, _, _ = self.rados.mon_command(json.dumps(cmd), '')
        eq(r, 0)

    def test_aio_read(self):
        # this is a list so that the local cb() can modify it
        retval = [None]
        lock = threading.Condition()
        def cb(_, buf):
            with lock:
                retval[0] = buf
                lock.notify()
        payload = "bar\000frob"
        self.ioctx.write("foo", payload)

        # test1: use wait_for_complete() and wait for cb by
        # watching retval[0]
        self._take_down_acting_set('test_pool', 'foo')
        comp = self.ioctx.aio_read("foo", len(payload), 0, cb)
        eq(False, comp.is_complete())
        time.sleep(3)
        eq(False, comp.is_complete())
        with lock:
            eq(None, retval[0])
        self._let_osds_back_up()
        comp.wait_for_complete()
        loops = 0
        with lock:
            while retval[0] is None and loops <= 10:
                lock.wait(timeout=5)
                loops += 1
        assert(loops <= 10)

        eq(retval[0], payload)

        # test2: use wait_for_complete_and_cb(), verify retval[0] is
        # set by the time we regain control

        retval[0] = None
        self._take_down_acting_set('test_pool', 'foo')
        comp = self.ioctx.aio_read("foo", len(payload), 0, cb)
        eq(False, comp.is_complete())
        time.sleep(3)
        eq(False, comp.is_complete())
        with lock:
            eq(None, retval[0])
        self._let_osds_back_up()

        comp.wait_for_complete_and_cb()
        assert(retval[0] is not None)
        eq(retval[0], payload)

        [i.remove() for i in self.ioctx.list_objects()]

    def test_lock(self):
        self.ioctx.lock_exclusive("foo", "lock", "locker", "desc_lock",
                                  10000, 0)
        assert_raises(ObjectExists,
                      self.ioctx.lock_exclusive,
                      "foo", "lock", "locker", "desc_lock", 10000, 0)
        self.ioctx.unlock("foo", "lock", "locker")
        assert_raises(ObjectNotFound, self.ioctx.unlock, "foo", "lock", "locker")

        self.ioctx.lock_shared("foo", "lock", "locker1", "tag", "desc_lock",
                               10000, 0)
        self.ioctx.lock_shared("foo", "lock", "locker2", "tag", "desc_lock",
                               10000, 0)
        assert_raises(ObjectBusy,
                      self.ioctx.lock_exclusive,
                      "foo", "lock", "locker3", "desc_lock", 10000, 0)
        self.ioctx.unlock("foo", "lock", "locker1")
        self.ioctx.unlock("foo", "lock", "locker2")
        assert_raises(ObjectNotFound, self.ioctx.unlock, "foo", "lock", "locker1")
        assert_raises(ObjectNotFound, self.ioctx.unlock, "foo", "lock", "locker2")


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
