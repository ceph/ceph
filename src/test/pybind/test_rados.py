from __future__ import print_function
from nose.tools import eq_ as eq, ok_ as ok, assert_raises
from rados import (Rados, Error, RadosStateError, Object, ObjectExists,
                   ObjectNotFound, ObjectBusy, requires, opt,
                   ANONYMOUS_AUID, ADMIN_AUID, LIBRADOS_ALL_NSPACES, WriteOpCtx, ReadOpCtx,
                   LIBRADOS_SNAP_HEAD, MonitorLog)
import time
import threading
import json
import errno
import sys

# Are we running Python 2.x
_python2 = sys.hexversion < 0x03000000

def test_rados_init_error():
    assert_raises(Error, Rados, conffile='', rados_id='admin',
                  name='client.admin')
    assert_raises(Error, Rados, conffile='', name='invalid')
    assert_raises(Error, Rados, conffile='', name='bad.invalid')

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

def test_parse_argv():
    args = ['osd', 'pool', 'delete', 'foobar', 'foobar', '--yes-i-really-really-mean-it']
    r = Rados()
    eq(args, r.conf_parse_argv(args))

def test_parse_argv_empty_str():
    args = ['']
    r = Rados()
    eq(args, r.conf_parse_argv(args))

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
        assert_raises(RadosStateError, rados.mon_command, '', b'')
        assert_raises(RadosStateError, rados.osd_command, 0, '', b'')
        assert_raises(RadosStateError, rados.pg_command, '', '', b'')
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

    def test_ping_monitor(self):
        assert_raises(ObjectNotFound, self.rados.ping_monitor, 'not_exists_monitor')
        cmd = {'prefix': 'mon dump', 'format':'json'}
        ret, buf, out = self.rados.mon_command(json.dumps(cmd), b'')
        for mon in json.loads(buf.decode('utf8'))['mons']:
            buf = json.loads(self.rados.ping_monitor(mon['name']))
            assert buf.get('health')

    def test_create(self):
        self.rados.create_pool('foo')
        self.rados.delete_pool('foo')

    def test_create_utf8(self):
        if _python2:
            # Use encoded bytestring
            poolname = b"\351\273\204"
        else:
            poolname = "\u9ec4"
        self.rados.create_pool(poolname)
        assert self.rados.pool_exists(u"\u9ec4")
        self.rados.delete_pool(poolname)

    def test_pool_lookup_utf8(self):
        if _python2:
            poolname = u'\u9ec4'
        else:
            poolname = '\u9ec4'
        self.rados.create_pool(poolname)
        try:
            poolid = self.rados.pool_lookup(poolname)
            eq(poolname, self.rados.pool_reverse_lookup(poolid))
        finally:
            self.rados.delete_pool(poolname)

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
                ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30)
                eq(ret, 0)

                try:
                    cmd = {"prefix":"osd tier cache-mode", "pool":"foo-cache", "tierpool":"foo-cache", "mode":"readonly", "sure":"--yes-i-really-mean-it"}
                    ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30)
                    eq(ret, 0)

                    eq(self.rados.wait_for_latest_osdmap(), 0)

                    eq(pool_id, self.rados.get_pool_base_tier(pool_id))
                    eq(pool_id, self.rados.get_pool_base_tier(tier_pool_id))
                finally:
                    cmd = {"prefix":"osd tier remove", "pool":"foo", "tierpool":"foo-cache"}
                    ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30)
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

    def test_get_cluster_stats(self):
        stats = self.rados.get_cluster_stats()
        assert stats['kb'] > 0
        assert stats['kb_avail'] > 0
        assert stats['kb_used'] > 0
        assert stats['num_objects'] >= 0

    def test_monitor_log(self):
        lock = threading.Condition()
        def cb(arg, line, who, sec, nsec, seq, level, msg):
            # NOTE(sileht): the old pyrados API was received the pointer as int
            # instead of the value of arg
            eq(arg, "arg")
            with lock:
                lock.notify()
            return 0

        # NOTE(sileht): force don't save the monitor into local var
        # to ensure all references are correctly tracked into the lib
        MonitorLog(self.rados, "debug", cb, "arg")
        with lock:
            lock.wait()
        MonitorLog(self.rados, "debug", None, None)
        eq(None, self.rados.monitor_callback)

class TestIoctx(object):

    def setUp(self):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        self.ioctx = self.rados.open_ioctx('test_pool')

    def tearDown(self):
        cmd = {"prefix":"osd unset", "key":"noup"}
        self.rados.mon_command(json.dumps(cmd), b'')
        self.ioctx.close()
        self.rados.delete_pool('test_pool')
        self.rados.shutdown()

    def test_get_last_version(self):
        version = self.ioctx.get_last_version()
        assert version >= 0

    def test_get_stats(self):
        stats = self.ioctx.get_stats()
        eq(stats, {'num_objects_unfound': 0,
                   'num_objects_missing_on_primary': 0,
                   'num_object_clones': 0,
                   'num_objects': 0,
                   'num_object_copies': 0,
                   'num_bytes': 0,
                   'num_rd_kb': 0,
                   'num_wr_kb': 0,
                   'num_kb': 0,
                   'num_wr': 0,
                   'num_objects_degraded': 0,
                   'num_rd': 0})

    def test_change_auid(self):
        self.ioctx.change_auid(ANONYMOUS_AUID)
        self.ioctx.change_auid(ADMIN_AUID)

    def test_write(self):
        self.ioctx.write('abc', b'abc')
        eq(self.ioctx.read('abc'), b'abc')

    def test_write_full(self):
        self.ioctx.write('abc', b'abc')
        eq(self.ioctx.read('abc'), b'abc')
        self.ioctx.write_full('abc', b'd')
        eq(self.ioctx.read('abc'), b'd')

    def test_append(self):
        self.ioctx.write('abc', b'a')
        self.ioctx.append('abc', b'b')
        self.ioctx.append('abc', b'c')
        eq(self.ioctx.read('abc'), b'abc')

    def test_write_zeros(self):
        self.ioctx.write('abc', b'a\0b\0c')
        eq(self.ioctx.read('abc'), b'a\0b\0c')

    def test_trunc(self):
        self.ioctx.write('abc', b'abc')
        self.ioctx.trunc('abc', 2)
        eq(self.ioctx.read('abc'), b'ab')
        size = self.ioctx.stat('abc')[0]
        eq(size, 2)

    def test_list_objects_empty(self):
        eq(list(self.ioctx.list_objects()), [])

    def test_list_objects(self):
        self.ioctx.write('a', b'')
        self.ioctx.write('b', b'foo')
        self.ioctx.write_full('c', b'bar')
        self.ioctx.append('d', b'jazz')
        object_names = [obj.key for obj in self.ioctx.list_objects()]
        eq(sorted(object_names), ['a', 'b', 'c', 'd'])

    def test_list_ns_objects(self):
        self.ioctx.write('a', b'')
        self.ioctx.write('b', b'foo')
        self.ioctx.write_full('c', b'bar')
        self.ioctx.append('d', b'jazz')
        self.ioctx.set_namespace("ns1")
        self.ioctx.write('ns1-a', b'')
        self.ioctx.write('ns1-b', b'foo')
        self.ioctx.write_full('ns1-c', b'bar')
        self.ioctx.append('ns1-d', b'jazz')
        self.ioctx.append('d', b'jazz')
        self.ioctx.set_namespace(LIBRADOS_ALL_NSPACES)
        object_names = [(obj.nspace, obj.key) for obj in self.ioctx.list_objects()]
        eq(sorted(object_names), [('', 'a'), ('','b'), ('','c'), ('','d'),\
                ('ns1', 'd'), ('ns1', 'ns1-a'), ('ns1', 'ns1-b'),\
                ('ns1', 'ns1-c'), ('ns1', 'ns1-d')])

    def test_xattrs(self):
        xattrs = dict(a=b'1', b=b'2', c=b'3', d=b'a\0b', e=b'\0')
        self.ioctx.write('abc', b'')
        for key, value in xattrs.items():
            self.ioctx.set_xattr('abc', key, value)
            eq(self.ioctx.get_xattr('abc', key), value)
        stored_xattrs = {}
        for key, value in self.ioctx.get_xattrs('abc'):
            stored_xattrs[key] = value
        eq(stored_xattrs, xattrs)

    def test_obj_xattrs(self):
        xattrs = dict(a=b'1', b=b'2', c=b'3', d=b'a\0b', e=b'\0')
        self.ioctx.write('abc', b'')
        obj = list(self.ioctx.list_objects())[0]
        for key, value in xattrs.items():
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

    def test_snap_rollback(self):
        self.ioctx.write("insnap", b"contents1")
        self.ioctx.create_snap("snap1")
        self.ioctx.remove_object("insnap")
        self.ioctx.snap_rollback("insnap", "snap1")
        eq(self.ioctx.read("insnap"), b"contents1")
        self.ioctx.remove_snap("snap1")
        self.ioctx.remove_object("insnap")

    def test_snap_read(self):
        self.ioctx.write("insnap", b"contents1")
        self.ioctx.create_snap("snap1")
        self.ioctx.remove_object("insnap")
        snap = self.ioctx.lookup_snap("snap1")
        self.ioctx.set_read(snap.snap_id)
        eq(self.ioctx.read("insnap"), b"contents1")
        self.ioctx.set_read(LIBRADOS_SNAP_HEAD)
        self.ioctx.write("inhead", b"contents2")
        eq(self.ioctx.read("inhead"), b"contents2")
        self.ioctx.remove_snap("snap1")
        self.ioctx.remove_object("inhead")

    def test_set_omap(self):
        keys = ("1", "2", "3", "4")
        values = (b"aaa", b"bbb", b"ccc", b"\x04\x04\x04\x04")
        with WriteOpCtx(self.ioctx) as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            self.ioctx.operate_write_op(write_op, "hw")
        with ReadOpCtx(self.ioctx) as read_op:
            iter, ret = self.ioctx.get_omap_vals(read_op, "", "", 4)
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            next(iter)
            eq(list(iter), [("2", b"bbb"), ("3", b"ccc"), ("4", b"\x04\x04\x04\x04")])
        with ReadOpCtx(self.ioctx) as read_op:
            iter, ret = self.ioctx.get_omap_vals(read_op, "2", "", 4)
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(("3", b"ccc"), next(iter))
            eq(list(iter), [("4", b"\x04\x04\x04\x04")])
        with ReadOpCtx(self.ioctx) as read_op:
            iter, ret = self.ioctx.get_omap_vals(read_op, "", "2", 4)
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(list(iter), [("2", b"bbb")])

    def test_get_omap_vals_by_keys(self):
        keys = ("1", "2", "3", "4")
        values = (b"aaa", b"bbb", b"ccc", b"\x04\x04\x04\x04")
        with WriteOpCtx(self.ioctx) as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            self.ioctx.operate_write_op(write_op, "hw")
        with ReadOpCtx(self.ioctx) as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op,("3","4",))
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(list(iter), [("3", b"ccc"), ("4", b"\x04\x04\x04\x04")])
        with ReadOpCtx(self.ioctx) as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op,("3","4",))
            eq(ret, 0)
            with assert_raises(ObjectNotFound):
                self.ioctx.operate_read_op(read_op, "no_such")

    def test_get_omap_keys(self):
        keys = ("1", "2", "3")
        values = (b"aaa", b"bbb", b"ccc")
        with WriteOpCtx(self.ioctx) as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            self.ioctx.operate_write_op(write_op, "hw")
        with ReadOpCtx(self.ioctx) as read_op:
            iter, ret = self.ioctx.get_omap_keys(read_op,"",2)
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(list(iter), [("1", None), ("2", None)])
        with ReadOpCtx(self.ioctx) as read_op:
            iter, ret = self.ioctx.get_omap_keys(read_op,"",2)
            eq(ret, 0)
            with assert_raises(ObjectNotFound):
                self.ioctx.operate_read_op(read_op, "no_such")

    def test_clear_omap(self):
        keys = ("1", "2", "3")
        values = (b"aaa", b"bbb", b"ccc")
        with WriteOpCtx(self.ioctx) as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            self.ioctx.operate_write_op(write_op, "hw")
        with WriteOpCtx(self.ioctx) as write_op_1:
            self.ioctx.clear_omap(write_op_1)
            self.ioctx.operate_write_op(write_op_1, "hw")
        with ReadOpCtx(self.ioctx) as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op,("1",))
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(list(iter), [])

    def test_locator(self):
        self.ioctx.set_locator_key("bar")
        self.ioctx.write('foo', b'contents1')
        objects = [i for i in self.ioctx.list_objects()]
        eq(len(objects), 1)
        eq(self.ioctx.get_locator_key(), "bar")
        self.ioctx.set_locator_key("")
        objects[0].seek(0)
        objects[0].write(b"contents2")
        eq(self.ioctx.get_locator_key(), "")
        self.ioctx.set_locator_key("bar")
        contents = self.ioctx.read("foo")
        eq(contents, b"contents2")
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
        comp = self.ioctx.aio_write("foo", b"bar", 0, cb, cb)
        comp.wait_for_complete()
        comp.wait_for_safe()
        with lock:
            while count[0] < 2:
                lock.wait()
        eq(comp.get_return_value(), 0)
        contents = self.ioctx.read("foo")
        eq(contents, b"bar")
        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_write_no_comp_ref(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        # NOTE(sileht): force don't save the comp into local var
        # to ensure all references are correctly tracked into the lib
        self.ioctx.aio_write("foo", b"bar", 0, cb, cb)
        with lock:
            while count[0] < 2:
                lock.wait()
        contents = self.ioctx.read("foo")
        eq(contents, b"bar")
        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_append(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        comp = self.ioctx.aio_write("foo", b"bar", 0, cb, cb)
        comp2 = self.ioctx.aio_append("foo", b"baz", cb, cb)
        comp.wait_for_complete()
        contents = self.ioctx.read("foo")
        eq(contents, b"barbaz")
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
        self.ioctx.aio_write("foo", b"barbaz", 0, cb, cb)
        comp = self.ioctx.aio_write_full("foo", b"bar", cb, cb)
        comp.wait_for_complete()
        comp.wait_for_safe()
        with lock:
            while count[0] < 2:
                lock.wait()
        eq(comp.get_return_value(), 0)
        contents = self.ioctx.read("foo")
        eq(contents, b"bar")
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
        r, jsonout, _ = self.rados.mon_command(json.dumps(cmd), b'')
        objmap = json.loads(jsonout.decode("utf-8"))
        acting_set = objmap['acting']
        cmd = {"prefix":"osd set", "key":"noup"}
        r, _, _ = self.rados.mon_command(json.dumps(cmd), b'')
        eq(r, 0)
        cmd = {"prefix":"osd down", "ids":[str(i) for i in acting_set]}
        r, _, _ = self.rados.mon_command(json.dumps(cmd), b'')
        eq(r, 0)

        # wait for OSDs to acknowledge the down
        eq(self.rados.wait_for_latest_osdmap(), 0)

    def _let_osds_back_up(self):
        cmd = {"prefix":"osd unset", "key":"noup"}
        r, _, _ = self.rados.mon_command(json.dumps(cmd), b'')
        eq(r, 0)

    def test_aio_read(self):
        # this is a list so that the local cb() can modify it
        retval = [None]
        lock = threading.Condition()
        def cb(_, buf):
            with lock:
                retval[0] = buf
                lock.notify()
        payload = b"bar\000frob"
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
        eq(sys.getrefcount(comp), 2)

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
        eq(sys.getrefcount(comp), 2)

        # test3: error case, use wait_for_complete_and_cb(), verify retval[0] is
        # set by the time we regain control

        retval[0] = 1
        self._take_down_acting_set('test_pool', 'bar')
        comp = self.ioctx.aio_read("bar", len(payload), 0, cb)
        eq(False, comp.is_complete())
        time.sleep(3)
        eq(False, comp.is_complete())
        with lock:
            eq(1, retval[0])
        self._let_osds_back_up()

        comp.wait_for_complete_and_cb()
        eq(None, retval[0])
        assert(comp.get_return_value() < 0)
        eq(sys.getrefcount(comp), 2)

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

    def test_execute(self):
        self.ioctx.write("foo", b"") # ensure object exists

        ret, buf = self.ioctx.execute("foo", "hello", "say_hello", b"")
        eq(buf, b"Hello, world!")

        ret, buf = self.ioctx.execute("foo", "hello", "say_hello", b"nose")
        eq(buf, b"Hello, nose!")

class TestObject(object):

    def setUp(self):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        self.ioctx = self.rados.open_ioctx('test_pool')
        self.ioctx.write('foo', b'bar')
        self.object = Object(self.ioctx, 'foo')

    def tearDown(self):
        self.ioctx.close()
        self.rados.delete_pool('test_pool')
        self.rados.shutdown()

    def test_read(self):
        eq(self.object.read(3), b'bar')
        eq(self.object.read(100), b'')

    def test_seek(self):
        self.object.write(b'blah')
        self.object.seek(0)
        eq(self.object.read(4), b'blah')
        self.object.seek(1)
        eq(self.object.read(3), b'lah')

    def test_write(self):
        self.object.write(b'barbaz')
        self.object.seek(0)
        eq(self.object.read(3), b'bar')
        eq(self.object.read(3), b'baz')

class TestCommand(object):

    def setUp(self):
        self.rados = Rados(conffile='')
        self.rados.connect()

    def tearDown(self):
        self.rados.shutdown()

    def test_monmap_dump(self):

        # check for success and some plain output with epoch in it
        cmd = {"prefix":"mon dump"}
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30)
        eq(ret, 0)
        assert len(buf) > 0
        assert(b'epoch' in buf)

        # JSON, and grab current epoch
        cmd['format'] = 'json'
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30)
        eq(ret, 0)
        assert len(buf) > 0
        d = json.loads(buf.decode("utf-8"))
        assert('epoch' in d)
        epoch = d['epoch']

        # assume epoch + 1000 does not exist; test for ENOENT
        cmd['epoch'] = epoch + 1000
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30)
        eq(ret, -errno.ENOENT)
        eq(len(buf), 0)
        del cmd['epoch']

        # send to specific target by name
        target = d['mons'][0]['name']
        print(target)
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30,
                                                target=target)
        eq(ret, 0)
        assert len(buf) > 0
        d = json.loads(buf.decode("utf-8"))
        assert('epoch' in d)

        # and by rank
        target = d['mons'][0]['rank']
        print(target)
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30,
                                                target=target)
        eq(ret, 0)
        assert len(buf) > 0
        d = json.loads(buf.decode("utf-8"))
        assert('epoch' in d)

    def test_osd_bench(self):
        cmd = dict(prefix='bench', size=4096, count=8192)
        ret, buf, err = self.rados.osd_command(0, json.dumps(cmd), b'',
                                               timeout=30)
        eq(ret, 0)
        assert len(err) > 0
        out = json.loads(err)
        eq(out['blocksize'], cmd['size'])
        eq(out['bytes_written'], cmd['count'])

    def test_ceph_osd_pool_create_utf8(self):
        if _python2:
            # Use encoded bytestring
            poolname = b"\351\273\205"
        else:
            poolname = "\u9ec5"

        cmd = {"prefix": "osd pool create", "pg_num": 16, "pool": poolname}
        ret, buf, out = self.rados.mon_command(json.dumps(cmd), b'')
        eq(ret, 0)
        assert len(out) > 0
        eq(u"pool '\u9ec5' created", out)
