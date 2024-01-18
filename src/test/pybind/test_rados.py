from __future__ import print_function
from assertions import assert_equal as eq, assert_raises
from rados import (Rados, Error, RadosStateError, Object, ObjectExists,
                   ObjectNotFound, ObjectBusy, NotConnected,
                   LIBRADOS_ALL_NSPACES, WriteOpCtx, ReadOpCtx, LIBRADOS_CREATE_EXCLUSIVE,
                   LIBRADOS_CMPXATTR_OP_EQ, LIBRADOS_CMPXATTR_OP_GT, LIBRADOS_CMPXATTR_OP_LT, OSError,
                   LIBRADOS_SNAP_HEAD, LIBRADOS_OPERATION_BALANCE_READS, LIBRADOS_OPERATION_SKIPRWLOCKS, MonitorLog, MAX_ERRNO, NoData, ExtendMismatch)
from datetime import timedelta
import time
import threading
import json
import errno
import os
import pytest
import re
import sys

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

class TestRadosStateError(object):
    def _requires_configuring(self, rados):
        assert_raises(RadosStateError, rados.connect)

    def _requires_configuring_or_connected(self, rados):
        assert_raises(RadosStateError, rados.conf_read_file)
        assert_raises(RadosStateError, rados.conf_parse_argv, None)
        assert_raises(RadosStateError, rados.conf_parse_env)
        assert_raises(RadosStateError, rados.conf_get, 'opt')
        assert_raises(RadosStateError, rados.conf_set, 'opt', 'val')
        assert_raises(RadosStateError, rados.ping_monitor, '0')

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
        assert_raises(RadosStateError, rados.blocklist_add, '127.0.0.1/123', 0)

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

    def setup_method(self, method):
        self.rados = Rados(conffile='')
        self.rados.conf_parse_env('FOO_DOES_NOT_EXIST_BLAHBLAH')
        self.rados.conf_parse_env()
        self.rados.connect()

        # Assume any pre-existing pools are the cluster's defaults
        self.default_pools = self.rados.list_pools()

    def teardown_method(self, method):
        self.rados.shutdown()

    def test_ping_monitor(self):
        assert_raises(ObjectNotFound, self.rados.ping_monitor, 'not_exists_monitor')
        cmd = {'prefix': 'mon dump', 'format':'json'}
        ret, buf, out = self.rados.mon_command(json.dumps(cmd), b'')
        for mon in json.loads(buf.decode('utf8'))['mons']:
            while True:
                output = self.rados.ping_monitor(mon['name'])
                if output is None:
                    continue
                buf = json.loads(output)
                if buf.get('health'):
                    break

    def test_annotations(self):
        with pytest.raises(TypeError):
            self.rados.create_pool(0xf00)

    def test_create(self):
        self.rados.create_pool('foo')
        self.rados.delete_pool('foo')

    def test_create_utf8(self):
        poolname = "\u9ec4"
        self.rados.create_pool(poolname)
        assert self.rados.pool_exists(u"\u9ec4")
        self.rados.delete_pool(poolname)

    def test_pool_lookup_utf8(self):
        poolname = '\u9ec4'
        self.rados.create_pool(poolname)
        try:
            poolid = self.rados.pool_lookup(poolname)
            eq(poolname, self.rados.pool_reverse_lookup(poolid))
        finally:
            self.rados.delete_pool(poolname)

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

    @pytest.mark.tier
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
                    cmd = {"prefix":"osd tier cache-mode", "pool":"foo-cache", "tierpool":"foo-cache", "mode":"readonly", "yes_i_really_mean_it": True}
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
        assert re.match('[0-9a-f\-]{36}', fsid, re.I)

    def test_blocklist_add(self):
        self.rados.blocklist_add("1.2.3.4/123", 1)

    @pytest.mark.stats
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

    def setup_method(self, method):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        self.ioctx = self.rados.open_ioctx('test_pool')

    def teardown_method(self, method):
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

    def test_write(self):
        self.ioctx.write('abc', b'abc')
        eq(self.ioctx.read('abc'), b'abc')

    def test_write_full(self):
        self.ioctx.write('abc', b'abc')
        eq(self.ioctx.read('abc'), b'abc')
        self.ioctx.write_full('abc', b'd')
        eq(self.ioctx.read('abc'), b'd')

    def test_writesame(self):
        self.ioctx.writesame('ob', b'rzx', 9)
        eq(self.ioctx.read('ob'), b'rzxrzxrzx')

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

    def test_cmpext(self):
        self.ioctx.write('test_object', b'abcdefghi')
        eq(0, self.ioctx.cmpext('test_object', b'abcdefghi', 0))
        eq(-MAX_ERRNO - 4, self.ioctx.cmpext('test_object', b'abcdxxxxx', 0))

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
        xattrs = dict(a=b'1', b=b'2', c=b'3', d=b'a\0b', e=b'\0', f=b'')
        self.ioctx.write('abc', b'')
        for key, value in xattrs.items():
            self.ioctx.set_xattr('abc', key, value)
            eq(self.ioctx.get_xattr('abc', key), value)
        stored_xattrs = {}
        for key, value in self.ioctx.get_xattrs('abc'):
            stored_xattrs[key] = value
        eq(stored_xattrs, xattrs)

    def test_obj_xattrs(self):
        xattrs = dict(a=b'1', b=b'2', c=b'3', d=b'a\0b', e=b'\0', f=b'')
        self.ioctx.write('abc', b'')
        obj = list(self.ioctx.list_objects())[0]
        for key, value in xattrs.items():
            obj.set_xattr(key, value)
            eq(obj.get_xattr(key), value)
        stored_xattrs = {}
        for key, value in obj.get_xattrs():
            stored_xattrs[key] = value
        eq(stored_xattrs, xattrs)

    def test_get_pool_id(self):
        eq(self.ioctx.get_pool_id(), self.rados.pool_lookup('test_pool'))

    def test_get_pool_name(self):
        eq(self.ioctx.get_pool_name(), 'test_pool')

    @pytest.mark.snap
    def test_create_snap(self):
        assert_raises(ObjectNotFound, self.ioctx.remove_snap, 'foo')
        self.ioctx.create_snap('foo')
        self.ioctx.remove_snap('foo')

    @pytest.mark.snap
    def test_list_snaps_empty(self):
        eq(list(self.ioctx.list_snaps()), [])

    @pytest.mark.snap
    def test_list_snaps(self):
        snaps = ['snap1', 'snap2', 'snap3']
        for snap in snaps:
            self.ioctx.create_snap(snap)
        listed_snaps = [snap.name for snap in self.ioctx.list_snaps()]
        eq(snaps, listed_snaps)

    @pytest.mark.snap
    def test_lookup_snap(self):
        self.ioctx.create_snap('foo')
        snap = self.ioctx.lookup_snap('foo')
        eq(snap.name, 'foo')

    @pytest.mark.snap
    def test_snap_timestamp(self):
        self.ioctx.create_snap('foo')
        snap = self.ioctx.lookup_snap('foo')
        snap.get_timestamp()

    @pytest.mark.snap
    def test_remove_snap(self):
        self.ioctx.create_snap('foo')
        (snap,) = self.ioctx.list_snaps()
        eq(snap.name, 'foo')
        self.ioctx.remove_snap('foo')
        eq(list(self.ioctx.list_snaps()), [])

    @pytest.mark.snap
    def test_snap_rollback(self):
        self.ioctx.write("insnap", b"contents1")
        self.ioctx.create_snap("snap1")
        self.ioctx.remove_object("insnap")
        self.ioctx.snap_rollback("insnap", "snap1")
        eq(self.ioctx.read("insnap"), b"contents1")
        self.ioctx.remove_snap("snap1")
        self.ioctx.remove_object("insnap")

    @pytest.mark.snap
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
        with WriteOpCtx() as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            write_op.set_flags(LIBRADOS_OPERATION_SKIPRWLOCKS)
            self.ioctx.operate_write_op(write_op, "hw")
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals(read_op, "", "", 4)
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            next(iter)
            eq(list(iter), [("2", b"bbb"), ("3", b"ccc"), ("4", b"\x04\x04\x04\x04")])
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals(read_op, "2", "", 4)
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(("3", b"ccc"), next(iter))
            eq(list(iter), [("4", b"\x04\x04\x04\x04")])
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals(read_op, "", "2", 4)
            eq(ret, 0)
            read_op.set_flags(LIBRADOS_OPERATION_BALANCE_READS)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(list(iter), [("2", b"bbb")])

    def test_set_omap_aio(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0

        keys = ("1", "2", "3", "4")
        values = (b"aaa", b"bbb", b"ccc", b"\x04\x04\x04\x04")
        with WriteOpCtx() as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            comp = self.ioctx.operate_aio_write_op(write_op, "hw", cb, cb)
            comp.wait_for_complete()
            with lock:
                while count[0] < 2:
                    lock.wait()
            eq(comp.get_return_value(), 0)

        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals(read_op, "", "", 4)
            eq(ret, 0)
            comp = self.ioctx.operate_aio_read_op(read_op, "hw", cb, cb)
            comp.wait_for_complete()
            with lock:
                while count[0] < 4:
                    lock.wait()
            eq(comp.get_return_value(), 0)
            next(iter)
            eq(list(iter), [("2", b"bbb"), ("3", b"ccc"), ("4", b"\x04\x04\x04\x04")])

    def test_write_ops(self):
        with WriteOpCtx() as write_op:
            write_op.new(0)
            self.ioctx.operate_write_op(write_op, "write_ops")
            eq(self.ioctx.read('write_ops'), b'')

            write_op.write_full(b'1')
            write_op.append(b'2')
            self.ioctx.operate_write_op(write_op, "write_ops")
            eq(self.ioctx.read('write_ops'), b'12')

            write_op.write_full(b'12345')
            write_op.write(b'x', 2)
            self.ioctx.operate_write_op(write_op, "write_ops")
            eq(self.ioctx.read('write_ops'), b'12x45')

            write_op.write_full(b'12345')
            write_op.zero(2, 2)
            self.ioctx.operate_write_op(write_op, "write_ops")
            eq(self.ioctx.read('write_ops'), b'12\x00\x005')

            write_op.write_full(b'12345')
            write_op.truncate(2)
            self.ioctx.operate_write_op(write_op, "write_ops")
            eq(self.ioctx.read('write_ops'), b'12')

            write_op.remove()
            self.ioctx.operate_write_op(write_op, "write_ops")
            with pytest.raises(ObjectNotFound):
                self.ioctx.read('write_ops')

    def test_execute_op(self):
        with WriteOpCtx() as write_op:
            write_op.execute("hello", "record_hello", b"ebs")
            self.ioctx.operate_write_op(write_op, "object")
        eq(self.ioctx.read('object'), b"Hello, ebs!")

    def test_writesame_op(self):
        with WriteOpCtx() as write_op:
            write_op.writesame(b'rzx', 9)
            self.ioctx.operate_write_op(write_op, 'abc')
            eq(self.ioctx.read('abc'), b'rzxrzxrzx')

    def test_get_omap_vals_by_keys(self):
        keys = ("1", "2", "3", "4")
        values = (b"aaa", b"bbb", b"ccc", b"\x04\x04\x04\x04")
        with WriteOpCtx() as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            self.ioctx.operate_write_op(write_op, "hw")
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op,("3","4",))
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(list(iter), [("3", b"ccc"), ("4", b"\x04\x04\x04\x04")])
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op,("3","4",))
            eq(ret, 0)
            with pytest.raises(ObjectNotFound):
                self.ioctx.operate_read_op(read_op, "no_such")

    def test_get_omap_keys(self):
        keys = ("1", "2", "3")
        values = (b"aaa", b"bbb", b"ccc")
        with WriteOpCtx() as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            self.ioctx.operate_write_op(write_op, "hw")
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_keys(read_op,"",2)
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(list(iter), [("1", None), ("2", None)])
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_keys(read_op,"",2)
            eq(ret, 0)
            with pytest.raises(ObjectNotFound):
                self.ioctx.operate_read_op(read_op, "no_such")

    def test_clear_omap(self):
        keys = ("1", "2", "3")
        values = (b"aaa", b"bbb", b"ccc")
        with WriteOpCtx() as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            self.ioctx.operate_write_op(write_op, "hw")
        with WriteOpCtx() as write_op_1:
            self.ioctx.clear_omap(write_op_1)
            self.ioctx.operate_write_op(write_op_1, "hw")
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op,("1",))
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "hw")
            eq(list(iter), [])

    def test_remove_omap_ramge2(self):
        keys = ("1", "2", "3", "4")
        values = (b"a", b"bb", b"ccc", b"dddd")
        with WriteOpCtx() as write_op:
            self.ioctx.set_omap(write_op, keys, values)
            self.ioctx.operate_write_op(write_op, "test_obj")
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op, keys)
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "test_obj")
            eq(list(iter), list(zip(keys, values)))
        with WriteOpCtx() as write_op:
            self.ioctx.remove_omap_range2(write_op, "1", "4")
            self.ioctx.operate_write_op(write_op, "test_obj")
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op, keys)
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, "test_obj")
            eq(list(iter), [("4", b"dddd")])

    def test_omap_cmp(self):
        object_id = 'test'
        self.ioctx.write(object_id, b'omap_cmp')
        with WriteOpCtx() as write_op:
            self.ioctx.set_omap(write_op, ('key1',), ('1',))
            self.ioctx.operate_write_op(write_op, object_id)
        with WriteOpCtx() as write_op:
            write_op.omap_cmp('key1', '1', LIBRADOS_CMPXATTR_OP_EQ)
            self.ioctx.set_omap(write_op, ('key1',), ('2',))
            self.ioctx.operate_write_op(write_op, object_id)
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op, ('key1',))
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, object_id)
            eq(list(iter), [('key1', b'2')])
        with WriteOpCtx() as write_op:
            write_op.omap_cmp('key1', '1', LIBRADOS_CMPXATTR_OP_GT)
            self.ioctx.set_omap(write_op, ('key1',), ('3',))
            self.ioctx.operate_write_op(write_op, object_id)
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op, ('key1',))
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, object_id)
            eq(list(iter), [('key1', b'3')])
        with WriteOpCtx() as write_op:
            write_op.omap_cmp('key1', '4', LIBRADOS_CMPXATTR_OP_LT)
            self.ioctx.set_omap(write_op, ('key1',), ('4',))
            self.ioctx.operate_write_op(write_op, object_id)
        with ReadOpCtx() as read_op:
            iter, ret = self.ioctx.get_omap_vals_by_keys(read_op, ('key1',))
            eq(ret, 0)
            self.ioctx.operate_read_op(read_op, object_id)
            eq(list(iter), [('key1', b'4')])
        with WriteOpCtx() as write_op:
            write_op.omap_cmp('key1', '1', LIBRADOS_CMPXATTR_OP_EQ)
            self.ioctx.set_omap(write_op, ('key1',), ('5',))
            try:
                self.ioctx.operate_write_op(write_op, object_id)
            except (OSError, ExtendMismatch) as e:
                eq(e.errno, 125)
            else:
                message = "omap_cmp did not raise Exception when omap content does not match"
                raise AssertionError(message)

    def test_cmpext_op(self):
        object_id = 'test'
        with WriteOpCtx() as write_op:
            write_op.write(b'12345', 0)
            self.ioctx.operate_write_op(write_op, object_id)
        with WriteOpCtx() as write_op:
            write_op.cmpext(b'12345', 0)
            write_op.write(b'54321', 0)
            self.ioctx.operate_write_op(write_op, object_id)
            eq(self.ioctx.read(object_id), b'54321')
        with WriteOpCtx() as write_op:
            write_op.cmpext(b'56789', 0)
            write_op.write(b'12345', 0)
            try:
                self.ioctx.operate_write_op(write_op, object_id)
            except ExtendMismatch as e:
                # the cmpext_result compare with expected error number, it should be (-MAX_ERRNO - 1)
                # where "1" is the offset of the first unmatched byte
                eq(-e.errno, -MAX_ERRNO - 1)
                eq(e.offset, 1)
            else:
                message = "cmpext did not raise Exception when object content does not match"
                raise AssertionError(message)
        with ReadOpCtx() as read_op:
            read_op.cmpext(b'54321', 0)
            self.ioctx.operate_read_op(read_op, object_id)
        with ReadOpCtx() as read_op:
            read_op.cmpext(b'54789', 0)
            try:
                self.ioctx.operate_read_op(read_op, object_id)
            except ExtendMismatch as e:
                # the cmpext_result compare with expected error number, it should be (-MAX_ERRNO - 2)
                # where "2" is the offset of the first unmatched byte
                eq(-e.errno, -MAX_ERRNO - 2)
                eq(e.offset, 2)
            else:
                message = "cmpext did not raise Exception when object content does not match"
                raise AssertionError(message)

    def test_xattrs_op(self):
        xattrs = dict(a=b'1', b=b'2', c=b'3', d=b'a\0b', e=b'\0')
        with WriteOpCtx() as write_op:
            write_op.new(LIBRADOS_CREATE_EXCLUSIVE)
            for key, value in xattrs.items():
                write_op.set_xattr(key, value)
                self.ioctx.operate_write_op(write_op, 'abc')
                eq(self.ioctx.get_xattr('abc', key), value)

            stored_xattrs_1 = {}
            for key, value in self.ioctx.get_xattrs('abc'):
                stored_xattrs_1[key] = value
            eq(stored_xattrs_1, xattrs)

            for key in xattrs.keys():
                write_op.rm_xattr(key)
                self.ioctx.operate_write_op(write_op, 'abc')
            stored_xattrs_2 = {}
            for key, value in self.ioctx.get_xattrs('abc'):
                stored_xattrs_2[key] = value
            eq(stored_xattrs_2, {})

            write_op.remove()
            self.ioctx.operate_write_op(write_op, 'abc')

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

    def test_operate_aio_write_op(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        with WriteOpCtx() as write_op:
            write_op.write(b'rzx')
            comp = self.ioctx.operate_aio_write_op(write_op, "object", cb, cb)
            comp.wait_for_complete()
            with lock:
                while count[0] < 2:
                    lock.wait()
            eq(comp.get_return_value(), 0)
            eq(self.ioctx.read('object'), b'rzx')

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
        with lock:
            while count[0] < 2:
                lock.wait()
        eq(comp.get_return_value(), 0)
        contents = self.ioctx.read("foo")
        eq(contents, b"bar")
        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_cmpext(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0

        self.ioctx.write('test_object', b'abcdefghi')
        comp = self.ioctx.aio_cmpext('test_object', b'abcdefghi', 0, cb)
        comp.wait_for_complete()
        with lock:
            while count[0] < 1:
                lock.wait()
        eq(comp.get_return_value(), 0)

    def test_aio_rmxattr(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        self.ioctx.set_xattr("xyz", "key", b'value')
        eq(self.ioctx.get_xattr("xyz", "key"), b'value')
        comp = self.ioctx.aio_rmxattr("xyz", "key", cb)
        comp.wait_for_complete()
        with lock:
            while count[0] < 1:
                lock.wait()
        eq(comp.get_return_value(), 0)
        with pytest.raises(NoData):
            self.ioctx.get_xattr("xyz", "key")

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
        with lock:
            while count[0] < 2:
                lock.wait()
        eq(comp.get_return_value(), 0)
        contents = self.ioctx.read("foo")
        eq(contents, b"bar")
        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_writesame(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        comp = self.ioctx.aio_writesame("abc", b"rzx", 9, 0, cb)
        comp.wait_for_complete()
        with lock:
            while count[0] < 1:
                lock.wait()
        eq(comp.get_return_value(), 0)
        eq(self.ioctx.read("abc"), b"rzxrzxrzx")
        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_stat(self):
        lock = threading.Condition()
        count = [0]
        def cb(_, size, mtime):
            with lock:
                count[0] += 1
                lock.notify()

        comp = self.ioctx.aio_stat("foo", cb)
        comp.wait_for_complete()
        with lock:
            while count[0] < 1:
                lock.wait()
        eq(comp.get_return_value(), -2)

        self.ioctx.write("foo", b"bar")

        comp = self.ioctx.aio_stat("foo", cb)
        comp.wait_for_complete()
        with lock:
            while count[0] < 2:
                lock.wait()
        eq(comp.get_return_value(), 0)

        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_remove(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        self.ioctx.write('foo', b'wrx')
        eq(self.ioctx.read('foo'), b'wrx')
        comp = self.ioctx.aio_remove('foo', cb, cb)
        comp.wait_for_complete()
        with lock:
            while count[0] < 2:
                lock.wait()
        eq(comp.get_return_value(), 0)
        eq(list(self.ioctx.list_objects()), [])

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

    def test_aio_read_wait_for_complete(self):
        # use wait_for_complete() and wait for cb by
        # watching retval[0]

        # this is a list so that the local cb() can modify it
        payload = b"bar\000frob"
        self.ioctx.write("foo", payload)
        self._take_down_acting_set('test_pool', 'foo')

        retval = [None]
        lock = threading.Condition()
        def cb(_, buf):
            with lock:
                retval[0] = buf
                lock.notify()

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

    def test_aio_read_wait_for_complete_and_cb(self):
        # use wait_for_complete_and_cb(), verify retval[0] is
        # set by the time we regain control
        payload = b"bar\000frob"
        self.ioctx.write("foo", payload)

        self._take_down_acting_set('test_pool', 'foo')
        # this is a list so that the local cb() can modify it
        retval = [None]
        lock = threading.Condition()
        def cb(_, buf):
            with lock:
                retval[0] = buf
                lock.notify()
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

    def test_aio_read_wait_for_complete_and_cb_error(self):
        # error case, use wait_for_complete_and_cb(), verify retval[0] is
        # set by the time we regain control
        self._take_down_acting_set('test_pool', 'bar')

        # this is a list so that the local cb() can modify it
        retval = [1]
        lock = threading.Condition()
        def cb(_, buf):
            with lock:
                retval[0] = buf
                lock.notify()

        # read from a DNE object
        comp = self.ioctx.aio_read("bar", 3, 0, cb)
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

    def test_aio_execute(self):
        count = [0]
        retval = [None]
        lock = threading.Condition()
        def cb(_, buf):
            with lock:
                if retval[0] is None:
                    retval[0] = buf
                count[0] += 1
                lock.notify()
        self.ioctx.write("foo", b"") # ensure object exists

        comp = self.ioctx.aio_execute("foo", "hello", "say_hello", b"", 32, cb, cb)
        comp.wait_for_complete()
        with lock:
            while count[0] < 2:
                lock.wait()
        eq(comp.get_return_value(), 13)
        eq(retval[0], b"Hello, world!")

        retval[0] = None
        comp = self.ioctx.aio_execute("foo", "hello", "say_hello", b"nose", 32, cb, cb)
        comp.wait_for_complete()
        with lock:
            while count[0] < 4:
                lock.wait()
        eq(comp.get_return_value(), 12)
        eq(retval[0], b"Hello, nose!")

        [i.remove() for i in self.ioctx.list_objects()]

    def test_aio_setxattr(self):
        lock = threading.Condition()
        count = [0]
        def cb(blah):
            with lock:
                count[0] += 1
                lock.notify()
            return 0
        comp = self.ioctx.aio_setxattr("obj", "key", b'value', cb)
        comp.wait_for_complete()
        with lock:
            while count[0] < 1:
                lock.wait()
        eq(comp.get_return_value(), 0)
        eq(self.ioctx.get_xattr("obj", "key"), b'value')

    def test_applications(self):
        cmd = {"prefix":"osd dump", "format":"json"}
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'')
        eq(ret, 0)
        assert len(buf) > 0
        release = json.loads(buf.decode("utf-8")).get("require_osd_release",
                                                      None)
        if not release or release[0] < 'l':
            pytest.skip('required_osd_release >= l')

        eq([], self.ioctx.application_list())

        self.ioctx.application_enable("app1")
        assert_raises(Error, self.ioctx.application_enable, "app2")
        self.ioctx.application_enable("app2", True)

        assert_raises(Error, self.ioctx.application_metadata_list, "dne")
        eq([], self.ioctx.application_metadata_list("app1"))

        assert_raises(Error, self.ioctx.application_metadata_set, "dne", "key",
                      "key")
        self.ioctx.application_metadata_set("app1", "key1", "val1")
        eq("val1", self.ioctx.application_metadata_get("app1", "key1"))
        self.ioctx.application_metadata_set("app1", "key2", "val2")
        eq("val2", self.ioctx.application_metadata_get("app1", "key2"))
        self.ioctx.application_metadata_set("app2", "key1", "val1")
        eq("val1", self.ioctx.application_metadata_get("app2", "key1"))

        eq([("key1", "val1"), ("key2", "val2")],
           self.ioctx.application_metadata_list("app1"))

        self.ioctx.application_metadata_remove("app1", "key1")
        eq([("key2", "val2")], self.ioctx.application_metadata_list("app1"))

    def test_service_daemon(self):
        name = "pid-" + str(os.getpid())
        metadata = {'version': '3.14', 'memory': '42'}
        self.rados.service_daemon_register("laundry", name, metadata)
        status = {'result': 'unknown', 'test': 'running'}
        self.rados.service_daemon_update(status)

    def test_alignment(self):
        eq(self.ioctx.alignment(), None)


@pytest.mark.ec
class TestIoctxEc(object):

    def setup_method(self, method):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.pool = 'test-ec'
        self.profile = 'testprofile-%s' % self.pool
        cmd = {"prefix": "osd erasure-code-profile set",
               "name": self.profile, "profile": ["k=2", "m=1", "crush-failure-domain=osd"]}
        ret, buf, out = self.rados.mon_command(json.dumps(cmd), b'', timeout=30)
        assert ret == 0, out
        # create ec pool with profile created above
        cmd = {'prefix': 'osd pool create', 'pg_num': 8, 'pgp_num': 8,
               'pool': self.pool, 'pool_type': 'erasure',
               'erasure_code_profile': self.profile}
        ret, buf, out = self.rados.mon_command(json.dumps(cmd), b'', timeout=30)
        assert ret == 0, out
        assert self.rados.pool_exists(self.pool)
        self.ioctx = self.rados.open_ioctx(self.pool)

    def teardown_method(self, method):
        cmd = {"prefix": "osd unset", "key": "noup"}
        self.rados.mon_command(json.dumps(cmd), b'')
        self.ioctx.close()
        self.rados.delete_pool(self.pool)
        self.rados.shutdown()

    def test_alignment(self):
        eq(self.ioctx.alignment(), 8192)


class TestIoctx2(object):

    def setup_method(self, method):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        pool_id = self.rados.pool_lookup('test_pool')
        assert pool_id > 0
        self.ioctx2 = self.rados.open_ioctx2(pool_id)

    def teardown_method(self, method):
        cmd = {"prefix": "osd unset", "key": "noup"}
        self.rados.mon_command(json.dumps(cmd), b'')
        self.ioctx2.close()
        self.rados.delete_pool('test_pool')
        self.rados.shutdown()

    def test_get_last_version(self):
        version = self.ioctx2.get_last_version()
        assert version >= 0

    def test_get_stats(self):
        stats = self.ioctx2.get_stats()
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


class TestObject(object):

    def setup_method(self, method):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        self.ioctx = self.rados.open_ioctx('test_pool')
        self.ioctx.write('foo', b'bar')
        self.object = Object(self.ioctx, 'foo')

    def teardown_method(self, method):
        self.ioctx.close()
        self.ioctx = None
        self.rados.delete_pool('test_pool')
        self.rados.shutdown()
        self.rados = None

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

@pytest.mark.snap
class TestIoCtxSelfManagedSnaps(object):
    def setup_method(self, method):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        self.ioctx = self.rados.open_ioctx('test_pool')

    def teardown_method(self, method):
        cmd = {"prefix":"osd unset", "key":"noup"}
        self.rados.mon_command(json.dumps(cmd), b'')
        self.ioctx.close()
        self.rados.delete_pool('test_pool')
        self.rados.shutdown()

    def test(self):
        # cannot mix-and-match pool and self-managed snapshot mode
        self.ioctx.set_self_managed_snap_write([])
        self.ioctx.write('abc', b'abc')
        snap_id_1 = self.ioctx.create_self_managed_snap()
        self.ioctx.set_self_managed_snap_write([snap_id_1])

        self.ioctx.write('abc', b'def')
        snap_id_2 = self.ioctx.create_self_managed_snap()
        self.ioctx.set_self_managed_snap_write([snap_id_1, snap_id_2])

        self.ioctx.write('abc', b'ghi')

        self.ioctx.rollback_self_managed_snap('abc', snap_id_1)
        eq(self.ioctx.read('abc'), b'abc')

        self.ioctx.rollback_self_managed_snap('abc', snap_id_2)
        eq(self.ioctx.read('abc'), b'def')

        self.ioctx.remove_self_managed_snap(snap_id_1)
        self.ioctx.remove_self_managed_snap(snap_id_2)

class TestCommand(object):

    def setup_method(self, method):
        self.rados = Rados(conffile='')
        self.rados.connect()

    def teardown_method(self, method):
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

        # send to specific target by name, rank
        cmd = {"prefix": "version"}

        target = d['mons'][0]['name']
        print(target)
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30,
                                                target=target)
        eq(ret, 0)
        assert len(buf) > 0
        e = json.loads(buf.decode("utf-8"))
        assert('release' in e)

        target = d['mons'][0]['rank']
        print(target)
        ret, buf, errs = self.rados.mon_command(json.dumps(cmd), b'', timeout=30,
                                                target=target)
        eq(ret, 0)
        assert len(buf) > 0
        e = json.loads(buf.decode("utf-8"))
        assert('release' in e)

    @pytest.mark.bench
    def test_osd_bench(self):
        cmd = dict(prefix='bench', size=4096, count=8192)
        ret, buf, err = self.rados.osd_command(0, json.dumps(cmd), b'',
                                               timeout=30)
        eq(ret, 0)
        assert len(buf) > 0
        out = json.loads(buf.decode('utf-8'))
        eq(out['blocksize'], cmd['size'])
        eq(out['bytes_written'], cmd['count'])

    def test_ceph_osd_pool_create_utf8(self):
        poolname = "\u9ec5"

        cmd = {"prefix": "osd pool create", "pg_num": 16, "pool": poolname}
        ret, buf, out = self.rados.mon_command(json.dumps(cmd), b'')
        eq(ret, 0)
        assert len(out) > 0
        eq(u"pool '\u9ec5' created", out)


@pytest.mark.watch
class TestWatchNotify(object):
    OID = "test_watch_notify"

    def setup_method(self, method):
        self.rados = Rados(conffile='')
        self.rados.connect()
        self.rados.create_pool('test_pool')
        assert self.rados.pool_exists('test_pool')
        self.ioctx = self.rados.open_ioctx('test_pool')
        self.ioctx.write(self.OID, b'test watch notify')
        self.lock = threading.Condition()
        self.notify_cnt = {}
        self.notify_data = {}
        self.notify_error = {}
        # aio related
        self.ack_cnt = {}
        self.ack_data = {}
        self.instance_id = self.rados.get_instance_id()

    def teardown_method(self, method):
        self.ioctx.close()
        self.rados.delete_pool('test_pool')
        self.rados.shutdown()

    def make_callback(self):
        def callback(notify_id, notifier_id, watch_id, data):
            with self.lock:
                if watch_id not in self.notify_cnt:
                    self.notify_cnt[watch_id] = 1
                elif  self.notify_data[watch_id] != data:
                    self.notify_cnt[watch_id] += 1
                self.notify_data[watch_id] = data
        return callback

    def make_error_callback(self):
        def callback(watch_id, error):
            with self.lock:
                self.notify_error[watch_id] = error
        return callback


    def test(self):
        with self.ioctx.watch(self.OID, self.make_callback(),
                              self.make_error_callback()) as watch1:
            watch_id1 = watch1.get_id()
            assert(watch_id1 > 0)

            with self.rados.open_ioctx('test_pool') as ioctx:
                watch2 = ioctx.watch(self.OID, self.make_callback(),
                                     self.make_error_callback())
            watch_id2 = watch2.get_id()
            assert(watch_id2 > 0)

            assert(self.ioctx.notify(self.OID, 'test'))
            with self.lock:
                assert(watch_id1 in self.notify_cnt)
                assert(watch_id2 in self.notify_cnt)
                eq(self.notify_cnt[watch_id1], 1)
                eq(self.notify_cnt[watch_id2], 1)
                eq(self.notify_data[watch_id1], b'test')
                eq(self.notify_data[watch_id2], b'test')

            assert(watch1.check() >= timedelta())
            assert(watch2.check() >= timedelta())

            assert(self.ioctx.notify(self.OID, 'best'))
            with self.lock:
                eq(self.notify_cnt[watch_id1], 2)
                eq(self.notify_cnt[watch_id2], 2)
                eq(self.notify_data[watch_id1], b'best')
                eq(self.notify_data[watch_id2], b'best')

            watch2.close()

            assert(self.ioctx.notify(self.OID, 'rest'))
            with self.lock:
                eq(self.notify_cnt[watch_id1], 3)
                eq(self.notify_cnt[watch_id2], 2)
                eq(self.notify_data[watch_id1], b'rest')
                eq(self.notify_data[watch_id2], b'best')

            assert(watch1.check() >= timedelta())

            self.ioctx.remove_object(self.OID)

            for i in range(10):
                with self.lock:
                    if watch_id1 in self.notify_error:
                        break
                time.sleep(1)
            eq(self.notify_error[watch_id1], -errno.ENOTCONN)
            assert_raises(NotConnected, watch1.check)

            assert_raises(ObjectNotFound, self.ioctx.notify, self.OID, 'test')

    def make_callback_reply(self):
        def callback(notify_id, notifier_id, watch_id, data):
            with self.lock:
                return data
        return callback

    def notify_callback(self, _, r, ack_list, timeout_list):
        eq(r, 0)
        with self.lock:
            for notifier_id, _, notifier_data in ack_list:
                if notifier_id not in self.ack_cnt:
                    self.ack_cnt[notifier_id] = 0
                self.ack_cnt[notifier_id] += 1
                self.ack_data[notifier_id] = notifier_data

    def notify_callback_err(self, _, r, ack_list, timeout_list):
        eq(r, -errno.ENOENT)

    def test_aio_notify(self):
        with self.ioctx.watch(self.OID, self.make_callback_reply(),
                              self.make_error_callback()) as watch1:
            watch_id1 = watch1.get_id()
            assert watch_id1 > 0

            with self.rados.open_ioctx('test_pool') as ioctx:
                watch2 = ioctx.watch(self.OID, self.make_callback_reply(),
                                     self.make_error_callback())
            watch_id2 = watch2.get_id()
            assert watch_id2 > 0

            comp = self.ioctx.aio_notify(self.OID, self.notify_callback, msg='test')
            comp.wait_for_complete_and_cb()
            with self.lock:
                assert self.instance_id in self.ack_cnt
                eq(self.ack_cnt[self.instance_id], 2)
                eq(self.ack_data[self.instance_id], b'test')

            assert watch1.check() >= timedelta()
            assert watch2.check() >= timedelta()

            comp = self.ioctx.aio_notify(self.OID, self.notify_callback, msg='best')
            comp.wait_for_complete_and_cb()
            with self.lock:
                eq(self.ack_cnt[self.instance_id], 4)
                eq(self.ack_data[self.instance_id], b'best')

            watch2.close()

            comp = self.ioctx.aio_notify(self.OID, self.notify_callback, msg='rest')
            comp.wait_for_complete_and_cb()
            with self.lock:
                eq(self.ack_cnt[self.instance_id], 5)
                eq(self.ack_data[self.instance_id], b'rest')

            assert(watch1.check() >= timedelta())
            self.ioctx.remove_object(self.OID)

            for i in range(10):
                with self.lock:
                    if watch_id1 in self.notify_error:
                        break
                time.sleep(1)
            eq(self.notify_error[watch_id1], -errno.ENOTCONN)
            assert_raises(NotConnected, watch1.check)

            comp = self.ioctx.aio_notify(self.OID, self.notify_callback_err, msg='test')
            comp.wait_for_complete_and_cb()
