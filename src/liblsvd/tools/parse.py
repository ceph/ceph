#!/usr/bin/python3

import os
import lsvd_types as lsvd
import sys
import uuid
from ctypes import *
import argparse

parser = argparse.ArgumentParser(description='Read backend object')
parser.add_argument('--rados', help='fetch from RADOS', action='store_true')
parser.add_argument('--nowrap', help='one entry per line', action='store_true')
parser.add_argument('object', help='object path')
args = parser.parse_args()

if args.rados:
    import rados
    pool,oid = args.object.split('/')
    cluster = rados.Rados(conffile='')
    cluster.connect()
    if not cluster.pool_exists(pool):
        raise RuntimeError('Pool not found: ' + pool)
    ioctx = cluster.open_ioctx(pool)
    obj = ioctx.read(oid, 4096, 0)
    h = lsvd.hdr.from_buffer(bytearray(obj[0:sizeof(lsvd.hdr)]))
    obj = ioctx.read(oid, 512*h.hdr_sectors, 0)
    ioctx.close()
else:
    f = open(args.object, 'rb')
    obj = f.read(None)
    f.close()

def read_obj(name):
    if args.rados:
        pool,oid = name.split('/')
        if not cluster.pool_exists(pool):
            raise RuntimeError('Pool not found: ' + pool)
        ioctx = cluster.open_ioctx(pool)
        obj = ioctx.read(oid)
        ioctx.close()
    else:
        f = open(name, 'rb')
        obj = f.read(None)
        f.close()
    return obj

o2 = l1 = lsvd.sizeof_hdr

def fmt_ckpt(ckpts):
    return map(lambda x: "%d (0x%x)" % (x,x), ckpts)

def fmt_obj_cleaned(objs):
    l = []
    for o in objs:
        l.append("[%d %d]" % (o.seq, o.was_deleted))
    return l

def fmt_obj(objs):
    l = []
    for o in objs:
        l.append("[obj=%d hdr=%d data=%d live=%d]" % (o.seq, o.hdr_sectors, o.data_sectors, o.live_sectors))
    return l

def fmt_data_map(maps):
    l = []
    for m in maps:
        l.append("[%d %d]" % (m.lba, m.len))
    return l

def fmt_ckpt_map(maps):
    l = []
    for m in maps:
        l.append("[%d+%d -> %d+%d]" % (m.lba, m.len, m.obj, m.offset))
    return l

def read_ckpts(buf, base, bytes):
    if bytes <= 0:
        return [ ]
    n = bytes//4
    ckpts = (c_int * n).from_buffer(buf[base:base+bytes])
    return [_ for _ in ckpts]

import zlib
print('crc:          %08x' % zlib.crc32(obj))

def print_clone(c, name, indent):
    uu = uuid.UUID(bytes=bytes(c.vol_uuid))
    print(indent + 'clone base:    ', str(name,'utf-8'))
    print(indent + '  last seq:    ', c.last_seq)
    print(indent + '      uuid:    ', str(uu))

    _name = str(name,'utf-8')
    obj = read_obj(_name)
    o2 = lsvd.sizeof_hdr
    o3 = o2+lsvd.sizeof_super_hdr
    sh = lsvd.super_hdr.from_buffer(bytearray(obj[o2:o3]))

    if sh.clones_len > 0:
        o1 = sh.clones_offset
        o2 = o1+lsvd.sizeof_clone
        c = lsvd.clone_info.from_buffer(bytearray(obj[o1:o2]))
        name = obj[o2:o2+c.name_len]
        print_clone(c, name, '  ')

    
h = lsvd.hdr.from_buffer(bytearray(obj[0:l1]))
if h.type == lsvd.LSVD_SUPER:
    o3 = o2+lsvd.sizeof_super_hdr
    sh = lsvd.super_hdr.from_buffer(bytearray(obj[o2:o3]))
    uu = uuid.UUID(bytes=bytes(h.vol_uuid))
    print('name:     ', args.object)
    print('magic:    ', 'OK' if h.magic == lsvd.LSVD_MAGIC else '**BAD**')
    print('UUID:     ', str(uu))
    print('version:  ', h.version)
    print('type:     ', 'SUPER')
    print('seq:      ', h.seq)
    print('n_hdr:    ', h.hdr_sectors)
    print('n_data:   ', h.data_sectors)
    print('crc:      ', '%08x' % h.crc)

    print('vol_size:      ', sh.vol_size)
    print('ckpts_offset:  ', sh.ckpts_offset)
    print('ckpts_len:     ', sh.ckpts_len)
    if sh.ckpts_len > 0:
        ckpts = read_ckpts(bytearray(obj), sh.ckpts_offset, sh.ckpts_len)
        print('ckpts:         ', ','.join(map(lambda x: '%08x' % x, ckpts)))
    if sh.clones_len > 0:
        o1 = sh.clones_offset
        o2 = o1+sizeof(lsvd.clone_info)
        c = lsvd.clone_info.from_buffer(bytearray(obj[o1:o2]))
        name = obj[o2:o2+c.name_len]
        print_clone(c, name, '')

    print('snaps:         ', '[tbd]')
    
elif h.type == lsvd.LSVD_DATA:
    o3 = o2+lsvd.sizeof_data_hdr
    dh = lsvd.data_hdr.from_buffer(bytearray(obj[o2:o3]))
        
    o5 = dh.objs_cleaned_offset; l5 = dh.objs_cleaned_len
    objs = (lsvd.obj_cleaned * (l5//lsvd.sizeof_obj_cleaned)).from_buffer(bytearray(obj[o5:o5+l5]))

    o6 = dh.map_offset; l6 = dh.map_len
    maps = (lsvd.data_map * (l6//lsvd.sizeof_data_map)).from_buffer(bytearray(obj[o6:o6+l6]))

    print('name:     ', args.object)
    print('magic:    ', 'OK' if h.magic == lsvd.LSVD_MAGIC else '**BAD**')
    print('version:  ', h.version)
    print('type:     ', 'DATA')
    print('seq:      ', h.seq)
    print('n_hdr:    ', h.hdr_sectors)
    print('n_data:   ', h.data_sectors)
    print('crc:      ', '%08x' % h.crc)
    print('wseq:     ', dh.cache_seq)
    print('cleaned:  ', dh.objs_cleaned_offset, ':', ', '.join(fmt_obj_cleaned(objs)))
    if args.nowrap:
            print('map:')
            print(' ' + '\n '.join(fmt_data_map(maps)))
    else:
        print('map:      ', '%d+%d' % (dh.map_offset,dh.map_len), ':', ', '.join(fmt_data_map(maps)))
    
elif h.type == lsvd.LSVD_CKPT:
    o3 = o2+lsvd.sizeof_ckpt_hdr
    ch = lsvd.ckpt_hdr.from_buffer(bytearray(obj[o2:o3]))

    o4 = ch.ckpts_offset; l4 = ch.ckpts_len
    ckpts = (c_uint * (l4//4)).from_buffer(bytearray(obj[o4:o4+l4]))
    
    o5 = ch.objs_offset; l5 = ch.objs_len

    if o5+l5 > len(obj):
        objs_txt = 'OBJECT TOO SHORT (%d bytes)' % len(obj)
    else:
        objs = (lsvd.ckpt_obj * (l5//lsvd.sizeof_ckpt_obj)).from_buffer(bytearray(obj[o5:o5+l5]))
        if args.nowrap:
            objs_txt = '\n ' + '\n '.join(fmt_obj(objs))
        else:
            objs_txt = ', '.join(fmt_obj(objs))

#    o6 = ch.deletes_offset; l6 = ch.deletes_len
#    dels = (lsvd.deferred_delete * (l5//lsvd.sizeof_deferred_delete)).from_buffer(bytearray(obj[o6:o6+l6]))

    o7 = ch.map_offset; l7 = ch.map_len
    if o7+l7 > len(obj):
        map_txt = 'OBJECT TOO SHORT (%d bytes)' % len(obj)
    else:
        maps = (lsvd.ckpt_mapentry * (l7//lsvd.sizeof_ckpt_mapentry)).from_buffer(bytearray(obj[o7:o7+l7]))
        if args.nowrap:
            map_txt = '\n ' + '\n '.join(fmt_ckpt_map(maps))
        else:
            map_txt = ', '.join(fmt_ckpt_map(maps))

    print('name:     ', args.object)
    print('magic:    ', 'OK' if h.magic == lsvd.LSVD_MAGIC else '**BAD**')
    print('version:  ', h.version)
    print('type:     ', 'CKPT')
    print('seq:      ', h.seq)
    print('n_hdr:    ', h.hdr_sectors)
    print('n_data:   ', h.data_sectors)
    print('crc:      ', '%08x' % h.crc)
    
    print('cache_seq:', ch.cache_seq)
    print('ckpts:    ', ch.ckpts_offset, ':', ', '.join(fmt_ckpt(ckpts)))
    print('objs:     ', ch.objs_offset, ':', objs_txt)
    print('map:      ', ch.map_offset, ':', map_txt)
    
else:
    print("invalid type:", h.type)
