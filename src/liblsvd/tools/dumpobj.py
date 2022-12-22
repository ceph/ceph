#!/usr/bin/python3

# binary/text encode and decode
# includes object definitions, so that images can be converted if
# encoding changes during development
#

import os
import sys
import uuid
from ctypes import *
import argparse
import zlib

#### matches commit f06dde of objects.h

LSVD_SUPER = 1
LSVD_DATA = 2
LSVD_CKPT = 3
LSVD_MAGIC = 0x4456534c

# these match version 453d93 of objects.cc

parser = argparse.ArgumentParser(description='LSVD object to/from text')
parser.add_argument('--rados', help='use RADOS', action='store_true')
parser.add_argument('--encode', help='text -> binary', action='store_true')
parser.add_argument('--decode', help='binary -> text', action='store_true')
parser.add_argument('src', help='source')
parser.add_argument('dst', help='destination', nargs='?')
args = parser.parse_args()

ver = 1

class hdr(Structure):
    _pack_ = 1
    _fields_ = [("magic",               c_uint),
                ("version",             c_uint),
                ("vol_uuid",            c_ubyte*16),
                ("type",                c_uint),
                ("seq",                 c_uint),
                ("hdr_sectors",         c_uint),
                ("data_sectors",        c_uint),
                ("crc",                 c_uint)]

class super_hdr(Structure):
    _pack_ = 1
    _fields_ = [("vol_size",            c_ulong),
                ("ckpts_offset",        c_uint),
                ("ckpts_len",           c_uint),
                ("clones_offset",       c_uint),
                ("clones_len",          c_uint),
                ("snaps_offset",        c_uint),
                ("snaps_len",           c_uint)]

# ckpts is array of uint32

class clone_info(Structure):
    _pack_ = 1
    _fields_ = [("vol_uuid",            c_ubyte*16),
                ("last_seq",            c_uint),
                ("name_len",            c_ubyte)]   # 21 bytes
        # followed by 'name_len' bytes of name

if ver == 1:
    class ckpt_hdr(Structure):
        _pack_ = 1
        _fields_ = [("cache_seq",           c_ulong),
                    ("ckpts_offset",        c_uint),
                    ("ckpts_len",           c_uint),
                    ("objs_offset",         c_uint),
                    ("objs_len",            c_uint),
                    ("deletes_offset",      c_uint),
                    ("deletes_len",         c_uint),
                    ("map_offset",          c_uint),
                    ("map_len",             c_uint)]


class ckpt_obj(Structure):
    _pack_ = 1
    _fields_ = [("seq",                 c_uint),
                ("hdr_sectors",         c_uint),
                ("data_sectors",        c_uint),
                ("live_sectors",        c_uint)]
sizeof_ckpt_obj = sizeof(ckpt_obj) # 16

class deferred_delete(Structure):
    _pack_ = 1
    _fields_ = [("seq",                 c_uint),
                ("time",                c_uint)]
sizeof_deferred_delete = sizeof(deferred_delete)

class ckpt_mapentry(LittleEndianStructure):
    _pack_ = 1
    _fields_ = [("lba",                 c_ulong, 36),
                ("len",                 c_ulong, 28),
                ("obj",                 c_uint),
                ("offset",              c_uint)]
sizeof_ckpt_mapentry = sizeof(ckpt_mapentry) # 16


if args.rados:
    import rados
    cluster = rados.Rados(conffile='')
    cluster.connect()
    
def read_obj(name):
    if args.rados:
        pool,oid = name.split('/')
        if not cluster.pool_exists(pool):
            raise RuntimeError('Pool not found: ' + pool)
        ioctx = cluster.open_ioctx(pool)
        obj = ioctx.read(oid)
        h = hdr.from_buffer(bytearray(obj[0:sizeof(hdr)]))
        if len(obj) < 512*h.hdr_sectors:
            obj = ioctx.read(oid, 512*h.hdr_sectors, 0)
        ioctx.close()
    else:
        f = open(name, 'rb')
        obj = f.read(None)
        f.close()
    return obj

def write_obj(name, data):
    if args.rados:
        pool,oid = name.split('/')
        if not cluster.pool_exists(pool):
            raise RuntimeError('Pool not found: ' + pool)
        ioctx = cluster.open_ioctx(pool)
        ioctx.write(oid, bytes(data))
        ioctx.close()
    else:
        fd = os.open(name, os.O_WRONLY|os.O_TRUNC|os.O_CREAT)
        os.write(fd, bytes(data))
        os.close(fd)

def decode_super(obj, fp):
    o1 = sizeof(hdr)
    h = hdr.from_buffer(bytearray(obj[:o1]))
    o2 = o1 + sizeof(super_hdr)
    sh = super_hdr.from_buffer(bytearray(obj[o1:o2]))

    uu = uuid.UUID(bytes=bytes(h.vol_uuid))
    print('type SUPER', file=fp);
    print('uuid', str(uu), file=fp)
    assert(h.seq == 0)
    assert(h.data_sectors == 0)

    print('vol_size', sh.vol_size, file=fp)
    
    if sh.ckpts_len > 0:
        _ckpts = obj[sh.ckpts_offset:sh.ckpts_offset+sh.ckpts_len]
        ckpts = (c_int * (len(_ckpts)//4)).from_buffer(bytearray(_ckpts))
        for c in ckpts:
            print('ckpt', c, file=fp)
    if sh.clones_len > 0:
        cl = clone_info.from_buffer(bytearray(obj[sh.clones_offset:]))
        o3 = sh.clones_offset + sizeof(cl)
        name = obj[o3:o3+cl.name_len]
        uu = uuid.UUID(bytes=bytes(cl.vol_uuid))
        print('clone', str(name,'utf-8'), str(uu), cl.last_seq, file=fp)
    
def decode_ckpt(obj, fp):
    o1 = sizeof(hdr)
    h = hdr.from_buffer(bytearray(obj[:o1]))
    o2 = o1 + sizeof(ckpt_hdr)
    ch = ckpt_hdr.from_buffer(bytearray(obj[o1:o2]))

    uu = uuid.UUID(bytes=bytes(h.vol_uuid))
    print('type CKPT', file=fp);
    print('uuid', str(uu), file=fp)
    print('seq', h.seq)
    print('cache_seq', ch.cache_seq)
    if ch.ckpts_len > 0:
        n = ch.ckpts_len // 4
        o = ch.ckpts_offset
        ckpts = (c_int * n).from_buffer(bytearray(obj[o:o+ch.ckpts_len]))
        for c in ckpts:
            print('ckpt', c)
    if ch.objs_len > 0:
        n = ch.objs_len // sizeof(ckpt_obj)
        o = ch.objs_offset
        objs = (ckpt_obj * n).from_buffer(bytearray(obj[o:o+ch.objs_len]))
        for obj in objs:
            print('obj %d,%d,%d,%d',
                      (obj.seq, obj.hdr_sectors, obj.data_sectors, obj.live_sectors))
    if ch.deletes_len > 0:
        n = ch.deletes_len // sizeof(deferred_delete)
        o = ch.deletes_offset
        dels = (deferred_delete * n).from_buffer(bytearray(obj[o:o+ch.deletes_len]))
        for d in dels:
            print('delete %d,%d', (d.seq, d.time))
    if ch.map_len > 0:
        n = ch.map_len // sizeof(ckpt_mapentry)
        o = ch.map_offset
        _map = (ckpt_mapentry * n).from_buffer(bytearray(obj[o:o+ch.map_len]))
        for m in _map:
            print('map %d,%d,%d,%d' % (m.lba, m.len, m.obj, m.offset))
    
def encode_ckpt(f, name):
    h = hdr(magic = LSVD_MAGIC, version=1, type=LSVD_CKPT,
                     seq=int(f['seq']), hdr_sectors=0, data_sectors=0)
    h.vol_uuid[:] = uuid.UUID(f['uuid']).bytes

    ckpts_data = b''
    objs_data = b''
    deletes_data = b''
    map_data = b''
    
    ch = ckpt_hdr(cache_seq = int(f['cache_seq']))
    offset = sizeof(h) + sizeof(ch)
    
    if 'ckpt' in f:
        ckpts = [int(_) for _ in f['ckpt']]
        ckpts_data = (c_int * len(ckpts))(*ckpts)
        ch.ckpts_offset = offset
        ch.ckpts_len = sizeof(ckpts_data)
        offset += sizeof(ckpts_data)

    if 'obj' in f:
        objs = [[int(x) for x in y.split(',')] for y in f['obj']]
        objs_data = (ckpt_obj * len(objs))()
        for i in range(len(objs)):
            s,h,d,l = objs[i]
            objs_data[i] = ckpt_obj(seq=s, hdr_sectors=h, data_sectors=d, live_sectors=l)
        ch.objs_offset = offset
        ch.objs_len = sizeof(objs_data)
        offset += sizeof(objs_data)

    if 'delete' in f:
        dels = [[int(x) for x in y.split(',')] for y in f['delete']]
        dels_data = (deferred_delete * len(dels))()
        for i in range(len(dels)):
            s,t = dels[i]
            dels_data[i] = deferred_delete(seq=s, time=t)
        ch.dels_offset = offset
        ch.dels_len = sizeof(dels_data)
        offset += sizeof(dels_data)

    if 'map' in f:
        maps = [[int(x) for x in y.split(',')] for y in f['delete']]
        map_data = (ckpt_mapentry * len(maps))()
        for i in range(len(maps)):
            a,l,o,f = maps[i]
            map_data[i] = ckpt_mapentry(lba=a, len=l, obj=o, offset=f)
        ch.map_offset = offset
        ch.map_len = sizeof(map_data)
        offset += sizeof(map_data)
    
    n_sectors = (offset + 511) // 512
    h.hdr_sectors = n_sectors
    pad = b'\0' * (n_sectors * 512 - offset)
    
    buf = bytearray() + h + ch + ckpts_data + objs_data + deletes_data + map_data + pad
    h.crc = zlib.crc32(buf)
    buf = bytearray() + h + ch + ckpts_data + objs_data + deletes_data + map_data + pad
    
    write_obj(name, buf)


def encode_super(f, name):
    print('writing super')
    h = hdr(magic = LSVD_MAGIC, version=1, type=LSVD_SUPER,
                     seq=0, hdr_sectors=8, data_sectors=0)
    h.vol_uuid[:] = uuid.UUID(f['uuid'][0]).bytes

    sh = super_hdr()
    sh.vol_size = int(f['vol_size'][0])
    ckpts = b''
    if 'ckpt' in f:
        ckpt_list = f['ckpt']
        c = (c_int * len(ckpt_list))()
        for i in range(len(ckpt_list)):
            c[i] = int(ckpt_list[i])
        ckpts = bytearray() + c
    clone = b''
    if 'clone' in f:
        name,uu,seq = f['clone'][0].split()
        cl = clone_info(last_seq = int(seq))
        cl.vol_uuid[:] = uuid.UUID(uu).bytes
        cl.name_len = int(len(name))
        clone = bytearray() + cl + bytes(name,'utf-8')

    offset = sizeof(h) + sizeof(sh)

    sh.ckpts_offset = offset
    sh.ckpts_len = len(ckpts)
    offset += len(ckpts)

    sh.clones_offset = offset
    sh.clone_len = len(clone)
    offset += len(clone)

    buf = bytearray() + h + sh + ckpts + clone
    buf += b'\0' * (4096 - len(buf))
    crc = zlib.crc32(buf)
    h.crc = crc
    buf = bytearray() + h + sh + ckpts + clone
    buf += b'\0' * (4096 - len(buf))

    write_obj(name, buf)

if args.encode:
    fields = dict()
    if args.src == '-':
        fp = sys.stdin
    else:
        fp = open(args.src, 'r')
    for line in fp:
        tmp = line.split()
        if tmp[0] in fields:
            fields[tmp[0]].extend(tmp[1:])
        else:
            fields[tmp[0]] = tmp[1:]
    if fields['type'][0] == 'SUPER':
        encode_super(fields, args.dst)
    elif fields['type'][0] == 'CKPT':
        encode_ckpt(fields, args.dst)
    else:
        print('type:', fields['type'])

elif args.decode:
    obj = read_obj(args.src)
    if args.dst:
        fp = open(args.dst, 'w')
    else:
        fp = sys.stdout
    h = hdr.from_buffer(bytearray(obj[:sizeof(hdr)]))
    if h.type == LSVD_SUPER:
        decode_super(obj, fp)
    elif h.type == LSVD_CKPT:
        decode_ckpt(obj, fp)
