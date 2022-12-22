#!/usr/bin/python3

import ctypes
import lsvd_types as lsvd
import os
import re
import argparse
import uuid
try:
    import rados
except:
    pass
    
def read_obj(name):
    if args.rados:
        pool,oid = name.split('/')
        if not cluster.pool_exists(pool):
            raise RuntimeError('Pool not found: ' + pool)
        ioctx = cluster.open_ioctx(pool)
        obj = ioctx.read(oid)
        h = lsvd.hdr.from_buffer(bytearray(obj[0:lsvd.sizeof_hdr]))
        if len(obj) < 512*h.hdr_sectors:
            obj = ioctx.read(oid, 512*h.hdr_sectors, 0)
        ioctx.close()
    else:
        fp = open(name, 'rb')
        obj = fp.read()
        fp.close()
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

# we assume that the base volume is completely shut down, so the 
# highest-numbered object is the last checkpoint
#
def mk_clone(old, new, uu):
    obj = read_obj(old)
    o2 = lsvd.sizeof_hdr
    h = lsvd.hdr.from_buffer(bytearray(obj[0:o2]))

    o3 = o2+lsvd.sizeof_super_hdr
    sh = lsvd.super_hdr.from_buffer(bytearray(obj[o2:o3]))

    # find the last checkpoint
    _ckpts = obj[sh.ckpts_offset:sh.ckpts_offset+sh.ckpts_len]
    ckpts = (ctypes.c_int * (len(_ckpts)//4)).from_buffer(bytearray(_ckpts))
    seq = ckpts[-1]

    # read it, and create a new one
    ckpt_name = "%s.%08x" % (old, seq)
    ckpt_obj = read_obj(ckpt_name)
    print("ckpt len", len(ckpt_obj))
    c_h = lsvd.hdr.from_buffer(bytearray(ckpt_obj))
    c_ch = lsvd.ckpt_hdr.from_buffer(bytearray(ckpt_obj[lsvd.sizeof_hdr:]))

    m_begin = c_ch.map_offset
    m_end = m_begin + c_ch.map_len
    ckpt_map = ckpt_obj[m_begin:m_end]

    c_ckpt_num = ctypes.c_int(seq+1)
    
    hdr_bytes = (ctypes.sizeof(c_h) + ctypes.sizeof(c_ch) +
                     ctypes.sizeof(c_ckpt_num) + len(ckpt_map))
    hdr_sectors = (hdr_bytes + 511) // 512

    c_ch.cache_seq = 0
    o = ctypes.sizeof(c_h) + ctypes.sizeof(c_ch)
    c_ch.ckpts_offset = o
    c_ch.ckpts_len = ctypes.sizeof(c_ckpt_num)
    o += ctypes.sizeof(c_ckpt_num)

    c_ch.objs_len = 0                     # omit the object list
    c_ch.deletes_len = 0

    c_ch.map_offset = o
    c_ch.map_len = len(ckpt_map)
    
    c_h.seq = seq+1
    c_h.vol_uuid[:] = uu
    c_h.hdr_sectors = hdr_sectors
    c_newobj = bytearray() + c_h + c_ch + c_ckpt_num + ckpt_map

    new_ckpt_name = "%s.%08x" % (new, seq+1)
    write_obj(new_ckpt_name, c_newobj)
    
    # and create a superblock object
    h2 = lsvd.hdr(magic=lsvd.LSVD_MAGIC, version=1, type=lsvd.LSVD_SUPER,
                      hdr_sectors=8, data_sectors=0)
    h2.vol_uuid[:] = uu

    # TODO: need to create a single checkpoint, copying the map from the
    # most recent checkpoint in the base, but omitting the object map.

    b_old = bytes(old,'utf-8')
    c = lsvd.clone_info(last_num=seq, name_len=len(b_old))
    c.vol_uuid[:] = h.vol_uuid[:]
    sizeof_clone = lsvd.sizeof_clone_info + len(b_old) + 1
    offset_ckpt = lsvd.sizeof_hdr+lsvd.sizeof_super_hdr
    offset_clone = offset_ckpt + ctypes.sizeof(c_ckpt_num)
    
    sh2 = lsvd.super_hdr(vol_size=sh.vol_size, ckpts_offset=offset_ckpt,
                             ckpts_len=ctypes.sizeof(c_ckpt_num), 
                             clones_offset=offset_clone,
                             clones_len=sizeof_clone)

    data = bytearray() + h2 + sh2 + c_ckpt_num + c + b_old
    data += b'\0' * (4096-len(data))

    write_obj(new, data)
    
if __name__ == '__main__':
    _rnd_uuid = str(uuid.uuid4())
    parser = argparse.ArgumentParser(description='create clone of LSVD disk image')
    parser.add_argument('--uuid', help='volume UUID', default=_rnd_uuid)
    parser.add_argument('--rados', help='use RADOS backend', action='store_true');
    parser.add_argument('base', help='base image')
    parser.add_argument('image', help='new (clone) image')
    args = parser.parse_args()

    _uuid = uuid.UUID(args.uuid).bytes

    if args.rados:
        cluster = rados.Rados(conffile='')
        cluster.connect()

    mk_clone(args.base, args.image, _uuid)

    if args.rados:
        cluster.shutdown()
