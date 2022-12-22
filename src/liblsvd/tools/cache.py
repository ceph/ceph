#!/usr/bin/python3

import sys
import os
import lsvd_types as lsvd
import test3 as t3
import uuid
import blkdev
import argparse
import ctypes

def prettyprint(p, insns):
    for field,fmt in insns:
        x = eval('p.' + field)
        pad = ' ' * (10 - len(field)) + ':'
        if type(fmt) == str:
            print(field, pad, fmt % x)
        elif callable(fmt):
            print(field, pad, fmt(x))
        elif type(fmt) == dict:
            print(field, pad, fmt[x])

fmt_uuid = lambda u: uuid.UUID(bytes=bytes(u[:]))

fieldnames = dict({(10, 'DATA'),(11, 'CKPT'),(12, 'PAD'),(13, 'SUPER'),
                       (14, 'W_SUPER'),(15, 'R_SUPER')})
magic = lambda x: 'ok' if x == lsvd.LSVD_MAGIC else '**BAD**'
blk_fmt = lambda x: '%d%s' % (x, '' if x < npages else ' *INVALID*')

hdr_pp = [['magic', magic], ['type', fieldnames], ['vol_uuid', fmt_uuid],
              ['write_super', blk_fmt], ['read_super', blk_fmt]]

wsup_pp = [['magic', magic], ['type', fieldnames], ['clean', lambda x: "YES" if x else "NO"],
               ['seq', '%d'], ['meta_base', '%d'], ['meta_limit', '%d'],
               ['base', '%d'], ['limit', '%d'], ['next', '%d'], 
               ['map_start', '%d'], ['map_entries', '%d'],
               ['len_start', '%d'], ['len_entries', '%d']]

rsup_pp = [["magic", magic], ["type", fieldnames], ["unit_size", '%d'], 
               ["base", '%d'], ["units", '%d'], ["map_start", '%d'],["map_blocks", '%d']]

parser = argparse.ArgumentParser(description='Read SSD cache')
parser.add_argument('--write', help='print write cache details', action='store_true')
parser.add_argument('--writemap', help='print write cache map', action='store_true')
parser.add_argument('--read', help='print read cache details', action='store_true')
parser.add_argument('--nowrap', help='one entry per line', action='store_true')
parser.add_argument('device', help='cache device/file')
args = parser.parse_args()

fd = os.open(args.device, os.O_RDONLY)
sb = os.fstat(fd)
if blkdev.S_ISBLK(sb.st_mode):
    npages = blkdev.dev_get_size(fd)
else:
    npages = sb.st_size // 4096

super = t3.c_super(fd)
print('superblock: (0)')
prettyprint(super, hdr_pp)
w_super = r_super = None
print('\nwrite superblock: (%s)' % blk_fmt(super.write_super))
if super.write_super < npages:
    w_super = t3.c_w_super(fd, super.write_super)
    prettyprint(w_super, wsup_pp)

print('\nread superblock: (%s)' % blk_fmt(super.read_super))
if super.read_super < npages:
    r_super = t3.c_r_super(fd, super.read_super)
    prettyprint(r_super, rsup_pp)

def read_exts(b, npgs, n):
    bytes = n*lsvd.sizeof_j_map_extent
    buf = os.pread(fd, bytes, b*4096)
    e = (lsvd.j_map_extent*n).from_buffer(bytearray(buf))
    return [(_.lba, _.len, _.plba) for _ in e]

def read_lens(b, npgs, n):
    bytes = n*lsvd.sizeof_j_length
    buf = os.pread(fd, bytes, b*4096)
    l = (lsvd.j_length*n).from_buffer(bytearray(buf))
    return [(_.page, _.len) for _ in l]
    
if args.write and super.write_super < npages:
    b = w_super.base
    while b < w_super.limit:
        [j,e] = t3.c_hdr(fd, b)
        if j.magic != lsvd.LSVD_MAGIC:
            b += 1
            continue

        h_pp = [["magic", magic], ["type", fieldnames], ["seq", '%d'], ["len", '%d'], ["prev", '%d']]
        print('\ndata: (%d)' % b)
        prettyprint(j, h_pp)
        print('extents    :', ' '.join(['%d+%d' % (_.lba,_.len) for _ in e]))
        b = b + j.len

def wrapjoin(prefix, linelen, items):
    val = ''
    line = prefix
    for i in items:
        if len(line)+len(i) > linelen:
            val = val + line + '\n'
            line = prefix
        line = line + ' ' + i
    if line != prefix:
        val += line
    return val
        
if args.writemap and w_super:
    print('\nwrite cache map:')
    e = read_exts(w_super.map_start, w_super.map_blocks, w_super.map_entries)
    if args.nowrap:
        print('\n'.join(['%d+%d->%d' % _ for _ in e]))
    else:
        print(wrapjoin(' ', 80, ['%d+%d->%d' % _ for _ in e]))

    print('\nwrite cache lengths:')
    l = read_lens(w_super.len_start, w_super.len_blocks, w_super.len_entries)
    if args.nowrap:
        print('\n'.join(['[%d]=%d' % _ for _ in l]))
    else:
        print(wrapjoin(' ', 80, ['[%d]=%d' % _ for _ in l]))

if args.read and r_super:
    nbytes = r_super.units * lsvd.sizeof_obj_offset
    print("map start:   ", r_super.map_start)
    buf = os.pread(fd, nbytes, r_super.map_start * 4096)
    oos = (lsvd.obj_offset * r_super.units).from_buffer(bytearray(buf))
    oos = [_ for _ in filter(lambda x: x.obj != 0, oos)]
    print("\nread map:")
    print(wrapjoin(' ', 80, ['[%d] = %d.%d' % _ for _ in
                                 [(i, oos[i].obj, oos[i].offset) for i in range(len(oos))]]))
