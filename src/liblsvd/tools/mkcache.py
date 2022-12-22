#!/usr/bin/python3

import lsvd_types as lsvd
import sys
import os
import argparse
import uuid
import blkdev

# TODO:
# 1. variable size
# 3. option to not write cache pages

def div_round_up(n, m):
    return (n + m - 1) // m

# rm -f <cachedir>/*.cache
def cleanup(name):
    cache_dir = os.path.dirname(name)
    if not os.access(cache_dir, os.R_OK | os.X_OK):
        os.mkdir(cache_dir)
    for f in os.listdir(cache_dir):
        if f.endswith('.cache'):
            os.unlink(cache_dir + '/' + f)
    
# https://stackoverflow.com/questions/42865724/parse-human-readable-filesizes-into-bytes
units = {"K": 2**10, "M": 2**20, "G": 2**30}
def parse_size(size):
    if size[-1:] in units:
        return int(float(size[:-1])*units[size[-1]])
    else:
        return int(size)

# backend batch size is 8MB, write cache should be >= 2 batches
# 
def mkcache(name, uuid=b'\0'*16, write_zeros=True, wblks=4096, rblks=4096):
    fd = os.open(name, os.O_RDWR | os.O_CREAT, 0o777)

    sup = lsvd.j_super(magic=lsvd.LSVD_MAGIC, type=lsvd.LSVD_J_SUPER,
                       write_super=1, read_super=2, backend_type=lsvd.LSVD_BE_FILE)
    sup.vol_uuid[:] = uuid
    data = bytearray() + sup
    data += b'\0' * (4096-len(data))
    os.write(fd, data) # page 0

    # assuming 4KB min write size, write cache has max metadata of 16 bytes
    # extent + 8 bytes length per 4KB, but we need an integer
    # number of pages for each section, and enough room for 2.
    #
    _map = div_round_up(wblks, 256)
    _len = div_round_up(wblks, 512)
    mblks = 2 * (_map + _len)
    wblks -= mblks
    
    # write cache has 125 single-page entries. default: map_blocks = map_entries = 0
    wsup = lsvd.j_write_super(magic=lsvd.LSVD_MAGIC, type=lsvd.LSVD_J_W_SUPER,
                                seq=1, meta_base = 3, meta_limit = 3+mblks,
                                base=3+mblks, limit=3+mblks+wblks,
                                next=3+mblks, oldest=3+mblks, clean=1)
    data = bytearray() + wsup
    data += b'\0' * (4096-len(data))
    os.write(fd, data) # page 1

    rbase = wblks+mblks+3
    units = rblks // 16
    map_blks = div_round_up(units*lsvd.sizeof_obj_offset, 4096)
    
    # 1 page for map
    rsup = lsvd.j_read_super(magic=lsvd.LSVD_MAGIC, type=lsvd.LSVD_J_R_SUPER,
                                unit_size=128, units=units,
                                map_start=rbase, map_blocks=map_blks,
                                base=rbase+map_blks)

    data = bytearray() + rsup
    data += b'\0' * (4096-len(data))
    os.write(fd, data) # page 2

    # zeros are invalid entries for cache map
    # 130 + 16*(64KB/4KB) = 386

    data = bytearray(b'\0'*4096)
    if (write_zeros):
        for i in range(3 + mblks + wblks + map_blks + rblks):
            os.write(fd, data)
    else:
        for i in range(rbase,rbase+map_blks):
            os.pwrite(fd, data, i*4096)
    
    os.close(fd)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='initialize LSVD cache')
    parser.add_argument('--uuid', help='volume UUID',
                            default='00000000-0000-0000-0000-000000000000')
    parser.add_argument('--size', help='volue size',
                            default=0)
    parser.add_argument('device', help='cache partition')
    args = parser.parse_args()

    uuid = uuid.UUID(args.uuid).bytes

    if not os.access(args.device, os.F_OK) and args.size:
        size = parse_size(args.size)
        buf = bytes(4096)
        fp = open(args.device, 'wb')
        for i in range(0, size, 4096):
            fp.write(buf)
        fp.close()
    
    if os.access(args.device, os.F_OK):
        s = os.stat(args.device)
        if blkdev.S_ISBLK(s.st_mode):
            bytes = blkdev.dev_get_size(os.open(args.device, os.O_RDONLY))
        else:
            bytes = s.st_size
        pages = bytes // 4096
        print('%d pages' % pages)
        r_units = int(0.75*pages) // 16
        r_pages = r_units * 16
        r_oh = div_round_up(r_units*lsvd.sizeof_obj_offset, 4096)
        w_pages = pages - r_pages - r_oh - 3     # mkcache subtracts write metadata
    
        mkcache(args.device, uuid, write_zeros=False, wblks=w_pages, rblks=r_pages)
    else:
        mkcache(args.device, uuid)

