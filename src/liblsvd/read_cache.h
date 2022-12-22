/*
 * file:        read_cache.h
 * description: interface for read cache
 * author:      Peter Desnoyers, Northeastern University
 *              Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#ifndef READ_CACHE_H
#define READ_CACHE_H

#include <stddef.h>
#include <stdint.h>

#include <vector>
#include <map>

class translate;
class objmap;
class backend;
class nvme;

struct j_read_super;
#include "extent.h"

class read_cache {
public:

    virtual ~read_cache() {};
    virtual std::tuple<size_t,size_t,request*>
        async_readv(size_t offset, smartiov *iov) = 0;
    virtual void write_map(void) = 0;
};

extern read_cache *make_read_cache(uint32_t blkno, int _fd, bool nt,
                                   translate *_be, extmap::objmap *map,
                                   std::shared_mutex *m, backend *_io);

#endif

