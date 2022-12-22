/*
 * file:        write_cache.h
 * description: full structure for write cache
 * 
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#ifndef WRITE_CACHE_H
#define WRITE_CACHE_H

struct write_cache_work {
public:
    request  *req;
    sector_t  lba;
    smartiov *iov;
    write_cache_work(request *r, sector_t a, smartiov *v) : req(r), lba(a), iov(v) {}
};

/* all addresses are in units of 4KB blocks
 */
class write_cache {
public:
    virtual void get_room(sector_t sectors) = 0; 
    virtual void release_room(sector_t sectors) = 0;
    virtual void flush(void) = 0;

    virtual ~write_cache() {}

    virtual write_cache_work* writev(request *req, sector_t lba, smartiov *iov) = 0;
    virtual std::tuple<size_t,size_t,request*>
        async_read(size_t offset, char* buf, size_t len) = 0;
    virtual std::tuple<size_t,size_t,request*>
        async_readv(size_t offset, smartiov *iovs) = 0;

    virtual void do_write_checkpoint(void) = 0;
};

extern write_cache *make_write_cache(uint32_t blkno, int fd,
                                     translate *be, lsvd_config *cfg);


#endif


