/*
 * file:        translate.h
 * description: core translation layer - interface
 * 
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#ifndef TRANSLATE_H
#define TRANSLATE_H

struct iovec;
class backend;
class lsvd_config;

class translate {
public:
    uuid_t    uuid;
    uint64_t  max_cache_seq;
    
    translate() {}
    virtual ~translate() {}

    virtual ssize_t init(const char *name, bool timedflush) = 0;
    virtual void shutdown(void) = 0;

    virtual int flush(void) = 0;      /* write out current batch */
    virtual int checkpoint(void) = 0; /* flush, then write checkpoint */

    virtual ssize_t writev(uint64_t cache_seq, size_t offset,
                           iovec *iov, int iovcnt) = 0;
    virtual void wait_for_room(void) = 0;
    virtual ssize_t readv(size_t offset, iovec *iov, int iovcnt) = 0;
    virtual bool check_object_ready(int obj) = 0; /* GC stalls */
    virtual void wait_object_ready(int obj) = 0;
    
    virtual const char *prefix(int seq) = 0; /* for read cache */

    virtual void stop_gc(void) = 0; /* do this before shutdown */
    virtual void start_gc(void) = 0;
};

extern translate *make_translate(backend *_io, lsvd_config *cfg,
                                 extmap::objmap *map, std::shared_mutex *m);

extern int translate_create_image(backend *objstore, const char *name,
                                  uint64_t size);
extern int translate_remove_image(backend *objstore, const char *name);
extern int translate_get_uuid(backend *objstore, const char *name, uuid_t &uu);

#endif
