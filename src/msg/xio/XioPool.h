// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef XIO_POOL_H
#define XIO_POOL_H

extern "C" {
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "libxio.h"
}
#include <iostream>
#include <vector>
#include "include/atomic.h"
#include "common/likely.h"


static inline int xpool_alloc(struct xio_mempool *pool, uint64_t size,
			      struct xio_mempool_obj* mp);
static inline void xpool_free(uint64_t size, struct xio_mempool_obj* mp);

using ceph::atomic_t;

class XioPool
{
private:
  struct xio_mempool *handle;

public:
  static bool trace_mempool;
  static bool trace_msgcnt;
  static const int MB = 8;

  struct xio_piece {
    struct xio_mempool_obj mp[1];
    struct xio_piece *next;
    int s;
    char payload[MB];
  } *first;

  XioPool(struct xio_mempool *_handle) :
    handle(_handle), first(0)
    {
    }
  ~XioPool()
    {
      struct xio_piece *p;
      while ((p = first)) {
	first = p->next;
	if (unlikely(trace_mempool)) {
	  memset(p->payload, 0xcf, p->s); // guard bytes
	}
	xpool_free(sizeof(struct xio_piece)+(p->s)-MB, p->mp);
      }
    }
  void *alloc(size_t _s)
    {
	void *r;
	struct xio_mempool_obj mp[1];
	struct xio_piece *x;
	int e = xpool_alloc(handle, (sizeof(struct xio_piece)-MB) + _s, mp);
	if (e) {
	  r = 0;
	} else {
	  x = reinterpret_cast<struct xio_piece *>(mp->addr);
	  *x->mp = *mp;
	  x->next = first;
	  x->s = _s;
	  first = x;
	  r = x->payload;
	}
	return r;
    }
};

class XioPoolStats {
private:
  enum pool_sizes {
    SLAB_64 = 0,
    SLAB_256,
    SLAB_1024,
    SLAB_PAGE,
    SLAB_MAX
  };

  atomic_t ctr_set[5];

  atomic_t msg_cnt;  // send msgs
  atomic_t hook_cnt; // recv msgs

public:
  XioPoolStats() : msg_cnt(0), hook_cnt(0) {
    for (int ix = 0; ix < 5; ++ix) {
      ctr_set[ix].set(0);
    }
  }

  void dump(const char* tag, uint64_t serial) {
    std::cout
      << tag << " #" << serial << ": "
      << "pool objs: "
      << "64: " << ctr_set[SLAB_64].read() << " "
      << "256: " << ctr_set[SLAB_256].read() << " "
      << "1024: " << ctr_set[SLAB_1024].read() << " "
      << "page: " << ctr_set[SLAB_PAGE].read() << " "
      << "max: " << ctr_set[SLAB_MAX].read() << " "
      << std::endl;
    std::cout
      << tag << " #" << serial << ": "
      << " msg objs: "
      << "in: " << hook_cnt.read() << " "
      << "out: " << msg_cnt.read() << " "
      << std::endl;
  }

  void inc(uint64_t size) {
    if (size <= 64) {
      (ctr_set[SLAB_64]).inc();
      return;
    }
    if (size <= 256) {
      (ctr_set[SLAB_256]).inc();
      return;
    }
    if (size <= 1024) {
      (ctr_set[SLAB_1024]).inc();
      return;
    }
    if (size <= 8192) {
      (ctr_set[SLAB_PAGE]).inc();
      return;
    }
    (ctr_set[SLAB_MAX]).inc();
  }

  void dec(uint64_t size) {
    if (size <= 64) {
      (ctr_set[SLAB_64]).dec();
      return;
    }
    if (size <= 256) {
      (ctr_set[SLAB_256]).dec();
      return;
    }
    if (size <= 1024) {
      (ctr_set[SLAB_1024]).dec();
      return;
    }
    if (size <= 8192) {
      (ctr_set[SLAB_PAGE]).dec();
      return;
    }
    (ctr_set[SLAB_MAX]).dec();
  }

  void inc_msgcnt() {
    if (unlikely(XioPool::trace_msgcnt)) {
      msg_cnt.inc();
    }
  }

  void dec_msgcnt() {
    if (unlikely(XioPool::trace_msgcnt)) {
      msg_cnt.dec();
    }
  }

  void inc_hookcnt() {
    if (unlikely(XioPool::trace_msgcnt)) {
      hook_cnt.inc();
    }
  }

  void dec_hookcnt() {
    if (unlikely(XioPool::trace_msgcnt)) {
      hook_cnt.dec();
    }
  }
};

extern XioPoolStats xp_stats;

static inline int xpool_alloc(struct xio_mempool *pool, uint64_t size,
			      struct xio_mempool_obj* mp)
{
  if (unlikely(XioPool::trace_mempool))
    xp_stats.inc(size);
  return xio_mempool_alloc(pool, size, mp);
}

static inline void xpool_free(uint64_t size, struct xio_mempool_obj* mp)
{
 if (unlikely(XioPool::trace_mempool))
    xp_stats.dec(size);
  xio_mempool_free(mp);
}

#define xpool_inc_msgcnt() \
  do { xp_stats.inc_msgcnt(); } while (0)

#define xpool_dec_msgcnt() \
  do { xp_stats.dec_msgcnt(); } while (0)

#define xpool_inc_hookcnt() \
  do { xp_stats.inc_hookcnt(); } while (0)

#define xpool_dec_hookcnt() \
  do { xp_stats.dec_hookcnt(); } while (0)

#endif /* XIO_POOL_H */
