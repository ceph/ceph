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

#include <atomic>
#include <vector>
#include <cstdlib>
#include <cstring>
#include <cstdint>

extern "C" {
#include "libxio.h"
}

#include "common/likely.h"

static inline int xpool_alloc(struct xio_mempool *pool, uint64_t size,
			      struct xio_reg_mem* mp);
static inline void xpool_free(uint64_t size, struct xio_reg_mem* mp);

class XioPool
{
private:
  struct xio_mempool *handle;

public:
  static bool trace_mempool;
  static bool trace_msgcnt;
  static const int MB = 8;

  struct xio_piece {
    struct xio_reg_mem mp[1];
    struct xio_piece *next;
    int s;
    char payload[MB];
  } *first;

  explicit XioPool(struct xio_mempool *_handle) :
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
	struct xio_reg_mem mp[1];
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
    SLAB_MAX,
    SLAB_OVERFLOW,
    NUM_SLABS,
  };

  std::atomic<unsigned> ctr_set[NUM_SLABS] = {};
  std::atomic<unsigned> msg_cnt = { 0 };  // send msgs
  std::atomic<unsigned> hook_cnt = { 0 }; // recv msgs

public:
  void dump(const char* tag, uint64_t serial);

  void inc(uint64_t size) {
    if (size <= 64) {
      (ctr_set[SLAB_64])++;
      return;
    }
    if (size <= 256) {
      (ctr_set[SLAB_256])++;
      return;
    }
    if (size <= 1024) {
      (ctr_set[SLAB_1024])++;
      return;
    }
    if (size <= 8192) {
      (ctr_set[SLAB_PAGE])++;
      return;
    }
    (ctr_set[SLAB_MAX])++;
  }

  void dec(uint64_t size) {
    if (size <= 64) {
      (ctr_set[SLAB_64])--;
      return;
    }
    if (size <= 256) {
      (ctr_set[SLAB_256])--;
      return;
    }
    if (size <= 1024) {
      (ctr_set[SLAB_1024])--;
      return;
    }
    if (size <= 8192) {
      (ctr_set[SLAB_PAGE])--;
      return;
    }
    (ctr_set[SLAB_MAX])--;
  }

  void inc_overflow() { ctr_set[SLAB_OVERFLOW]++; }
  void dec_overflow() { ctr_set[SLAB_OVERFLOW]--; }

  void inc_msgcnt() {
    if (unlikely(XioPool::trace_msgcnt)) {
      msg_cnt++;
    }
  }

  void dec_msgcnt() {
    if (unlikely(XioPool::trace_msgcnt)) {
      msg_cnt--;
    }
  }

  void inc_hookcnt() {
    if (unlikely(XioPool::trace_msgcnt)) {
      hook_cnt++;
    }
  }

  void dec_hookcnt() {
    if (unlikely(XioPool::trace_msgcnt)) {
      hook_cnt--;
    }
  }
};

extern XioPoolStats xp_stats;

static inline int xpool_alloc(struct xio_mempool *pool, uint64_t size,
			      struct xio_reg_mem* mp)
{
  // try to allocate from the xio pool
  int r = xio_mempool_alloc(pool, size, mp);
  if (r == 0) {
    if (unlikely(XioPool::trace_mempool))
      xp_stats += size;
    return 0;
  }
  // fall back to malloc on errors
  mp->addr = malloc(size);
  assert(mp->addr);
  mp->length = 0;
  if (unlikely(XioPool::trace_mempool))
    xp_stats.inc_overflow();
  return 0;
}

static inline void xpool_free(uint64_t size, struct xio_reg_mem* mp)
{
  if (mp->length) {
    if (unlikely(XioPool::trace_mempool))
      xp_stats -= size;
    xio_mempool_free(mp);
  } else { // from malloc
    if (unlikely(XioPool::trace_mempool))
      xp_stats.dec_overflow();
    free(mp->addr);
  }
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
