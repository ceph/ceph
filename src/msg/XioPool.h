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
#include "libxio.h"
}

class XioPool
{
public:
  struct xio_mempool *handle;
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
	memset(p->payload, 0xcf, p->s);	// XXX DEBUG
	xio_mempool_free(p->mp);
      }
    }
  void *alloc(size_t _s)
    {
	void *r;
	struct xio_mempool_obj mp[1];
	struct xio_piece *x;
	int e = xio_mempool_alloc(handle, (sizeof(*x)-MB) + _s, mp);
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
#endif /* XIO_POOL_H */
