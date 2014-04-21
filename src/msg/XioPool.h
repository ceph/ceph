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
#include "libxio.h"
}

class XioPool
{
public:
  struct xio_mempool *handle;
  struct xio_piece {
    struct xio_piece *next;
    struct xio_mempool_obj mp[1];
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
	xio_mempool_free(p->mp);
      }
    }
  void *alloc(size_t _s)
    {
	void *r;
	struct xio_piece *x = static_cast<struct xio_piece *>(malloc(sizeof *x));
	if (!x) return 0;
	int e = xio_mempool_alloc(handle, _s, x->mp);
	if (e) {
	  free(x);
	  r = 0;
	} else {
	  x->next = first;
	  first = x;
	  r = x->mp->addr;
	}
	return r;
    }
};
#endif /* XIO_POOL_H */
