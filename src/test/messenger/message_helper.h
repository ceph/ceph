// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef MESSAGE_HELPER_H
#define MESSAGE_HELPER_H

#include "msg/msg_types.h"
#include "messages/MDataPing.h"
#if defined(HAVE_XIO)
#include "msg/xio/XioMessenger.h"
#endif

static inline Message* new_ping_monstyle(const char *tag, int mult)
{
  Message *m = new MPing();
  Formatter *f = new JSONFormatter(true);

  string str = "one giant step for ";

  f->open_object_section(tag);
  for (int ix = 0; ix < mult; ++ix) {
    f->dump_string(tag, str);
  }
  f->close_section();

  bufferlist bl;
  stringstream ss;

  f->flush(ss);
  ::encode(ss.str(), bl);
  m->set_payload(bl);

  return m;
}

#if defined(HAVE_XIO)
extern struct xio_mempool *xio_msgr_mpool;

void xio_hook_func(struct xio_reg_mem *mp)
{
  xio_mempool_free(mp);
}

static inline Message* new_ping_with_data(const char *tag, uint32_t size)
{
  static uint32_t counter;

  MDataPing *m = new MDataPing();
  m->counter = counter++;
  m->tag = tag;

  bufferlist bl;
  void *p;

  struct xio_reg_mem *mp = m->get_mp();
  int e = xio_mempool_alloc(xio_msgr_mpool, size, mp);
  assert(e == 0);
  p = mp->addr;
  m->set_rdma_hook(xio_hook_func);

  strcpy((char*) p, tag);
  uint32_t* t = (uint32_t* ) (((char*) p) + size - 32);
  *t = counter;

  bl.append(buffer::create_static(size, (char*) p));
  m->set_data(bl);

  return static_cast<Message*>(m);
}
#endif

static inline Message* new_simple_ping_with_data(const char *tag,
						 uint32_t size,
						 uint32_t nfrags)
{
  static size_t pagesize = sysconf(_SC_PAGESIZE);
  static uint32_t counter;
  uint32_t segsize;
  int do_page_alignment;

  MDataPing *m = new MDataPing();
  m->counter = counter++;
  m->tag = tag;

  bufferlist bl;
  void *p;

  segsize = (size+nfrags-1)/nfrags;
  segsize = (segsize + 7) & ~7;
  if (segsize < 32) segsize = 32;

  do_page_alignment = segsize >= 1024;
  if (do_page_alignment)
    segsize = (segsize + pagesize - 1) & ~(pagesize - 1);
  m->free_data = true;
  for (uint32_t i = 0; i < nfrags; ++i) {
    if (do_page_alignment) {
      if (posix_memalign(&p, pagesize, segsize))
	p = NULL;
    } else {
	p = malloc(segsize);
    }

    strcpy((char*) p, tag);
    uint32_t* t = (uint32_t* ) (((char*) p) + segsize - 32);
    *t = counter;
    t[1] = i;

    bl.append(buffer::create_static(segsize, (char*) p));
  }
  m->set_data(bl);

  return static_cast<Message*>(m);
}

static inline Message* new_simple_ping_with_data(const char *tag,
						 uint32_t size)
{
  return new_simple_ping_with_data(tag, size, 1);
}


#endif /* MESSAGE_HELPER_H */
