// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#ifndef __EBOFS_BUFFERPOOL_H
#define __EBOFS_BUFFERPOOL_H


#include <iostream>
#include <list>
using namespace std;

// for posix_memalign
#define _XOPEN_SOURCE 600
#include <stdlib.h>
#include <malloc.h>

// for mmap
#include <sys/mman.h>

#include "include/buffer.h"
#include "include/bufferlist.h"

#include "config.h"



class AlignedBufferPool {
  int alignment;              // err, this isn't actually enforced!  we just use mmap.

  bool dommap;

  off_t talloc;

 public:
  AlignedBufferPool(int a) : alignment(a), dommap(true), talloc(0) {}
  ~AlignedBufferPool() {
  }

#ifdef __CYGWIN__
  
  bufferptr alloc(size_t bytes) {
    // overallocate.
    bufferptr bp = new buffer(bytes+4095);
    int off = (unsigned)bp.c_str() % 4096;
    if (off) 
      bp.set_offset(4096-off);
    bp.set_length(bytes);
    return bp;
  }

  bufferptr alloc_page() {
    return alloc(4096);
  }

  void alloc(size_t bytes, bufferlist& bl) {
    bl.clear();

    // keep allocations reasonably small to avoid fragmenting memory
    while (bytes > 0) {
      size_t max = MIN(bytes, g_conf.ebofs_abp_max_alloc);
      bufferptr bp = alloc(max);
      bl.push_back( bp );
      bytes -= max;
    }
  }

#else

  void free(char *p, unsigned len) {
    dout(10) << "bufferpool(" << (void*)this << ").free " << (void*)p << " len " << len << " ... total " << talloc << endl;
    talloc -= len;
    if (dommap)
      ::munmap(p, len);
    else 
      ::free((void*)p);
  }

  static void aligned_buffer_free_func(void *arg, char *ptr, unsigned len) {
    AlignedBufferPool *pool = (AlignedBufferPool*)arg;
    pool->free(ptr, len);
  }

  buffer *alloc(size_t bytes) {
    assert(bytes % alignment == 0);
    char *p = 0;

    // use mmap or posix_memalign.
    if (dommap)
      p = (char*)::mmap(NULL, bytes, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    else 
      ::posix_memalign((void**)&p, alignment, bytes);
    assert(p);
    
    talloc += bytes;

    if (g_conf.ebofs_abp_zero)
      ::bzero(p, bytes);  // only to shut up valgrind

    dout(10) << "bufferpool(" << (void*)this << ").alloc " << (void*)p << " len " << bytes << " ... total " << talloc << endl;

    return
      new buffer(p, bytes, BUFFER_MODE_NOCOPY|BUFFER_MODE_NOFREE|BUFFER_MODE_CUSTOMFREE,
                 bytes,
                 aligned_buffer_free_func, this);
  }

  // allocate a single buffer
  buffer* alloc_page() {
    return alloc(alignment);
  }


  // bufferlists
  void alloc(size_t bytes, bufferlist& bl) {
    bl.clear();

    // keep allocations reasonably small to avoid fragmenting memory
    while (bytes > 0) {
      size_t max = MIN(bytes, g_conf.ebofs_abp_max_alloc);
      bl.push_back( alloc(max) );
      bytes -= max;
    }
  }

#endif

};


#endif
