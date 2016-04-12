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

#ifndef CEPH_MEMORYMODEL_H
#define CEPH_MEMORYMODEL_H

class CephContext;

class MemoryModel {
public:
  struct snap {
    int peak;
    int size;
    int hwm;
    int rss;
    int data;
    int lib;
    
    int heap, malloc, mmap;

    snap() : peak(0), size(0), hwm(0), rss(0), data(0), lib(0),
	     heap(0), malloc(0), mmap(0)
    {}

    int get_total() { return size; }
    int get_rss() { return rss; }
    int get_heap() { return heap; }
  } last;

private:
  CephContext *cct;
  void _sample(snap *p);

public:
  explicit MemoryModel(CephContext *cct);
  void sample(snap *p = 0) {
    _sample(&last);
    if (p)
      *p = last;
  }
};

#endif
