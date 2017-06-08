// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_TRACKEDINTPTR_H
#define CEPH_TRACKEDINTPTR_H

#include <map>
#include <list>
#include <memory>
#include <utility>
#include "common/Mutex.h"
#include "common/Cond.h"

template <class T>
class TrackedIntPtr {
  T *ptr;
  uint64_t id;
public:
  TrackedIntPtr() : ptr(NULL), id(0) {}
  TrackedIntPtr(T *ptr) : ptr(ptr), id(ptr ? get_with_id(ptr) : 0) {}
  ~TrackedIntPtr() {
    if (ptr)
      put_with_id(ptr, id);
    else
      assert(id == 0);
  }
  void swap(TrackedIntPtr &other) {
    T *optr = other.ptr;
    uint64_t oid = other.id;
    other.ptr = ptr;
    other.id = id;
    ptr = optr;
    id = oid;
  }
  TrackedIntPtr(const TrackedIntPtr &rhs) :
    ptr(rhs.ptr), id(ptr ? get_with_id(ptr) : 0) {}

  void operator=(const TrackedIntPtr &rhs) {
    TrackedIntPtr o(rhs.ptr);
    swap(o);
  }
  const T &operator*() const {
    return *ptr;
  }
  T &operator*() {
    return *ptr;
  }
  const T *operator->() const {
    return ptr;
  }
  T *operator->() {
    return ptr;
  }
  operator bool() const {
    return ptr != NULL;
  }
  bool operator<(const TrackedIntPtr &lhs) const {
    return ptr < lhs.ptr;
  }
  bool operator==(const TrackedIntPtr &lhs) const {
    return ptr == lhs.ptr;
  }
};

#endif
