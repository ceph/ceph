// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COUNTER_H
#define CEPH_COUNTER_H

#include <atomic>

template <typename T>
class Counter {
public:
  Counter() {
    _count()++;
    _increments()++;
  }
  Counter(const Counter &rhs) {
    _count()++;
    _increments()++;
  }
  Counter(Counter &&rhs) {}
  ~Counter() {
    _count()--;
  }
  static uint64_t count() {
    return _count();
  }
  static uint64_t increments() {
    return _increments();
  }
  static uint64_t decrements() {
    return increments()-count();
  }

private:
  static std::atomic<uint64_t> &_count() {
    static std::atomic<uint64_t> c;
    return c;
  }
  static std::atomic<uint64_t> &_increments() {
    static std::atomic<uint64_t> i;
    return i;
  }
};

#endif
