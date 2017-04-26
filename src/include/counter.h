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
  static unsigned long count() {
    return _count();
  }
  static unsigned long increments() {
    return _increments();
  }
  static unsigned long decrements() {
    return increments()-count();
  }

private:
  static unsigned long &_count() {
    static unsigned long c;
    return c;
  }
  static unsigned long &_increments() {
    static unsigned long i;
    return i;
  }
};

#endif
