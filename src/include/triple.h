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

#ifndef CEPH_TRIPLE_H
#define CEPH_TRIPLE_H

template<class A, class B, class C>
class triple {
 public:
  A first;
  B second;
  C third;

  triple() {}
  triple(A f, B s, C t) : first(f), second(s), third(t) {}
};

#endif
