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

#ifndef CEPH_XLIST_PRINT_H
#define CEPH_XLIST_PRINT_H

#include "include/xlist.h"

#include <ostream>

template<typename T>
std::ostream &operator<<(std::ostream &oss, const xlist<T> &list) {
  bool first = true;
  for (const auto &item : list) {
    if (!first) {
      oss << ", ";
    }
    oss << *item; /* item should be a pointer */
    first = false;
  }
  return oss;
}

#endif
