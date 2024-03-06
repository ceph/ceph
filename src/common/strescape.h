// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_STRESCAPE_H
#define CEPH_STRESCAPE_H

#include <algorithm>
#include <ostream>
#include <string_view>

#include <ctype.h>

inline std::string binstrprint(std::string_view sv, size_t maxlen=0)
{
  std::string s;
  if (maxlen == 0 || sv.size() < maxlen) {
    s = std::string(sv);
  } else {
    maxlen = std::max<size_t>(8, maxlen);
    s = std::string(sv.substr(0, maxlen-3)) + "...";
  }
  std::replace_if(s.begin(), s.end(), [](char c){ return !(isalnum(c) || ispunct(c)); }, '.');
  return s;
}

#endif
