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
#ifndef CEPH_WEIGHTF_T_H
#define CEPH_SI_

#include <cstdint>
#include <iosfwd>

struct weightf_t {
  float v;
  // cppcheck-suppress noExplicitConstructor
  weightf_t(float _v) : v(_v) {}
};

std::ostream& operator<<(std::ostream& out, const weightf_t& w);

#endif
