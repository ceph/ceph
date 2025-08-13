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
#ifndef CEPH_SI_U_T_H
#define CEPH_SI_U_T_H

#include <cstdint>
#include <iosfwd>

/*
 * Use this struct to pretty print values that should be formatted with a
 * decimal unit prefix (the classic SI units). No actual unit will be added.
 */
struct si_u_t {
  uint64_t v;
  explicit si_u_t(uint64_t _v) : v(_v) {};
};

std::ostream& operator<<(std::ostream& out, const si_u_t& b);

#endif
