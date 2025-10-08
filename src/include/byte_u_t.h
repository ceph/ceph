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
#ifndef CEPH_BYTE_U_T_H
#define CEPH_BYTE_U_T_H

#include <cstdint>
#include <iosfwd>

/*
 * Use this struct to pretty print values that should be formatted with a
 * binary unit prefix (IEC units). Since binary unit prefixes are to be used for
 * "multiples of units in data processing, data transmission, and digital
 * information" (so bits and bytes) and so far bits are not printed, the unit
 * "B" for "byte" is added besides the multiplier.
 */
struct byte_u_t {
  uint64_t v;
  explicit byte_u_t(uint64_t _v) : v(_v) {};
};

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<byte_u_t> : fmt::ostream_formatter {};
#endif

std::ostream& operator<<(std::ostream& out, const byte_u_t& b);

#endif
