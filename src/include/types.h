// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#ifndef CEPH_TYPES_H
#define CEPH_TYPES_H

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

// this is needed for ceph_fs to compile in userland
#include "int_types.h"
#include "platform_errno.h"

#include <string.h>

extern "C" {
#include <stdint.h>
}

#include <iosfwd>

#include "acconfig.h"

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

// DARWIN compatibility
#ifdef __APPLE__
typedef long long loff_t;
typedef long long off64_t;
#define O_DIRECT 00040000
#endif

// FreeBSD compatibility
#ifdef __FreeBSD__
typedef off_t loff_t;
typedef off_t off64_t;
#endif

#if defined(__sun) || defined(_AIX)
typedef off_t loff_t;
#endif


/*
 * comparators for stl containers
 */
// for std::unordered_map:
//   std::unordered_map<const char*, long, hash<const char*>, eqstr> vals;
struct eqstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) == 0;
  }
};

// for set, map
struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};

// ----------------------
// some basic types

// NOTE: these must match ceph_fs.h typedefs
typedef uint64_t ceph_tid_t; // transaction id
typedef uint64_t version_t;
typedef __u32 epoch_t;       // map epoch  (32bits -> 13 epochs/second for 10 years)

// --

/*
 * Use this struct to pretty print values that should be formatted with a
 * decimal unit prefix (the classic SI units). No actual unit will be added.
 */
struct si_u_t {
  uint64_t v;
  explicit si_u_t(uint64_t _v) : v(_v) {};
};

std::ostream& operator<<(std::ostream& out, const si_u_t& b);

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

struct ceph_mon_subscribe_item;
std::ostream& operator<<(std::ostream& out, const ceph_mon_subscribe_item& i);

struct weightf_t {
  float v;
  // cppcheck-suppress noExplicitConstructor
  weightf_t(float _v) : v(_v) {}
};

std::ostream& operator<<(std::ostream& out, const weightf_t& w);

#endif
