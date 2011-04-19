// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_DEBUG_H
#define CEPH_DEBUG_H

#include "common/likely.h"
#include "common/config.h"		    // need for g_conf
#include "include/assert.h"

#include <iostream>
#include <pthread.h>
#include <streambuf>

template <typename T, typename U>
class DoutStreambuf;

extern std::ostream *_dout;
extern DoutStreambuf <char, std::basic_string<char>::traits_type> *_doss;
extern bool _dout_need_open;
extern pthread_mutex_t _dout_lock;

extern void _dout_open_log();

extern int dout_handle_daemonize();

extern void dout_emergency(const char * const str);

extern void dout_emergency(const std::string &str);

class DoutLocker
{
public:
  DoutLocker() {
    pthread_mutex_lock(&_dout_lock);
  }
  ~DoutLocker() {
    pthread_mutex_unlock(&_dout_lock);
  }
};

static inline void _dout_begin_line(signed int prio) {
  if (unlikely(_dout_need_open))
    _dout_open_log();

  // Put priority information into dout
  std::streambuf *doss = (std::streambuf*)_doss;
  doss->sputc(prio + 12);

  // Some information that goes in every dout message
  *_dout << std::hex << pthread_self() << std::dec << " ";
}

// intentionally conflict with endl
class _bad_endl_use_dendl_t { public: _bad_endl_use_dendl_t(int) {} };
static const _bad_endl_use_dendl_t endl = 0;
inline std::ostream& operator<<(std::ostream& out, _bad_endl_use_dendl_t) {
  assert(0 && "you are using the wrong endl.. use std::endl or dendl");
  return out;
}

// generic macros
#define debug_DOUT_SUBSYS debug
#define dout_prefix *_dout
#define DOUT_CONDVAR(x) g_conf.debug_ ## x
#define XDOUT_CONDVAR(x) DOUT_CONDVAR(x)
#define DOUT_COND(l) l <= XDOUT_CONDVAR(DOUT_SUBSYS)

// The array declaration will trigger a compiler error if 'l' is
// out of range
#define dout_impl(v) \
  if (0) {\
    char __array[((v >= -1) && (v <= 200)) ? 0 : -1] __attribute__((unused)); \
  }\
  DoutLocker __dout_locker; \
  _dout_begin_line(v); \

#define dout(v) \
  do { if (DOUT_COND(v)) {\
    dout_impl(v) \
    dout_prefix

#define cdout(sys, v)	      \
  do { if (v <= XDOUT_CONDVAR(sys)) { \
    dout_impl(v) \
    dout_prefix

#define pdout(v, p) \
  do { if ((v) <= (p)) {\
    dout_impl(v) \
    *_dout

#define generic_dout(v) \
  pdout(v, g_conf.debug)

#define dendl std::endl; } } while (0)

#define derr dout(-1)

#endif
