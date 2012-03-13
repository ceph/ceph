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

#ifndef CEPH_DOUT_H
#define CEPH_DOUT_H

#include "global/global_context.h"
#include "common/DoutStreambuf.h"
#include "common/config.h"
#include "common/likely.h"
#include "include/assert.h"

#include <iostream>
#include <pthread.h>
#include <streambuf>

extern void dout_emergency(const char * const str);

extern void dout_emergency(const std::string &str);

class DoutLocker
{
public:
  DoutLocker(pthread_mutex_t *lock_)
    : lock(lock_)
  {
    pthread_mutex_lock(lock);
  }
  DoutLocker()
    : lock(NULL)
  {
  }
  ~DoutLocker() {
    if (lock)
      pthread_mutex_unlock(lock);
  }
  pthread_mutex_t *lock;
};

static inline void _dout_begin_line(CephContext *cct, signed int prio) {
  // Put priority information into dout
  cct->_doss->sputc(prio + 12);

  // Some information that goes in every dout message
  cct->_dout << std::hex << pthread_self() << std::dec << " ";
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
#define DOUT_CONDVAR(cct, x) cct->_conf->debug_ ## x
#define XDOUT_CONDVAR(cct, x) DOUT_CONDVAR(cct, x)
#define DOUT_COND(cct, l) cct && (l <= XDOUT_CONDVAR(cct, DOUT_SUBSYS))

// The array declaration will trigger a compiler error if 'l' is
// out of range
#define dout_impl(cct, v) \
  if (0) {\
    char __array[((v >= -1) && (v <= 200)) ? 0 : -1] __attribute__((unused)); \
  }\
  DoutLocker __dout_locker; \
  cct->dout_lock(&__dout_locker); \
  _dout_begin_line(cct, v); \

#define ldout(cct, v) \
  do { if (DOUT_COND(cct, v)) {\
    dout_impl(cct, v) \
    std::ostream* _dout = &(cct->_dout); \
    dout_prefix

#define lpdout(cct, v, p) \
  do { if ((v) <= (p)) {\
    dout_impl(cct, v) \
    std::ostream* _dout = &(cct->_dout); \
    *_dout

#define lgeneric_dout(cct, v) \
  lpdout(cct, v, cct->_conf->debug)

#define lderr(cct) ldout(cct, -1)

#define lgeneric_derr(cct) lgeneric_dout(cct, -1)

#define dendl std::endl; } } while (0)

#endif
