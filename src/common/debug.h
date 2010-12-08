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

#include "Clock.h"
#include "Mutex.h"
#include "common/DoutStreambuf.h"
#include "common/likely.h"
#include "include/assert.h"

#include <iosfwd>

extern std::ostream *_dout;
extern DoutStreambuf <char> *_doss;
extern bool _dout_need_open;
extern Mutex _dout_lock;

extern void _dout_open_log();

// Call when the pid changes. For example, after calling daemon().
extern int dout_handle_pid_change();

// Skip output to stderr.
extern void dout_disable_stderr();

extern int dout_create_rank_symlink(int n);

static inline void _dout_begin_line(int prio) {
  _dout_lock.Lock();
  if (unlikely(_dout_need_open))
    _dout_open_log();

  // Put priority information into dout
  _doss->sputc(1);
  _doss->sputc(prio + 11);

  // Some information that goes in every dout message
  *_dout << g_clock.now() << " " << std::hex << pthread_self()
	 << std::dec << " ";
}

static inline void _dout_end_line() {
  _dout_lock.Unlock();
}

// intentionally conflict with endl
class _bad_endl_use_dendl_t { public: _bad_endl_use_dendl_t(int) {} };
static const _bad_endl_use_dendl_t endl = 0;
inline std::ostream& operator<<(std::ostream& out, _bad_endl_use_dendl_t) {
  assert(0 && "you are using the wrong endl.. use std::endl or dendl");
  return out;
}

// generic macros
#define generic_dout(x) do { if ((x) <= g_conf.debug) {\
  _dout_begin_line(x); *_dout

#define pdout(x,p) do { if ((x) <= (p)) {\
  _dout_begin_line(x); *_dout

#define debug_DOUT_SUBSYS debug
#define dout_prefix *_dout
#define DOUT_CONDVAR(x) g_conf.debug_ ## x
#define XDOUT_CONDVAR(x) DOUT_CONDVAR(x)
#define DOUT_COND(l) l <= XDOUT_CONDVAR(DOUT_SUBSYS)

#define dout(l) do { if (DOUT_COND(l)) {\
  _dout_begin_line(l); dout_prefix

#define dendl std::endl; _dout_end_line(); } } while (0)


extern void hex2str(const char *s, int len, char *buf, int dest_len);

extern void hexdump(string msg, const char *s, int len);

#endif
