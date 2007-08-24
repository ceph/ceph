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

#ifndef __DEBUG_H
#define __DEBUG_H

#include "config.h"

#undef dout

/**
 * for cleaner output, bracket each line with
 * dbeginl (in the dout macro) and dendl (in place of endl).
 */
extern Mutex _dout_lock;
struct _dbeginl_t {
  _dbeginl_t(int) {}
};
struct _dendl_t {
  _dendl_t(int) {}
};
static const _dbeginl_t dbeginl = 0;
static const _dendl_t dendl = 0;

inline ostream& operator<<(ostream& out, _dbeginl_t) {
  _dout_lock.Lock();
  return out;
}
inline ostream& operator<<(ostream& out, _dendl_t) {
  out << endl;
  _dout_lock.Unlock();
  return out;
}

#endif
