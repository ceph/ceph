// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#ifndef __REQID_H
#define __REQID_H


#include "include/types.h"
#include "msg/msg_types.h"

/* reqid_t - caller name + incarnation# + tid to unique identify this request
 * use for metadata and osd ops.
 */
class reqid_t {
public:
  entity_name_t name; // who
  int           inc;  // incarnation
  tid_t         tid;
  reqid_t() : inc(0), tid(0) {}
  reqid_t(const entity_name_t& a, int i, tid_t t) : name(a), inc(i), tid(t) {}
};

inline ostream& operator<<(ostream& out, const reqid_t& r) {
  return out << r.name << "." << r.inc << ":" << r.tid;
}

inline bool operator==(const reqid_t& l, const reqid_t& r) {
  return (l.name == r.name) && (l.inc == r.inc) && (l.tid == r.tid);
}
inline bool operator!=(const reqid_t& l, const reqid_t& r) {
  return (l.name != r.name) || (l.inc != r.inc) || (l.tid != r.tid);
}
inline bool operator<(const reqid_t& l, const reqid_t& r) {
  return (l.name < r.name) || (l.inc < r.inc) || 
    (l.name == r.name && l.inc == r.inc && l.tid < r.tid);
}
inline bool operator<=(const reqid_t& l, const reqid_t& r) {
  return (l.name < r.name) || (l.inc < r.inc) ||
    (l.name == r.name && l.inc == r.inc && l.tid <= r.tid);
}
inline bool operator>(const reqid_t& l, const reqid_t& r) { return !(l <= r); }
inline bool operator>=(const reqid_t& l, const reqid_t& r) { return !(l < r); }

namespace __gnu_cxx {
  template<> struct hash<reqid_t> {
    size_t operator()(const reqid_t &r) const { 
      static blobhash H;
      return H((const char*)&r, sizeof(r));
    }
  };
}


#endif
