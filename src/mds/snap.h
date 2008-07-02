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

#ifndef __CEPH_MDS_SNAP_H
#define __CEPH_MDS_SNAP_H

struct SnapInfo {
  snapid_t snapid;
  inodeno_t base;
  utime_t stamp;
  string name;
  
  void encode(bufferlist& bl) const {
    ::encode(snapid, bl);
    ::encode(base, bl);
    ::encode(stamp, bl);
    ::encode(name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(snapid, bl);
    ::decode(base, bl);
    ::decode(stamp, bl);
    ::decode(name, bl);
  }
};
WRITE_CLASS_ENCODER(SnapInfo)

inline ostream& operator<<(ostream& out, const SnapInfo &sn) {
  return out << "snap(" << sn.snapid << " " << sn.base << " '" << sn.name << "' " << sn.stamp << ")";
}

#endif
