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

#ifndef CEPH_MON_MONITORSTORE_H
#define CEPH_MON_MONITORSTORE_H

#include "include/types.h"
#include "include/buffer.h"

#include <iosfwd>
#include <string.h>

class MonitorStore {
  string dir;
  int lock_fd;

  int write_bl_ss_impl(bufferlist& bl, const char *a, const char *b,
		       bool append);
  int write_bl_ss(bufferlist& bl, const char *a, const char *b,
		  bool append);
public:
  MonitorStore(const std::string &d) : dir(d), lock_fd(-1) { }
  ~MonitorStore() { }

  int mkfs();  // wipe
  int mount();
  int umount();
  void sync();

  // ints (stored as ascii)
  version_t get_int(const char *a, const char *b=0);
  void put_int(version_t v, const char *a, const char *b=0);

  // buffers
  // ss and sn varieties.
  bool exists_bl_ss(const char *a, const char *b=0);
  int get_bl_ss(bufferlist& bl, const char *a, const char *b);
  int put_bl_ss(bufferlist& bl, const char *a, const char *b) {
    return write_bl_ss(bl, a, b, false);
  }
  int append_bl_ss(bufferlist& bl, const char *a, const char *b) {
    return write_bl_ss(bl, a, b, true);
  }
  bool exists_bl_sn(const char *a, version_t b) {
    char bs[20];
    snprintf(bs, sizeof(bs), "%llu", (unsigned long long)b);
    return exists_bl_ss(a, bs);
  }
  int get_bl_sn(bufferlist& bl, const char *a, version_t b) {
    char bs[20];
    snprintf(bs, sizeof(bs), "%llu", (unsigned long long)b);
    return get_bl_ss(bl, a, bs);
  }
  int put_bl_sn(bufferlist& bl, const char *a, version_t b) {
    char bs[20];
    snprintf(bs, sizeof(bs), "%llu", (unsigned long long)b);
    return put_bl_ss(bl, a, bs);
  }
  /**
   * Put a whole set of values efficiently and safely.
   *
   * @param a - prefix/directory
   * @param vals - map of int name -> values
   * @return 0 for success or negative error code
   */
  int put_bl_sn_map(const char *a,
		    map<version_t,bufferlist>::iterator start,
		    map<version_t,bufferlist>::iterator end);

  int erase_ss(const char *a, const char *b);
  int erase_sn(const char *a, version_t b) {
    char bs[20];
    snprintf(bs, sizeof(bs), "%llu", (unsigned long long)b);
    return erase_ss(a, bs);
  }

  /*
  version_t get_incarnation() { return get_int("incarnation"); }
  void set_incarnation(version_t i) { set_int(i, "incarnation"); }
  
  version_t get_last_proposal() { return get_int("last_proposal"); }
  void set_last_proposal(version_t i) { set_int(i, "last_proposal"); }
  */
};


#endif
