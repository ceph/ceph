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

#include "common/compiler_extensions.h"

#include <iosfwd>
#include <string.h>
#include <errno.h>

class MonitorStore {
  string dir;
  int lock_fd;

  void write_bl_ss(bufferlist& bl, const char *a, const char *b,
		  bool append);
public:
  MonitorStore(const std::string &d) : dir(d), lock_fd(-1) { }
  ~MonitorStore() { }

  int mkfs();  // wipe
  int mount();
  int umount();
  void sync();

  // ints (stored as ascii)
  version_t get_int(const char *a, const char *b=0) WARN_UNUSED_RESULT;
  void put_int(version_t v, const char *a, const char *b=0);

  version_t get_global_version(const char *a, version_t b) WARN_UNUSED_RESULT;

  // buffers
  // ss and sn varieties.
  bool exists_bl_ss(const char *a, const char *b=0);
  int get_bl_ss(bufferlist& bl, const char *a, const char *b) WARN_UNUSED_RESULT;
  void get_bl_ss_safe(bufferlist& bl, const char *a, const char *b) {
    int ret = get_bl_ss(bl, a, b);
    assert (ret >= 0 || ret == -ENOENT);
  }
  void put_bl_ss(bufferlist& bl, const char *a, const char *b) {
    write_bl_ss(bl, a, b, false);
  }
  void append_bl_ss(bufferlist& bl, const char *a, const char *b) {
    write_bl_ss(bl, a, b, true);
  }
  bool exists_bl_sn(const char *a, version_t b) {
    char bs[20];
    snprintf(bs, sizeof(bs), "%llu", (unsigned long long)b);
    return exists_bl_ss(a, bs);
  }
  int get_bl_sn(bufferlist& bl, const char *a, version_t b) WARN_UNUSED_RESULT {
    char bs[20];
    snprintf(bs, sizeof(bs), "%llu", (unsigned long long)b);
    return get_bl_ss(bl, a, bs);
  }
  void get_bl_sn_safe(bufferlist& bl, const char *a, version_t b) {
    int ret = get_bl_sn(bl, a, b);
    assert(ret >= 0 || ret == -ENOENT);
  }
  void put_bl_sn(bufferlist& bl, const char *a, version_t b) {
    char bs[20];
    snprintf(bs, sizeof(bs), "%llu", (unsigned long long)b);
    put_bl_ss(bl, a, bs);
  }
  /**
   * Put a whole set of values efficiently and safely.
   *
   * @param a - prefix/directory
   * @param vals - map of int name -> values
   * @return 0 for success or negative error code
   */
  void put_bl_sn_map(const char *a,
		    map<version_t,bufferlist>::iterator start,
		    map<version_t,bufferlist>::iterator end);

  void erase_ss(const char *a, const char *b);
  void erase_sn(const char *a, version_t b) {
    char bs[20];
    snprintf(bs, sizeof(bs), "%llu", (unsigned long long)b);
    erase_ss(a, bs);
  }

  /*
  version_t get_incarnation() { return get_int("incarnation"); }
  void set_incarnation(version_t i) { set_int(i, "incarnation"); }
  
  version_t get_last_proposal() { return get_int("last_proposal"); }
  void set_last_proposal(version_t i) { set_int(i, "last_proposal"); }
  */
};


#endif
