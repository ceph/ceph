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
 */

#ifndef CEPH_FSMAPCOMPACT_H
#define CEPH_FSMAPCOMPACT_H

#include <map>
#include <string>
#include <string_view>

#include "mds/mdstypes.h"

class FSMapUser {
public:
  struct fs_info_t {
    fs_cluster_id_t cid;
    std::string name;
    fs_info_t() : cid(FS_CLUSTER_ID_NONE) {}
    void encode(bufferlist& bl, uint64_t features) const;
    void decode(bufferlist::const_iterator &bl);
  };

  epoch_t epoch;
  fs_cluster_id_t legacy_client_fscid;
  std::map<fs_cluster_id_t, fs_info_t> filesystems;

  FSMapUser()
    : epoch(0), legacy_client_fscid(FS_CLUSTER_ID_NONE) { }

  epoch_t get_epoch() const { return epoch; }

  fs_cluster_id_t get_fs_cid(std::string_view name) const {
    for (auto &p : filesystems) {
      if (p.second.name == name)
	return p.first;
    }
    return FS_CLUSTER_ID_NONE;
  }

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::const_iterator& bl);

  void print(ostream& out) const;
  void print_summary(Formatter *f, ostream *out);

  static void generate_test_instances(std::list<FSMapUser*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(FSMapUser::fs_info_t)
WRITE_CLASS_ENCODER_FEATURES(FSMapUser)

inline ostream& operator<<(ostream& out, FSMapUser& m) {
  m.print_summary(NULL, &out);
  return out;
}
#endif
