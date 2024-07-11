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
    fs_info_t() {}
    void encode(ceph::buffer::list& bl, uint64_t features) const;
    void decode(ceph::buffer::list::const_iterator &bl);
    std::string name;
    fs_cluster_id_t cid = FS_CLUSTER_ID_NONE;
  };

  FSMapUser() {}

  epoch_t get_epoch() const { return epoch; }

  fs_cluster_id_t get_fs_cid(std::string_view name) const {
    for (auto &p : filesystems) {
      if (p.second.name == name)
	return p.first;
    }
    return FS_CLUSTER_ID_NONE;
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);

  void print(std::ostream& out) const;
  void print_summary(ceph::Formatter *f, std::ostream *out) const;

  static void generate_test_instances(std::list<FSMapUser*>& ls);

  std::map<fs_cluster_id_t, fs_info_t> filesystems;
  fs_cluster_id_t legacy_client_fscid = FS_CLUSTER_ID_NONE;
  epoch_t epoch = 0;
};
WRITE_CLASS_ENCODER_FEATURES(FSMapUser::fs_info_t)
WRITE_CLASS_ENCODER_FEATURES(FSMapUser)

inline std::ostream& operator<<(std::ostream& out, const FSMapUser& m) {
  m.print_summary(NULL, &out);
  return out;
}
#endif
