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


#ifndef CEPH_MMDSMAP_H
#define CEPH_MMDSMAP_H

#include "msg/Message.h"
#include "mds/MDSMap.h"
#include "include/ceph_features.h"

class MMDSMap final : public SafeMessage {
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;
public:
  uuid_d fsid;
  epoch_t epoch = 0;
  ceph::buffer::list encoded;
  // don't really need map_fs_name. it was accidentally added in 51af2346fdf
  // set it to empty string.
  std::string map_fs_name = std::string();

  version_t get_epoch() const { return epoch; }
  const ceph::buffer::list& get_encoded() const { return encoded; }

protected:
  MMDSMap() : 
    SafeMessage{CEPH_MSG_MDS_MAP, HEAD_VERSION, COMPAT_VERSION} {}

  MMDSMap(const uuid_d &f, const MDSMap &mm) :
    SafeMessage{CEPH_MSG_MDS_MAP, HEAD_VERSION, COMPAT_VERSION},
    fsid(f) {
    epoch = mm.get_epoch();
    mm.encode(encoded, -1);  // we will reencode with fewer features as necessary
  }

  ~MMDSMap() final {}

public:
  std::string_view get_type_name() const override { return "mdsmap"; }
  void print(std::ostream& out) const override {
    out << "mdsmap(e " << epoch << ")";
  }

  // marshalling
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(epoch, p);
    decode(encoded, p);
    if (header.version >= 2) {
      decode(map_fs_name, p);
    }
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(fsid, payload);
    encode(epoch, payload);
    if ((features & CEPH_FEATURE_PGID64) == 0 ||
	(features & CEPH_FEATURE_MDSENC) == 0 ||
	(features & CEPH_FEATURE_MSG_ADDR2) == 0 ||
	!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      // reencode for old clients.
      MDSMap m;
      m.decode(encoded);
      encoded.clear();
      m.encode(encoded, features);
    }
    encode(encoded, payload);
    encode(map_fs_name, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
