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

#ifndef CEPH_MCLIENTRECONNECT_H
#define CEPH_MCLIENTRECONNECT_H

#include "msg/Message.h"
#include "mds/mdstypes.h"
#include "include/ceph_features.h"


class MClientReconnect : public MessageInstance<MClientReconnect> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 5;
  static constexpr int COMPAT_VERSION = 4;

public:
  map<inodeno_t, cap_reconnect_t> caps; // only head inodes
  vector<snaprealm_reconnect_t> realms;
  bool more = false;

  MClientReconnect() :
    MessageInstance(CEPH_MSG_CLIENT_RECONNECT, HEAD_VERSION, COMPAT_VERSION) {}
private:
  ~MClientReconnect() override {}

  size_t cap_size = 0;
  size_t realm_size = 0;
  size_t approx_size = sizeof(__u32) + sizeof(__u32) + 1;

  void calc_item_size() {
    using ceph::encode;
    {
      bufferlist bl;
      inodeno_t ino;
      cap_reconnect_t cr;
      encode(ino, bl);
      encode(cr, bl);
      cap_size = bl.length();
    }
    {
      bufferlist bl;
      snaprealm_reconnect_t sr;
      encode(sr, bl);
      realm_size = bl.length();
    }
  }

public:
  std::string_view get_type_name() const override { return "client_reconnect"; }
  void print(ostream& out) const override {
    out << "client_reconnect("
	<< caps.size() << " caps " << realms.size() << " realms )";
  }

  // Force to use old encoding.
  // Use connection's features to choose encoding if version is set to 0.
  void set_encoding_version(int v) {
    header.version = v;
    if (v <= 3)
      header.compat_version = 0;
  }
  size_t get_approx_size() {
    return approx_size;
  }
  void mark_more() { more = true; }
  bool has_more() const { return more; }

  void add_cap(inodeno_t ino, uint64_t cap_id, inodeno_t pathbase, const string& path,
	       int wanted, int issued, inodeno_t sr, snapid_t sf, bufferlist& lb)
  {
    caps[ino] = cap_reconnect_t(cap_id, pathbase, path, wanted, issued, sr, sf, lb);
    if (!cap_size)
      calc_item_size();
    approx_size += cap_size + path.length() + lb.length();
  }
  void add_snaprealm(inodeno_t ino, snapid_t seq, inodeno_t parent) {
    snaprealm_reconnect_t r;
    r.realm.ino = ino;
    r.realm.seq = seq;
    r.realm.parent = parent;
    realms.push_back(r);
    if (!realm_size)
      calc_item_size();
    approx_size += realm_size;
  }

  void encode_payload(uint64_t features) override {
    if (header.version == 0) {
      if (features & CEPH_FEATURE_MDSENC)
	header.version = 3;
      else if (features & CEPH_FEATURE_FLOCK)
	header.version = 2;
      else
	header.version = 1;
    }

    using ceph::encode;
    data.clear();

    if (header.version >= 4) {
	encode(caps, data);
	encode(realms, data);
	encode(more, data);
    } else {
      // compat crap
      if (header.version == 3) {
	encode(caps, data);
      } else if (header.version == 2) {
	__u32 n = caps.size();
	encode(n, data);
	for (auto& p : caps) {
	  encode(p.first, data);
	  p.second.encode_old(data);
	}
      } else {
	map<inodeno_t, old_cap_reconnect_t> ocaps;
	for (auto& p : caps) {
	  ocaps[p.first] = p.second;
	encode(ocaps, data);
      }
      for (auto& r : realms)
	r.encode_old(data);
      }
    }
  }
  void decode_payload() override {
    auto p = data.cbegin();
    if (header.version >= 4) {
      decode(caps, p);
      decode(realms, p);
      if (header.version >= 5)
	decode(more, p);
    } else {
      // compat crap
      if (header.version == 3) {
	decode(caps, p);
      } else if (header.version == 2) {
	__u32 n;
	decode(n, p);
	inodeno_t ino;
	while (n--) {
	  decode(ino, p);
	  caps[ino].decode_old(p);
	}
      } else {
	map<inodeno_t, old_cap_reconnect_t> ocaps;
	decode(ocaps, p);
	for (auto &q : ocaps)
	  caps[q.first] = q.second;
      }
      while (!p.end()) {
	realms.push_back(snaprealm_reconnect_t());
	realms.back().decode_old(p);
      }
    }
  }
};


#endif
