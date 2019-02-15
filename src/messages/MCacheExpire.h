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

#ifndef CEPH_MCACHEEXPIRE_H
#define CEPH_MCACHEEXPIRE_H

#include <string_view>

#include "msg/Message.h"

#include "mds/mdstypes.h"

class MCacheExpire : public MessageInstance<MCacheExpire> {
public:
  friend factory;
private:
  __s32 from;

public:
  /*
    group things by realm (auth delgation root), since that's how auth is determined.
    that makes it less work to process when exports are in progress.
  */
  struct realm {
    map<vinodeno_t, uint32_t> inodes;
    map<dirfrag_t, uint32_t> dirs;
    map<dirfrag_t, map<pair<string,snapid_t>,uint32_t> > dentries;

    void merge(const realm& o) {
      inodes.insert(o.inodes.begin(), o.inodes.end());
      dirs.insert(o.dirs.begin(), o.dirs.end());
      for (const auto &p : o.dentries) {
        auto em = dentries.emplace(std::piecewise_construct, std::forward_as_tuple(p.first), std::forward_as_tuple(p.second));
        if (!em.second) {
	  em.first->second.insert(p.second.begin(), p.second.end());
        }
      }
    }

    void encode(bufferlist &bl) const {
      using ceph::encode;
      encode(inodes, bl);
      encode(dirs, bl);
      encode(dentries, bl);
    }
    void decode(bufferlist::const_iterator &bl) {
      using ceph::decode;
      decode(inodes, bl);
      decode(dirs, bl);
      decode(dentries, bl);
    }
  };
  WRITE_CLASS_ENCODER(realm)

  map<dirfrag_t, realm> realms;

  int get_from() const { return from; }

protected:
  MCacheExpire() : MessageInstance(MSG_MDS_CACHEEXPIRE), from(-1) {}
  MCacheExpire(int f) : 
    MessageInstance(MSG_MDS_CACHEEXPIRE),
    from(f) { }
  ~MCacheExpire() override {}

public:
  std::string_view get_type_name() const override { return "cache_expire";}
  
  void add_inode(dirfrag_t r, vinodeno_t vino, unsigned nonce) {
    realms[r].inodes[vino] = nonce;
  }
  void add_dir(dirfrag_t r, dirfrag_t df, unsigned nonce) {
    realms[r].dirs[df] = nonce;
  }
  void add_dentry(dirfrag_t r, dirfrag_t df, std::string_view dn, snapid_t last, unsigned nonce) {
    realms[r].dentries[df][pair<string,snapid_t>(dn,last)] = nonce;
  }

  void add_realm(dirfrag_t df, const realm& r) {
    auto em = realms.emplace(std::piecewise_construct, std::forward_as_tuple(df), std::forward_as_tuple(r));
    if (!em.second) {
      em.first->second.merge(r);
    }
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(from, p);
    decode(realms, p);
  }
    
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(from, payload);
    encode(realms, payload);
  }
};

WRITE_CLASS_ENCODER(MCacheExpire::realm)

#endif
