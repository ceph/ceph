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


#ifndef CEPH_MINODEFILECAPS_H
#define CEPH_MINODEFILECAPS_H

#include "messages/MMDSOp.h"

class MInodeFileCaps : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
  inodeno_t ino;
  __u32     caps = 0;

public:
  inodeno_t get_ino() const { return ino; }
  int       get_caps() const { return caps; }

protected:
  MInodeFileCaps() : MMDSOp(MSG_MDS_INODEFILECAPS, HEAD_VERSION, COMPAT_VERSION) {}
  MInodeFileCaps(inodeno_t ino, int caps) :
    MMDSOp(MSG_MDS_INODEFILECAPS, HEAD_VERSION, COMPAT_VERSION) {
    this->ino = ino;
    this->caps = caps;
  }
  ~MInodeFileCaps() override {}

public:
  std::string_view get_type_name() const override { return "inode_file_caps";}
  void print(std::ostream& out) const override {
    out << "inode_file_caps(" << ino << " " << ccap_string(caps) << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
    encode(caps, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(ino, p);
    decode(caps, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
