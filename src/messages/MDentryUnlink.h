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


#ifndef CEPH_MDENTRYUNLINK_H
#define CEPH_MDENTRYUNLINK_H

#include <string_view>

#include "messages/MMDSOp.h"

class MDentryUnlink final : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

  dirfrag_t dirfrag;
  std::string dn;
  bool unlinking = false;

 public:
  dirfrag_t get_dirfrag() const { return dirfrag; }
  const std::string& get_dn() const { return dn; }
  bool is_unlinking() const { return unlinking; }

  ceph::buffer::list straybl;
  ceph::buffer::list snapbl;

protected:
  MDentryUnlink() :
    MMDSOp(MSG_MDS_DENTRYUNLINK, HEAD_VERSION, COMPAT_VERSION) { }
  MDentryUnlink(dirfrag_t df, std::string_view n, bool u=false) :
    MMDSOp(MSG_MDS_DENTRYUNLINK, HEAD_VERSION, COMPAT_VERSION),
    dirfrag(df), dn(n), unlinking(u) {}
  ~MDentryUnlink() final {}

public:
  std::string_view get_type_name() const override { return "dentry_unlink";}
  void print(std::ostream& o) const override {
    o << "dentry_unlink(" << dirfrag << " " << dn << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(dn, p);
    decode(straybl, p);
    if (header.version >= 2)
      decode(unlinking, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(dn, payload);
    encode(straybl, payload);
    encode(unlinking, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

class MDentryUnlinkAck final : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  dirfrag_t dirfrag;
  std::string dn;

 public:
  dirfrag_t get_dirfrag() const { return dirfrag; }
  const std::string& get_dn() const { return dn; }

protected:
  MDentryUnlinkAck() :
    MMDSOp(MSG_MDS_DENTRYUNLINK_ACK, HEAD_VERSION, COMPAT_VERSION) { }
  MDentryUnlinkAck(dirfrag_t df, std::string_view n) :
    MMDSOp(MSG_MDS_DENTRYUNLINK_ACK, HEAD_VERSION, COMPAT_VERSION),
    dirfrag(df), dn(n) {}
  ~MDentryUnlinkAck() final {}

public:
  std::string_view get_type_name() const override { return "dentry_unlink_ack";}
  void print(std::ostream& o) const override {
    o << "dentry_unlink_ack(" << dirfrag << " " << dn << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(dn, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(dn, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
