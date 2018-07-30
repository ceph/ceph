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

#ifndef CEPH_MMDSFRAGMENTNOTIFY_H
#define CEPH_MMDSFRAGMENTNOTIFY_H

#include "msg/Message.h"

class MMDSFragmentNotify : public Message {
public:
  typedef boost::intrusive_ptr<MMDSFragmentNotify> ref;
  typedef boost::intrusive_ptr<MMDSFragmentNotify const> const_ref;
  using factory = MessageFactory<MMDSFragmentNotify>;
  friend factory;
private:
  inodeno_t ino;
  frag_t basefrag;
  int8_t bits = 0;

 public:
  inodeno_t get_ino() const { return ino; }
  frag_t get_basefrag() const { return basefrag; }
  int get_bits() const { return bits; }

  bufferlist basebl;

protected:
  MMDSFragmentNotify() : Message(MSG_MDS_FRAGMENTNOTIFY) {}
  MMDSFragmentNotify(dirfrag_t df, int b) :
	Message(MSG_MDS_FRAGMENTNOTIFY),
    ino(df.ino), basefrag(df.frag), bits(b) { }
  ~MMDSFragmentNotify() override {}

public:  
  const char *get_type_name() const override { return "fragment_notify"; }
  void print(ostream& o) const override {
    o << "fragment_notify(" << ino << "." << basefrag
      << " " << (int)bits << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(ino, payload);
    encode(basefrag, payload);
    encode(bits, payload);
    encode(basebl, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(ino, p);
    decode(basefrag, p);
    decode(bits, p);
    decode(basebl, p);
  }
  
};

#endif
