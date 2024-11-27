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

#ifndef CEPH_MMDSSCRUB_H
#define CEPH_MMDSSCRUB_H

#include "messages/MMDSOp.h"

#include "include/types.h"
#include "include/frag.h"

class MMDSScrub : public MMDSOp {
public:
  static constexpr int OP_QUEUEDIR	= 1;
  static constexpr int OP_QUEUEDIR_ACK	= -1;
  static constexpr int OP_QUEUEINO	= 2;
  static constexpr int OP_QUEUEINO_ACK 	= -2;
  static constexpr int OP_ABORT		= 3;
  static constexpr int OP_PAUSE		= 4;
  static constexpr int OP_RESUME	= 5;

  static const char *get_opname(int o) {
    switch (o) {
    case OP_QUEUEDIR: return "queue_dir";
    case OP_QUEUEDIR_ACK: return "queue_dir_ack";
    case OP_QUEUEINO: return "queue_ino";
    case OP_QUEUEINO_ACK: return "queue_ino_ack";
    case OP_ABORT: return "abort";
    case OP_PAUSE: return "pause";
    case OP_RESUME: return "resume";
    default: ceph_abort(); return nullptr;
    }
  }

  std::string_view get_type_name() const override { return "mds_scrub"; }

  void print(std::ostream& out) const override {
    out << "mds_scrub(" << get_opname(op) << " "
	<< ino << " " << frags << " " << tag;
    if (is_force()) out << " force";
    if (is_recursive()) out << " recursive";
    if (is_repair()) out << " repair";
    out << ")";
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(ino, payload);
    encode(frags, payload);
    encode(tag, payload);
    encode(origin, payload);
    encode(flags, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(op, p);
    decode(ino, p);
    decode(frags, p);
    decode(tag, p);
    decode(origin, p);
    decode(flags, p);
  }
  inodeno_t get_ino() const {
    return ino;
  }
  const fragset_t& get_frags() const {
    return frags;
  }
  const std::string& get_tag() const {
    return tag;
  }
  inodeno_t get_origin() const {
    return origin;
  }
  int get_op() const {
    return op;
  }
  bool is_internal_tag() const {
    return flags & FLAG_INTERNAL_TAG;
  }
  bool is_force() const {
    return flags & FLAG_FORCE;
  }
  bool is_recursive() const {
    return flags & FLAG_RECURSIVE;
  }
  bool is_repair() const {
    return flags & FLAG_REPAIR;
  }

protected:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  MMDSScrub() : MMDSOp(MSG_MDS_SCRUB, HEAD_VERSION, COMPAT_VERSION) {}
  MMDSScrub(int o)
    : MMDSOp(MSG_MDS_SCRUB, HEAD_VERSION, COMPAT_VERSION), op(o) {}
  MMDSScrub(int o, inodeno_t i, fragset_t&& _frags, std::string_view _tag,
	    inodeno_t _origin=inodeno_t(), bool internal_tag=false,
	    bool force=false, bool recursive=false, bool repair=false)
    : MMDSOp(MSG_MDS_SCRUB, HEAD_VERSION, COMPAT_VERSION),
    op(o), ino(i), frags(std::move(_frags)), tag(_tag), origin(_origin) {
    if (internal_tag) flags |= FLAG_INTERNAL_TAG;
    if (force) flags |= FLAG_FORCE;
    if (recursive) flags |= FLAG_RECURSIVE;
    if (repair) flags |= FLAG_REPAIR;
  }

  ~MMDSScrub() override {}
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);

  static constexpr unsigned FLAG_INTERNAL_TAG	= 1<<0;
  static constexpr unsigned FLAG_FORCE		= 1<<1;
  static constexpr unsigned FLAG_RECURSIVE	= 1<<2;
  static constexpr unsigned FLAG_REPAIR		= 1<<3;

  int32_t op;
  inodeno_t ino;
  fragset_t frags;
  std::string tag;
  inodeno_t origin;
  uint32_t flags = 0;
};
#endif // CEPH_MMDSSCRUB_H
