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

#ifndef CEPH_MMDSRSTATFLUSH_H
#define CEPH_MMDSRSTATFLUSH_H

#include "msg/Message.h"
#include "mds/mdstypes.h"

class MMDSRstatFlush : public Message {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
public:
  static constexpr int OP_RSTATFLUSH =		1;
  static constexpr int OP_RSTATFLUSH_FINISH =	2;
  static constexpr int OP_RSTATFLUSH_SUBTREECOMPELETE =	3;
protected:
  MMDSRstatFlush() : Message{MSG_MDS_RSTATFLUSH, HEAD_VERSION, COMPAT_VERSION} {}
  MMDSRstatFlush(metareqid_t id, int o, utime_t t, filepath rf_root) :
    Message{MSG_MDS_RSTATFLUSH, HEAD_VERSION, COMPAT_VERSION},
    reqid(id), op(o), op_stamp(t), rstat_flush_root(rf_root) {}
  MMDSRstatFlush(metareqid_t id, int o, const map<inodeno_t, pair<filepath, vector<frag_t>>>& completed) :
    Message{MSG_MDS_RSTATFLUSH, HEAD_VERSION, COMPAT_VERSION},
    reqid(id), op(o), op_stamp(utime_t()), subtree_rstatflushed(completed) {}
private:
  metareqid_t reqid;
  int op;
  utime_t op_stamp;
  filepath rstat_flush_root;
  map<inodeno_t, pair<filepath, vector<frag_t>>> subtree_rstatflushed;
public:
  metareqid_t get_reqid() const { return reqid; }
  int get_op() const { return op; }
  utime_t get_op_stamp() const { return op_stamp; }
  const filepath& get_rstatflush_root() const { return rstat_flush_root; }
  map<inodeno_t, pair<filepath, vector<frag_t>>>& get_subtree_rstatflushed() { return subtree_rstatflushed; }


  static const char* get_opname(int o) {
    switch(o) {
    case OP_RSTATFLUSH: return "rstat_flush";
    case OP_RSTATFLUSH_FINISH: return "rstat_flush_finish";
    case OP_RSTATFLUSH_SUBTREECOMPELETE: return "rstat_flush_subtreecomplete";
    default: ceph_abort_msg("invalid MMDSRstatFlush op."); return 0;
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(reqid, payload);
    encode(op, payload);
    encode(op_stamp, payload);
    encode(rstat_flush_root, payload);
    encode(subtree_rstatflushed, payload);
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(reqid, p);
    decode(op, p);
    decode(op_stamp, p);
    decode(rstat_flush_root, p);
    decode(subtree_rstatflushed, p);
  }

  std::string_view get_type_name() const override { return "rstat_flush"; }
  void print(ostream& out) const override {
    out << "rstatflush_request(" << reqid
      << "." << get_opname(op)
      << " " << get_op_stamp()
      << " ";
    if (op == OP_RSTATFLUSH || op == OP_RSTATFLUSH_FINISH) 
      out << rstat_flush_root;
    else
      out << subtree_rstatflushed;
     out << ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
