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


#ifndef CEPH_MEXPORTDIRPREP_H
#define CEPH_MEXPORTDIRPREP_H

#include "include/types.h"
#include "messages/MMDSOp.h"

class MExportDirPrep : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  dirfrag_t dirfrag;
public:
  ceph::buffer::list basedir;
  std::list<dirfrag_t> bounds;
  std::list<ceph::buffer::list> traces;
private:
  std::set<mds_rank_t> bystanders;
  bool b_did_assim = false;

public:
  dirfrag_t get_dirfrag() const { return dirfrag; }
  const std::list<dirfrag_t>& get_bounds() const { return bounds; }
  const std::set<mds_rank_t> &get_bystanders() const { return bystanders; }

  bool did_assim() const { return b_did_assim; }
  void mark_assim() { b_did_assim = true; }

protected:
  MExportDirPrep() = default;
  MExportDirPrep(dirfrag_t df, uint64_t tid) :
    MMDSOp{MSG_MDS_EXPORTDIRPREP, HEAD_VERSION, COMPAT_VERSION},
    dirfrag(df)
  {
    set_tid(tid);
  }
  ~MExportDirPrep() override {}

public:
  std::string_view get_type_name() const override { return "ExP"; }
  void print(std::ostream& o) const override {
    o << "export_prep(" << dirfrag << ")";
  }

  void add_bound(dirfrag_t df) {
    bounds.push_back( df );
  }
  void add_trace(ceph::buffer::list& bl) {
    traces.push_back(bl);
  }
  void add_bystander(mds_rank_t who) {
    bystanders.insert(who);
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(dirfrag, p);
    decode(basedir, p);
    decode(bounds, p);
    decode(traces, p);
    decode(bystanders, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dirfrag, payload);
    encode(basedir, payload);
    encode(bounds, payload);
    encode(traces, payload);
    encode(bystanders, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
