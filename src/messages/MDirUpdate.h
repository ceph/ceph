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


#ifndef CEPH_MDIRUPDATE_H
#define CEPH_MDIRUPDATE_H

#include "messages/MMDSOp.h"

class MDirUpdate : public MMDSOp {
public:
  mds_rank_t get_source_mds() const { return from_mds; }
  dirfrag_t get_dirfrag() const { return dirfrag; }
  int get_dir_rep() const { return dir_rep; }
  const std::set<int32_t>& get_dir_rep_by() const { return dir_rep_by; }
  bool should_discover() const { return discover > tried_discover; }
  const filepath& get_path() const { return path; }

  bool has_tried_discover() const { return tried_discover > 0; }
  void inc_tried_discover() const { ++tried_discover; }

  std::string_view get_type_name() const override { return "dir_update"; }
  void print(std::ostream& out) const override {
    out << "dir_update(" << get_dirfrag() << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(from_mds, p);
    decode(dirfrag, p);
    decode(dir_rep, p);
    decode(discover, p);
    decode(dir_rep_by, p);
    decode(path, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(from_mds, payload);
    encode(dirfrag, payload);
    encode(dir_rep, payload);
    encode(discover, payload);
    encode(dir_rep_by, payload);
    encode(path, payload);
  }

protected:
  ~MDirUpdate() {}
  MDirUpdate() : MMDSOp(MSG_MDS_DIRUPDATE, HEAD_VERSION, COMPAT_VERSION) {}
  MDirUpdate(mds_rank_t f,
	     dirfrag_t dirfrag,
             int dir_rep,
             const std::set<int32_t>& dir_rep_by,
             filepath& path,
             bool discover = false) :
    MMDSOp(MSG_MDS_DIRUPDATE, HEAD_VERSION, COMPAT_VERSION), from_mds(f), dirfrag(dirfrag),
    dir_rep(dir_rep), dir_rep_by(dir_rep_by), path(path) {
    this->discover = discover ? 5 : 0;
  }
  MDirUpdate(const MDirUpdate& m)
  : MMDSOp{MSG_MDS_DIRUPDATE},
    from_mds(m.from_mds),
    dirfrag(m.dirfrag),
    dir_rep(m.dir_rep),
    discover(m.discover),
    dir_rep_by(m.dir_rep_by),
    path(m.path),
    tried_discover(m.tried_discover)
  {}

  mds_rank_t from_mds = -1;
  dirfrag_t dirfrag;
  int32_t dir_rep = 5;
  int32_t discover = 5;
  std::set<int32_t> dir_rep_by;
  filepath path;
  mutable int tried_discover = 0; // XXX HACK

private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
