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

#ifndef CEPH_MEXPORTDIRDISCOVER_H
#define CEPH_MEXPORTDIRDISCOVER_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirDiscover : public MessageInstance<MExportDirDiscover> {
public:
  friend factory;
private:
  mds_rank_t from = -1;
  dirfrag_t dirfrag;
  filepath path;

 public:
  mds_rank_t get_source_mds() const { return from; }
  inodeno_t get_ino() const { return dirfrag.ino; }
  dirfrag_t get_dirfrag() const { return dirfrag; }
  const filepath& get_path() const { return path; }

  bool started;

protected:
  MExportDirDiscover() :     
    MessageInstance(MSG_MDS_EXPORTDIRDISCOVER),
    started(false) { }
  MExportDirDiscover(dirfrag_t df, filepath& p, mds_rank_t f, uint64_t tid) :
    MessageInstance(MSG_MDS_EXPORTDIRDISCOVER),
    from(f), dirfrag(df), path(p), started(false) {
    set_tid(tid);
  }
  ~MExportDirDiscover() override {}

public:
  std::string_view get_type_name() const override { return "ExDis"; }
  void print(ostream& o) const override {
    o << "export_discover(" << dirfrag << " " << path << ")";
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(from, p);
    decode(dirfrag, p);
    decode(path, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(from, payload);
    encode(dirfrag, payload);
    encode(path, payload);
  }
};

#endif
