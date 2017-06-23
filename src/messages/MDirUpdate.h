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

#include "msg/Message.h"

class MDirUpdate : public Message {
  mds_rank_t from_mds;
  dirfrag_t dirfrag;
  int32_t dir_rep;
  int32_t discover;
  compact_set<int32_t> dir_rep_by;
  filepath path;
  int tried_discover;

 public:
  mds_rank_t get_source_mds() const { return from_mds; }
  dirfrag_t get_dirfrag() const { return dirfrag; }
  int get_dir_rep() const { return dir_rep; }
  const compact_set<int>& get_dir_rep_by() const { return dir_rep_by; }
  bool should_discover() const { return discover > tried_discover; }
  const filepath& get_path() const { return path; }

  bool has_tried_discover() const { return tried_discover > 0; }
  void inc_tried_discover() { ++tried_discover; }

  MDirUpdate() : Message(MSG_MDS_DIRUPDATE), tried_discover(0) {}
  MDirUpdate(mds_rank_t f, 
	     dirfrag_t dirfrag,
             int dir_rep,
             compact_set<int>& dir_rep_by,
             filepath& path,
             bool discover = false) :
    Message(MSG_MDS_DIRUPDATE), tried_discover(0) {
    this->from_mds = f;
    this->dirfrag = dirfrag;
    this->dir_rep = dir_rep;
    this->dir_rep_by = dir_rep_by;
    this->discover = discover ? 5 : 0;
    this->path = path;
  }
private:
  ~MDirUpdate() override {}

public:
  const char *get_type_name() const override { return "dir_update"; }
  void print(ostream& out) const override {
    out << "dir_update(" << get_dirfrag() << ")";
  }

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    ::decode(from_mds, p);
    ::decode(dirfrag, p);
    ::decode(dir_rep, p);
    ::decode(discover, p);
    ::decode(dir_rep_by, p);
    ::decode(path, p);
  }

  void encode_payload(uint64_t features) override {
    ::encode(from_mds, payload);
    ::encode(dirfrag, payload);
    ::encode(dir_rep, payload);
    ::encode(discover, payload);
    ::encode(dir_rep_by, payload);
    ::encode(path, payload);
  }
};

#endif
