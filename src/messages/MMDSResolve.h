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

#ifndef CEPH_MMDSRESOLVE_H
#define CEPH_MMDSRESOLVE_H

#include "msg/Message.h"

#include "include/types.h"

class MMDSResolve : public MessageInstance<MMDSResolve> {
public:
  friend factory;

  map<dirfrag_t, vector<dirfrag_t> > subtrees;
  map<dirfrag_t, vector<dirfrag_t> > ambiguous_imports;

  struct slave_request {
    bufferlist inode_caps;
    bool committing;
    slave_request() : committing(false) {}
    void encode(bufferlist &bl) const {
      using ceph::encode;
      encode(inode_caps, bl);
      encode(committing, bl);
    }
    void decode(bufferlist::const_iterator &bl) {
      using ceph::decode;
      decode(inode_caps, bl);
      decode(committing, bl);
    }
  };

  map<metareqid_t, slave_request> slave_requests;

  // table client information
  struct table_client {
    __u8 type;
    set<version_t> pending_commits;

    table_client() : type(0) {}
    table_client(int _type, const set<version_t>& commits)
      : type(_type), pending_commits(commits) {}

    void encode(bufferlist& bl) const {
      using ceph::encode;
      encode(type, bl);
      encode(pending_commits, bl);
    }
    void decode(bufferlist::const_iterator& bl) {
      using ceph::decode;
      decode(type, bl);
      decode(pending_commits, bl);
    }
  };

  list<table_client> table_clients;

protected:
  MMDSResolve() : MessageInstance(MSG_MDS_RESOLVE) {}
  ~MMDSResolve() override {}

public:
  std::string_view get_type_name() const override { return "mds_resolve"; }

  void print(ostream& out) const override {
    out << "mds_resolve(" << subtrees.size()
	<< "+" << ambiguous_imports.size()
	<< " subtrees +" << slave_requests.size() << " slave requests)";
  }
  
  void add_subtree(dirfrag_t im) {
    subtrees[im].clear();
  }
  void add_subtree_bound(dirfrag_t im, dirfrag_t ex) {
    subtrees[im].push_back(ex);
  }

  void add_ambiguous_import(dirfrag_t im, const vector<dirfrag_t>& m) {
    ambiguous_imports[im] = m;
  }

  void add_slave_request(metareqid_t reqid, bool committing) {
    slave_requests[reqid].committing = committing;
  }

  void add_slave_request(metareqid_t reqid, bufferlist& bl) {
    slave_requests[reqid].inode_caps.claim(bl);
  }

  void add_table_commits(int table, const set<version_t>& pending_commits) {
    table_clients.push_back(table_client(table, pending_commits));
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(subtrees, payload);
    encode(ambiguous_imports, payload);
    encode(slave_requests, payload);
    encode(table_clients, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(subtrees, p);
    decode(ambiguous_imports, p);
    decode(slave_requests, p);
    decode(table_clients, p);
  }
};

inline ostream& operator<<(ostream& out, const MMDSResolve::slave_request&) {
    return out;
}

WRITE_CLASS_ENCODER(MMDSResolve::slave_request)
WRITE_CLASS_ENCODER(MMDSResolve::table_client)
#endif
