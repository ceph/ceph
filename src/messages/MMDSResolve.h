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

#include "include/types.h"
#include "mds/Capability.h"
#include "messages/MMDSOp.h"

class MMDSResolve final : public MMDSOp {
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

public:
  static constexpr int OP_PEER		= 1;
  static constexpr int OP_SUBTREE	= 2;

  bool is_op_peer() const { return op == OP_PEER; }
  bool is_op_subtree() const { return op == OP_SUBTREE; }

  std::map<dirfrag_t, std::vector<dirfrag_t>> subtrees;

  struct peer_inode_cap {
    inodeno_t ino;
    std::map<client_t,Capability::Export> cap_exports;
    peer_inode_cap() {}
    peer_inode_cap(inodeno_t a, map<client_t, Capability::Export> b) : ino(a), cap_exports(b) {}
    void encode(ceph::buffer::list &bl) const
    {
      ENCODE_START(1, 1, bl);
      encode(ino, bl);
      encode(cap_exports, bl);
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator &blp)
    {
      DECODE_START(1, blp);
      decode(ino, blp);
      decode(cap_exports, blp);
      DECODE_FINISH(blp);
    }
  };

  struct peer_request {
    ceph::buffer::list inode_caps;
    bool committing;
    peer_request() : committing(false) {}
    void encode(ceph::buffer::list &bl) const {
      ENCODE_START(1, 1, bl);
      encode(inode_caps, bl);
      encode(committing, bl);
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator &blp) {
      DECODE_START(1, blp);
      decode(inode_caps, blp);
      decode(committing, blp);
      DECODE_FINISH(blp);
    }
  };
  std::map<metareqid_t, peer_request> peer_requests;

  struct ambig_import {
    uint64_t tid = 0;
    bool finishing = false;
    bool bystander = false;
    void encode(ceph::buffer::list &bl) const {
      ENCODE_START(2, 2, bl);
      encode(tid, bl);
      encode(finishing, bl);
      encode(bystander, bl);
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator &blp) {
      DECODE_START(2, blp);
      decode(tid, blp);
      decode(finishing, blp);
      decode(bystander, blp);
      DECODE_FINISH(blp);
    }
  };
  std::map<dirfrag_t, ambig_import> ambiguous_imports;

  // table client information
  struct table_client {
    __u8 type;
    std::set<version_t> pending_commits;

    table_client() : type(0) {}
    table_client(int _type, const std::set<version_t>& commits)
      : type(_type), pending_commits(commits) {}

    void encode(ceph::buffer::list& bl) const {
      using ceph::encode;
      encode(type, bl);
      encode(pending_commits, bl);
    }
    void decode(ceph::buffer::list::const_iterator& bl) {
      using ceph::decode;
      decode(type, bl);
      decode(pending_commits, bl);
    }
  };

  std::list<table_client> table_clients;

public:
  std::string_view get_type_name() const override { return "mds_resolve"; }

  void print(std::ostream& out) const override {
    out << "mds_resolve(" << subtrees.size()
	<< "+" << ambiguous_imports.size()
	<< " subtrees +" << peer_requests.size() << " peer requests)";
  }
  
  void add_subtree(dirfrag_t im) {
    subtrees[im].clear();
  }
  void add_subtree_bound(dirfrag_t im, dirfrag_t ex) {
    subtrees[im].push_back(ex);
  }

  void add_ambiguous_import(dirfrag_t im, uint64_t tid, bool finishing) {
    auto& ambig_import = ambiguous_imports[im];
    ambig_import.tid = tid;
    ambig_import.finishing = finishing;
  }

  void add_other_ambiguous_import(dirfrag_t im, uint64_t tid) {
    auto& ambig_import = ambiguous_imports[im];
    ambig_import.tid = tid;
    ambig_import.bystander = true;
  }

  void add_peer_request(metareqid_t reqid, bool committing) {
    peer_requests[reqid].committing = committing;
  }

  void add_peer_request(metareqid_t reqid, ceph::buffer::list& bl) {
    peer_requests[reqid].inode_caps = std::move(bl);
  }

  void add_table_commits(int table, const std::set<version_t>& pending_commits) {
    table_clients.push_back(table_client(table, pending_commits));
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(subtrees, payload);
    encode(ambiguous_imports, payload);
    encode(peer_requests, payload);
    encode(table_clients, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(op, p);
    decode(subtrees, p);
    decode(ambiguous_imports, p);
    decode(peer_requests, p);
    decode(table_clients, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);

  MMDSResolve() : MMDSOp{MSG_MDS_RESOLVE, HEAD_VERSION, COMPAT_VERSION} {}
  MMDSResolve(int o) : MMDSResolve() { op = o; }
  ~MMDSResolve() final {}

  int32_t op = 0;
};

inline std::ostream& operator<<(std::ostream& out, const MMDSResolve::peer_request&) {
    return out;
}

WRITE_CLASS_ENCODER(MMDSResolve::peer_inode_cap)
WRITE_CLASS_ENCODER(MMDSResolve::peer_request)
WRITE_CLASS_ENCODER(MMDSResolve::ambig_import)
WRITE_CLASS_ENCODER(MMDSResolve::table_client)

#endif
