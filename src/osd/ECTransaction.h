// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef ECTRANSACTION_H
#define ECTRANSACTION_H

#include "OSD.h"
#include "PGBackend.h"
#include "osd_types.h"
#include "ECUtil.h"
#include <boost/optional/optional_io.hpp>
#include "erasure-code/ErasureCodeInterface.h"

class ECTransaction : public PGBackend::PGTransaction {
public:
  struct AppendOp {
    hobject_t oid;
    uint64_t off;
    bufferlist bl;
    uint32_t fadvise_flags;
    AppendOp(const hobject_t &oid, uint64_t off, bufferlist &bl, uint32_t flags)
      : oid(oid), off(off), bl(bl), fadvise_flags(flags) {}
  };
  struct CloneOp {
    hobject_t source;
    hobject_t target;
    CloneOp(const hobject_t &source, const hobject_t &target)
      : source(source), target(target) {}
  };
  struct RenameOp {
    hobject_t source;
    hobject_t destination;
    RenameOp(const hobject_t &source, const hobject_t &destination)
      : source(source), destination(destination) {}
  };
  struct StashOp {
    hobject_t oid;
    version_t version;
    StashOp(const hobject_t &oid, version_t version)
      : oid(oid), version(version) {}
  };
  struct TouchOp {
    hobject_t oid;
    explicit TouchOp(const hobject_t &oid) : oid(oid) {}
  };
  struct RemoveOp {
    hobject_t oid;
    explicit RemoveOp(const hobject_t &oid) : oid(oid) {}
  };
  struct SetAttrsOp {
    hobject_t oid;
    map<string, bufferlist> attrs;
    SetAttrsOp(const hobject_t &oid, map<string, bufferlist> &_attrs)
      : oid(oid) {
      attrs.swap(_attrs);
    }
    SetAttrsOp(const hobject_t &oid, const string &key, bufferlist &val)
      : oid(oid) {
      attrs.insert(make_pair(key, val));
    }
  };
  struct RmAttrOp {
    hobject_t oid;
    string key;
    RmAttrOp(const hobject_t &oid, const string &key) : oid(oid), key(key) {}
  };
  struct AllocHintOp {
    hobject_t oid;
    uint64_t expected_object_size;
    uint64_t expected_write_size;
    uint32_t flags;
    AllocHintOp(const hobject_t &oid,
                uint64_t expected_object_size,
                uint64_t expected_write_size,
		uint32_t flags)
      : oid(oid),
	expected_object_size(expected_object_size),
        expected_write_size(expected_write_size),
	flags(flags) {}
  };
  struct NoOp {};
  typedef boost::variant<
    AppendOp,
    CloneOp,
    RenameOp,
    StashOp,
    TouchOp,
    RemoveOp,
    SetAttrsOp,
    RmAttrOp,
    AllocHintOp,
    NoOp> Op;
  list<Op> ops;
  uint64_t written;

  ECTransaction() : written(0) {}
  /// Write
  void touch(
    const hobject_t &hoid) {
    ops.push_back(TouchOp(hoid));
  }
  void append(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    bufferlist &bl,
    uint32_t fadvise_flags) {
    if (len == 0) {
      touch(hoid);
      return;
    }
    written += len;
    assert(len == bl.length());
    ops.push_back(AppendOp(hoid, off, bl, fadvise_flags));
  }
  void stash(
    const hobject_t &hoid,
    version_t former_version) {
    ops.push_back(StashOp(hoid, former_version));
  }
  void remove(
    const hobject_t &hoid) {
    ops.push_back(RemoveOp(hoid));
  }
  void setattrs(
    const hobject_t &hoid,
    map<string, bufferlist> &attrs) {
    ops.push_back(SetAttrsOp(hoid, attrs));
  }
  void setattr(
    const hobject_t &hoid,
    const string &attrname,
    bufferlist &bl) {
    ops.push_back(SetAttrsOp(hoid, attrname, bl));
  }
  void rmattr(
    const hobject_t &hoid,
    const string &attrname) {
    ops.push_back(RmAttrOp(hoid, attrname));
  }
  void clone(
    const hobject_t &from,
    const hobject_t &to) {
    ops.push_back(CloneOp(from, to));
  }
  void rename(
    const hobject_t &from,
    const hobject_t &to) {
    ops.push_back(RenameOp(from, to));
  }
  void set_alloc_hint(
    const hobject_t &hoid,
    uint64_t expected_object_size,
    uint64_t expected_write_size,
    uint32_t flags) {
    ops.push_back(AllocHintOp(hoid, expected_object_size, expected_write_size,
			      flags));
  }

  void append(PGTransaction *_to_append) {
    ECTransaction *to_append = static_cast<ECTransaction*>(_to_append);
    written += to_append->written;
    to_append->written = 0;
    ops.splice(ops.end(), to_append->ops,
	       to_append->ops.begin(), to_append->ops.end());
  }
  void nop() {
    ops.push_back(NoOp());
  }
  bool empty() const {
    return ops.empty();
  }
  uint64_t get_bytes_written() const {
    return written;
  }
  template <typename T>
  void visit(T &vis) const {
    for (list<Op>::const_iterator i = ops.begin(); i != ops.end(); ++i) {
      boost::apply_visitor(vis, *i);
    }
  }
  template <typename T>
  void reverse_visit(T &vis) const {
    for (list<Op>::const_reverse_iterator i = ops.rbegin();
	 i != ops.rend();
	 ++i) {
      boost::apply_visitor(vis, *i);
    }
  }
  void get_append_objects(
     set<hobject_t, hobject_t::BitwiseComparator> *out) const;
  void generate_transactions(
    map<hobject_t, ECUtil::HashInfoRef, hobject_t::BitwiseComparator> &hash_infos,
    ErasureCodeInterfaceRef &ecimpl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    map<shard_id_t, ObjectStore::Transaction> *transactions,
    set<hobject_t, hobject_t::BitwiseComparator> *temp_added,
    set<hobject_t, hobject_t::BitwiseComparator> *temp_removed,
    stringstream *out = 0) const;
};

#endif
