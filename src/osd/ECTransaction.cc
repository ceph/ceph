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

#include <boost/variant.hpp>
#include <boost/optional/optional_io.hpp>
#include <iostream>
#include <vector>
#include <sstream>

#include "ECBackend.h"
#include "ECUtil.h"
#include "os/ObjectStore.h"

struct AppendObjectsGenerator: public boost::static_visitor<void> {
  set<hobject_t, hobject_t::BitwiseComparator> *out;
  explicit AppendObjectsGenerator(set<hobject_t, hobject_t::BitwiseComparator> *out) : out(out) {}
  void operator()(const ECTransaction::AppendOp &op) {
    out->insert(op.oid);
  }
  void operator()(const ECTransaction::TouchOp &op) {
    out->insert(op.oid);
  }
  void operator()(const ECTransaction::CloneOp &op) {
    out->insert(op.source);
    out->insert(op.target);
  }
  void operator()(const ECTransaction::RenameOp &op) {
    out->insert(op.source);
    out->insert(op.destination);
  }
  void operator()(const ECTransaction::StashOp &op) {
    out->insert(op.oid);
  }
  void operator()(const ECTransaction::RemoveOp &op) {
    out->insert(op.oid);
  }
  void operator()(const ECTransaction::SetAttrsOp &op) {}
  void operator()(const ECTransaction::RmAttrOp &op) {}
  void operator()(const ECTransaction::AllocHintOp &op) {}
  void operator()(const ECTransaction::NoOp &op) {}
};
void ECTransaction::get_append_objects(
  set<hobject_t, hobject_t::BitwiseComparator> *out) const
{
  AppendObjectsGenerator gen(out);
  reverse_visit(gen);
}

struct TransGenerator : public boost::static_visitor<void> {
  map<hobject_t, ECUtil::HashInfoRef, hobject_t::BitwiseComparator> &hash_infos;

  ErasureCodeInterfaceRef &ecimpl;
  const pg_t pgid;
  const ECUtil::stripe_info_t sinfo;
  map<shard_id_t, ObjectStore::Transaction> *trans;
  set<int> want;
  set<hobject_t, hobject_t::BitwiseComparator> *temp_added;
  set<hobject_t, hobject_t::BitwiseComparator> *temp_removed;
  stringstream *out;
  TransGenerator(
    map<hobject_t, ECUtil::HashInfoRef, hobject_t::BitwiseComparator> &hash_infos,
    ErasureCodeInterfaceRef &ecimpl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    map<shard_id_t, ObjectStore::Transaction> *trans,
    set<hobject_t, hobject_t::BitwiseComparator> *temp_added,
    set<hobject_t, hobject_t::BitwiseComparator> *temp_removed,
    stringstream *out)
    : hash_infos(hash_infos),
      ecimpl(ecimpl), pgid(pgid),
      sinfo(sinfo),
      trans(trans),
      temp_added(temp_added), temp_removed(temp_removed),
      out(out) {
    for (unsigned i = 0; i < ecimpl->get_chunk_count(); ++i) {
      want.insert(i);
    }
  }

  coll_t get_coll_ct(shard_id_t shard, const hobject_t &hoid) {
    if (hoid.is_temp()) {
      temp_removed->erase(hoid);
      temp_added->insert(hoid);
    }
    return get_coll(shard);
  }
  coll_t get_coll_rm(shard_id_t shard, const hobject_t &hoid) {
    if (hoid.is_temp()) {
      temp_added->erase(hoid);
      temp_removed->insert(hoid);
    }
    return get_coll(shard);
  }
  coll_t get_coll(shard_id_t shard) {
    return coll_t(spg_t(pgid, shard));
  }

  void operator()(const ECTransaction::TouchOp &op) {
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.touch(
	get_coll_ct(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first));

      /* No change, but write it out anyway in case the object did not
       * previously exist. */
      assert(hash_infos.count(op.oid));
      ECUtil::HashInfoRef hinfo = hash_infos[op.oid];
      bufferlist hbuf;
      ::encode(
	*hinfo,
	hbuf);
      i->second.setattr(
	get_coll_ct(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	ECUtil::get_hinfo_key(),
	hbuf);
    }
  }
  void operator()(const ECTransaction::AppendOp &op) {
    uint64_t offset = op.off;
    bufferlist bl(op.bl);
    assert(bl.length());
    assert(offset % sinfo.get_stripe_width() == 0);
    map<int, bufferlist> buffers;

    assert(hash_infos.count(op.oid));
    ECUtil::HashInfoRef hinfo = hash_infos[op.oid];

    // align
    if (bl.length() % sinfo.get_stripe_width())
      bl.append_zero(
	sinfo.get_stripe_width() -
	((offset + bl.length()) % sinfo.get_stripe_width()));
    assert(bl.length() - op.bl.length() < sinfo.get_stripe_width());
    int r = ECUtil::encode(
      sinfo, ecimpl, bl, want, &buffers);

    hinfo->append(
      sinfo.aligned_logical_offset_to_chunk_offset(op.off),
      buffers);
    bufferlist hbuf;
    ::encode(
      *hinfo,
      hbuf);

    assert(r == 0);
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      assert(buffers.count(i->first));
      bufferlist &enc_bl = buffers[i->first];
      i->second.write(
	get_coll_ct(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	sinfo.logical_to_prev_chunk_offset(
	  offset),
	enc_bl.length(),
	enc_bl,
	op.fadvise_flags);
      i->second.setattr(
	get_coll_ct(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	ECUtil::get_hinfo_key(),
	hbuf);
    }
  }
  void operator()(const ECTransaction::CloneOp &op) {
    assert(hash_infos.count(op.source));
    assert(hash_infos.count(op.target));
    *(hash_infos[op.target]) = *(hash_infos[op.source]);
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.clone(
	get_coll_ct(i->first, op.source),
	ghobject_t(op.source, ghobject_t::NO_GEN, i->first),
	ghobject_t(op.target, ghobject_t::NO_GEN, i->first));
    }
  }
  void operator()(const ECTransaction::RenameOp &op) {
    assert(hash_infos.count(op.source));
    assert(hash_infos.count(op.destination));
    *(hash_infos[op.destination]) = *(hash_infos[op.source]);
    hash_infos[op.source]->clear();
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.collection_move_rename(
	get_coll_rm(i->first, op.source),
	ghobject_t(op.source, ghobject_t::NO_GEN, i->first),
	get_coll_ct(i->first, op.destination),
	ghobject_t(op.destination, ghobject_t::NO_GEN, i->first));
    }
  }
  void operator()(const ECTransaction::StashOp &op) {
    assert(hash_infos.count(op.oid));
    hash_infos[op.oid]->clear();
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      coll_t cid(get_coll_rm(i->first, op.oid));
      i->second.collection_move_rename(
	cid,
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	cid,
	ghobject_t(op.oid, op.version, i->first));
    }
  }
  void operator()(const ECTransaction::RemoveOp &op) {
    assert(hash_infos.count(op.oid));
    hash_infos[op.oid]->clear();
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.remove(
	get_coll_rm(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first));
    }
  }
  void operator()(const ECTransaction::SetAttrsOp &op) {
    map<string, bufferlist> attrs(op.attrs);
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.setattrs(
	get_coll_ct(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	attrs);
    }
  }
  void operator()(const ECTransaction::RmAttrOp &op) {
    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
	 i != trans->end();
	 ++i) {
      i->second.rmattr(
	get_coll_ct(i->first, op.oid),
	ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
	op.key);
    }
  }
  void operator()(const ECTransaction::AllocHintOp &op) {
    // logical_to_next_chunk_offset() scales down both aligned and
    // unaligned offsets
    uint64_t object_size = sinfo.logical_to_next_chunk_offset(
                                                    op.expected_object_size);
    uint64_t write_size = sinfo.logical_to_next_chunk_offset(
                                                   op.expected_write_size);

    for (map<shard_id_t, ObjectStore::Transaction>::iterator i = trans->begin();
         i != trans->end();
         ++i) {
      i->second.set_alloc_hint(
        get_coll_ct(i->first, op.oid),
        ghobject_t(op.oid, ghobject_t::NO_GEN, i->first),
        object_size, write_size);
    }
  }
  void operator()(const ECTransaction::NoOp &op) {}
};


void ECTransaction::generate_transactions(
  map<hobject_t, ECUtil::HashInfoRef, hobject_t::BitwiseComparator> &hash_infos,
  ErasureCodeInterfaceRef &ecimpl,
  pg_t pgid,
  const ECUtil::stripe_info_t &sinfo,
  map<shard_id_t, ObjectStore::Transaction> *transactions,
  set<hobject_t, hobject_t::BitwiseComparator> *temp_added,
  set<hobject_t, hobject_t::BitwiseComparator> *temp_removed,
  stringstream *out) const
{
  TransGenerator gen(
    hash_infos,
    ecimpl,
    pgid,
    sinfo,
    transactions,
    temp_added,
    temp_removed,
    out);
  visit(gen);
}
