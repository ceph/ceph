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

#include <iostream>
#include <sstream>
#include <fmt/ostream.h>

#include "ECCommon.h"

#include <valarray>

#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpRead.h"
#include "ECMsgTypes.h"
#include "PGLog.h"
#include "osd_tracer.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::dec;
using std::hex;
using std::less;
using std::list;
using std::make_pair;
using std::map;
using std::pair;
using std::ostream;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferhash;
using ceph::bufferlist;
using ceph::bufferptr;
using ceph::ErasureCodeInterfaceRef;
using ceph::Formatter;

static ostream& _prefix(std::ostream *_dout, ECCommon::RMWPipeline *rmw_pipeline) {
  return rmw_pipeline->get_parent()->gen_dbg_prefix(*_dout);
}
static ostream& _prefix(std::ostream *_dout, ECCommon::ReadPipeline *read_pipeline) {
  return read_pipeline->get_parent()->gen_dbg_prefix(*_dout);
}
static ostream& _prefix(std::ostream *_dout,
			ECCommon::UnstableHashInfoRegistry *unstable_hash_info_registry) {
  // TODO: backref to ECListener?
  return *_dout;
}
static ostream& _prefix(std::ostream *_dout, struct ClientReadCompleter *read_completer);

ostream &operator<<(ostream &lhs, const ECCommon::ec_align_t &rhs)
{
  return lhs << rhs.offset << ","
	     << rhs.size << ","
	     << rhs.flags;
}

ostream &operator<<(ostream &lhs, const ECCommon::ec_extent_t &rhs)
{
  return lhs << rhs.err << ","
	     << rhs.emap;
}

ostream &operator<<(ostream &lhs, const ECCommon::shard_read_t &rhs)
{
  return lhs << "shard_read_t(extents=[" << rhs.extents << "]"
             << ", zero_pad=" << rhs.zero_pad
	     << ", subchunk=" << rhs.subchunk
	     << ")";
}

ostream &operator<<(ostream &lhs, const ECCommon::read_request_t &rhs)
{
  return lhs << "read_request_t(to_read=[" << rhs.to_read << "]"
             << ", flags=" << rhs.flags
             << ", shard_want_to_read=" << rhs.shard_want_to_read
	     << ", shard_reads=" << rhs.shard_reads
	     << ", want_attrs=" << rhs.want_attrs
	     << ")";
}

ostream &operator<<(ostream &lhs, const ECCommon::read_result_t &rhs)
{
  lhs << "read_result_t(r=" << rhs.r
      << ", errors=" << rhs.errors;
  if (rhs.attrs) {
    lhs << ", attrs=" << *(rhs.attrs);
  } else {
    lhs << ", noattrs";
  }
  return lhs << ", buffers_read=" << rhs.buffers_read << ")";
}

ostream &operator<<(ostream &lhs, const ECCommon::ReadOp &rhs)
{
  lhs << "ReadOp(tid=" << rhs.tid;
#ifndef WITH_SEASTAR
  if (rhs.op && rhs.op->get_req()) {
    lhs << ", op=";
    rhs.op->get_req()->print(lhs);
  }
#endif
  return lhs << ", to_read=" << rhs.to_read
	     << ", complete=" << rhs.complete
	     << ", priority=" << rhs.priority
	     << ", obj_to_source=" << rhs.obj_to_source
	     << ", source_to_obj=" << rhs.source_to_obj
	     << ", in_progress=" << rhs.in_progress
             << ", debug_log=" << rhs.debug_log
             << ")";
}

void ECCommon::ReadOp::dump(Formatter *f) const
{
  f->dump_unsigned("tid", tid);
#ifndef WITH_SEASTAR
  if (op && op->get_req()) {
    f->dump_stream("op") << *(op->get_req());
  }
#endif
  f->dump_stream("to_read") << to_read;
  f->dump_stream("complete") << complete;
  f->dump_int("priority", priority);
  f->dump_stream("obj_to_source") << obj_to_source;
  f->dump_stream("source_to_obj") << source_to_obj;
  f->dump_stream("in_progress") << in_progress;
}

ostream &operator<<(ostream &lhs, const ECCommon::RMWPipeline::Op &rhs)
{
  lhs << "Op(" << rhs.hoid
      << " v=" << rhs.version
      << " tt=" << rhs.trim_to
      << " tid=" << rhs.tid
      << " reqid=" << rhs.reqid;
#ifndef WITH_SEASTAR
  if (rhs.client_op && rhs.client_op->get_req()) {
    lhs << " client_op=";
    rhs.client_op->get_req()->print(lhs);
  }
#endif
  lhs << " pg_committed_to=" << rhs.pg_committed_to
      << " temp_added=" << rhs.temp_added
      << " temp_cleared=" << rhs.temp_cleared
      << " remote_read_result=" << rhs.remote_shard_extent_map
      << " pending_commit=" << rhs.pending_commit
      << " plan.to_read=" << rhs.plan
      << ")";
  return lhs;
}

void ECCommon::ReadPipeline::complete_read_op(ReadOp &&rop)
{
  dout(20) << __func__ << " completing " << rop << dendl;
  map<hobject_t, read_request_t>::iterator req_iter =
    rop.to_read.begin();
  map<hobject_t, read_result_t>::iterator resiter =
    rop.complete.begin();
  ceph_assert(rop.to_read.size() == rop.complete.size());
  for (; req_iter != rop.to_read.end(); ++req_iter, ++resiter) {
    rop.on_complete->finish_single_request(
      req_iter->first,
      std::move(resiter->second),
      req_iter->second);
  }
  ceph_assert(rop.on_complete);
  std::move(*rop.on_complete).finish(rop.priority);
  rop.on_complete = nullptr;
  // if the read op is over. clean all the data of this tid.
  for (set<pg_shard_t>::iterator iter = rop.in_progress.begin();
    iter != rop.in_progress.end();
    iter++) {
    shard_to_read_map[*iter].erase(rop.tid);
  }
  rop.in_progress.clear();
  tid_to_read_map.erase(rop.tid);
}

void ECCommon::ReadPipeline::on_change()
{
  for (map<ceph_tid_t, ReadOp>::iterator i = tid_to_read_map.begin();
       i != tid_to_read_map.end();
       ++i) {
    dout(10) << __func__ << ": cancelling " << i->second << dendl;
  }
  tid_to_read_map.clear();
  shard_to_read_map.clear();
  in_progress_client_reads.clear();
}

void ECCommon::ReadPipeline::get_all_avail_shards(
  const hobject_t &hoid,
  set<int> &have,
  map<shard_id_t, pg_shard_t> &shards,
  bool for_recovery,
  const std::optional<set<pg_shard_t>>& error_shards)
{
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_shards().begin();
       i != get_parent()->get_acting_shards().end();
       ++i) {
    dout(10) << __func__ << ": checking acting " << *i << dendl;
    const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
    if (error_shards && error_shards->contains(*i)) {
      continue;
    }
    if (cct->_conf->bluestore_debug_inject_read_err &&
        ec_inject_test_read_error1(ghobject_t(hoid, ghobject_t::NO_GEN, i->shard))) {
      dout(0) << __func__ << " Error inject - Missing shard " << i->shard << dendl;
      continue;
    }
    if (!missing.is_missing(hoid)) {
      ceph_assert(!have.count(i->shard));
      have.insert(i->shard);
      ceph_assert(!shards.count(i->shard));
      shards.insert(make_pair(i->shard, *i));
    }
  }

  if (for_recovery) {
    for (set<pg_shard_t>::const_iterator i =
	   get_parent()->get_backfill_shards().begin();
	 i != get_parent()->get_backfill_shards().end();
	 ++i) {
      if (error_shards && error_shards->find(*i) != error_shards->end())
	continue;
      if (have.count(i->shard)) {
	ceph_assert(shards.count(i->shard));
	continue;
      }
      dout(10) << __func__ << ": checking backfill " << *i << dendl;
      ceph_assert(!shards.count(i->shard));
      const pg_info_t &info = get_parent()->get_shard_info(*i);
      const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
      if (hoid < info.last_backfill &&
	  !missing.is_missing(hoid)) {
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }

    map<hobject_t, set<pg_shard_t>>::const_iterator miter =
      get_parent()->get_missing_loc_shards().find(hoid);
    if (miter != get_parent()->get_missing_loc_shards().end()) {
      for (set<pg_shard_t>::iterator i = miter->second.begin();
	   i != miter->second.end();
	   ++i) {
	dout(10) << __func__ << ": checking missing_loc " << *i << dendl;
	auto m = get_parent()->maybe_get_shard_missing(*i);
	if (m) {
	  ceph_assert(!(*m).is_missing(hoid));
	}
	if (error_shards && error_shards->find(*i) != error_shards->end())
	  continue;
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }
  }
}

int ECCommon::ReadPipeline::get_min_avail_to_read_shards(
  const hobject_t &hoid,
  bool for_recovery,
  bool do_redundant_reads,
  read_request_t &read_request,
  const std::optional<set<pg_shard_t>>& error_shards) {
  // Make sure we don't do redundant reads for recovery
  ceph_assert(!for_recovery || !do_redundant_reads);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;

  get_all_avail_shards(hoid, have, shards, for_recovery, error_shards);

  map<int, vector<pair<int, int>>> need;
  set<int> want;

  for (auto &&[shard, _] : read_request.shard_want_to_read) {
    want.insert(shard);
  }

  int r = ec_impl->minimum_to_decode(want, have, &need);
  if (r < 0) {
    dout(20) << "minimum_to_decode_failed r: " << r <<"want: " << want
      << " have: " << have << " need: " << need << dendl;
    return r;
  }

  if (do_redundant_reads) {
    vector<pair<int, int>> subchunks_list;
    subchunks_list.push_back(make_pair(0, ec_impl->get_sub_chunk_count()));
    for (auto &&i: have) {
      need[i] = subchunks_list;
    }
  }

  extent_set extra_extents;
  extent_set read_superset;
  map<int, extent_set> read_mask;
  map<int, extent_set> zero_mask;

  ceph_assert(read_request.object_size != 0);

  sinfo.ro_size_to_read_mask(read_request.object_size, read_mask);
  sinfo.ro_size_to_zero_mask(read_request.object_size, zero_mask);

  /* First deal with missing shards */
  for (auto &&[shard, extent_set] : read_request.shard_want_to_read) {
    /* Work out what extra extents we need to read on each shard. If do
     * redundant reads is set, then we want to have the same reads on
     * every extent. Otherwise, we need to read every shard only if the
     * necessary shard is missing.
     */
    if (!have.contains(shard) || do_redundant_reads) {
      extra_extents.union_of(extent_set);
    }
    read_superset.union_of(extent_set);
  }

  for (auto &&[shard, subchunk] : need) {
    if (!have.contains(shard)) {
      continue;
    }
    pg_shard_t pg_shard = shards[shard_id_t(shard)];
    extent_set extents = extra_extents;
    shard_read_t shard_read;
    shard_read.subchunk = subchunk;

    if (read_request.shard_want_to_read.contains(shard)) {
      extents.union_of(read_request.shard_want_to_read.at(shard));
    }

    extents.align(CEPH_PAGE_SIZE);
    if (zero_mask.contains(shard)) {
      shard_read.zero_pad.intersection_of(extents, zero_mask.at(shard));
    }
    if (read_mask.contains(shard)) {
      shard_read.extents.intersection_of(extents, read_mask.at(shard));
    }

    ceph_assert(!shard_read.zero_pad.empty() || !shard_read.extents.empty());
    read_request.shard_reads[pg_shard] = shard_read;
  }

  dout(20) << __func__ << " for_recovery: " << for_recovery
    << " do_redundant_reads: " << do_redundant_reads
    << " read_request: " << read_request
    << " error_shards: " << error_shards
    << dendl;
  return 0;
}


void ECCommon::ReadPipeline::get_min_want_to_read_shards(
  const ec_align_t &to_read,
  map<int, extent_set> &want_shard_reads)
{
  sinfo.ro_range_to_shard_extent_set(to_read.offset, to_read.size, want_shard_reads);;
  dout(20) << __func__ << ": to_read " << to_read
	   << " read_request " << want_shard_reads << dendl;
}

int ECCommon::ReadPipeline::get_remaining_shards(
  const hobject_t &hoid,
  read_result_t &read_result,
  read_request_t &read_request,
  bool for_recovery,
  bool fast_read)
{
  set<int> have;
  map<shard_id_t, pg_shard_t> shards;
  set<pg_shard_t> error_shards;
  for (auto &&[shard, _] : read_result.errors) {
    error_shards.insert(shard);
  }

  int r = get_min_avail_to_read_shards(
    hoid,
    for_recovery,
    fast_read,
    read_request,
    error_shards);

  if (r) {
    dout(0) << __func__ << " not enough shards left to try for " << hoid
	    << " read result was " << read_result << dendl;
    return -EIO;
  }

  // Rather than repeating whole read, we can remove everything we already have.
  for (auto iter = read_request.shard_reads.begin();
      iter != read_request.shard_reads.end();) {
    auto &&[pg_shard, shard_read] = *(iter++);

    // Ignore where shard has not been read at all.
    if (read_result.processed_read_requests.contains(pg_shard.shard)) {

      shard_read.extents.subtract(read_result.processed_read_requests.at(pg_shard.shard));
      shard_read.zero_pad.subtract(read_result.processed_read_requests.at(pg_shard.shard));
      if (shard_read.extents.empty() && shard_read.zero_pad.empty()) {
        read_request.shard_reads.erase(pg_shard);
      }
    }
  }

  return 0;
}

void ECCommon::ReadPipeline::start_read_op(
  int priority,
  map<hobject_t, read_request_t> &to_read,
  OpRequestRef _op,
  bool do_redundant_reads,
  bool for_recovery,
  std::unique_ptr<ECCommon::ReadCompleter> on_complete)
{
  ceph_tid_t tid = get_parent()->get_tid();
  ceph_assert(!tid_to_read_map.count(tid));
  auto &op = tid_to_read_map.emplace(
    tid,
    ReadOp(
      priority,
      tid,
      do_redundant_reads,
      for_recovery,
      std::move(on_complete),
      _op,
      std::move(to_read))).first->second;
  dout(10) << __func__ << ": starting " << op << dendl;
  if (_op) {
#ifndef WITH_SEASTAR
    op.trace = _op->pg_trace;
#endif
    op.trace.event("start ec read");
  }
  do_read_op(op);
}

void ECCommon::ReadPipeline::do_read_op(ReadOp &op)
{
  int priority = op.priority;
  ceph_tid_t tid = op.tid;

  dout(10) << __func__ << ": starting read " << op << dendl;

  map<pg_shard_t, ECSubRead> messages;
  for (auto &&[hoid, read_request] : op.to_read) {
    bool need_attrs = read_request.want_attrs;

    for (auto &&[shard, shard_read] : read_request.shard_reads) {
      if (need_attrs) {
	messages[shard].attrs_to_read.insert(hoid);
	need_attrs = false;
      }
      messages[shard].subchunks[hoid] = shard_read.subchunk;
      op.obj_to_source[hoid].insert(shard);
      op.source_to_obj[shard].insert(hoid);
    }
    for (auto &&[shard, shard_read] : read_request.shard_reads) {
      bool empty = true;
      if (!shard_read.extents.empty()) {
        op.debug_log.emplace_back(ECUtil::READ_REQUEST, shard, shard_read.extents);
        empty = false;
      }
      if (!shard_read.zero_pad.empty()) {
        op.debug_log.emplace_back(ECUtil::ZERO_REQUEST, shard, shard_read.zero_pad);
        empty = false;
      }

      ceph_assert(!empty);
      for (auto extent = shard_read.extents.begin();
      		extent != shard_read.extents.end();
		extent++) {
	messages[shard].to_read[hoid].push_back(boost::make_tuple(extent.get_start(), extent.get_len(), read_request.flags));
      }
    }
    ceph_assert(!need_attrs);
  }

  std::vector<std::pair<int, Message*>> m;
  m.reserve(messages.size());
  for (map<pg_shard_t, ECSubRead>::iterator i = messages.begin();
       i != messages.end();
       ++i) {
    op.in_progress.insert(i->first);
    shard_to_read_map[i->first].insert(op.tid);
    i->second.tid = tid;
    MOSDECSubOpRead *msg = new MOSDECSubOpRead;
    msg->set_priority(priority);
    msg->pgid = spg_t(
      get_info().pgid.pgid,
      i->first.shard);
    msg->map_epoch = get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_interval_start_epoch();
    msg->op = i->second;
    msg->op.from = get_parent()->whoami_shard();
    msg->op.tid = tid;
    if (op.trace) {
      // initialize a child span for this shard
      msg->trace.init("ec sub read", nullptr, &op.trace);
      msg->trace.keyval("shard", i->first.shard.id);
    }
    m.push_back(std::make_pair(i->first.osd, msg));
  }
  if (!m.empty()) {
    get_parent()->send_message_osd_cluster(m, get_osdmap_epoch());
  }

  dout(10) << __func__ << ": started " << op << dendl;
}

void ECCommon::ReadPipeline::get_want_to_read_shards(
  const list<ec_align_t> &to_read,
  std::map<int, extent_set> &want_shard_reads)
{
  if (sinfo.supports_partial_reads() && cct->_conf->osd_ec_partial_reads) {
      //optimised.
    for (const auto& single_region : to_read) {
      get_min_want_to_read_shards(single_region, want_shard_reads);
    }
    return;
  }

  // Non-optimised version.
  const std::vector<int> &chunk_mapping = ec_impl->get_chunk_mapping();
  for (int i = 0; i < (int)ec_impl->get_data_chunk_count(); ++i) {
    int chunk = (int)chunk_mapping.size() > i ? chunk_mapping[i] : i;

    for (auto &&read : to_read) {
      auto offset_len = sinfo.chunk_aligned_offset_len_to_chunk(read.offset, read.size);
      want_shard_reads[chunk].insert(offset_len.first, offset_len.second);
    }
  }
}

struct ClientReadCompleter : ECCommon::ReadCompleter {
  ClientReadCompleter(ECCommon::ReadPipeline &read_pipeline,
                      ECCommon::ClientAsyncReadStatus *status)
    : read_pipeline(read_pipeline),
      status(status) {}

  void finish_single_request(
    const hobject_t &hoid,
    ECCommon::read_result_t &&res,
    ECCommon::read_request_t &req) override
  {
    auto* cct = read_pipeline.cct;
    dout(20) << __func__ << " completing hoid=" << hoid
             << " res=" << res << " req="  << req << dendl;
    extent_map result;
    if (res.r != 0)
      goto out;
    ceph_assert(res.errors.empty());
#if DEBUG_EC_BUFFERS
    dout(20) << __func__ << "before decode: " << res.buffers_read.debug_string(2048, 8) << dendl;
#endif
    /* Decode any missing buffers */
    res.buffers_read.decode(read_pipeline.ec_impl, req.shard_want_to_read);


#if DEBUG_EC_BUFFERS
    dout(20) << __func__ << "after decode: " << res.buffers_read.debug_string(2048, 8) << dendl;
#endif

    for (auto &&read: req.to_read) {
      result.insert(read.offset, read.size,
        res.buffers_read.get_ro_buffer(read.offset, read.size));
    }
out:
    dout(20) << __func__ << " calling complete_object with result="
             << result << dendl;
    status->complete_object(hoid, res.r, std::move(result), std::move(res.buffers_read));
    read_pipeline.kick_reads();
  }

  void finish(int priority) && override
  {
    // NOP
  }

  ECCommon::ReadPipeline &read_pipeline;
  ECCommon::ClientAsyncReadStatus *status;
};
static ostream& _prefix(std::ostream *_dout, ClientReadCompleter *read_completer) {
  return _prefix(_dout, &read_completer->read_pipeline);
}

void ECCommon::ReadPipeline::objects_read_and_reconstruct(
  const map<hobject_t, std::list<ECCommon::ec_align_t>> &reads,
  bool fast_read,
  uint64_t object_size,
  GenContextURef<ECCommon::ec_extents_t &&> &&func)
{
  in_progress_client_reads.emplace_back(
    reads.size(), std::move(func));
  if (!reads.size()) {
    kick_reads();
    return;
  }

  map<hobject_t, read_request_t> for_read_op;
  for (auto &&[hoid, to_read]: reads) {
    map<int, extent_set> want_shard_reads;
    get_want_to_read_shards(to_read, want_shard_reads);

    read_request_t read_request(to_read, want_shard_reads, false, object_size);
    int r = get_min_avail_to_read_shards(
      hoid,
      false,
      fast_read,
      read_request);
    ceph_assert(r == 0);

    int subchunk_size =
      sinfo.get_chunk_size() / ec_impl->get_sub_chunk_count();
    dout(20) << __func__
             << " to_read=" << to_read
             << " subchunk_size=" << subchunk_size
             << " chunk_size=" << sinfo.get_chunk_size() << dendl;

    for_read_op.insert(make_pair(hoid, read_request));
  }

  start_read_op(
    CEPH_MSG_PRIO_DEFAULT,
    for_read_op,
    OpRequestRef(),
    fast_read,
    false,
    std::make_unique<ClientReadCompleter>(*this, &(in_progress_client_reads.back())));
}

void ECCommon::ReadPipeline::objects_read_and_reconstruct_for_rmw(
  map<hobject_t, read_request_t> &&to_read,
  GenContextURef<ECCommon::ec_extents_t &&> &&func)
{
  in_progress_client_reads.emplace_back(to_read.size(), std::move(func));
  if (!to_read.size()) {
    kick_reads();
    return;
  }

  map<hobject_t, set<int>> obj_want_to_read;

  map<hobject_t, read_request_t> for_read_op;
  for (auto &&[hoid, read_request]: to_read) {

    int r = get_min_avail_to_read_shards(
      hoid,
      false,
      false,
      read_request);
    ceph_assert(r == 0);

    int subchunk_size =
      sinfo.get_chunk_size() / ec_impl->get_sub_chunk_count();
    dout(20) << __func__
             << " read_request=" << read_request
             << " subchunk_size=" << subchunk_size
             << " chunk_size=" << sinfo.get_chunk_size() << dendl;

    for_read_op.insert(make_pair(hoid, read_request));
  }

  start_read_op(
    CEPH_MSG_PRIO_DEFAULT,
    for_read_op,
    OpRequestRef(),
    false,
    false,
    std::make_unique<ClientReadCompleter>(*this, &(in_progress_client_reads.back())));
}



int ECCommon::ReadPipeline::send_all_remaining_reads(
  const hobject_t &hoid,
  ReadOp &rop)
{
  // (Note cuixf) If we need to read attrs and we read failed, try to read again.
  bool want_attrs =
    rop.to_read.find(hoid)->second.want_attrs &&
    (!rop.complete.at(hoid).attrs || rop.complete.at(hoid).attrs->empty());
  if (want_attrs) {
    dout(10) << __func__ << " want attrs again" << dendl;
  }

  read_request_t &read_request = rop.to_read.at(hoid);
  // reset the old shard reads, we are going to read them again.
  read_request.shard_reads = std::map<pg_shard_t, shard_read_t>();
  int r = get_remaining_shards(hoid, rop.complete.at(hoid), read_request,
        rop.do_redundant_reads, want_attrs);

  if (r)
    return r;

  return 0;
}

void ECCommon::ReadPipeline::kick_reads()
{
  while (in_progress_client_reads.size() &&
         in_progress_client_reads.front().is_complete()) {
    in_progress_client_reads.front().run();
    in_progress_client_reads.pop_front();
  }
}

bool ECCommon::ec_align_t::operator==(const ec_align_t &other) const {
  return offset == other.offset && size == other.size && flags == other.flags;
}

bool ECCommon::shard_read_t::operator==(const shard_read_t &other) const {
  return extents == other.extents && subchunk == other.subchunk;
}

bool ECCommon::read_request_t::operator==(const read_request_t &other) const {
  return to_read == other.to_read &&
    flags == other.flags &&
    shard_want_to_read == other.shard_want_to_read &&
    shard_reads == other.shard_reads &&
    want_attrs == other.want_attrs;
}

void ECCommon::RMWPipeline::start_rmw(OpRef op)
{
  dout(20) << __func__ << " op=" << *op << dendl;

  ceph_assert(!tid_to_op_map.count(op->tid));
  tid_to_op_map[op->tid] = op;

  op->pending_cache_ops = op->plan.plans.size();
  for (auto &&[oid, plan] : op->plan.plans) {
    ECExtentCache::OpRef cache_op = extent_cache.request(oid,
      plan.to_read,
      plan.will_write,
      plan.orig_size,
      plan.projected_size,
      [op](ECExtentCache::OpRef &cache_op)
      {
        op->cache_ops.emplace(op->hoid, cache_op);
        op->cache_ready(op->hoid, cache_op->get_result());
      });
  }
}

void ECCommon::RMWPipeline::cache_ready(Op &op)
{
  waiting_commit.push_back(op);

  get_parent()->apply_stats(
    op.hoid,
    op.delta_stats);

  map<shard_id_t, ObjectStore::Transaction> trans;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    trans[i->shard];
  }

  op.trace.event("start ec write");

  map<hobject_t, ECUtil::shard_extent_map_t> written;
  op.generate_transactions(
    ec_impl,
    get_parent()->get_info().pgid.pgid,
    sinfo,
    &written,
    &trans,
    get_parent()->get_dpp(),
    get_osdmap()->require_osd_release);

  dout(20) << __func__ << ": written: " << written << ", op: " << op << dendl;

  if (!sinfo.supports_ec_overwrites()) {
    for (auto &&i: op.log_entries) {
      if (i.requires_kraken()) {
        derr << __func__ << ": log entry " << i << " requires kraken"
             << " but overwrites are not enabled!" << dendl;
        ceph_abort();
      }
    }
  }

  op.remote_shard_extent_map.clear();

  ObjectStore::Transaction empty;
  bool should_write_local = false;
  ECSubWrite local_write_op;
  std::vector<std::pair<int, Message*>> messages;
  messages.reserve(get_parent()->get_acting_recovery_backfill_shards().size());
  set<pg_shard_t> backfill_shards = get_parent()->get_backfill_shards();

  if (op.version.version != 0) {
    if (oid_to_version.contains(op.hoid)) {
      ceph_assert(oid_to_version.at(op.hoid) <= op.version);
    }
    oid_to_version[op.hoid] = op.version;
  }
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    op.pending_commit.insert(*i);
    map<shard_id_t, ObjectStore::Transaction>::iterator iter =
      trans.find(i->shard);
    ceph_assert(iter != trans.end());
    bool should_send = get_parent()->should_send_op(*i, op.hoid);
    const pg_stat_t &stats =
      (should_send || !backfill_shards.count(*i)) ?
      get_info().stats :
      get_parent()->get_shard_info().find(*i)->second.stats;

    ECSubWrite sop(
      get_parent()->whoami_shard(),
      op.tid,
      op.reqid,
      op.hoid,
      stats,
      should_send ? iter->second : empty,
      op.version,
      op.trim_to,
      op.pg_committed_to,
      op.log_entries,
      op.updated_hit_set_history,
      op.temp_added,
      op.temp_cleared,
      !should_send);

    ZTracer::Trace trace;
    if (op.trace) {
      // initialize a child span for this shard
      trace.init("ec sub write", nullptr, &op.trace);
      trace.keyval("shard", i->shard.id);
    }

    if (*i == get_parent()->whoami_shard()) {
      should_write_local = true;
      local_write_op.claim(sop);
    } else if (cct->_conf->bluestore_debug_inject_read_err &&
	       ec_inject_test_write_error1(ghobject_t(op.hoid,
		 ghobject_t::NO_GEN, i->shard))) {
      dout(0) << " Error inject - Dropping write message to shard " <<
	i->shard << dendl;
    } else {
      MOSDECSubOpWrite *r = new MOSDECSubOpWrite(sop);
      r->pgid = spg_t(get_parent()->primary_spg_t().pgid, i->shard);
      r->map_epoch = get_osdmap_epoch();
      r->min_epoch = get_parent()->get_interval_start_epoch();
      r->trace = trace;
      messages.push_back(std::make_pair(i->osd, r));
    }
  }

  if (!messages.empty()) {
    get_parent()->send_message_osd_cluster(messages, get_osdmap_epoch());
  }

  if (should_write_local) {
    handle_sub_write(
      get_parent()->whoami_shard(),
      op.client_op,
      local_write_op,
      op.trace);
  }

  for (auto i = op.on_write.begin();
       i != op.on_write.end();
       op.on_write.erase(i++)) {
    (*i)();
  }

  for (auto &&[oid, cop]: op.cache_ops) {
    if (written.contains(oid)) {
      extent_cache.write_done(cop, std::move(written.at(oid)));
    } else {
      extent_cache.write_done(cop, ECUtil::shard_extent_map_t(&sinfo));
    }
  }
}

struct ECDummyOp : ECCommon::RMWPipeline::Op {
  void generate_transactions(
    ceph::ErasureCodeInterfaceRef &ecimpl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    map<hobject_t, ECUtil::shard_extent_map_t>* written,
    std::map<shard_id_t, ObjectStore::Transaction> *transactions,
    DoutPrefixProvider *dpp,
    const ceph_release_t require_osd_release) final
  {
    // NOP, as -- in constrast to ECClassicalOp -- there is no
    // transaction involved
  }
};

void ECCommon::RMWPipeline::try_finish_rmw()
{
  while (!waiting_commit.empty() && waiting_commit.front().pending_commit.empty())
  {
    Op &op = waiting_commit.front();
    waiting_commit.pop_front();

    if (op.pg_committed_to > completed_to)
      completed_to = op.pg_committed_to;
    if (op.version > committed_to)
      committed_to = op.version;

    if (get_osdmap()->require_osd_release >= ceph_release_t::kraken && extent_cache.idle()) {
      // FIXME: This needs to be implemented as a flushing write.
      if (op.version > get_parent()->get_log().get_can_rollback_to()) {
        // submit a dummy, transaction-empty op to kick the rollforward
        auto tid = get_parent()->get_tid();
        auto nop = std::make_shared<ECDummyOp>();
        nop->hoid = op.hoid;
        nop->trim_to = op.trim_to;
        nop->pg_committed_to = op.version;
        nop->tid = tid;
        nop->reqid = op.reqid;
        nop->pending_cache_ops = 1;
        nop->pipeline = this;

        ECExtentCache::OpRef cache_op = extent_cache.request(op.hoid,
          std::nullopt,
          map<int, extent_set>(),
          op.plan.plans.at(op.hoid).orig_size,
          op.plan.plans.at(op.hoid).projected_size,
          [nop](ECExtentCache::OpRef cache_op)
          {
            nop->cache_ops.emplace(nop->hoid, std::move(cache_op));
            nop->cache_ready(nop->hoid, std::nullopt);
          });

        tid_to_op_map[tid] = std::move(nop);
      }
    }

    for (auto &&[_, c]: op.cache_ops) {
      extent_cache.complete(c);
    }

    op.cache_ops.clear();

    tid_to_op_map.erase(op.tid);
  }
}

void ECCommon::RMWPipeline::on_change()
{
  dout(10) << __func__ << dendl;

  completed_to = eversion_t();
  committed_to = eversion_t();
  waiting_commit.clear();
  tid_to_op_map.clear();
  oid_to_version.clear();
}

void ECCommon::RMWPipeline::on_change2() {
  extent_cache.on_change();
  ceph_assert(extent_cache.idle());
}

void ECCommon::RMWPipeline::call_write_ordered(std::function<void(void)> &&cb) {
  // FIXME: The original function waits for all pipeline writes to complete.
  //        I think what we want to do here is to flush the cache and queue a
  //        a write behind it.  I have no idea how critical ordered writes are.
  ceph_abort("Ordered writes not implemented");
  cb();
}

ECUtil::HashInfoRef ECCommon::UnstableHashInfoRegistry::maybe_put_hash_info(
  const hobject_t &hoid,
  ECUtil::HashInfo &&hinfo)
{
  return registry.lookup_or_create(hoid, hinfo);
}

ECUtil::HashInfoRef ECCommon::UnstableHashInfoRegistry::get_hash_info(
  const hobject_t &hoid,
  bool create,
  const map<string, bufferlist, less<>>& attrs,
  uint64_t size)
{
  dout(10) << __func__ << ": Getting attr on " << hoid << dendl;
  ECUtil::HashInfoRef ref = registry.lookup(hoid);
  if (!ref) {
    dout(10) << __func__ << ": not in cache " << hoid << dendl;
    ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
    bufferlist bl;
    map<string, bufferlist>::const_iterator k = attrs.find(ECUtil::get_hinfo_key());
    if (k == attrs.end()) {
      dout(5) << __func__ << " " << hoid << " missing hinfo attr" << dendl;
    } else {
      bl = k->second;
    }
    if (bl.length() > 0) {
      auto bp = bl.cbegin();
      try {
        decode(hinfo, bp);
      } catch(...) {
        dout(0) << __func__ << ": Can't decode hinfo for " << hoid << dendl;
        return ECUtil::HashInfoRef();
      }
      if (hinfo.get_total_chunk_size() != size) {
        dout(0) << __func__ << ": Mismatch of total_chunk_size "
      		       << hinfo.get_total_chunk_size() << dendl;
        return ECUtil::HashInfoRef();
      } else {
        create = true;
      }
    } else if (size == 0) { // If empty object and no hinfo, create it
      create = true;
    }
    if (create) {
      ref = registry.lookup_or_create(hoid, hinfo);
    }
  }
  return ref;
}

// Error inject interfaces
static ceph::recursive_mutex ec_inject_lock =
  ceph::make_recursive_mutex("ECCommon::ec_inject_lock");
static std::map<ghobject_t,std::pair<int64_t,int64_t>> ec_inject_read_failures0;
static std::map<ghobject_t,std::pair<int64_t,int64_t>> ec_inject_read_failures1;
static std::map<ghobject_t,std::pair<int64_t,int64_t>> ec_inject_write_failures0;
static std::map<ghobject_t,std::pair<int64_t,int64_t>> ec_inject_write_failures1;
static std::map<ghobject_t,std::pair<int64_t,int64_t>> ec_inject_write_failures2;
static std::map<ghobject_t,std::pair<int64_t,int64_t>> ec_inject_write_failures3;
static std::map<ghobject_t,shard_id_t> ec_inject_write_failures0_shard;
static std::set<osd_reqid_t> ec_inject_write_failures0_reqid;

/**
 * Configure a read error inject that typically forces additional reads of
 * shards in an EC pool to recover data using the redundancy. With multiple
 * errors it is possible to force client reads to fail.
 *
 * Type 0 - Simulate a medium error. Fail a read with -EIO to force
 * additional reads and a decode
 *
 * Type 1 - Simulate a missing OSD. Dont even try to read a shard
 *
 * @brief Set up a read error inject for an object in an EC pool.
 * @param o Target object for the error inject.
 * @param when Error inject starts after this many object store reads.
 * @param duration Error inject affects this many object store reads.
 * @param type Type of error inject 0 = EIO, 1 = missing shard.
 * @return string Result of configuring the error inject.
 */
std::string ec_inject_read_error(const ghobject_t& o,
				 const int64_t type,
				 const int64_t when,
				 const int64_t duration) {
  std::lock_guard<ceph::recursive_mutex> l(ec_inject_lock);
  ghobject_t os = o;
  if (os.hobj.oid.name == "*") {
    os.hobj.set_hash(0);
  }
  switch (type) {
  case 0:
    ec_inject_read_failures0[os] = std::pair(when, duration);
    return "ok - read returns EIO";
  case 1:
    ec_inject_read_failures1[os] = std::pair(when, duration);
    return "ok - read pretends shard is missing";
  default:
    break;
  }
  return "unrecognized error inject type";
}

/**
 * Configure a write error inject that either fails an OSD or causes a
 * client write operation to be rolled back.
 *
 * Type 0 - Tests rollback. Drop a write I/O to a shard, then simulate an OSD
 * down to force rollback to occur, lastly fail the retried write from the
 * client so the results of the rollback can be inspected.
 *
 * Type 1 - Drop a write I/O to a shard. Used on its own this will hang a
 * write I/O.
 *
 * Type 2 - Simulate an OSD down (ceph osd down) to force a new epoch. Usually
 * used together with type 1 to force a rollback
 *
 * Type 3 - Abort when an OSD processes a write I/O to a shard. Typically the
 * client write will be commited while the OSD is absent which will result in
 * recovery or backfill later when the OSD returns.
 *
 * @brief Set up a write error inject for an object in an EC pool.
 * @param o Target object for the error inject.
 * @param when Error inject starts after this many object store reads.
 * @param duration Error inject affects this many object store reads.
 * @param type Type of error inject 0 = EIO, 1 = missing shard.
 * @return string Result of configuring the error inect.
 */
std::string ec_inject_write_error(const ghobject_t& o,
				  const int64_t type,
				  const int64_t when,
				  const int64_t duration) {
  std::lock_guard<ceph::recursive_mutex> l(ec_inject_lock);
  std::map<ghobject_t,std::pair<int64_t,int64_t>> *failures;
  ghobject_t os = o;
  bool no_shard = true;
  std::string result;
  switch (type) {
  case 0:
    failures = &ec_inject_write_failures0;
    result = "ok - drop write, sim OSD down and fail client retry with EINVAL";
    break;
  case 1:
    failures = &ec_inject_write_failures1;
    no_shard = false;
    result = "ok - drop write to shard";
    break;
  case 2:
    failures = &ec_inject_write_failures2;
    result = "ok - inject OSD down";
    break;
  case 3:
    if (duration != 1) {
      return "duration must be 1";
    }
    failures = &ec_inject_write_failures3;
    result = "ok - write abort OSDs";
    break;
  default:
    return "unrecognized error inject type";
  }
  if (no_shard) {
    os.set_shard(shard_id_t::NO_SHARD);
  }
  if (os.hobj.oid.name == "*") {
    os.hobj.set_hash(0);
  }
  (*failures)[os] = std::pair(when, duration);
  if (type == 0) {
    ec_inject_write_failures0_shard[os] = o.shard_id;
  }
  return result;
}

/**
 * @brief Clear a previously configured read error inject.
 * @param o Target object for the error inject.
 * @param type Type of error inject 0 = EIO, 1 = missing shard.
 * @return string Indication of how many errors were cleared.
 */
std::string ec_inject_clear_read_error(const ghobject_t& o,
				       const int64_t type) {
  std::lock_guard<ceph::recursive_mutex> l(ec_inject_lock);
  std::map<ghobject_t,std::pair<int64_t,int64_t>> *failures;
  ghobject_t os = o;
  int64_t remaining = 0;
  switch (type) {
  case 0:
    failures = &ec_inject_read_failures0;
    break;
  case 1:
    failures = &ec_inject_read_failures1;
    break;
  default:
    return "unrecognized error inject type";
  }
  if (os.hobj.oid.name == "*") {
    os.hobj.set_hash(0);
  }
  auto it = failures->find(os);
  if (it != failures->end()) {
    remaining = it->second.second;
    failures->erase(it);
  }
  if (remaining == 0) {
    return "no outstanding error injects";
  } else if (remaining == 1) {
    return "ok - 1 inject cleared";
  }
  return "ok - " + std::to_string(remaining) + " injects cleared";
}

/**
 * @brief Clear a previously configured write error inject.
 * @param o Target object for the error inject.
 * @param type Type of error inject 0 = EIO, 1 = missing shard.
 * @return string Indication of how many errors were cleared.
 */
std::string ec_inject_clear_write_error(const ghobject_t& o,
					const int64_t type) {
  std::lock_guard<ceph::recursive_mutex> l(ec_inject_lock);
  std::map<ghobject_t,std::pair<int64_t,int64_t>> *failures;
  ghobject_t os = o;
  bool no_shard = true;
  int64_t remaining = 0;
  switch (type) {
  case 0:
    failures = &ec_inject_write_failures0;
    break;
  case 1:
    failures = &ec_inject_write_failures1;
    no_shard = false;
    break;
  case 2:
    failures = &ec_inject_write_failures2;
    break;
  case 3:
    failures = &ec_inject_write_failures3;
    break;
  default:
    return "unrecognized error inject type";
  }
  if (no_shard) {
    os.set_shard(shard_id_t::NO_SHARD);
  }
  if (os.hobj.oid.name == "*") {
    os.hobj.set_hash(0);
  }
  auto it = failures->find(os);
  if (it != failures->end()) {
    remaining = it->second.second;
    failures->erase(it);
    if (type == 0) {
      ec_inject_write_failures0_shard.erase(os);
    }
  }
  if (remaining == 0) {
    return "no outstanding error injects";
  } else if (remaining == 1) {
    return "ok - 1 inject cleared";
  }
  return "ok - " + std::to_string(remaining) + " injects cleared";
}

static bool ec_inject_test_error(const ghobject_t& o,
  std::map<ghobject_t,std::pair<int64_t,int64_t>> *failures)
{
  std::lock_guard<ceph::recursive_mutex> l(ec_inject_lock);
  auto it = failures->find(o);
  if (it == failures->end()) {
    ghobject_t os = o;
    os.hobj.oid.name = "*";
    os.hobj.set_hash(0);
    it = failures->find(os);
  }
  if (it != failures->end()) {
    auto && [when,duration] = it->second;
    if (when > 0) {
      when--;
      return false;
    }
    if (--duration <= 0) {
      failures->erase(it);
    }
    return true;
  }
  return false;
}

bool ec_inject_test_read_error0(const ghobject_t& o)
{
  return ec_inject_test_error(o, &ec_inject_read_failures0);
}

bool ec_inject_test_read_error1(const ghobject_t& o)
{
  return ec_inject_test_error(o, &ec_inject_read_failures1);
}

bool ec_inject_test_write_error0(const hobject_t& o,
				 const osd_reqid_t& reqid) {
  std::lock_guard<ceph::recursive_mutex> l(ec_inject_lock);
  ghobject_t os = ghobject_t(o, ghobject_t::NO_GEN, shard_id_t::NO_SHARD);
  if (ec_inject_write_failures0_reqid.count(reqid)) {
    // Matched reqid of retried write - flag for failure
    ec_inject_write_failures0_reqid.erase(reqid);
    return true;
  }
  auto it = ec_inject_write_failures0.find(os);
  if (it == ec_inject_write_failures0.end()) {
    os.hobj.oid.name = "*";
    os.hobj.set_hash(0);
    it = ec_inject_write_failures0.find(os);
  }
  if (it != ec_inject_write_failures0.end()) {
    auto && [when, duration] = it->second;
    auto shard = ec_inject_write_failures0_shard.find(os)->second;
    if (when > 0) {
      when--;
    } else {
      if (--duration <= 0) {
	ec_inject_write_failures0.erase(it);
	ec_inject_write_failures0_shard.erase(os);
      }
      // Error inject triggered - save reqid
      ec_inject_write_failures0_reqid.insert(reqid);
      // Set up error inject to drop message to primary
      ec_inject_write_error(ghobject_t(o, ghobject_t::NO_GEN, shard), 1, 0, 1);
    }
  }
  return false;
}

bool ec_inject_test_write_error1(const ghobject_t& o) {
  bool rc = ec_inject_test_error(o, &ec_inject_write_failures1);
  if (rc) {
    // Set up error inject to generate OSD down
    ec_inject_write_error(o, 2, 0, 1);
  }
  return rc;
}

bool ec_inject_test_write_error2(const hobject_t& o) {
  return ec_inject_test_error(
    ghobject_t(o, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
    &ec_inject_write_failures2);
}

bool ec_inject_test_write_error3(const hobject_t& o) {
  return ec_inject_test_error(
    ghobject_t(o, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
    &ec_inject_write_failures3);
}
