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

#include "ECCommon.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
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
static ostream& _prefix(std::ostream *_dout, ECCommon::RecoveryBackend *pgb) {
  return pgb->get_parent()->gen_dbg_prefix(*_dout);
}

ostream &operator<<(ostream &lhs, const ECCommon::RMWPipeline::pipeline_state_t &rhs) {
  switch (rhs.pipeline_state) {
  case ECCommon::RMWPipeline::pipeline_state_t::CACHE_VALID:
    return lhs << "CACHE_VALID";
  case ECCommon::RMWPipeline::pipeline_state_t::CACHE_INVALID:
    return lhs << "CACHE_INVALID";
  default:
    ceph_abort_msg("invalid pipeline state");
  }
  return lhs; // unreachable
}

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

ostream &operator<<(ostream &lhs, const ECCommon::read_request_t &rhs)
{
  return lhs << "read_request_t(to_read=[" << rhs.to_read << "]"
	     << ", need=" << rhs.need
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
  return lhs << ", returned=" << rhs.returned << ")";
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
	     << ", want_to_read" << rhs.want_to_read
	     << ", in_progress=" << rhs.in_progress << ")";
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
  f->dump_stream("want_to_read") << want_to_read;
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
      << " pending_read=" << rhs.pending_read
      << " remote_read=" << rhs.remote_read
      << " remote_read_result=" << rhs.remote_read_result
      << " pending_apply=" << rhs.pending_apply
      << " pending_commit=" << rhs.pending_commit
      << " plan.to_read=" << rhs.plan.to_read
      << " plan.will_write=" << rhs.plan.will_write
      << ")";
  return lhs;
}

ostream &operator<<(ostream &lhs, const ECCommon::RecoveryBackend::RecoveryOp &rhs)
{
  return lhs << "RecoveryOp("
	     << "hoid=" << rhs.hoid
	     << " v=" << rhs.v
	     << " missing_on=" << rhs.missing_on
	     << " missing_on_shards=" << rhs.missing_on_shards
	     << " recovery_info=" << rhs.recovery_info
	     << " recovery_progress=" << rhs.recovery_progress
	     << " obc refcount=" << rhs.obc.use_count()
	     << " state=" << ECCommon::RecoveryBackend::RecoveryOp::tostr(rhs.state)
	     << " waiting_on_pushes=" << rhs.waiting_on_pushes
	     << " extent_requested=" << rhs.extent_requested
	     << ")";
}

void ECCommon::RecoveryBackend::RecoveryOp::dump(Formatter *f) const
{
  f->dump_stream("hoid") << hoid;
  f->dump_stream("v") << v;
  f->dump_stream("missing_on") << missing_on;
  f->dump_stream("missing_on_shards") << missing_on_shards;
  f->dump_stream("recovery_info") << recovery_info;
  f->dump_stream("recovery_progress") << recovery_progress;
  f->dump_stream("state") << tostr(state);
  f->dump_stream("waiting_on_pushes") << waiting_on_pushes;
  f->dump_stream("extent_requested") << extent_requested;
}

void ECCommon::ReadPipeline::complete_read_op(ReadOp &rop)
{
  dout(20) << __func__ << " completing " << rop << dendl;
  map<hobject_t, read_request_t>::iterator req_iter =
    rop.to_read.begin();
  map<hobject_t, read_result_t>::iterator resiter =
    rop.complete.begin();
  ceph_assert(rop.to_read.size() == rop.complete.size());
  for (; req_iter != rop.to_read.end(); ++req_iter, ++resiter) {
    ceph_assert(rop.want_to_read.contains(req_iter->first));
    rop.on_complete->finish_single_request(
      req_iter->first,
      resiter->second,
      req_iter->second.to_read,
      rop.want_to_read[req_iter->first]);
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
  const set<pg_shard_t> &error_shards,
  set<int> &have,
  map<shard_id_t, pg_shard_t> &shards,
  bool for_recovery)
{
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_shards().begin();
       i != get_parent()->get_acting_shards().end();
       ++i) {
    dout(10) << __func__ << ": checking acting " << *i << dendl;
    const pg_missing_t &missing = get_parent()->get_shard_missing(*i);
    if (error_shards.find(*i) != error_shards.end())
      continue;
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
      if (error_shards.find(*i) != error_shards.end())
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
	if (error_shards.find(*i) != error_shards.end())
	  continue;
	have.insert(i->shard);
	shards.insert(make_pair(i->shard, *i));
      }
    }
  }
}

int ECCommon::ReadPipeline::get_min_avail_to_read_shards(
  const hobject_t &hoid,
  const set<int> &want,
  bool for_recovery,
  bool do_redundant_reads,
  map<pg_shard_t, vector<pair<int, int>>> *to_read)
{
  // Make sure we don't do redundant reads for recovery
  ceph_assert(!for_recovery || !do_redundant_reads);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;
  set<pg_shard_t> error_shards;

  get_all_avail_shards(hoid, error_shards, have, shards, for_recovery);

  map<int, vector<pair<int, int>>> need;
  int r = ec_impl->minimum_to_decode(want, have, &need);
  if (r < 0)
    return r;

  if (do_redundant_reads) {
      vector<pair<int, int>> subchunks_list;
      subchunks_list.push_back(make_pair(0, ec_impl->get_sub_chunk_count()));
      for (auto &&i: have) {
        need[i] = subchunks_list;
      }
  } 

  if (!to_read)
    return 0;

  for (auto &&i:need) {
    ceph_assert(shards.count(shard_id_t(i.first)));
    to_read->insert(make_pair(shards[shard_id_t(i.first)], i.second));
  }
  return 0;
}

// a static for the sake of unittesting
void ECCommon::ReadPipeline::get_min_want_to_read_shards(
  const uint64_t offset,
  const uint64_t length,
  const ECUtil::stripe_info_t& sinfo,
  const vector<int>& chunk_mapping,
  set<int> *want_to_read)
{
  const auto [left_chunk_index, right_chunk_index] =
    sinfo.offset_length_to_data_chunk_indices(offset, length);
  const auto distance =
    std::min(right_chunk_index - left_chunk_index,
             sinfo.get_data_chunk_count());
  for(uint64_t i = 0; i < distance; i++) {
    auto raw_chunk = (left_chunk_index + i) % sinfo.get_data_chunk_count();
    auto chunk = chunk_mapping.size() > raw_chunk ?
      chunk_mapping[raw_chunk] : static_cast<int>(raw_chunk);
    want_to_read->insert(chunk);
  }
}

void ECCommon::ReadPipeline::get_min_want_to_read_shards(
  const uint64_t offset,
  const uint64_t length,
  set<int> *want_to_read)
{
  get_min_want_to_read_shards(
    offset, length, sinfo, ec_impl->get_chunk_mapping(), want_to_read);
  dout(20) << __func__ << ": offset " << offset << " length " << length
	   << " want_to_read " << *want_to_read << dendl;
}

int ECCommon::ReadPipeline::get_remaining_shards(
  const hobject_t &hoid,
  const set<int> &avail,
  const set<int> &want,
  const read_result_t &result,
  map<pg_shard_t, vector<pair<int, int>>> *to_read,
  bool for_recovery)
{
  ceph_assert(to_read);

  set<int> have;
  map<shard_id_t, pg_shard_t> shards;
  set<pg_shard_t> error_shards;
  for (auto &p : result.errors) {
    error_shards.insert(p.first);
  }

  get_all_avail_shards(hoid, error_shards, have, shards, for_recovery);

  map<int, vector<pair<int, int>>> need;
  int r = ec_impl->minimum_to_decode(want, have, &need);
  if (r < 0) {
    dout(0) << __func__ << " not enough shards left to try for " << hoid
	    << " read result was " << result << dendl;
    return -EIO;
  }

  set<int> shards_left;
  for (auto p : need) {
    if (avail.find(p.first) == avail.end()) {
      shards_left.insert(p.first);
    }
  }

  vector<pair<int, int>> subchunks;
  subchunks.push_back(make_pair(0, ec_impl->get_sub_chunk_count()));
  for (set<int>::iterator i = shards_left.begin();
       i != shards_left.end();
       ++i) {
    ceph_assert(shards.count(shard_id_t(*i)));
    ceph_assert(avail.find(*i) == avail.end());
    to_read->insert(make_pair(shards[shard_id_t(*i)], subchunks));
  }
  return 0;
}

void ECCommon::ReadPipeline::start_read_op(
  int priority,
  map<hobject_t, set<int>> &want_to_read,
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
      std::move(want_to_read),
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
  for (map<hobject_t, read_request_t>::iterator i = op.to_read.begin();
       i != op.to_read.end();
       ++i) {
    bool need_attrs = i->second.want_attrs;

    for (auto j = i->second.need.begin();
	 j != i->second.need.end();
	 ++j) {
      if (need_attrs) {
	messages[j->first].attrs_to_read.insert(i->first);
	need_attrs = false;
      }
      messages[j->first].subchunks[i->first] = j->second;
      op.obj_to_source[i->first].insert(j->first);
      op.source_to_obj[j->first].insert(i->first);
    }
    for (const auto& read : i->second.to_read) {
      auto p = make_pair(read.offset, read.size);
      pair<uint64_t, uint64_t> chunk_off_len = sinfo.chunk_aligned_offset_len_to_chunk(p);
      for (auto k = i->second.need.begin();
	   k != i->second.need.end();
	   ++k) {
	messages[k->first].to_read[i->first].push_back(
	  boost::make_tuple(
	    chunk_off_len.first,
	    chunk_off_len.second,
	    read.flags));
      }
      ceph_assert(!need_attrs);
    }
  }

  std::optional<ECSubRead> local_read_op;
  std::vector<std::pair<int, Message*>> m;
  m.reserve(messages.size());
  for (map<pg_shard_t, ECSubRead>::iterator i = messages.begin();
       i != messages.end();
       /* see below */) {
    op.in_progress.insert(i->first);
    shard_to_read_map[i->first].insert(op.tid);
    i->second.tid = tid;
    if (i->first == get_parent()->whoami_shard()) {
      local_read_op = std::move(i->second);
      i = messages.erase(i);
      continue;
    }
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
    dout(10) << __func__ << ": will send msg " << *msg
             << " to osd." << i->first.osd << dendl;
    ++i;
  }
  if (!m.empty()) {
    get_parent()->send_message_osd_cluster(m, get_osdmap_epoch());
  }
  dout(10) << __func__ << ": started " << op << dendl;
  if (local_read_op) {
    handle_sub_read_n_reply(
      get_parent()->whoami_shard(),
      *local_read_op,
      op.trace);
  }
}

void ECCommon::ReadPipeline::get_want_to_read_shards(
  std::set<int> *want_to_read) const
{
  const std::vector<int> &chunk_mapping = ec_impl->get_chunk_mapping();
  for (int i = 0; i < (int)ec_impl->get_data_chunk_count(); ++i) {
    int chunk = (int)chunk_mapping.size() > i ? chunk_mapping[i] : i;
    want_to_read->insert(chunk);
  }
}

struct ClientReadCompleter : ECCommon::ReadCompleter {
  ClientReadCompleter(ECCommon::ReadPipeline &read_pipeline,
                      ECCommon::ClientAsyncReadStatus *status)
    : read_pipeline(read_pipeline),
      status(status) {}

  void finish_single_request(
    const hobject_t &hoid,
    ECCommon::read_result_t &res,
    list<ECCommon::ec_align_t> to_read,
    set<int> wanted_to_read) override
  {
    auto* cct = read_pipeline.cct;
    dout(20) << __func__ << " completing hoid=" << hoid
             << " res=" << res << " to_read="  << to_read << dendl;
    extent_map result;
    if (res.r != 0)
      goto out;
    ceph_assert(res.returned.size() == to_read.size());
    ceph_assert(res.errors.empty());
    for (auto &&read: to_read) {
      const auto bounds = make_pair(read.offset, read.size);
      // the configurable serves only the preservation of old behavior
      // which will be dropped. ReadPipeline is actually able to handle
      // reads aligned to chunk size.
      const auto aligned = g_conf()->osd_ec_partial_reads \
        ? read_pipeline.sinfo.offset_len_to_chunk_bounds(bounds)
        : read_pipeline.sinfo.offset_len_to_stripe_bounds(bounds);
      ceph_assert(res.returned.front().get<0>() == aligned.first);
      ceph_assert(res.returned.front().get<1>() == aligned.second);
      map<int, bufferlist> to_decode;
      bufferlist bl;
      for (map<pg_shard_t, bufferlist>::iterator j =
	     res.returned.front().get<2>().begin();
	   j != res.returned.front().get<2>().end();
	   ++j) {
	to_decode[j->first.shard] = std::move(j->second);
      }
      dout(20) << __func__ << " going to decode: "
               << " wanted_to_read=" << wanted_to_read
               << " to_decode=" << to_decode
               << dendl;
      int r = ECUtil::decode(
	read_pipeline.sinfo,
	read_pipeline.ec_impl,
	wanted_to_read,
	to_decode,
	&bl);
      if (r < 0) {
        dout(10) << __func__ << " error on ECUtil::decode r=" << r << dendl;
        res.r = r;
        goto out;
      }
      bufferlist trimmed;
      auto off = read.offset - aligned.first;
      auto len = std::min(read.size, bl.length() - off);
      dout(20) << __func__ << " bl.length()=" << bl.length()
	       << " len=" << len << " read.size=" << read.size
	       << " off=" << off << " read.offset=" << read.offset
	       << dendl;
      trimmed.substr_of(bl, off, len);
      result.insert(
	read.offset, trimmed.length(), std::move(trimmed));
      res.returned.pop_front();
    }
out:
    dout(20) << __func__ << " calling complete_object with result="
             << result << dendl;
    status->complete_object(hoid, res.r, std::move(result));
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
  GenContextURef<ECCommon::ec_extents_t &&> &&func)
{
  in_progress_client_reads.emplace_back(
    reads.size(), std::move(func));
  if (!reads.size()) {
    kick_reads();
    return;
  }

  map<hobject_t, set<int>> obj_want_to_read;
    
  map<hobject_t, read_request_t> for_read_op;
  for (auto &&to_read: reads) {
    set<int> want_to_read;
    if (cct->_conf->osd_ec_partial_reads) {
      for (const auto& single_region : to_read.second) {
        get_min_want_to_read_shards(single_region.offset,
				    single_region.size,
				    &want_to_read);
      }
    } else {
      get_want_to_read_shards(&want_to_read);
    }
    map<pg_shard_t, vector<pair<int, int>>> shards;
    int r = get_min_avail_to_read_shards(
      to_read.first,
      want_to_read,
      false,
      fast_read,
      &shards);
    ceph_assert(r == 0);

    int subchunk_size =
      sinfo.get_chunk_size() / ec_impl->get_sub_chunk_count();
    dout(20) << __func__
             << " subchunk_size=" << subchunk_size
             << " chunk_size=" << sinfo.get_chunk_size() << dendl;

    for_read_op.insert(
      make_pair(
	to_read.first,
	read_request_t(
	  to_read.second,
	  shards,
	  false)));
    obj_want_to_read.insert(make_pair(to_read.first, want_to_read));
  }

  start_read_op(
    CEPH_MSG_PRIO_DEFAULT,
    obj_want_to_read,
    for_read_op,
    OpRequestRef(),
    fast_read,
    false,
    std::make_unique<ClientReadCompleter>(*this, &(in_progress_client_reads.back())));
}


int ECCommon::ReadPipeline::send_all_remaining_reads(
  const hobject_t &hoid,
  ReadOp &rop)
{
  set<int> already_read;
  const set<pg_shard_t>& ots = rop.obj_to_source[hoid];
  for (set<pg_shard_t>::iterator i = ots.begin(); i != ots.end(); ++i)
    already_read.insert(i->shard);
  dout(10) << __func__ << " have/error shards=" << already_read << dendl;
  map<pg_shard_t, vector<pair<int, int>>> shards;
  int r = get_remaining_shards(hoid, already_read, rop.want_to_read[hoid],
			       rop.complete[hoid], &shards, rop.for_recovery);
  if (r)
    return r;

  list<ec_align_t> to_read = rop.to_read.find(hoid)->second.to_read;

  // (Note cuixf) If we need to read attrs and we read failed, try to read again.
  bool want_attrs =
    rop.to_read.find(hoid)->second.want_attrs &&
    (!rop.complete[hoid].attrs || rop.complete[hoid].attrs->empty());
  if (want_attrs) {
    dout(10) << __func__ << " want attrs again" << dendl;
  }

  rop.to_read.erase(hoid);
  rop.to_read.insert(make_pair(
      hoid,
      read_request_t(
	to_read,
	shards,
	want_attrs)));
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


void ECCommon::RMWPipeline::start_rmw(OpRef op)
{
  ceph_assert(op);
  dout(10) << __func__ << ": " << *op << dendl;

  ceph_assert(!tid_to_op_map.count(op->tid));
  waiting_state.push_back(*op);
  tid_to_op_map[op->tid] = std::move(op);
  check_ops();
}

bool ECCommon::RMWPipeline::try_state_to_reads()
{
  if (waiting_state.empty())
    return false;

  Op *op = &(waiting_state.front());
  if (op->requires_rmw() && pipeline_state.cache_invalid()) {
    ceph_assert(get_parent()->get_pool().allows_ecoverwrites());
    dout(20) << __func__ << ": blocking " << *op
	     << " because it requires an rmw and the cache is invalid "
	     << pipeline_state
	     << dendl;
    return false;
  }

  if (!pipeline_state.caching_enabled()) {
    op->using_cache = false;
  } else if (op->invalidates_cache()) {
    dout(20) << __func__ << ": invalidating cache after this op"
	     << dendl;
    pipeline_state.invalidate();
  }

  waiting_state.pop_front();
  waiting_reads.push_back(*op);

  if (op->using_cache) {
    cache.open_write_pin(op->pin);

    extent_set empty;
    for (auto &&hpair: op->plan.will_write) {
      auto to_read_plan_iter = op->plan.to_read.find(hpair.first);
      const extent_set &to_read_plan =
	to_read_plan_iter == op->plan.to_read.end() ?
	empty :
	to_read_plan_iter->second;

      extent_set remote_read = cache.reserve_extents_for_rmw(
	hpair.first,
	op->pin,
	hpair.second,
	to_read_plan);

      extent_set pending_read = to_read_plan;
      pending_read.subtract(remote_read);

      if (!remote_read.empty()) {
	op->remote_read[hpair.first] = std::move(remote_read);
      }
      if (!pending_read.empty()) {
	op->pending_read[hpair.first] = std::move(pending_read);
      }
    }
  } else {
    op->remote_read = op->plan.to_read;
  }

  dout(10) << __func__ << ": " << *op << dendl;

  if (!op->remote_read.empty()) {
    ceph_assert(get_parent()->get_pool().allows_ecoverwrites());
    objects_read_async_no_cache(
      op->remote_read,
      [op, this](ec_extents_t &&results) {
	for (auto &&i: results) {
	  op->remote_read_result.emplace(make_pair(i.first, i.second.emap));
	}
	check_ops();
      });
  }

  return true;
}

bool ECCommon::RMWPipeline::try_reads_to_commit()
{
  if (waiting_reads.empty())
    return false;
  Op *op = &(waiting_reads.front());
  if (op->read_in_progress())
    return false;
  waiting_reads.pop_front();
  waiting_commit.push_back(*op);

  dout(10) << __func__ << ": starting commit on " << *op << dendl;
  dout(20) << __func__ << ": " << cache << dendl;

  get_parent()->apply_stats(
    op->hoid,
    op->delta_stats);

  if (op->using_cache) {
    for (auto &&hpair: op->pending_read) {
      op->remote_read_result[hpair.first].insert(
	cache.get_remaining_extents_for_rmw(
	  hpair.first,
	  op->pin,
	  hpair.second));
    }
    op->pending_read.clear();
  } else {
    ceph_assert(op->pending_read.empty());
  }

  map<shard_id_t, ObjectStore::Transaction> trans;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    trans[i->shard];
  }

  op->trace.event("start ec write");

  map<hobject_t,extent_map> written;
  op->generate_transactions(
    ec_impl,
    get_parent()->get_info().pgid.pgid,
    sinfo,
    &written,
    &trans,
    get_parent()->get_dpp(),
    get_osdmap()->require_osd_release);

  dout(20) << __func__ << ": " << cache << dendl;
  dout(20) << __func__ << ": written: " << written << dendl;
  dout(20) << __func__ << ": op: " << *op << dendl;

  if (!get_parent()->get_pool().allows_ecoverwrites()) {
    for (auto &&i: op->log_entries) {
      if (i.requires_kraken()) {
	derr << __func__ << ": log entry " << i << " requires kraken"
	     << " but overwrites are not enabled!" << dendl;
	ceph_abort();
      }
    }
  }

  map<hobject_t,extent_set> written_set;
  for (auto &&i: written) {
    written_set[i.first] = i.second.get_interval_set();
  }
  dout(20) << __func__ << ": written_set: " << written_set << dendl;
  ceph_assert(written_set == op->plan.will_write);

  if (op->using_cache) {
    for (auto &&hpair: written) {
      dout(20) << __func__ << ": " << hpair << dendl;
      cache.present_rmw_update(hpair.first, op->pin, hpair.second);
    }
  }
  op->remote_read.clear();
  op->remote_read_result.clear();

  ObjectStore::Transaction empty;
  bool should_write_local = false;
  ECSubWrite local_write_op;
  std::vector<std::pair<int, Message*>> messages;
  messages.reserve(get_parent()->get_acting_recovery_backfill_shards().size());
  set<pg_shard_t> backfill_shards = get_parent()->get_backfill_shards();
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    op->pending_apply.insert(*i);
    op->pending_commit.insert(*i);
    map<shard_id_t, ObjectStore::Transaction>::iterator iter =
      trans.find(i->shard);
    ceph_assert(iter != trans.end());
    bool should_send = get_parent()->should_send_op(*i, op->hoid);
    const pg_stat_t &stats =
      (should_send || !backfill_shards.count(*i)) ?
      get_info().stats :
      get_parent()->get_shard_info().find(*i)->second.stats;

    ECSubWrite sop(
      get_parent()->whoami_shard(),
      op->tid,
      op->reqid,
      op->hoid,
      stats,
      should_send ? iter->second : empty,
      op->version,
      op->trim_to,
      op->pg_committed_to,
      op->log_entries,
      op->updated_hit_set_history,
      op->temp_added,
      op->temp_cleared,
      !should_send);

    ZTracer::Trace trace;
    if (op->trace) {
      // initialize a child span for this shard
      trace.init("ec sub write", nullptr, &op->trace);
      trace.keyval("shard", i->shard.id);
    }

    if (*i == get_parent()->whoami_shard()) {
      should_write_local = true;
      local_write_op.claim(sop);
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
      op->client_op,
      local_write_op,
      op->trace);
  }

  for (auto i = op->on_write.begin();
       i != op->on_write.end();
       op->on_write.erase(i++)) {
    (*i)();
  }

  return true;
}

struct ECDummyOp : ECCommon::RMWPipeline::Op {
  void generate_transactions(
      ceph::ErasureCodeInterfaceRef &ecimpl,
      pg_t pgid,
      const ECUtil::stripe_info_t &sinfo,
      std::map<hobject_t,extent_map> *written,
      std::map<shard_id_t, ObjectStore::Transaction> *transactions,
      DoutPrefixProvider *dpp,
      const ceph_release_t require_osd_release) final
  {
    // NOP, as -- in constrast to ECClassicalOp -- there is no
    // transaction involved
  }
};

bool ECCommon::RMWPipeline::try_finish_rmw()
{
  if (waiting_commit.empty())
    return false;
  Op *op = &(waiting_commit.front());
  if (op->write_in_progress())
    return false;
  waiting_commit.pop_front();

  dout(10) << __func__ << ": " << *op << dendl;
  dout(20) << __func__ << ": " << cache << dendl;

  if (op->pg_committed_to > completed_to)
    completed_to = op->pg_committed_to;
  if (op->version > committed_to)
    committed_to = op->version;

  if (get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
    if (op->version > get_parent()->get_log().get_can_rollback_to() &&
	waiting_reads.empty() &&
	waiting_commit.empty()) {
      // submit a dummy, transaction-empty op to kick the rollforward
      auto tid = get_parent()->get_tid();
      auto nop = std::make_unique<ECDummyOp>();
      nop->hoid = op->hoid;
      nop->trim_to = op->trim_to;
      nop->pg_committed_to = op->version;
      nop->tid = tid;
      nop->reqid = op->reqid;
      waiting_reads.push_back(*nop);
      tid_to_op_map[tid] = std::move(nop);
    }
  }

  if (op->using_cache) {
    cache.release_write_pin(op->pin);
  }
  tid_to_op_map.erase(op->tid);

  if (waiting_reads.empty() &&
      waiting_commit.empty()) {
    pipeline_state.clear();
    dout(20) << __func__ << ": clearing pipeline_state "
	     << pipeline_state
	     << dendl;
  }
  return true;
}

void ECCommon::RMWPipeline::check_ops()
{
  while (try_state_to_reads() ||
	 try_reads_to_commit() ||
	 try_finish_rmw());
}

void ECCommon::RMWPipeline::on_change()
{
  dout(10) << __func__ << dendl;

  completed_to = eversion_t();
  committed_to = eversion_t();
  pipeline_state.clear();
  waiting_reads.clear();
  waiting_state.clear();
  waiting_commit.clear();
  for (auto &&op: tid_to_op_map) {
    cache.release_write_pin(op.second->pin);
  }
  tid_to_op_map.clear();
}

void ECCommon::RMWPipeline::call_write_ordered(std::function<void(void)> &&cb) {
  if (!waiting_state.empty()) {
    waiting_state.back().on_write.emplace_back(std::move(cb));
  } else if (!waiting_reads.empty()) {
    waiting_reads.back().on_write.emplace_back(std::move(cb));
  } else {
    // Nothing earlier in the pipeline, just call it
    cb();
  }
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

ECCommon::RecoveryBackend::RecoveryBackend(
  CephContext* cct,
  const coll_t &coll,
  ceph::ErasureCodeInterfaceRef ec_impl,
  const ECUtil::stripe_info_t& sinfo,
  ReadPipeline& read_pipeline,
  UnstableHashInfoRegistry& unstable_hashinfo_registry,
  ECListener* parent)
  : cct(cct),
    coll(coll),
    ec_impl(std::move(ec_impl)),
    sinfo(sinfo),
    read_pipeline(read_pipeline),
    unstable_hashinfo_registry(unstable_hashinfo_registry),
    parent(parent) {
}

void ECCommon::RecoveryBackend::_failed_push(const hobject_t &hoid, ECCommon::read_result_t &res)
{
  dout(10) << __func__ << ": Read error " << hoid << " r="
	   << res.r << " errors=" << res.errors << dendl;
  dout(10) << __func__ << ": canceling recovery op for obj " << hoid
	   << dendl;
  ceph_assert(recovery_ops.count(hoid));
  eversion_t v = recovery_ops[hoid].v;
  recovery_ops.erase(hoid);

  set<pg_shard_t> fl;
  for (auto&& i : res.errors) {
    fl.insert(i.first);
  }
  get_parent()->on_failed_pull(fl, hoid, v);
}

void ECCommon::RecoveryBackend::handle_recovery_push(
  const PushOp &op,
  RecoveryMessages *m,
  bool is_repair)
{
  if (get_parent()->check_failsafe_full()) {
    dout(10) << __func__ << " Out of space (failsafe) processing push request." << dendl;
    ceph_abort();
  }

  bool oneshot = op.before_progress.first && op.after_progress.data_complete;
  ghobject_t tobj;
  if (oneshot) {
    tobj = ghobject_t(op.soid, ghobject_t::NO_GEN,
		      get_parent()->whoami_shard().shard);
  } else {
    tobj = ghobject_t(get_parent()->get_temp_recovery_object(op.soid,
							     op.version),
		      ghobject_t::NO_GEN,
		      get_parent()->whoami_shard().shard);
    if (op.before_progress.first) {
      dout(10) << __func__ << ": Adding oid "
	       << tobj.hobj << " in the temp collection" << dendl;
      add_temp_obj(tobj.hobj);
    }
  }

  if (op.before_progress.first) {
    m->t.remove(coll, tobj);
    m->t.touch(coll, tobj);
  }

  if (!op.data_included.empty()) {
    uint64_t start = op.data_included.range_start();
    uint64_t end = op.data_included.range_end();
    ceph_assert(op.data.length() == (end - start));

    m->t.write(
      coll,
      tobj,
      start,
      op.data.length(),
      op.data);
  } else {
    ceph_assert(op.data.length() == 0);
  }

  if (op.before_progress.first) {
    ceph_assert(op.attrset.count(string("_")));
    m->t.setattrs(
      coll,
      tobj,
      op.attrset);
  }

  if (op.after_progress.data_complete && !oneshot) {
    dout(10) << __func__ << ": Removing oid "
	     << tobj.hobj << " from the temp collection" << dendl;
    clear_temp_obj(tobj.hobj);
    m->t.remove(coll, ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
    m->t.collection_move_rename(
      coll, tobj,
      coll, ghobject_t(
	op.soid, ghobject_t::NO_GEN, get_parent()->whoami_shard().shard));
  }
  if (op.after_progress.data_complete) {
    if ((get_parent()->pgb_is_primary())) {
      ceph_assert(recovery_ops.count(op.soid));
      ceph_assert(recovery_ops[op.soid].obc);
      if (get_parent()->pg_is_repair() || is_repair)
        get_parent()->inc_osd_stat_repaired();
      get_parent()->on_local_recover(
	op.soid,
	op.recovery_info,
	recovery_ops[op.soid].obc,
	false,
	&m->t);
    } else {
      // If primary told us this is a repair, bump osd_stat_t::num_objects_repaired
      if (is_repair)
        get_parent()->inc_osd_stat_repaired();
      get_parent()->on_local_recover(
	op.soid,
	op.recovery_info,
	ObjectContextRef(),
	false,
	&m->t);
    }
  }
  m->push_replies[get_parent()->primary_shard()].push_back(PushReplyOp());
  m->push_replies[get_parent()->primary_shard()].back().soid = op.soid;
}

void ECCommon::RecoveryBackend::handle_recovery_push_reply(
  const PushReplyOp &op,
  pg_shard_t from,
  RecoveryMessages *m)
{
  if (!recovery_ops.count(op.soid))
    return;
  RecoveryOp &rop = recovery_ops[op.soid];
  ceph_assert(rop.waiting_on_pushes.count(from));
  rop.waiting_on_pushes.erase(from);
  continue_recovery_op(rop, m);
}

void ECCommon::RecoveryBackend::handle_recovery_read_complete(
  const hobject_t &hoid,
  boost::tuple<uint64_t, uint64_t, map<pg_shard_t, bufferlist> > &to_read,
  std::optional<map<string, bufferlist, less<>> > attrs,
  RecoveryMessages *m)
{
  dout(10) << __func__ << ": returned " << hoid << " "
	   << "(" << to_read.get<0>()
	   << ", " << to_read.get<1>()
	   << ", " << to_read.get<2>()
	   << ")"
	   << dendl;
  ceph_assert(recovery_ops.count(hoid));
  RecoveryBackend::RecoveryOp &op = recovery_ops[hoid];
  ceph_assert(op.returned_data.empty());
  map<int, bufferlist*> target;
  for (set<shard_id_t>::iterator i = op.missing_on_shards.begin();
       i != op.missing_on_shards.end();
       ++i) {
    target[*i] = &(op.returned_data[*i]);
  }
  map<int, bufferlist> from;
  for(map<pg_shard_t, bufferlist>::iterator i = to_read.get<2>().begin();
      i != to_read.get<2>().end();
      ++i) {
    from[i->first.shard] = std::move(i->second);
  }
  dout(10) << __func__ << ": " << from << dendl;
  int r;
  r = ECUtil::decode(sinfo, ec_impl, from, target);
  ceph_assert(r == 0);
  if (attrs) {
    op.xattrs.swap(*attrs);
#ifdef WITH_SEASTAR
    ceph_assert(hoid == op.hoid);
#endif
    maybe_load_obc(op.xattrs, op);
    ECUtil::HashInfo hinfo(ec_impl->get_chunk_count());
    if (op.obc->obs.oi.size > 0) {
      ceph_assert(op.xattrs.count(ECUtil::get_hinfo_key()));
      auto bp = op.xattrs[ECUtil::get_hinfo_key()].cbegin();
      decode(hinfo, bp);
    }
    op.hinfo = unstable_hashinfo_registry.maybe_put_hash_info(hoid, std::move(hinfo));
  }
  ceph_assert(op.xattrs.size());
  ceph_assert(op.obc);
  continue_recovery_op(op, m);
}

struct RecoveryReadCompleter : ECCommon::ReadCompleter {
  RecoveryReadCompleter(ECCommon::RecoveryBackend& backend)
    : backend(backend) {}

  void finish_single_request(
    const hobject_t &hoid,
    ECCommon::read_result_t &res,
    list<ECCommon::ec_align_t>,
    set<int> wanted_to_read) override
  {
    if (!(res.r == 0 && res.errors.empty())) {
      backend._failed_push(hoid, res);
      return;
    }
    ceph_assert(res.returned.size() == 1);
    backend.handle_recovery_read_complete(
      hoid,
      res.returned.back(),
      res.attrs,
      &rm);
  }

  void finish(int priority) && override
  {
    backend.dispatch_recovery_messages(rm, priority);
  }

  ECCommon::RecoveryBackend& backend;
  RecoveryMessages rm;
};


void ECCommon::RecoveryBackend::dispatch_recovery_messages(RecoveryMessages &m, int priority)
{
  for (map<pg_shard_t, vector<PushOp> >::iterator i = m.pushes.begin();
       i != m.pushes.end();
       m.pushes.erase(i++)) {
    MOSDPGPush *msg = new MOSDPGPush();
    msg->set_priority(priority);
    msg->map_epoch = get_parent()->pgb_get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->pushes.swap(i->second);
    msg->compute_cost(cct);
    msg->is_repair = get_parent()->pg_is_repair();
    get_parent()->send_message_osd_cluster(i->first.osd, msg, msg->map_epoch);
  }
  map<int, MOSDPGPushReply*> replies;
  for (map<pg_shard_t, vector<PushReplyOp> >::iterator i =
	 m.push_replies.begin();
       i != m.push_replies.end();
       m.push_replies.erase(i++)) {
    MOSDPGPushReply *msg = new MOSDPGPushReply();
    msg->set_priority(priority);
    msg->map_epoch = get_parent()->pgb_get_osdmap_epoch();
    msg->min_epoch = get_parent()->get_last_peering_reset_epoch();
    msg->from = get_parent()->whoami_shard();
    msg->pgid = spg_t(get_parent()->get_info().pgid.pgid, i->first.shard);
    msg->replies.swap(i->second);
    msg->compute_cost(cct);
    replies.insert(make_pair(i->first.osd, msg));
  }

  if (!replies.empty()) {
    commit_txn_send_replies(std::move(m.t), std::move(replies));
  }

  if (m.recovery_reads.empty())
    return;
  read_pipeline.start_read_op(
    priority,
    m.want_to_read,
    m.recovery_reads,
    OpRequestRef(),
    false,
    true,
    std::make_unique<RecoveryReadCompleter>(*this));
}

void ECCommon::RecoveryBackend::continue_recovery_op(
  RecoveryBackend::RecoveryOp &op,
  RecoveryMessages *m)
{
  dout(10) << __func__ << ": continuing " << op << dendl;
  using RecoveryOp = RecoveryBackend::RecoveryOp;
  while (1) {
    switch (op.state) {
    case RecoveryOp::IDLE: {
      // start read
      op.state = RecoveryOp::READING;
      ceph_assert(!op.recovery_progress.data_complete);
      set<int> want(op.missing_on_shards.begin(), op.missing_on_shards.end());
      uint64_t from = op.recovery_progress.data_recovered_to;
      uint64_t amount = get_recovery_chunk_size();

      if (op.recovery_progress.first && op.obc) {
        // need to use the `xattrs` instead of the `obc::attr_cache` as
        // the key for hinfo gets filtered out. grep for `sanitized_attrs`
        op.hinfo = unstable_hashinfo_registry.get_hash_info(
          op.hoid, false, op.xattrs, op.recovery_info.size);
	if (!op.hinfo) {
          derr << __func__ << ": " << op.hoid << " has inconsistent hinfo"
               << dendl;
          ceph_assert(recovery_ops.count(op.hoid));
          eversion_t v = recovery_ops[op.hoid].v;
          recovery_ops.erase(op.hoid);
	  // TODO: not in crimson yet
          get_parent()->on_failed_pull({get_parent()->whoami_shard()},
                                       op.hoid, v);
          return;
        }
	op.xattrs = op.obc->attr_cache;
	encode(*(op.hinfo), op.xattrs[ECUtil::get_hinfo_key()]);
      }

      map<pg_shard_t, vector<pair<int, int>>> to_read;
      int r = read_pipeline.get_min_avail_to_read_shards(
	op.hoid, want, true, false, &to_read);
      if (r != 0) {
	// we must have lost a recovery source
	ceph_assert(!op.recovery_progress.first);
	dout(10) << __func__ << ": canceling recovery op for obj " << op.hoid
		 << dendl;
	// in crimson
	get_parent()->cancel_pull(op.hoid);
	recovery_ops.erase(op.hoid);
	return;
      }
      m->recovery_read(
	op.hoid,
	op.recovery_progress.data_recovered_to,
	amount,
	std::move(want),
	to_read,
	op.recovery_progress.first && !op.obc);
      op.extent_requested = make_pair(
	from,
	amount);
      dout(10) << __func__ << ": IDLE return " << op << dendl;
      return;
    }
    case RecoveryOp::READING: {
      // read completed, start write
      ceph_assert(op.xattrs.size());
      ceph_assert(op.returned_data.size());
      op.state = RecoveryOp::WRITING;
      ObjectRecoveryProgress after_progress = op.recovery_progress;
      after_progress.data_recovered_to += op.extent_requested.second;
      after_progress.first = false;
      if (after_progress.data_recovered_to >= op.obc->obs.oi.size) {
	after_progress.data_recovered_to =
	  sinfo.logical_to_next_stripe_offset(
	    op.obc->obs.oi.size);
	after_progress.data_complete = true;
      }
      for (set<pg_shard_t>::iterator mi = op.missing_on.begin();
	   mi != op.missing_on.end();
	   ++mi) {
	ceph_assert(op.returned_data.count(mi->shard));
	m->pushes[*mi].push_back(PushOp());
	PushOp &pop = m->pushes[*mi].back();
	pop.soid = op.hoid;
	pop.version = op.v;
	pop.data = op.returned_data[mi->shard];
	dout(10) << __func__ << ": before_progress=" << op.recovery_progress
		 << ", after_progress=" << after_progress
		 << ", pop.data.length()=" << pop.data.length()
		 << ", size=" << op.obc->obs.oi.size << dendl;
	ceph_assert(
	  pop.data.length() ==
	  sinfo.aligned_logical_offset_to_chunk_offset(
	    after_progress.data_recovered_to -
	    op.recovery_progress.data_recovered_to)
	  );
	if (pop.data.length())
	  pop.data_included.insert(
	    sinfo.aligned_logical_offset_to_chunk_offset(
	      op.recovery_progress.data_recovered_to),
	    pop.data.length()
	    );
	if (op.recovery_progress.first) {
	  pop.attrset = op.xattrs;
	}
	pop.recovery_info = op.recovery_info;
	pop.before_progress = op.recovery_progress;
	pop.after_progress = after_progress;
	if (*mi != get_parent()->primary_shard())
	  // already in crimson -- junction point with PeeringState
	  get_parent()->begin_peer_recover(
	    *mi,
	    op.hoid);
      }
      op.returned_data.clear();
      op.waiting_on_pushes = op.missing_on;
      op.recovery_progress = after_progress;
      dout(10) << __func__ << ": READING return " << op << dendl;
      return;
    }
    case RecoveryOp::WRITING: {
      if (op.waiting_on_pushes.empty()) {
	if (op.recovery_progress.data_complete) {
	  op.state = RecoveryOp::COMPLETE;
	  for (set<pg_shard_t>::iterator i = op.missing_on.begin();
	       i != op.missing_on.end();
	       ++i) {
	    if (*i != get_parent()->primary_shard()) {
	      dout(10) << __func__ << ": on_peer_recover on " << *i
		       << ", obj " << op.hoid << dendl;
	      get_parent()->on_peer_recover(
		*i,
		op.hoid,
		op.recovery_info);
	    }
	  }
	  object_stat_sum_t stat;
	  stat.num_bytes_recovered = op.recovery_info.size;
	  stat.num_keys_recovered = 0; // ??? op ... omap_entries.size(); ?
	  stat.num_objects_recovered = 1;
	  // TODO: not in crimson yet
	  if (get_parent()->pg_is_repair())
	    stat.num_objects_repaired = 1;
	  // pg_recovery.cc in crimson has it
	  get_parent()->on_global_recover(op.hoid, stat, false);
	  dout(10) << __func__ << ": WRITING return " << op << dendl;
	  recovery_ops.erase(op.hoid);
	  return;
	} else {
	  op.state = RecoveryOp::IDLE;
	  dout(10) << __func__ << ": WRITING continue " << op << dendl;
	  continue;
	}
      }
      return;
    }
    // should never be called once complete
    case RecoveryOp::COMPLETE:
    default: {
      ceph_abort();
    };
    }
  }
}

ECCommon::RecoveryBackend::RecoveryOp
ECCommon::RecoveryBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc)
{
  RecoveryOp op;
  op.v = v;
  op.hoid = hoid;
  op.obc = obc;
  op.recovery_info.soid = hoid;
  op.recovery_info.version = v;
  if (obc) {
    op.recovery_info.size = obc->obs.oi.size;
    op.recovery_info.oi = obc->obs.oi;
  }
  if (hoid.is_snap()) {
    if (obc) {
      ceph_assert(obc->ssc);
      op.recovery_info.ss = obc->ssc->snapset;
    } else if (head) {
      ceph_assert(head->ssc);
      op.recovery_info.ss = head->ssc->snapset;
    } else {
      ceph_abort_msg("neither obc nor head set for a snap object");
    }
  }
  op.recovery_progress.omap_complete = true;
  for (set<pg_shard_t>::const_iterator i =
	 get_parent()->get_acting_recovery_backfill_shards().begin();
       i != get_parent()->get_acting_recovery_backfill_shards().end();
       ++i) {
    dout(10) << "checking " << *i << dendl;
    if (get_parent()->get_shard_missing(*i).is_missing(hoid)) {
      op.missing_on.insert(*i);
      op.missing_on_shards.insert(i->shard);
    }
  }
  dout(10) << __func__ << ": built op " << op << dendl;
  return op;
}
